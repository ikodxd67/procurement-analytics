"""Агент как сервис: кэш одинаковых вопросов и разбор ответа для клиента.

Кэш нужен не ради скорости, а ради денег: каждый проход агента — несколько
платных вызовов модели. Повтор того же вопроса за час получает тот же ответ
бесплатно.

Два одинаковых вопроса, пришедших одновременно, кэш сам по себе не спасает:
оба не найдут ответа и оба запустят агента. Поэтому второй присоединяется к
уже идущему проходу (single-flight, как в ``singleflight`` из Go) — платим один раз.

Кэш в памяти процесса: при нескольких процессах API у каждого свой. Для
одного экземпляра этого хватает; общий кэш (Redis) — когда экземпляров станет
больше одного.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from procurement.assistant.agent import Agent, AgentAnswer, AgentStop

# Кэшируем только законченные ответы. Таймаут или предел шагов — сбой этого
# прохода, а не ответ на вопрос: следующая попытка может пройти.
CACHEABLE = frozenset({AgentStop.ANSWERED, AgentStop.OFF_TOPIC})

_SPACES = re.compile(r"\s+")


def normalize(question: str) -> str:
    """Ключ кэша: «Сколько договоров?» и «  сколько   договоров» — один вопрос."""
    return _SPACES.sub(" ", question).strip().rstrip("?!. ").casefold()


class AnswerCache:
    """LRU со сроком жизни записи. Часы подменяются в тестах."""

    def __init__(
        self,
        *,
        ttl_s: float,
        max_entries: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl_s = ttl_s
        self._max_entries = max_entries
        self._clock = clock
        self._items: OrderedDict[str, tuple[float, AgentAnswer]] = OrderedDict()

    def __len__(self) -> int:
        return len(self._items)

    def get(self, key: str) -> AgentAnswer | None:
        item = self._items.get(key)
        if item is None:
            return None
        expires, answer = item
        if self._clock() >= expires:
            del self._items[key]
            return None
        self._items.move_to_end(key)
        return answer

    def put(self, key: str, answer: AgentAnswer) -> None:
        self._items[key] = (self._clock() + self._ttl_s, answer)
        self._items.move_to_end(key)
        while len(self._items) > self._max_entries:
            self._items.popitem(last=False)


@dataclass(frozen=True, slots=True)
class Asked:
    answer: AgentAnswer
    cached: bool
    """Ответ не стоил нового прохода агента: из кэша или от одновременного соседа."""


class AskService:
    def __init__(self, agent: Agent, cache: AnswerCache) -> None:
        self._agent = agent
        self._cache = cache
        self._in_flight: dict[str, asyncio.Task[AgentAnswer]] = {}

    async def ask(self, question: str) -> Asked:
        key = normalize(question)
        answer = self._cache.get(key)
        if answer is not None:
            return Asked(answer, cached=True)

        task = self._in_flight.get(key)
        joined = task is not None
        if task is None:
            task = asyncio.create_task(self._answer(key, question))
            self._in_flight[key] = task
            task.add_done_callback(lambda _: self._in_flight.pop(key, None))
        # shield: если клиент первого запроса отвалится, проход не отменится
        # у всех, кто к нему присоединился, — и уже оплаченный ответ попадёт в кэш.
        return Asked(await asyncio.shield(task), cached=joined)

    async def _answer(self, key: str, question: str) -> AgentAnswer:
        answer = await self._agent.ask(question)
        if answer.stop in CACHEABLE:
            self._cache.put(key, answer)
        return answer


# --- что показать клиенту из журнала агента ----------------------------------


@dataclass(frozen=True, slots=True)
class Source:
    title: str
    url: str
    cited: bool
    """Ссылка есть в тексте ответа. Остальные модель видела, но не назвала."""


@dataclass(frozen=True, slots=True)
class SqlRun:
    query: str
    ok: bool


def sources(answer: AgentAnswer) -> list[Source]:
    """Куски документов, найденные search_docs, без повторов, в порядке находок."""
    seen: dict[str, str] = {}
    for call in answer.tool_calls:
        if call.name != "search_docs" or call.is_error:
            continue
        hits: list[dict[str, Any]] = call.output or []
        for hit in hits:
            seen.setdefault(hit["url"], hit["title"])
    return [Source(title, url, cited=url in answer.text) for url, title in seen.items()]


def sql_runs(answer: AgentAnswer) -> list[SqlRun]:
    """SQL, который написала модель, — и прошёл ли он проверку и базу."""
    return [
        SqlRun(query=str(call.input.get("query", "")), ok=not call.is_error)
        for call in answer.tool_calls
        if call.name == "run_sql"
    ]
