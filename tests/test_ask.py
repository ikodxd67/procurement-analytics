"""Ручка POST /v1/ask: кэш, одновременные вопросы, лимиты, формат ответа.

Без баз и без ключа: агент собран на поддельной модели и поддельных
инструментах, приложение — без lifespan, ручке подставлен готовый AskEndpoint.
Лишний вызов модели FakeLLM превращает в FakeScriptError — так проверяется,
что кэш и single-flight действительно экономят платные проходы.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict

from procurement.api.app import build_app
from procurement.api.ratelimit import SlidingWindowLimiter
from procurement.api.routers.assistant import WINDOW_S, AskEndpoint
from procurement.assistant.agent import Agent, AgentStop
from procurement.assistant.errors import LLMAuthError, LLMUnavailableError
from procurement.assistant.fake import FakeLLM
from procurement.assistant.models import LLMResponse, StopReason, TextBlock, ToolUse, Usage
from procurement.assistant.service import (
    AnswerCache,
    AskService,
    normalize,
    sources,
    sql_runs,
)
from procurement.assistant.tools import Tool, ToolBox, ToolError
from procurement.config import Settings

# Настоящие куски из снимка правил (evals/data): п. 598 и п. 599 главы 19.
RULE_URL = "https://adilet.zan.kz/rus/docs/V2400035238#z1277"
OTHER_URL = "https://adilet.zan.kz/rus/docs/V2400035238#z1280"
RULE_TITLE = "Глава 19. Порядок обжалования … › п. 598"
OTHER_TITLE = "Глава 19. Порядок обжалования … › п. 599"
QUESTION = "В каких случаях жалобу возвращают без рассмотрения?"


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def route(kind: str = "docs") -> LLMResponse:
    data = {"kind": kind, "year_from": None, "year_to": None, "reason": "тест"}
    return LLMResponse(
        content=[TextBlock(text=json.dumps(data))],
        stop_reason=StopReason.END_TURN,
        usage=Usage(input_tokens=10, output_tokens=5),
    )


def calls(*items: tuple[str, str, dict[str, Any]]) -> LLMResponse:
    return LLMResponse(
        content=[ToolUse(id=cid, name=name, input=args) for cid, name, args in items],
        stop_reason=StopReason.TOOL_USE,
        usage=Usage(input_tokens=100, output_tokens=20),
    )


def final(text: str) -> LLMResponse:
    return LLMResponse(
        content=[TextBlock(text=text)],
        stop_reason=StopReason.END_TURN,
        usage=Usage(input_tokens=200, output_tokens=40),
    )


class QueryArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str


def toolbox() -> ToolBox:
    async def search_docs(a: QueryArgs) -> list[dict[str, Any]]:
        return [
            {"title": RULE_TITLE, "url": RULE_URL, "text": "…", "score": 0.03},
            {"title": OTHER_TITLE, "url": OTHER_URL, "text": "…", "score": 0.02},
        ]

    async def run_sql(a: QueryArgs) -> dict[str, Any]:
        if "totl" in a.query:
            raise ToolError("нет колонки totl")
        return {"columns": ["n"], "rows": [[42]]}

    return ToolBox(
        [
            Tool("search_docs", "Поиск по правилам", QueryArgs, search_docs),
            Tool("run_sql", "SELECT", QueryArgs, run_sql),
        ]
    )


def docs_pass() -> list[LLMResponse]:
    """Один проход агента: маршрутизатор → поиск дважды → ответ со ссылкой."""
    return [
        route(),
        calls(("c1", "search_docs", {"query": "возврат жалобы без рассмотрения"})),
        calls(("c2", "search_docs", {"query": "жалоба не рассматривается"})),
        # Текст ответа сверен с п. 598 в снимке правил.
        final(
            "Жалобу возвращают без рассмотрения в течение двух рабочих дней, если она "
            "не соответствует требованиям п. 596 или не подписана уполномоченным лицом "
            f"(п. 598, {RULE_URL})."
        ),
    ]


def service(llm: FakeLLM, clock: Clock | None = None, *, max_steps: int = 8) -> AskService:
    cache = AnswerCache(ttl_s=3600, max_entries=8, clock=clock or Clock())
    return AskService(Agent(llm, toolbox(), max_steps=max_steps), cache)


# --- нормализация и кэш --------------------------------------------------------


def test_normalize_ignores_case_spaces_and_trailing_punctuation() -> None:
    assert normalize("  Сколько   договоров\n в 2025?") == normalize("сколько договоров в 2025")
    assert normalize("Срок жалобы") != normalize("Срок обеспечения")


def test_cache_entry_expires_after_ttl() -> None:
    clock = Clock()
    cache = AnswerCache(ttl_s=10, max_entries=4, clock=clock)
    answer = object()
    cache.put("q", answer)  # type: ignore[arg-type]
    clock.now = 9.9
    assert cache.get("q") is answer
    clock.now = 10.0
    assert cache.get("q") is None
    assert len(cache) == 0


def test_cache_evicts_least_recently_used() -> None:
    cache = AnswerCache(ttl_s=10, max_entries=2, clock=Clock())
    a, b, c = object(), object(), object()
    cache.put("a", a)  # type: ignore[arg-type]
    cache.put("b", b)  # type: ignore[arg-type]
    cache.get("a")  # a свежее b
    cache.put("c", c)  # type: ignore[arg-type]
    assert cache.get("b") is None
    assert cache.get("a") is a
    assert cache.get("c") is c


# --- сервис --------------------------------------------------------------------


async def test_repeated_question_is_served_from_cache() -> None:
    llm = FakeLLM(docs_pass())
    svc = service(llm)

    first = await svc.ask("Какой срок подачи жалобы?")
    second = await svc.ask("какой срок подачи жалобы")

    assert not first.cached
    assert second.cached
    assert second.answer is first.answer
    assert llm.remaining == 0  # второй вопрос модель не звал: иначе FakeScriptError


async def test_concurrent_identical_questions_share_one_pass() -> None:
    llm = FakeLLM(docs_pass())
    svc = service(llm)

    a, b = await asyncio.gather(svc.ask("Срок жалобы?"), svc.ask("срок жалобы"))

    assert a.answer is b.answer
    assert [a.cached, b.cached] == [False, True]
    assert len(llm.calls) == 4  # один проход: маршрутизатор + три шага


async def test_failed_pass_is_not_cached() -> None:
    # Предел шагов — сбой прохода, а не ответ. Следующая попытка идёт заново.
    llm = FakeLLM(
        [
            route(),
            calls(("c1", "search_docs", {"query": "x"})),
            *[route(), final("Ответ со второй попытки.")],
        ]
    )
    svc = service(llm, max_steps=1)

    first = await svc.ask("Вопрос")
    second = await svc.ask("Вопрос")

    assert first.answer.stop is AgentStop.STEP_LIMIT
    assert second.answer.stop is AgentStop.ANSWERED
    assert not second.cached


async def test_model_error_reaches_every_waiter_and_clears_in_flight() -> None:
    llm = FakeLLM([LLMUnavailableError("529"), *docs_pass()])
    svc = service(llm)

    results = await asyncio.gather(svc.ask("Срок?"), svc.ask("срок"), return_exceptions=True)
    assert all(isinstance(r, LLMUnavailableError) for r in results)

    # Упавший проход не залип в «идущих»: новый вопрос запускает новый проход.
    retry = await svc.ask("Срок?")
    assert retry.answer.stop is AgentStop.ANSWERED


async def test_sources_deduplicated_and_marked_cited() -> None:
    svc = service(FakeLLM(docs_pass()))
    answer = (await svc.ask("Срок жалобы")).answer

    found = sources(answer)

    # Два поиска вернули одни и те же два пункта — в ответе по одному разу.
    assert [(s.url, s.cited) for s in found] == [(RULE_URL, True), (OTHER_URL, False)]


async def test_sql_runs_keep_query_and_outcome() -> None:
    llm = FakeLLM(
        [
            route("data"),
            calls(("c1", "run_sql", {"query": "SELECT totl FROM contracts"})),
            calls(("c2", "run_sql", {"query": "SELECT count() FROM contracts"})),
            final("42 договора (данные синтетические)."),
        ]
    )
    answer = (await service(llm).ask("Сколько договоров?")).answer

    assert [(r.query, r.ok) for r in sql_runs(answer)] == [
        ("SELECT totl FROM contracts", False),
        ("SELECT count() FROM contracts", True),
    ]
    assert sources(answer) == []


# --- лимит ---------------------------------------------------------------------


def test_limiter_moving_window() -> None:
    clock = Clock()
    limiter = SlidingWindowLimiter(2, 60, clock=clock)

    assert limiter.retry_after("ip") is None
    clock.now = 30
    assert limiter.retry_after("ip") is None
    clock.now = 59.5
    # Окно скользящее: первый запрос (t=0) освободит место на t=60.
    assert limiter.retry_after("ip") == 1
    assert limiter.retry_after("other") is None  # у другого ключа свой счёт
    clock.now = 60
    assert limiter.retry_after("ip") is None
    clock.now = 61
    assert limiter.retry_after("ip") == 29  # теперь ждём запрос с t=30


def test_limiter_drops_idle_keys() -> None:
    clock = Clock()
    limiter = SlidingWindowLimiter(5, 60, clock=clock)
    for i in range(100):
        limiter.retry_after(f"10.0.0.{i}")
    clock.now = 1000
    for _ in range(100):
        limiter.retry_after("10.0.0.200")
    # Остался только живой ключ: журнал не растёт по числу когда-либо пришедших адресов.
    assert len(limiter) == 1


# --- ручка ---------------------------------------------------------------------


def endpoint(llm: FakeLLM, *, per_client: int = 5, total: int = 30) -> AskEndpoint:
    return AskEndpoint(
        service=service(llm),
        model="claude-haiku-5-5",
        per_client=SlidingWindowLimiter(per_client, WINDOW_S),
        total=SlidingWindowLimiter(total, WINDOW_S),
    )


@pytest.fixture
def app() -> FastAPI:
    # Без lifespan: базы не нужны, ручке подставляется готовый AskEndpoint.
    return build_app(Settings())


@pytest.fixture
async def http(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def test_ask_returns_answer_sources_and_usage(app: FastAPI, http: httpx.AsyncClient) -> None:
    app.state.ask = endpoint(FakeLLM(docs_pass()))

    response = await http.post("/v1/ask", json={"question": QUESTION})

    assert response.status_code == 200
    body = response.json()
    assert body["stop"] == "answered"
    assert body["kind"] == "docs"
    assert "п. 598" in body["answer"]
    assert body["sources"] == [
        {"title": RULE_TITLE, "url": RULE_URL, "cited": True},
        {"title": OTHER_TITLE, "url": OTHER_URL, "cited": False},
    ]
    assert body["sql"] == []
    assert [c["name"] for c in body["tool_calls"]] == ["search_docs", "search_docs"]
    assert body["steps"] == 3
    # Маршрутизатор 10/5 + шаги 100/20, 100/20, 200/40.
    assert body["usage"]["input_tokens"] == 410
    assert body["usage"]["output_tokens"] == 85
    assert body["usage"]["cost_usd"] == pytest.approx((410 * 0.10 + 85 * 0.50) / 1e6)
    assert body["cached"] is False

    again = await http.post("/v1/ask", json={"question": QUESTION.lower().rstrip("?")})
    assert again.json()["cached"] is True
    assert again.json()["answer"] == body["answer"]


async def test_ask_disabled_by_default(http: httpx.AsyncClient) -> None:
    response = await http.post("/v1/ask", json={"question": "Срок жалобы?"})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "assistant_disabled"


@pytest.mark.parametrize("question", ["", "  ", "аб", "я" * 1001])
async def test_ask_rejects_bad_question(
    app: FastAPI, http: httpx.AsyncClient, question: str
) -> None:
    app.state.ask = endpoint(FakeLLM([]))  # модель не позовётся: проверка до агента
    response = await http.post("/v1/ask", json={"question": question})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_failed"


async def test_ask_per_client_limit_returns_429_with_retry_after(
    app: FastAPI, http: httpx.AsyncClient
) -> None:
    app.state.ask = endpoint(FakeLLM(docs_pass()), per_client=1)

    assert (await http.post("/v1/ask", json={"question": "Срок жалобы"})).status_code == 200
    # Даже вопрос из кэша считается: лимит защищает и сам сервис, а не только бюджет.
    response = await http.post("/v1/ask", json={"question": "Срок жалобы"})

    assert response.status_code == 429
    assert response.json()["error"]["code"] == "rate_limited"
    assert 1 <= int(response.headers["Retry-After"]) <= 60


async def test_ask_total_limit(app: FastAPI, http: httpx.AsyncClient) -> None:
    app.state.ask = endpoint(FakeLLM(docs_pass()), per_client=5, total=1)
    await http.post("/v1/ask", json={"question": "Срок жалобы"})
    response = await http.post("/v1/ask", json={"question": "Срок жалобы"})
    assert response.status_code == 429
    assert "загружен" in response.json()["error"]["message"]


@pytest.mark.parametrize(
    ("error", "status", "code"),
    [
        (LLMUnavailableError("529 overloaded"), 503, "model_unavailable"),
        (LLMAuthError("401 invalid x-api-key"), 502, "model_failed"),
    ],
)
async def test_ask_model_errors(
    app: FastAPI, http: httpx.AsyncClient, error: Exception, status: int, code: str
) -> None:
    app.state.ask = endpoint(FakeLLM([error]))
    response = await http.post("/v1/ask", json={"question": "Срок жалобы"})
    assert response.status_code == status
    body = response.json()["error"]
    assert body["code"] == code
    assert "x-api-key" not in body["message"]  # текст ошибки провайдера наружу не идёт
