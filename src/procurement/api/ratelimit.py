"""Лимит запросов: не больше N за последние W секунд на ключ.

Скользящее окно (moving window) по журналу отметок времени, а не счётчик на
календарную минуту. У счётчика есть известная дыра: N запросов в последнюю
секунду минуты и ещё N в первую секунду следующей — вдвое больше лимита за
две секунды. Журнал хранит не больше N отметок на ключ, так что памяти это
почти не стоит.

Отказ — 429 с заголовком Retry-After (RFC 9110): клиент знает, сколько ждать,
а не долбит наугад.

В памяти процесса, как и кэш ответов: для одного экземпляра API достаточно,
для нескольких лимит придётся вынести в общее хранилище (Redis).
"""

from __future__ import annotations

import math
import time
from collections import deque
from collections.abc import Callable


class SlidingWindowLimiter:
    def __init__(
        self,
        limit: int,
        window_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._limit = limit
        self._window_s = window_s
        self._clock = clock
        self._hits: dict[str, deque[float]] = {}

    def __len__(self) -> int:
        """Сколько ключей сейчас в журнале — для теста на утечку памяти."""
        return len(self._hits)

    def retry_after(self, key: str) -> int | None:
        """Свободно — записать запрос и вернуть None; занято — через сколько секунд."""
        now = self._clock()
        self._sweep_one(now)
        hits = self._trim(self._hits.setdefault(key, deque()), now)
        if len(hits) >= self._limit:
            return max(1, math.ceil(hits[0] + self._window_s - now))
        hits.append(now)
        return None

    def _trim(self, hits: deque[float], now: float) -> deque[float]:
        while hits and hits[0] <= now - self._window_s:
            hits.popleft()
        return hits

    def _sweep_one(self, now: float) -> None:
        # Ключи по адресам клиентов копились бы вечно. На каждый запрос
        # проверяем один самый старый ключ: пустой — удаляем, живой — в конец
        # очереди. Так за C запросов обходятся все C ключей, а каждый запрос
        # стоит O(1), без отдельной фоновой задачи.
        if not self._hits:
            return
        oldest = next(iter(self._hits))
        hits = self._trim(self._hits.pop(oldest), now)
        if hits:
            self._hits[oldest] = hits
