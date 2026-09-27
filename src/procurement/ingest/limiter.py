"""Адаптивное ограничение конкурентности.

Задача: держать столько одновременных запросов, сколько источник готов
терпеть, и подстраивать это число на ходу. Явных лимитов goszakup не
публикует (проверено 2026-09-27: заголовков X-RateLimit-* и Retry-After в
ответе нет), поэтому порог нащупывается по факту получения 429.

Почему нельзя обойтись обычным asyncio.Semaphore: число разрешений задаётся
при создании и изменению не подлежит. Обходим так — создаём семафор сразу на
максимум, а лишние разрешения паркуем, то есть забираем себе и не отдаём.
Уменьшить конкурентность значит запарковать ещё несколько разрешений,
увеличить — вернуть запаркованные обратно.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from types import TracebackType
from typing import Protocol, Self

from procurement.logging import get_logger

log = get_logger(__name__)


class RateFeedback(Protocol):
    """То, что слой повторов сообщает лимитеру.

    Протокол, а не конкретный класс, чтобы retry не зависел от limiter:
    в тестах на его место подставляется заглушка.
    """

    def on_rate_limited(self) -> None: ...

    def on_success(self) -> None: ...


class AdaptiveLimiter:
    """Семафор с изменяемым на ходу числом слотов.

    Разделение ответственности: этот класс решает, СКОЛЬКО запросов идёт
    одновременно. Он не решает, повторять ли конкретный запрос и сколько
    ждать, — это работа retry.py. На одно событие 429 оба реагируют, но
    по-разному: retry повторяет этот запрос, лимитер притормаживает все
    последующие.
    """

    def __init__(
        self,
        *,
        start: int,
        minimum: int,
        maximum: int,
        decrease_factor: float = 0.5,
        quiet_period_s: float = 10.0,
        recover_step: int = 1,
        recover_after_successes: int = 10,
        decrease_cooldown_s: float = 1.0,
    ) -> None:
        if not 1 <= minimum <= start <= maximum:
            msg = f"нужно 1 <= minimum <= start <= maximum, дано {minimum}, {start}, {maximum}"
            raise ValueError(msg)
        if not 0.0 < decrease_factor < 1.0:
            msg = f"decrease_factor должен быть в интервале (0, 1), дано {decrease_factor}"
            raise ValueError(msg)

        self._minimum = minimum
        self._maximum = maximum
        self._target = start
        self._decrease_factor = decrease_factor
        self._quiet_period_s = quiet_period_s
        self._recover_step = recover_step
        self._recover_after_successes = recover_after_successes
        self._decrease_cooldown_s = decrease_cooldown_s

        # Семафор всегда на максимум; реальный предел задаётся парковкой.
        self._sem = asyncio.Semaphore(maximum)
        self._parked = 0
        self._in_flight = 0

        # time.monotonic, а не time.time: монотонные часы не прыгают назад при
        # переводе системного времени, а нас интересуют именно интервалы.
        self._last_rate_limit = float("-inf")
        self._last_decrease = float("-inf")
        self._successes_since_decrease = 0

        self._wake = asyncio.Event()
        self._regulator: asyncio.Task[None] | None = None

    # --- наблюдение за состоянием ---------------------------------------------

    @property
    def target(self) -> int:
        """Сколько запросов разрешено одновременно прямо сейчас."""
        return self._target

    @property
    def in_flight(self) -> int:
        return self._in_flight

    # --- обратная связь от слоя повторов --------------------------------------

    def on_rate_limited(self) -> None:
        """Источник ответил 429. Сжимаем окно.

        Уменьшаем множителем, а не на единицу: при перегрузе надо быстро уйти
        вниз, шаг в единицу для этого слишком медленный.

        Защита от лавины: если несколько параллельных запросов получили 429
        почти одновременно, наивная реализация уменьшила бы цель столько раз,
        сколько пришло ответов, и мгновенно свалилась бы в минимум. Поэтому
        уменьшение не чаще одного раза в decrease_cooldown_s.
        """
        now = time.monotonic()
        self._last_rate_limit = now

        if now - self._last_decrease < self._decrease_cooldown_s:
            return

        new_target = max(self._minimum, int(self._target * self._decrease_factor))
        if new_target < self._target:
            log.info("limiter.decrease", old=self._target, new=new_target)
            self._target = new_target
            self._last_decrease = now
            self._successes_since_decrease = 0
            self._wake.set()

    def on_success(self) -> None:
        self._successes_since_decrease += 1

    # --- внутренняя механика ---------------------------------------------------

    async def _reconcile(self) -> None:
        """Привести число запаркованных разрешений в соответствие с целью.

        Вызывается только из регулятора и из __aenter__, поэтому гонок нет.
        """
        want_parked = self._maximum - self._target

        while self._parked < want_parked:
            # Может подождать: если все слоты заняты, парковка случится, как
            # только кто-нибудь освободит свой. Это правильно — отбирать слот
            # у уже выполняющегося запроса нельзя.
            await self._sem.acquire()
            self._parked += 1

        while self._parked > want_parked:
            self._sem.release()
            self._parked -= 1

    def _maybe_recover(self) -> None:
        """Плавный рост обратно, если давно не было 429."""
        if self._target >= self._maximum:
            return
        now = time.monotonic()
        if now - self._last_rate_limit < self._quiet_period_s:
            return
        if self._successes_since_decrease < self._recover_after_successes:
            return

        new_target = min(self._maximum, self._target + self._recover_step)
        log.info("limiter.recover", old=self._target, new=new_target)
        self._target = new_target
        self._successes_since_decrease = 0

    async def _regulate(self) -> None:
        """Фоновая задача: просыпается по событию или по таймеру."""
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._quiet_period_s)
            except TimeoutError:
                # Тишина — повод попробовать подрасти.
                self._maybe_recover()
            else:
                self._wake.clear()
            await self._reconcile()

    # --- жизненный цикл ---------------------------------------------------------

    async def __aenter__(self) -> Self:
        # Стартовую парковку делаем до запуска регулятора, чтобы между входом в
        # контекст и первым тиком не было окна с конкурентностью = maximum.
        await self._reconcile()
        self._regulator = asyncio.create_task(self._regulate(), name="limiter-regulator")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._regulator is not None:
            self._regulator.cancel()
            # Ждём фактического завершения: отменённая задача исчезает не
            # мгновенно, а без ожидания получим предупреждение о брошенной задаче.
            with suppress(asyncio.CancelledError):
                await self._regulator
            self._regulator = None

    # --- выдача слота -------------------------------------------------------------

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """Занять один слот на время запроса."""
        await self._sem.acquire()
        self._in_flight += 1
        try:
            yield
        finally:
            # release синхронный, без await. Это важно: блок finally может
            # выполняться во время отмены, и любой await внутри него рискует
            # быть отменён повторно, оставив слот занятым навсегда.
            self._in_flight -= 1
            self._sem.release()
