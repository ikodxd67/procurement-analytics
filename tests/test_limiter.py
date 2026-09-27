"""Тесты адаптивного лимитера.

Проверяем не «код выполнился», а поведение: сжимается ли окно на 429, не
проваливается ли ниже минимума, восстанавливается ли в тишине и держит ли
слот заявленный предел одновременности.
"""

from __future__ import annotations

import asyncio

import pytest

from procurement.ingest.limiter import AdaptiveLimiter


def test_rejects_inconsistent_bounds() -> None:
    with pytest.raises(ValueError, match="minimum"):
        AdaptiveLimiter(start=1, minimum=4, maximum=8)


def test_rejects_meaningless_factor() -> None:
    with pytest.raises(ValueError, match="decrease_factor"):
        AdaptiveLimiter(start=4, minimum=1, maximum=8, decrease_factor=1.5)


def test_rate_limit_shrinks_target_by_factor() -> None:
    limiter = AdaptiveLimiter(start=8, minimum=1, maximum=8, decrease_factor=0.5)

    limiter.on_rate_limited()

    assert limiter.target == 4


def test_target_never_drops_below_minimum() -> None:
    limiter = AdaptiveLimiter(
        start=4, minimum=3, maximum=8, decrease_factor=0.5, decrease_cooldown_s=0.0
    )

    for _ in range(10):
        limiter.on_rate_limited()

    assert limiter.target == 3


def test_burst_of_429_shrinks_only_once() -> None:
    """Пять параллельных запросов получили 429 одновременно.

    Без защиты цель упала бы 16 -> 8 -> 4 -> 2 -> 1 за одно мгновение, хотя
    перегруз был один. Ожидаем ровно один шаг вниз.
    """
    limiter = AdaptiveLimiter(
        start=16, minimum=1, maximum=16, decrease_factor=0.5, decrease_cooldown_s=60.0
    )

    for _ in range(5):
        limiter.on_rate_limited()

    assert limiter.target == 8


async def test_recovers_after_quiet_period() -> None:
    limiter = AdaptiveLimiter(
        start=8,
        minimum=1,
        maximum=8,
        decrease_factor=0.5,
        quiet_period_s=0.05,
        recover_step=1,
        recover_after_successes=1,
    )

    async with limiter:
        limiter.on_rate_limited()
        assert limiter.target == 4

        for _ in range(5):
            limiter.on_success()

        await asyncio.sleep(0.3)

        assert limiter.target > 4, "после тишины и успехов окно должно расширяться"


async def test_does_not_recover_while_429_keep_coming() -> None:
    limiter = AdaptiveLimiter(
        start=8,
        minimum=2,
        maximum=8,
        decrease_factor=0.5,
        quiet_period_s=0.05,
        recover_after_successes=1,
        decrease_cooldown_s=60.0,
    )

    async with limiter:
        limiter.on_rate_limited()
        assert limiter.target == 4

        for _ in range(6):
            limiter.on_success()
            limiter.on_rate_limited()  # тишины нет
            await asyncio.sleep(0.03)

        assert limiter.target == 4, "пока приходят 429, расти нельзя"


async def test_slot_enforces_current_target() -> None:
    limiter = AdaptiveLimiter(start=3, minimum=1, maximum=10, quiet_period_s=60.0)

    peak = 0
    current = 0

    async def worker() -> None:
        nonlocal peak, current
        async with limiter.slot():
            current += 1
            peak = max(peak, current)
            await asyncio.sleep(0.01)
            current -= 1

    async with limiter:
        await asyncio.gather(*(worker() for _ in range(20)))

    assert peak == 3, f"одновременно работало {peak}, а разрешено 3"


async def test_slot_released_on_cancellation() -> None:
    """Отменённая задача обязана вернуть слот, иначе загрузчик встанет.

    Забираем единственный слот, отменяем держателя и проверяем, что слот
    снова доступен.
    """
    limiter = AdaptiveLimiter(start=1, minimum=1, maximum=1, quiet_period_s=60.0)
    holding = asyncio.Event()

    async def holder() -> None:
        async with limiter.slot():
            holding.set()
            await asyncio.sleep(3600)

    async with limiter:
        task = asyncio.create_task(holder())
        await holding.wait()
        assert limiter.in_flight == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert limiter.in_flight == 0
        # Слот должен браться сразу, без ожидания.
        async with asyncio.timeout(1), limiter.slot():
            pass
