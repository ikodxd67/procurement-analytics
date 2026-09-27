"""Тесты политики повторов.

Настоящих пауз не ждём: asyncio.sleep подменяется и записывает, сколько его
просили спать. Так тест проверяет расчёт пауз и при этом идёт миллисекунды.
"""

from __future__ import annotations

import random
from typing import Any

import pytest

from procurement.ingest.errors import (
    SourceAuthError,
    SourceRateLimitError,
    SourceUnavailableError,
)
from procurement.ingest.retry import RetryPolicy, compute_delay, run_with_retry


class FeedbackSpy:
    """Заглушка лимитера: запоминает, что ей сообщили."""

    def __init__(self) -> None:
        self.rate_limited = 0
        self.successes = 0

    def on_rate_limited(self) -> None:
        self.rate_limited += 1

    def on_success(self) -> None:
        self.successes += 1


@pytest.fixture
def slept(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Подменяет asyncio.sleep и копит запрошенные длительности."""
    recorded: list[float] = []

    async def fake_sleep(delay: float, *args: Any, **kwargs: Any) -> None:
        recorded.append(delay)

    monkeypatch.setattr("procurement.ingest.retry.asyncio.sleep", fake_sleep)
    return recorded


# --- расчёт паузы ------------------------------------------------------------


def test_delay_grows_exponentially_without_jitter() -> None:
    policy = RetryPolicy(base_delay_s=1.0, max_delay_s=100.0, jitter="none")

    assert compute_delay(policy, 1) == 1.0
    assert compute_delay(policy, 2) == 2.0
    assert compute_delay(policy, 3) == 4.0
    assert compute_delay(policy, 4) == 8.0


def test_delay_is_capped() -> None:
    policy = RetryPolicy(base_delay_s=1.0, max_delay_s=5.0, jitter="none")

    assert compute_delay(policy, 10) == 5.0


def test_retry_after_overrides_backoff() -> None:
    policy = RetryPolicy(base_delay_s=1.0, max_delay_s=100.0, jitter="none")

    assert compute_delay(policy, 1, retry_after_s=42.0) == 42.0


def test_retry_after_is_also_capped() -> None:
    """Сломанный или злонамеренный ответ не должен усыпить загрузчик на сутки."""
    policy = RetryPolicy(base_delay_s=1.0, max_delay_s=30.0, jitter="none")

    assert compute_delay(policy, 1, retry_after_s=86400.0) == 30.0


def test_full_jitter_stays_within_bounds() -> None:
    policy = RetryPolicy(base_delay_s=1.0, max_delay_s=100.0, jitter="full")
    rng = random.Random(0)

    values = [compute_delay(policy, 4, rng=rng) for _ in range(200)]

    assert all(0.0 <= v <= 8.0 for v in values)
    assert len(set(values)) > 1, "джиттер обязан давать разброс"


def test_equal_jitter_keeps_half_of_the_delay() -> None:
    policy = RetryPolicy(base_delay_s=1.0, max_delay_s=100.0, jitter="equal")
    rng = random.Random(0)

    values = [compute_delay(policy, 4, rng=rng) for _ in range(200)]

    assert all(4.0 <= v <= 8.0 for v in values)


# --- поведение цикла повторов -------------------------------------------------


async def test_returns_immediately_on_success(slept: list[float]) -> None:
    feedback = FeedbackSpy()

    async def op() -> str:
        return "готово"

    result = await run_with_retry(op, policy=RetryPolicy(), feedback=feedback)

    assert result == "готово"
    assert slept == []
    assert feedback.successes == 1
    assert feedback.rate_limited == 0


async def test_retries_until_success(slept: list[float]) -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise SourceUnavailableError("502")
        return "готово"

    result = await run_with_retry(op, policy=RetryPolicy(max_attempts=5))

    assert result == "готово"
    assert calls == 3
    assert len(slept) == 2, "две неудачи — две паузы"


async def test_gives_up_after_max_attempts(slept: list[float]) -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise SourceUnavailableError("503")

    with pytest.raises(SourceUnavailableError):
        await run_with_retry(op, policy=RetryPolicy(max_attempts=4))

    assert calls == 4
    assert len(slept) == 3, "после последней попытки спать уже незачем"


async def test_does_not_retry_auth_error(slept: list[float]) -> None:
    """403 от goszakup означает негодный токен. Повтор не поможет никогда."""
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise SourceAuthError("403 Access denied")

    with pytest.raises(SourceAuthError):
        await run_with_retry(op, policy=RetryPolicy(max_attempts=5))

    assert calls == 1
    assert slept == []


async def test_tells_limiter_about_rate_limit(slept: list[float]) -> None:
    feedback = FeedbackSpy()
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise SourceRateLimitError("429")
        return "готово"

    await run_with_retry(op, policy=RetryPolicy(), feedback=feedback)

    assert feedback.rate_limited == 1
    assert feedback.successes == 1


async def test_server_side_unavailable_does_not_shrink_window(slept: list[float]) -> None:
    """5xx — это не просьба сбавить темп, а поломка на той стороне.

    Сжимать окно на каждую пятисотку значит наказывать себя за чужую аварию.
    """
    feedback = FeedbackSpy()
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise SourceUnavailableError("500")
        return "готово"

    await run_with_retry(op, policy=RetryPolicy(), feedback=feedback)

    assert feedback.rate_limited == 0


async def test_uses_retry_after_from_error(slept: list[float]) -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise SourceRateLimitError("429", retry_after_s=7.0)
        return "готово"

    await run_with_retry(op, policy=RetryPolicy(max_delay_s=60.0))

    assert slept == [7.0]
