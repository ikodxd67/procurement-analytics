"""Повторы с экспоненциальной паузой и джиттером.

Граница ответственности с limiter.py: здесь решается судьба ОДНОГО запроса —
повторить или сдаться, и сколько ждать перед повтором. Сколько запросов идёт
одновременно, решает лимитер. На событие 429 срабатывают оба, и это не
дублирование: один повторяет упавший запрос, другой сжимает окно для всех
следующих.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from procurement.ingest.errors import SourceError, SourceRateLimitError
from procurement.ingest.limiter import RateFeedback
from procurement.logging import get_logger

log = get_logger(__name__)

JitterKind = Literal["full", "equal", "none"]

# Свой генератор, а не функции модуля random: у модуля они висят на общем
# скрытом состоянии, а типизированный объект Random позволяет подменить его в
# тесте и получить воспроизводимые паузы.
_default_rng = random.Random()  # noqa: S311  # паузы, а не криптография


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay_s: float = 0.5
    max_delay_s: float = 60.0
    jitter: JitterKind = "full"


def compute_delay(
    policy: RetryPolicy,
    attempt: int,
    *,
    retry_after_s: float | None = None,
    rng: random.Random | None = None,
) -> float:
    """Сколько ждать перед попыткой номер attempt + 1.

    Retry-After от источника главнее нашего расчёта: сервер лучше знает, когда
    ему станет легче. Но ограничиваем сверху, иначе злонамеренный или просто
    сломанный ответ усыпит загрузчик на сутки.

    Зачем джиттер. Без него все корутины, получившие 429 в один момент,
    проснутся тоже в один момент и ударят по источнику синхронной пачкой —
    он снова ответит 429, и так по кругу. Джиттер разносит пробуждения во
    времени. Вариант full (пауза равномерно из отрезка от нуля до расчётной)
    разносит сильнее всего; equal оставляет гарантированную половину паузы.
    """
    if retry_after_s is not None:
        return min(retry_after_s, policy.max_delay_s)

    # Основание вещественное намеренно: 2 ** n у целых даёт тип Any, потому что
    # при отрицательном показателе результат дробный, и вывод типа ломается.
    exponential = policy.base_delay_s * (2.0 ** (attempt - 1))
    capped = min(exponential, policy.max_delay_s)

    if policy.jitter == "none":
        return capped

    generator = rng if rng is not None else _default_rng
    if policy.jitter == "equal":
        half = capped / 2
        return half + generator.uniform(0, half)
    return generator.uniform(0, capped)


async def run_with_retry[T](
    operation: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy,
    feedback: RateFeedback | None = None,
    what: str = "request",
) -> T:
    """Выполнить операцию, повторяя её при временных ошибках.

    Что повторяем и что нет, определяет флаг retryable на самом исключении, а
    не список кодов здесь. Так знание про коды остаётся в одном месте — в
    реализации источника.
    """
    for attempt in range(1, policy.max_attempts + 1):
        try:
            result = await operation()
        except SourceError as err:
            if not err.retryable:
                # 401, 403, 400, 404, битый ответ. Повтор ничего не изменит.
                raise

            if isinstance(err, SourceRateLimitError) and feedback is not None:
                # Сообщаем лимитеру до сна, чтобы окно сжалось немедленно,
                # а не через паузу.
                feedback.on_rate_limited()

            if attempt == policy.max_attempts:
                log.error("retry.exhausted", what=what, attempts=attempt, error=str(err))
                raise

            retry_after = err.retry_after_s if isinstance(err, SourceRateLimitError) else None
            delay = compute_delay(policy, attempt, retry_after_s=retry_after)
            log.warning(
                "retry.sleeping",
                what=what,
                attempt=attempt,
                delay_s=round(delay, 3),
                error=type(err).__name__,
            )
            await asyncio.sleep(delay)
        else:
            if feedback is not None:
                feedback.on_success()
            return result

    msg = "недостижимо: цикл повторов всегда завершается через return или raise"
    raise AssertionError(msg)
