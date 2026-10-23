"""Ошибки слоя модели.

Тот же приём, что в ``ingest.errors``: выше этого слоя никто не знает ни про
SDK провайдера, ни про коды ответов. Решение «повторять или нет» принимается
по флагу ``retryable`` на типе исключения.
"""

from __future__ import annotations

from typing import ClassVar


class LLMError(Exception):
    """Базовая ошибка модели."""

    retryable: ClassVar[bool] = False


class LLMAuthError(LLMError):
    """Ключа нет, он отозван или не даёт доступа к модели. Повтор не поможет."""


class LLMRequestError(LLMError):
    """Запрос собран неверно: нет такой модели, слишком длинный контекст и т.п.

    Виноват наш код, повтор вернёт ту же ошибку.
    """


class LLMRateLimitError(LLMError):
    """Провайдер просит сбавить темп."""

    retryable: ClassVar[bool] = True


class LLMUnavailableError(LLMError):
    """Таймаут, обрыв сети, перегрузка или ошибка на стороне провайдера."""

    retryable: ClassVar[bool] = True


class LLMResponseError(LLMError):
    """Ответ пришёл, но он не такой, как мы ждали: пустой, не по схеме и т.п."""
