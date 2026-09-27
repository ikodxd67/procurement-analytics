"""Ошибки слоя источника данных.

Смысл этого модуля — не дать HTTP-специфике протечь дальше. Слои повторов и
конвейера не должны знать ни про httpx, ни про коды ответов: они принимают
решение по типу исключения. Иначе добавление второго транспорта потребует
переписывать всё, что лежит выше.
"""

from __future__ import annotations

from typing import ClassVar


class SourceError(Exception):
    """Базовая ошибка источника."""

    # Повторять ли запрос. Значение читает retry.run_with_retry.
    retryable: ClassVar[bool] = False


class SourceAuthError(SourceError):
    """401 или 403. Токена нет, он протух или не даёт доступа.

    Повторять бессмысленно: через секунду токен валидным не станет.
    Проверено 2026-09-27: без токена goszakup отдаёт 401, с неверным — 403.
    """


class SourceRequestError(SourceError):
    """400 или 404. Запрос собран неверно — виноват наш код, а не сеть."""


class SourceResponseError(SourceError):
    """Ответ пришёл, но разобрать не удалось: не JSON, нет ключа items и т.п."""


class SourceRateLimitError(SourceError):
    """429. Источник просит сбавить темп."""

    retryable: ClassVar[bool] = True

    def __init__(self, message: str = "rate limited", retry_after_s: float | None = None) -> None:
        super().__init__(message)
        # goszakup заголовок Retry-After не отдаёт (проверено 2026-09-27),
        # но поле оставлено: другой источник или другой день могут его прислать.
        self.retry_after_s = retry_after_s


class SourceUnavailableError(SourceError):
    """5xx, обрыв соединения, таймаут. Беда временная, повтор осмыслен."""

    retryable: ClassVar[bool] = True
