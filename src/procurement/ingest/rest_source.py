"""Источник поверх REST v3 goszakup.gov.kz.

Единственное место в проекте, где знают про HTTP-коды. Наружу отдаются только
доменные исключения из errors.py.
"""

from __future__ import annotations

from types import TracebackType
from typing import Self

import httpx
import orjson

from procurement.ingest.errors import (
    SourceAuthError,
    SourceRateLimitError,
    SourceRequestError,
    SourceResponseError,
    SourceUnavailableError,
)
from procurement.ingest.models import Page
from procurement.ingest.source import page_from_payload, resolve_path
from procurement.logging import get_logger

log = get_logger(__name__)

# Коды, проверенные живыми запросами 2026-09-27:
#   без токена           -> 401 Unauthorized, заголовок Www-Authenticate
#   с неверным токеном   -> 403 Forbidden, {"message": "Access denied"}
# Причины разные, реакция одна: повторять бессмысленно.
_AUTH_CODES = frozenset({401, 403})


def _parse_retry_after(value: str | None) -> float | None:
    """Разобрать заголовок Retry-After, если он есть.

    По стандарту значение бывает двух видов: число секунд или дата. Разбираем
    только число — дата у публичных API встречается редко, а неверно понятая
    дата опаснее её отсутствия.

    У goszakup этого заголовка нет вовсе (проверено 2026-09-27), но код должен
    пережить его появление.
    """
    if value is None:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


class RestSource:
    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        page_limit: int = 50,
        request_timeout_s: float = 30.0,
        connect_timeout_s: float = 10.0,
        user_agent: str = "procurement-analytics/0.1",
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._page_limit = page_limit
        # Клиент, переданный снаружи, закрывать не наше дело: его создал и
        # закроет тот, кто дал. Своим распоряжаемся сами.
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(request_timeout_s, connect=connect_timeout_s),
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": user_agent,
            },
            # Один клиент на весь прогон, а не по клиенту на запрос. Внутри
            # живёт пул соединений: TCP-рукопожатие и TLS-рукопожатие делаются
            # один раз и переиспользуются. Новый клиент на каждый запрос — это
            # оба рукопожатия заново плюс утечка сокетов, если его не закрыть.
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
        )

    async def fetch_page(self, entity: str, cursor: str | None) -> Page:
        path = resolve_path(entity)
        params: dict[str, str | int] = {"limit": self._page_limit}
        if cursor is not None:
            params["page"] = "next"
            params["search_after"] = cursor

        try:
            response = await self._client.get(path, params=params)
        except httpx.TimeoutException as err:
            msg = f"таймаут запроса {entity}: {err}"
            raise SourceUnavailableError(msg) from err
        except httpx.TransportError as err:
            # Сюда попадают обрыв соединения, сброс TLS, недоступный хост.
            msg = f"обрыв связи при запросе {entity}: {type(err).__name__}: {err}"
            raise SourceUnavailableError(msg) from err

        self._raise_for_status(response, entity)

        try:
            payload = orjson.loads(response.content)
        except orjson.JSONDecodeError as err:
            preview = response.content[:200].decode("utf-8", errors="replace")
            msg = f"ответ не является JSON: {preview!r}"
            raise SourceResponseError(msg) from err

        page = page_from_payload(entity, payload)
        log.debug(
            "source.page",
            entity=entity,
            cursor=cursor,
            items=len(page.items),
            next_cursor=page.next_cursor,
            kind=page.kind.value,
        )
        return page

    @staticmethod
    def _raise_for_status(response: httpx.Response, entity: str) -> None:
        code = response.status_code
        if code < 400:
            return

        if code == httpx.codes.TOO_MANY_REQUESTS:
            raise SourceRateLimitError(
                f"429 при запросе {entity}",
                retry_after_s=_parse_retry_after(response.headers.get("Retry-After")),
            )
        if code in _AUTH_CODES:
            msg = f"{code} при запросе {entity}: токен отсутствует или не даёт доступа"
            raise SourceAuthError(msg)
        if code >= httpx.codes.INTERNAL_SERVER_ERROR:
            msg = f"{code} при запросе {entity}: сбой на стороне источника"
            raise SourceUnavailableError(msg)

        # Остальные 4xx — наша вина: неверный путь, параметр, сущность.
        msg = f"{code} при запросе {entity}: запрос собран неверно"
        raise SourceRequestError(msg)

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()
