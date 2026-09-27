"""Тесты сетевого источника на respx.

respx перехватывает запросы httpx на транспортном уровне: код источника при
этом работает целиком настоящий, включая сборку URL, заголовки и разбор тела.
Подменяется только сеть.

Формы ответов взяты из документации и живых проверок 2026-09-27:
без токена 401, с неверным токеном 403, заголовков про лимиты нет.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from procurement.ingest.errors import (
    SourceAuthError,
    SourceRateLimitError,
    SourceRequestError,
    SourceResponseError,
    SourceUnavailableError,
)
from procurement.ingest.limiter import AdaptiveLimiter
from procurement.ingest.models import PageKind
from procurement.ingest.rest_source import RestSource
from procurement.ingest.retry import RetryPolicy, run_with_retry

BASE = "https://api.test/v3"
CONTRACT_URL = f"{BASE}/contract"


def payload(*, ids: list[int], next_after: int | None, total: int = 100) -> dict[str, Any]:
    return {
        "total": total,
        "limit": 50,
        "next_page": None
        if next_after is None
        else f"/contract?page=next&search_after={next_after}",
        "items": [{"id": i, "contract_sum": 1000 * i} for i in ids],
    }


def make_source(**kwargs: Any) -> RestSource:
    return RestSource(base_url=BASE, token="test-token", page_limit=50, **kwargs)


# --- разбор успешного ответа --------------------------------------------------


@respx.mock
async def test_parses_page_and_cursor() -> None:
    respx.get(CONTRACT_URL).mock(
        return_value=httpx.Response(200, json=payload(ids=[1, 2, 3], next_after=3))
    )
    source = make_source()

    page = await source.fetch_page("contracts", None)
    await source.aclose()

    assert len(page.items) == 3
    assert page.next_cursor == "3"
    assert page.total == 100
    assert page.kind is PageKind.DATA


@respx.mock
async def test_absent_next_page_means_end() -> None:
    respx.get(CONTRACT_URL).mock(
        return_value=httpx.Response(200, json=payload(ids=[7], next_after=None))
    )
    source = make_source()

    page = await source.fetch_page("contracts", None)
    await source.aclose()

    assert page.next_cursor is None
    assert page.kind is PageKind.END


@respx.mock
async def test_empty_items_with_cursor_is_not_the_end() -> None:
    """Пустой список записей при живом курсоре — не конец выдачи.

    Считать его концом значит молча потерять весь хвост.
    """
    respx.get(CONTRACT_URL).mock(
        return_value=httpx.Response(200, json=payload(ids=[], next_after=42))
    )
    source = make_source()

    page = await source.fetch_page("contracts", None)
    await source.aclose()

    assert page.kind is PageKind.EMPTY
    assert page.next_cursor == "42"


@respx.mock
async def test_cursor_is_sent_as_search_after() -> None:
    route = respx.get(CONTRACT_URL).mock(
        return_value=httpx.Response(200, json=payload(ids=[9], next_after=None))
    )
    source = make_source()

    await source.fetch_page("contracts", "4996100")
    await source.aclose()

    sent = route.calls.last.request.url
    assert sent.params["search_after"] == "4996100"
    assert sent.params["page"] == "next"
    assert sent.params["limit"] == "50"


@respx.mock
async def test_token_goes_into_authorization_header() -> None:
    route = respx.get(CONTRACT_URL).mock(
        return_value=httpx.Response(200, json=payload(ids=[1], next_after=None))
    )
    source = make_source()

    await source.fetch_page("contracts", None)
    await source.aclose()

    assert route.calls.last.request.headers["Authorization"] == "Bearer test-token"


# --- ошибки -------------------------------------------------------------------


@respx.mock
async def test_429_becomes_rate_limit_error() -> None:
    respx.get(CONTRACT_URL).mock(return_value=httpx.Response(429))
    source = make_source()

    with pytest.raises(SourceRateLimitError) as caught:
        await source.fetch_page("contracts", None)
    await source.aclose()

    assert caught.value.retryable is True
    assert caught.value.retry_after_s is None


@respx.mock
async def test_retry_after_header_is_picked_up() -> None:
    respx.get(CONTRACT_URL).mock(return_value=httpx.Response(429, headers={"Retry-After": "12"}))
    source = make_source()

    with pytest.raises(SourceRateLimitError) as caught:
        await source.fetch_page("contracts", None)
    await source.aclose()

    assert caught.value.retry_after_s == 12.0


@respx.mock
async def test_unparseable_retry_after_is_ignored_not_fatal() -> None:
    """Retry-After в виде даты мы не разбираем, но и падать из-за него не должны."""
    respx.get(CONTRACT_URL).mock(
        return_value=httpx.Response(429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
    )
    source = make_source()

    with pytest.raises(SourceRateLimitError) as caught:
        await source.fetch_page("contracts", None)
    await source.aclose()

    assert caught.value.retry_after_s is None


@pytest.mark.parametrize("code", [401, 403])
@respx.mock
async def test_auth_codes_are_not_retryable(code: int) -> None:
    respx.get(CONTRACT_URL).mock(return_value=httpx.Response(code))
    source = make_source()

    with pytest.raises(SourceAuthError) as caught:
        await source.fetch_page("contracts", None)
    await source.aclose()

    assert caught.value.retryable is False


@pytest.mark.parametrize("code", [500, 502, 503])
@respx.mock
async def test_server_errors_are_retryable(code: int) -> None:
    respx.get(CONTRACT_URL).mock(return_value=httpx.Response(code))
    source = make_source()

    with pytest.raises(SourceUnavailableError) as caught:
        await source.fetch_page("contracts", None)
    await source.aclose()

    assert caught.value.retryable is True


@respx.mock
async def test_404_is_our_fault_not_retryable() -> None:
    respx.get(CONTRACT_URL).mock(return_value=httpx.Response(404))
    source = make_source()

    with pytest.raises(SourceRequestError) as caught:
        await source.fetch_page("contracts", None)
    await source.aclose()

    assert caught.value.retryable is False


@respx.mock
async def test_connection_drop_is_retryable() -> None:
    respx.get(CONTRACT_URL).mock(side_effect=httpx.ConnectError("соединение сброшено"))
    source = make_source()

    with pytest.raises(SourceUnavailableError) as caught:
        await source.fetch_page("contracts", None)
    await source.aclose()

    assert caught.value.retryable is True


@respx.mock
async def test_read_timeout_is_retryable() -> None:
    respx.get(CONTRACT_URL).mock(side_effect=httpx.ReadTimeout("ждали слишком долго"))
    source = make_source()

    with pytest.raises(SourceUnavailableError):
        await source.fetch_page("contracts", None)
    await source.aclose()


@respx.mock
async def test_non_json_body_is_response_error() -> None:
    respx.get(CONTRACT_URL).mock(
        return_value=httpx.Response(200, text="<html>502 Bad Gateway</html>")
    )
    source = make_source()

    with pytest.raises(SourceResponseError):
        await source.fetch_page("contracts", None)
    await source.aclose()


@respx.mock
async def test_missing_items_key_is_response_error() -> None:
    respx.get(CONTRACT_URL).mock(return_value=httpx.Response(200, json={"total": 5}))
    source = make_source()

    with pytest.raises(SourceResponseError, match="items"):
        await source.fetch_page("contracts", None)
    await source.aclose()


@respx.mock
async def test_next_page_without_cursor_is_response_error() -> None:
    """Ссылка есть, а search_after в ней нет — источник нарушил свой же контракт."""
    respx.get(CONTRACT_URL).mock(
        return_value=httpx.Response(
            200, json={"total": 5, "items": [], "next_page": "/contract?page=next"}
        )
    )
    source = make_source()

    with pytest.raises(SourceResponseError, match="search_after"):
        await source.fetch_page("contracts", None)
    await source.aclose()


async def test_unknown_entity_rejected_before_any_request() -> None:
    source = make_source()

    with pytest.raises(ValueError, match="неизвестная сущность"):
        await source.fetch_page("автомобили", None)
    await source.aclose()


# --- связка с повторами и лимитером -------------------------------------------


@respx.mock
async def test_429_then_success_retries_and_shrinks_window(slept: list[float]) -> None:
    """Сквозной сценарий: два отказа по лимиту, потом успех.

    Проверяем сразу три вещи: запрос повторяется, доходит до успеха, и окно
    конкурентности после 429 сжалось.
    """
    respx.get(CONTRACT_URL).mock(
        side_effect=[
            httpx.Response(429),
            httpx.Response(429),
            httpx.Response(200, json=payload(ids=[1, 2], next_after=None)),
        ]
    )
    source = make_source()
    limiter = AdaptiveLimiter(start=8, minimum=1, maximum=8, decrease_cooldown_s=0.0)

    page = await run_with_retry(
        lambda: source.fetch_page("contracts", None),
        policy=RetryPolicy(max_attempts=5, jitter="none"),
        feedback=limiter,
    )
    await source.aclose()

    assert len(page.items) == 2
    assert len(slept) == 2, "две неудачи — две паузы"
    assert limiter.target == 2, "8 -> 4 -> 2 после двух ответов 429"


@respx.mock
async def test_connection_drops_then_recovers(slept: list[float]) -> None:
    respx.get(CONTRACT_URL).mock(
        side_effect=[
            httpx.ConnectError("сброс"),
            httpx.ReadTimeout("таймаут"),
            httpx.Response(200, json=payload(ids=[5], next_after=None)),
        ]
    )
    source = make_source()
    limiter = AdaptiveLimiter(start=4, minimum=1, maximum=4)

    page = await run_with_retry(
        lambda: source.fetch_page("contracts", None),
        policy=RetryPolicy(max_attempts=5, jitter="none"),
        feedback=limiter,
    )
    await source.aclose()

    assert len(page.items) == 1
    assert limiter.target == 4, "обрыв связи — не повод сжимать окно"
