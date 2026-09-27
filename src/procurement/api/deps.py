"""Зависимости FastAPI: подключения, период, пагинация.

Клиенты к базам создаются один раз на время жизни приложения и лежат в
app.state. Создавать их на каждый запрос значило бы делать рукопожатие TCP и
TLS заново несколько раз в секунду — та же ошибка, что и клиент httpx на
запрос в загрузчике.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Annotated

from clickhouse_connect.driver import AsyncClient
from fastapi import Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from procurement.api.errors import BadRequestError
from procurement.api.queries import Marts, Paging, Period

# Верхняя граница страницы. Без неё один клиент с limit=1000000 кладёт сервис
# на ровном месте.
MAX_LIMIT = 500

# Ограничение на глубину пагинации. OFFSET в ClickHouse не бесплатен: чтобы
# пропустить сто тысяч строк, их придётся сначала посчитать. Для витрин, где
# результат после группировки невелик, этого предела хватает с запасом.
MAX_OFFSET = 10_000

# Предел ширины периода. Пять лет закрывают любой осмысленный запрос, а
# случайный date_from=1900 перестаёт быть способом устроить полный перебор.
MAX_PERIOD_DAYS = 366 * 5

DEFAULT_PERIOD_DAYS = 365


def get_clickhouse(request: Request) -> AsyncClient:
    return request.app.state.clickhouse  # type: ignore[no-any-return]


def get_sessions(request: Request) -> async_sessionmaker[AsyncSession]:
    return request.app.state.sessions  # type: ignore[no-any-return]


def get_marts(client: Annotated[AsyncClient, Depends(get_clickhouse)]) -> Marts:
    return Marts(client)


def get_period(
    date_from: Annotated[
        date | None,
        Query(description="Начало периода включительно. По умолчанию год назад от date_to"),
    ] = None,
    date_to: Annotated[
        date | None,
        Query(description="Конец периода включительно. По умолчанию сегодня"),
    ] = None,
) -> Period:
    resolved_to = date_to or date.today()
    resolved_from = date_from or (resolved_to - timedelta(days=DEFAULT_PERIOD_DAYS))

    if resolved_from > resolved_to:
        msg = f"Начало периода {resolved_from} позже его конца {resolved_to}"
        raise BadRequestError(msg)

    span = (resolved_to - resolved_from).days
    if span > MAX_PERIOD_DAYS:
        msg = f"Период в {span} дней шире допустимых {MAX_PERIOD_DAYS}"
        raise BadRequestError(msg)

    return Period(date_from=resolved_from, date_to=resolved_to)


def get_paging(
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT, description="Размер страницы")] = 50,
    offset: Annotated[int, Query(ge=0, le=MAX_OFFSET, description="Сдвиг от начала")] = 0,
) -> Paging:
    return Paging(limit=limit, offset=offset)


PeriodDep = Annotated[Period, Depends(get_period)]
PagingDep = Annotated[Paging, Depends(get_paging)]
MartsDep = Annotated[Marts, Depends(get_marts)]
SessionsDep = Annotated["async_sessionmaker[AsyncSession]", Depends(get_sessions)]
