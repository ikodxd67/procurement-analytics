"""Срезы витрин: по заказчику, поставщику, категории и периоду."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Path, Query

from procurement.api.deps import MartsDep, PagingDep, PeriodDep
from procurement.api.errors import ErrorResponse, NotFoundError
from procurement.api.schemas import (
    ActivityPeriod,
    Bin,
    CategoryRollup,
    Concentration,
    CustomerShare,
    MonthlyPoint,
    Page,
    PageMeta,
    PriceOutlier,
    SupplierShare,
)

router = APIRouter(
    prefix="/v1",
    tags=["аналитика"],
    responses={
        422: {"model": ErrorResponse, "description": "Параметры не прошли проверку"},
        503: {"model": ErrorResponse, "description": "Хранилище недоступно"},
    },
)

CustomerPath = Annotated[Bin, Path(description="БИН заказчика, двенадцать цифр")]
SupplierPath = Annotated[Bin, Path(description="БИН или ИИН поставщика, двенадцать цифр")]


def _meta(returned: int, has_more: bool, paging: PagingDep) -> PageMeta:
    return PageMeta(limit=paging.limit, offset=paging.offset, returned=returned, has_more=has_more)


@router.get(
    "/customers/{customer_bin}/suppliers",
    summary="Поставщики заказчика и их доли",
    responses={404: {"model": ErrorResponse, "description": "Заказчик не найден"}},
)
async def customer_suppliers(
    customer_bin: CustomerPath,
    marts: MartsDep,
    period: PeriodDep,
    paging: PagingDep,
) -> Page[SupplierShare]:
    """Доля каждого поставщика в закупках заказчика за период.

    Доля — результат деления сумм договоров и ничего кроме. Причины
    распределения из неё не следуют.
    """
    rows, has_more = await marts.supplier_shares(customer_bin, period, paging)

    if not rows and paging.offset == 0 and not await marts.customer_exists(customer_bin):
        # Различаем «такого заказчика нет вовсе» и «за этот период у него
        # ничего не было». Первое — 404, второе — пустая страница: клиент
        # по-разному реагирует на опечатку в БИН и на тихий период.
        msg = f"Заказчик с БИН {customer_bin} в данных не найден"
        raise NotFoundError(msg)

    items = [
        SupplierShare(supplier_biin=r[0], contracts=r[1], total=r[2], share_pct=r[3]) for r in rows
    ]
    return Page(meta=_meta(len(items), has_more, paging), items=items)


@router.get(
    "/suppliers/{supplier_biin}/customers",
    summary="Заказчики поставщика и их доли",
    responses={404: {"model": ErrorResponse, "description": "Поставщик не найден"}},
)
async def supplier_customers(
    supplier_biin: SupplierPath,
    marts: MartsDep,
    period: PeriodDep,
    paging: PagingDep,
) -> Page[CustomerShare]:
    rows, has_more = await marts.customer_shares(supplier_biin, period, paging)

    if not rows and paging.offset == 0 and not await marts.supplier_exists(supplier_biin):
        msg = f"Поставщик с БИН/ИИН {supplier_biin} в данных не найден"
        raise NotFoundError(msg)

    items = [
        CustomerShare(customer_bin=r[0], contracts=r[1], total=r[2], share_pct=r[3]) for r in rows
    ]
    return Page(meta=_meta(len(items), has_more, paging), items=items)


@router.get("/categories", summary="Свёртка сумм лотов по дереву классификатора")
async def categories(
    marts: MartsDep,
    period: PeriodDep,
    paging: PagingDep,
    level: Annotated[
        int,
        Query(ge=0, le=3, description="Уровень свёртки: 0 раздел, 1 группа, 2 подгруппа, 3 лист"),
    ] = 1,
) -> Page[CategoryRollup]:
    """Суммы лотов, свёрнутые на выбранный уровень классификатора.

    Дерево обходится рекурсивным CTE, а не разбором кода строкой: связь
    родитель-потомок хранится явно, и запрос не зависит от формы кода.
    """
    rows, has_more = await marts.category_rollup(level, period, paging)
    items = [
        CategoryRollup(
            code=r[0], name_ru=r[1], level=r[2], lots=r[3], total_amount=r[4], avg_amount=r[5]
        )
        for r in rows
    ]
    return Page(meta=_meta(len(items), has_more, paging), items=items)


@router.get("/contracts/monthly", summary="Помесячная динамика договоров")
async def contracts_monthly(
    marts: MartsDep,
    period: PeriodDep,
    customer_bin: Annotated[Bin | None, Query(description="Оставить один заказчик")] = None,
    supplier_biin: Annotated[Bin | None, Query(description="Оставить один поставщик")] = None,
) -> list[MonthlyPoint]:
    """Суммы по месяцам с изменением к предыдущему месяцу.

    У первого месяца выборки `prev_total` и `change_pct` пусты: предыдущего
    месяца в данных нет. Ноль на этом месте означал бы «в прошлом месяце
    закупок не было», что неправда.
    """
    rows = await marts.monthly(period, customer_bin=customer_bin, supplier_biin=supplier_biin)
    return [
        MonthlyPoint(month=r[0], contracts=r[1], total=r[2], prev_total=r[3], change_pct=r[4])
        for r in rows
    ]


@router.get("/lots/price-outliers", summary="Лоты с наибольшим отклонением цены от медианы")
async def price_outliers(
    marts: MartsDep,
    period: PeriodDep,
    paging: PagingDep,
    enstru_code: Annotated[
        str | None, Query(max_length=32, description="Ограничить одной товарной позицией")
    ] = None,
    min_lots: Annotated[
        int,
        Query(
            ge=5,
            le=10_000,
            description="Сколько лотов должно быть в позиции, чтобы считать медиану",
        ),
    ] = 30,
) -> Page[PriceOutlier]:
    """Отклонение цены за единицу от медианы по товарной позиции.

    Сравнивается цена за единицу, а не сумма лота: лот на тысячу пачек бумаги
    и лот на одну пачку по сумме сравнивать бессмысленно. Медиана, а не
    среднее: среднее утаскивается вверх единичным дорогим лотом.
    """
    rows, has_more = await marts.price_outliers(
        period, paging, min_lots=min_lots, enstru_code=enstru_code
    )
    items = [
        PriceOutlier(
            lot_id=r[0],
            enstru_code=r[1],
            unit_price=r[2],
            median_price=r[3],
            lots_in_position=r[4],
            deviation_pct=r[5],
        )
        for r in rows
    ]
    return Page(meta=_meta(len(items), has_more, paging), items=items)


@router.get("/customers/concentration", summary="Концентрация закупок заказчиков")
async def concentration(
    marts: MartsDep,
    period: PeriodDep,
    paging: PagingDep,
    min_suppliers: Annotated[
        int, Query(ge=1, le=1000, description="Минимум поставщиков у заказчика")
    ] = 5,
) -> Page[Concentration]:
    """Индекс Херфиндаля и доля первых поставщиков.

    Показатель описывает распределение сумм и только его. Высокое значение
    бывает при единственном поставщике на рынке, при узком предмете закупки,
    при небольшом размере заказчика. Выводов о добросовестности из него не
    следует.
    """
    rows, has_more = await marts.concentration(period, paging, min_suppliers=min_suppliers)
    items = [
        Concentration(
            customer_bin=r[0],
            suppliers=r[1],
            hhi=r[2],
            top1_share_pct=r[3],
            top3_share_pct=r[4],
        )
        for r in rows
    ]
    return Page(meta=_meta(len(items), has_more, paging), items=items)


@router.get(
    "/suppliers/{supplier_biin}/activity",
    summary="Периоды непрерывной активности поставщика",
    responses={404: {"model": ErrorResponse, "description": "Поставщик не найден"}},
)
async def supplier_activity(
    supplier_biin: SupplierPath,
    marts: MartsDep,
    period: PeriodDep,
    paging: PagingDep,
) -> Page[ActivityPeriod]:
    """Отрезки месяцев подряд, в которые у поставщика были договоры."""
    rows, has_more = await marts.supplier_activity(supplier_biin, period, paging)

    if not rows and paging.offset == 0 and not await marts.supplier_exists(supplier_biin):
        msg = f"Поставщик с БИН/ИИН {supplier_biin} в данных не найден"
        raise NotFoundError(msg)

    items = [
        ActivityPeriod(supplier_biin=r[0], started=r[1], ended=r[2], months_in_row=r[3])
        for r in rows
    ]
    return Page(meta=_meta(len(items), has_more, paging), items=items)
