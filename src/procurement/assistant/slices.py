"""Срезы витрин как инструменты агента.

Каждый инструмент вызывает ту же функцию роутера, что и API. Не HTTP-запрос
к своему же сервису и не копия запроса: проверка БИН, период, пагинация,
различие «не найден» и «пусто за период» — общие. Исправили в API —
исправилось у агента.

Готовые срезы надёжнее свободного SQL: запрос в них проверен тестами и
замерами. Поэтому в описаниях модели прямо сказано — сначала срез, ``run_sql``
только когда подходящего нет.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from procurement.api.queries import Marts, Paging, Period
from procurement.api.routers import analytics
from procurement.api.schemas import Bin
from procurement.assistant.tools import Tool

# Строк на один вызов. В API до 500, но модели столько не нужно: ей нужен
# ответ на вопрос, а не выгрузка, и каждая строка — токены.
MAX_TOOL_LIMIT = 50

# Общая приписка к описаниям: из долей и индексов выводов о нарушениях не
# следует. То же правило, что в описании ручек API.
FACTS_ONLY = (
    "Показатель описывает распределение сумм и ничего больше; "
    "выводов о нарушениях или сговоре из него не делай."
)


class _PeriodArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date_from: date | None = Field(
        default=None, description="Начало периода включительно, ГГГГ-ММ-ДД"
    )
    date_to: date | None = Field(
        default=None,
        description="Конец периода включительно, ГГГГ-ММ-ДД. Период не шире пяти лет",
    )

    def period(self) -> Period:
        return Period.resolve(self.date_from, self.date_to)


class _PagedArgs(_PeriodArgs):
    limit: int = Field(default=10, ge=1, le=MAX_TOOL_LIMIT, description="Сколько строк вернуть")

    def paging(self) -> Paging:
        return Paging(limit=self.limit, offset=0)


class CustomerArgs(_PagedArgs):
    customer_bin: Bin = Field(description="БИН заказчика, 12 цифр")


class SupplierArgs(_PagedArgs):
    supplier_biin: Bin = Field(description="БИН или ИИН поставщика, 12 цифр")


class CategoryArgs(_PagedArgs):
    level: int = Field(
        default=1,
        ge=0,
        le=3,
        description="Уровень классификатора: 0 раздел, 1 группа, 2 подгруппа, 3 лист",
    )


class MonthlyArgs(_PeriodArgs):
    customer_bin: Bin | None = Field(default=None, description="Оставить одного заказчика")
    supplier_biin: Bin | None = Field(default=None, description="Оставить одного поставщика")


class OutlierArgs(_PagedArgs):
    enstru_code: str | None = Field(
        default=None, max_length=32, description="Код товарной позиции ЕНС ТРУ"
    )
    min_lots: int = Field(
        default=30, ge=5, le=10_000, description="Сколько лотов нужно в позиции для медианы"
    )


class ConcentrationArgs(_PagedArgs):
    min_suppliers: int = Field(
        default=5, ge=1, le=1000, description="Минимум поставщиков у заказчика"
    )


def build_slice_tools(marts: Marts) -> list[Tool[Any]]:
    async def customer_suppliers(a: CustomerArgs) -> Any:
        return await analytics.customer_suppliers(
            customer_bin=a.customer_bin, marts=marts, period=a.period(), paging=a.paging()
        )

    async def supplier_customers(a: SupplierArgs) -> Any:
        return await analytics.supplier_customers(
            supplier_biin=a.supplier_biin, marts=marts, period=a.period(), paging=a.paging()
        )

    async def categories(a: CategoryArgs) -> Any:
        return await analytics.categories(
            marts=marts, period=a.period(), paging=a.paging(), level=a.level
        )

    async def contracts_monthly(a: MonthlyArgs) -> Any:
        return await analytics.contracts_monthly(
            marts=marts,
            period=a.period(),
            customer_bin=a.customer_bin,
            supplier_biin=a.supplier_biin,
        )

    async def price_outliers(a: OutlierArgs) -> Any:
        return await analytics.price_outliers(
            marts=marts,
            period=a.period(),
            paging=a.paging(),
            enstru_code=a.enstru_code,
            min_lots=a.min_lots,
        )

    async def concentration(a: ConcentrationArgs) -> Any:
        return await analytics.concentration(
            marts=marts, period=a.period(), paging=a.paging(), min_suppliers=a.min_suppliers
        )

    async def supplier_activity(a: SupplierArgs) -> Any:
        return await analytics.supplier_activity(
            supplier_biin=a.supplier_biin, marts=marts, period=a.period(), paging=a.paging()
        )

    return [
        Tool(
            "customer_suppliers",
            "Поставщики одного заказчика: число договоров, сумма и доля каждого в "
            "закупках заказчика за период, по убыванию суммы. " + FACTS_ONLY,
            CustomerArgs,
            customer_suppliers,
        ),
        Tool(
            "supplier_customers",
            "Заказчики одного поставщика: число договоров, сумма и доля каждого в "
            "выручке поставщика за период, по убыванию суммы. " + FACTS_ONLY,
            SupplierArgs,
            supplier_customers,
        ),
        Tool(
            "category_rollup",
            "Суммы и число лотов по дереву классификатора ЕНС ТРУ, свёрнутые до "
            "выбранного уровня, за период. Отвечает на «на что тратят больше всего».",
            CategoryArgs,
            categories,
        ),
        Tool(
            "contracts_monthly",
            "Помесячная динамика договоров: число, сумма и изменение к прошлому месяцу "
            "в процентах. Можно оставить одного заказчика или поставщика.",
            MonthlyArgs,
            contracts_monthly,
        ),
        Tool(
            "price_outliers",
            "Лоты, у которых цена за единицу сильнее всего отличается от медианы по "
            "той же товарной позиции. Отклонение цены — повод проверить данные "
            "(единицы измерения, опечатку в количестве), а не вывод о нарушении.",
            OutlierArgs,
            price_outliers,
        ),
        Tool(
            "customer_concentration",
            "Заказчики с самой высокой концентрацией закупок: индекс Херфиндаля "
            "(0–10000), доля первого и первых трёх поставщиков. " + FACTS_ONLY,
            ConcentrationArgs,
            concentration,
        ),
        Tool(
            "supplier_activity",
            "Отрезки месяцев подряд, в которые у поставщика были договоры.",
            SupplierArgs,
            supplier_activity,
        ),
    ]
