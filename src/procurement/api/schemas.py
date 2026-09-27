"""Модели запросов и ответов.

Все ответы завёрнуты в конверт с данными о странице. Отдавать голый список
плохо: в него потом нельзя добавить ничего, не сломав клиентов, а добавить
рано или поздно захочется.

Формулировки в описаниях полей фактические. Доля поставщика — результат
деления сумм, и только. Никаких выводов о причинах такой доли из неё не
следует.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints

# БИН и ИИН в Казахстане — ровно двенадцать цифр. Проверка на входе экономит
# поход в базу за заведомо пустым ответом.
Bin = Annotated[str, StringConstraints(pattern=r"^\d{12}$")]


class PageMeta(BaseModel):
    limit: int = Field(description="Сколько записей запрошено")
    offset: int = Field(description="Сколько пропущено от начала")
    returned: int = Field(description="Сколько записей в этом ответе")
    has_more: bool = Field(
        description=(
            "Есть ли ещё записи дальше. Считается без COUNT по всей выборке: "
            "запрашивается на одну запись больше, чем нужно"
        )
    )


class Page[ItemT](BaseModel):
    meta: PageMeta
    items: list[ItemT]


# --- элементы витрин -------------------------------------------------------------


class SupplierShare(BaseModel):
    supplier_biin: str
    contracts: int
    total: Decimal = Field(description="Сумма договоров поставщика у этого заказчика")
    share_pct: float = Field(description="Доля от всех закупок заказчика за период, в процентах")


class CustomerShare(BaseModel):
    customer_bin: str
    contracts: int
    total: Decimal
    share_pct: float = Field(description="Доля от всей выручки поставщика за период, в процентах")


class CategoryRollup(BaseModel):
    code: str = Field(description="Код узла классификатора")
    name_ru: str
    level: int
    lots: int
    total_amount: Decimal
    avg_amount: Decimal


class MonthlyPoint(BaseModel):
    month: date
    contracts: int
    total: Decimal
    prev_total: Decimal | None = Field(
        default=None,
        description="Сумма предыдущего месяца. Пусто у первого месяца выборки: предыдущего нет",
    )
    change_pct: float | None = Field(
        default=None, description="Изменение к предыдущему месяцу, в процентах"
    )


class PriceOutlier(BaseModel):
    lot_id: int
    enstru_code: str
    unit_price: Decimal = Field(description="Цена за единицу: сумма лота, делённая на количество")
    median_price: Decimal = Field(description="Медиана цены за единицу по этой товарной позиции")
    lots_in_position: int
    deviation_pct: float


class Concentration(BaseModel):
    customer_bin: str
    suppliers: int
    hhi: int = Field(
        description=(
            "Индекс Херфиндаля: сумма квадратов долей поставщиков в процентах. "
            "От 10000/N при равных долях до 10000, когда весь объём у одного"
        )
    )
    top1_share_pct: float
    top3_share_pct: float | None = Field(
        default=None, description="Пусто, если поставщиков меньше трёх"
    )


class ActivityPeriod(BaseModel):
    supplier_biin: str
    started: date
    ended: date
    months_in_row: int = Field(description="Длина непрерывного отрезка активности в месяцах")


class HealthStatus(BaseModel):
    status: str
    checks: dict[str, str] = Field(default_factory=dict)
