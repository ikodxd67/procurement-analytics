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


# --- ассистент -------------------------------------------------------------------

# Длиннее вопрос не нужен, а каждый символ уходит модели токенами.
MAX_QUESTION_CHARS = 1000


class AskRequest(BaseModel):
    question: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=3, max_length=MAX_QUESTION_CHARS)
    ] = Field(description="Вопрос по-русски: о цифрах закупок или о правилах их проведения")


class AskSource(BaseModel):
    title: str = Field(description="Документ › глава › параграф › пункт")
    url: str
    cited: bool = Field(
        description="Ссылка есть в тексте ответа. Остальные модель нашла, но не назвала"
    )


class AskSql(BaseModel):
    query: str = Field(description="SELECT, который написала модель")
    ok: bool = Field(description="Прошёл проверку и выполнился под пользователем только на чтение")


class AskToolCall(BaseModel):
    name: str
    ok: bool
    duration_ms: int


class AskUsage(BaseModel):
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float | None = Field(
        description="По ценам документации Claude API; пусто для модели не из таблицы цен"
    )


class AskResponse(BaseModel):
    answer: str
    stop: str = Field(
        description=(
            "Чем закончился проход: answered, off_topic — ответ; step_limit, timeout, "
            "max_tokens, refusal — ответа по существу нет"
        )
    )
    kind: str | None = Field(
        description="Тип вопроса по маршрутизатору: data, docs, mixed, off_topic"
    )
    sources: list[AskSource] = Field(description="Найденные пункты правил и описания данных")
    sql: list[AskSql] = Field(description="Свободный SQL агента, если готовых срезов не хватило")
    tool_calls: list[AskToolCall]
    steps: int = Field(description="Сколько раз агент вызвал модель, не считая маршрутизатора")
    usage: AskUsage = Field(
        description="Расход на этот ответ. У ответа из кэша — расход первого прохода"
    )
    cached: bool = Field(description="Ответ не стоил нового прохода агента")
