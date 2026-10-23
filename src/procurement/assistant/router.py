"""Маршрутизатор: куда отправить вопрос — к данным, к документам или туда и туда.

Первая задача модели в проекте и самая простая: один вызов, ответ строго по
JSON-схеме (structured output). Модель не отвечает на вопрос, она только
решает, кто будет отвечать. Агент (этап 4) возьмёт отсюда тип и период.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from procurement.assistant.errors import LLMResponseError
from procurement.assistant.llm import LLMClient
from procurement.assistant.models import Message, StopReason


class QuestionKind(StrEnum):
    DATA = "data"
    """Ответ — число или таблица из ClickHouse: суммы, топы, динамика."""

    DOCS = "docs"
    """Ответ — в правилах госзакупок или описании витрин: что такое X, как устроено Y."""

    MIXED = "mixed"
    """Нужны оба: определение из документов и цифры из данных."""

    OFF_TOPIC = "off_topic"
    """Вопрос не о госзакупках. Отвечать не будем."""


class Route(BaseModel):
    # extra=forbid даёт в схеме additionalProperties: false — этого требует
    # structured output, и лишние поля от модели тоже не пройдут.
    model_config = ConfigDict(extra="forbid")

    kind: QuestionKind
    # Без значений по умолчанию: поле обязано быть в ответе, пусть и null.
    # Иначе не отличить «модель не нашла период» от «модель забыла поле».
    year_from: int | None = Field(ge=2000, le=2100)
    year_to: int | None = Field(ge=2000, le=2100)
    reason: str = Field(description="Одна фраза: почему выбран этот тип")


SYSTEM_PROMPT = """\
Ты маршрутизатор вопросов к аналитической системе по государственным закупкам \
Казахстана. Сам на вопрос не отвечай. Определи, откуда брать ответ.

Типы:
- data — нужны цифры из базы договоров и лотов: суммы, количество, топы \
заказчиков и поставщиков, динамика по месяцам, доли, отклонения цен.
- docs — нужно объяснение из правил госзакупок или из описания данных: \
что значит термин, как устроена процедура, что лежит в поле или витрине.
- mixed — нужно и то и другое, например определение показателя и его значения.
- off_topic — вопрос не о госзакупках и не о данных системы.

year_from и year_to — годы, о которых спрашивают; если период не назван, null. \
Один год — одинаковые year_from и year_to. reason — одна короткая фраза по-русски.\
"""


def route_schema() -> dict[str, Any]:
    return Route.model_json_schema()


async def route_question(llm: LLMClient, question: str) -> Route:
    response = await llm.complete(
        [Message.user(question)],
        system=SYSTEM_PROMPT,
        output_schema=route_schema(),
    )
    if response.stop_reason is not StopReason.END_TURN:
        # Обрезанный JSON (max_tokens) или отказ — разбирать нечего.
        msg = f"маршрутизатор: ответ не закончен, stop_reason={response.stop_reason.value}"
        raise LLMResponseError(msg)
    try:
        route = Route.model_validate_json(response.text)
    except ValidationError as exc:
        # Схема гарантирует форму JSON, но не наши ограничения вроде ge=2000:
        # числовые пределы structured output не поддерживает, клиент их вырезает.
        msg = f"маршрутизатор: ответ не по схеме: {exc.error_count()} ошибок"
        raise LLMResponseError(msg) from exc
    if (
        route.year_from is not None
        and route.year_to is not None
        and route.year_from > route.year_to
    ):
        route = route.model_copy(update={"year_from": route.year_to, "year_to": route.year_from})
    return route
