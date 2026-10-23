"""Маршрутизатор вопросов на поддельной модели."""

from __future__ import annotations

import json

import pytest

from procurement.assistant.errors import LLMResponseError
from procurement.assistant.fake import FakeLLM
from procurement.assistant.models import LLMResponse, StopReason, TextBlock
from procurement.assistant.router import SYSTEM_PROMPT, QuestionKind, route_question, route_schema


def route_json(**fields: object) -> LLMResponse:
    data = {"kind": "data", "year_from": None, "year_to": None, "reason": "нужны суммы"} | fields
    return FakeLLM.text(json.dumps(data, ensure_ascii=False))


async def test_router_sends_system_prompt_and_schema() -> None:
    llm = FakeLLM([route_json()])

    await route_question(llm, "Сколько договоров в 2024?")

    (call,) = llm.calls
    assert call.system == SYSTEM_PROMPT
    assert call.output_schema == route_schema()
    assert call.tools == []
    assert call.messages[0].content == [TextBlock(text="Сколько договоров в 2024?")]


def test_schema_fits_structured_output() -> None:
    # Без этого API вернёт 400: каждое поле обязательно, лишних полей нет.
    schema = route_schema()

    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {"kind", "year_from", "year_to", "reason"}


@pytest.mark.parametrize("kind", list(QuestionKind))
async def test_router_parses_every_kind(kind: QuestionKind) -> None:
    llm = FakeLLM([route_json(kind=kind.value, year_from=2024, year_to=2025)])

    route = await route_question(llm, "вопрос")

    assert route.kind is kind
    assert (route.year_from, route.year_to) == (2024, 2025)


async def test_router_swaps_reversed_period() -> None:
    llm = FakeLLM([route_json(year_from=2025, year_to=2023)])

    route = await route_question(llm, "с 2025 по 2023")

    assert (route.year_from, route.year_to) == (2023, 2025)


@pytest.mark.parametrize(
    "bad",
    [
        route_json(kind="weather"),
        route_json(year_from=1990),  # пределы годов схема в API не держит, держим мы
        FakeLLM.text("не JSON"),
        FakeLLM.text('{"kind": "data"}'),
    ],
)
async def test_router_rejects_answer_out_of_schema(bad: LLMResponse) -> None:
    with pytest.raises(LLMResponseError, match="не по схеме"):
        await route_question(FakeLLM([bad]), "вопрос")


@pytest.mark.parametrize("stop_reason", [StopReason.MAX_TOKENS, StopReason.REFUSAL])
async def test_router_rejects_unfinished_answer(stop_reason: StopReason) -> None:
    cut = LLMResponse(content=[TextBlock(text='{"kind": "da')], stop_reason=stop_reason)

    with pytest.raises(LLMResponseError, match=stop_reason.value):
        await route_question(FakeLLM([cut]), "вопрос")
