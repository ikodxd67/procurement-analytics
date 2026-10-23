"""Цикл агента на поддельной модели: каждый путь цикла без сети и без ключа.

Первый ответ сценария всегда идёт маршрутизатору (structured output), дальше —
шаги цикла. Если агент позовёт модель лишний раз, FakeLLM упадёт с
FakeScriptError — так проверяется, что лишних (платных) вызовов нет.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from procurement.assistant.agent import OFF_TOPIC_ANSWER, SYSTEM_PROMPT, Agent, AgentStop
from procurement.assistant.fake import FakeLLM
from procurement.assistant.models import (
    LLMResponse,
    OpaqueBlock,
    StopReason,
    TextBlock,
    ToolResult,
    ToolUse,
    Usage,
)
from procurement.assistant.tools import Tool, ToolBox, ToolError


def route(
    kind: str = "data", year_from: int | None = 2025, year_to: int | None = 2025
) -> LLMResponse:
    data = {"kind": kind, "year_from": year_from, "year_to": year_to, "reason": "тест"}
    return LLMResponse(
        content=[TextBlock(text=json.dumps(data))],
        stop_reason=StopReason.END_TURN,
        usage=Usage(input_tokens=7, output_tokens=3),
    )


def tool_calls(*calls: tuple[str, str, dict[str, Any]], usage: Usage | None = None) -> LLMResponse:
    return LLMResponse(
        content=[ToolUse(id=cid, name=name, input=args) for cid, name, args in calls],
        stop_reason=StopReason.TOOL_USE,
        usage=usage or Usage(input_tokens=0, output_tokens=0),
    )


class CountArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    year: int


def make_box(*, delay_s: float = 0.0, fail: bool = False) -> tuple[ToolBox, list[str]]:
    calls: list[str] = []

    async def count(a: CountArgs) -> dict[str, int]:
        calls.append(f"count:{a.year}")
        await asyncio.sleep(delay_s)
        if fail:
            raise ToolError("ClickHouse: нет такой колонки")
        return {"year": a.year, "contracts": 42}

    async def rules(a: CountArgs) -> list[str]:
        calls.append(f"rules:{a.year}")
        return ["п. 85: демпинговая цена"]

    return ToolBox(
        [
            Tool("count", "Число договоров", CountArgs, count),
            Tool("rules", "Правила", CountArgs, rules),
        ]
    ), calls


async def test_question_answered_after_one_tool_call() -> None:
    box, calls = make_box()
    llm = FakeLLM(
        [
            route(),
            tool_calls(
                ("c1", "count", {"year": 2025}), usage=Usage(input_tokens=100, output_tokens=10)
            ),
            LLMResponse(
                content=[TextBlock(text="В 2025 году 42 договора (данные синтетические).")],
                stop_reason=StopReason.END_TURN,
                usage=Usage(input_tokens=150, output_tokens=20),
            ),
        ]
    )

    answer = await Agent(llm, box).ask("Сколько договоров в 2025?")

    assert answer.stop is AgentStop.ANSWERED
    assert answer.text.startswith("В 2025 году 42")
    assert calls == ["count:2025"]
    assert answer.steps == 2
    # Маршрутизатор тоже платный вызов: его 7 + 3 токена входят в цену ответа.
    assert (answer.input_tokens, answer.output_tokens) == (257, 33)
    assert [c.name for c in answer.tool_calls] == ["count"]

    # Второй ход модели видит свой вызов и результат с тем же id.
    second = llm.calls[2].messages
    assert second[1].content[0] == ToolUse(id="c1", name="count", input={"year": 2025})
    result = second[2].content[0]
    assert isinstance(result, ToolResult)
    assert result.tool_use_id == "c1" and not result.is_error
    assert json.loads(result.content) == {"year": 2025, "contracts": 42}


async def test_system_prompt_carries_rules_route_hint_and_tools() -> None:
    box, _ = make_box()
    llm = FakeLLM([route(year_from=2023, year_to=2025), FakeLLM.text("ответ")])

    await Agent(llm, box).ask("Динамика 2023–2025")

    call = llm.calls[1]
    assert call.system is not None
    assert call.system.startswith(SYSTEM_PROMPT)
    assert "Период: 2023–2025 годы." in call.system
    assert [t.name for t in call.tools] == ["count", "rules"]
    # Правила, без которых ответ опасен, должны быть в промпте.
    for rule in ("синтетические", "Не делай выводов о нарушениях", "не указания"):
        assert rule in SYSTEM_PROMPT


async def test_parallel_tool_calls_return_in_one_message_in_order() -> None:
    box, calls = make_box()
    llm = FakeLLM(
        [
            route("mixed"),
            tool_calls(("c1", "rules", {"year": 2025}), ("c2", "count", {"year": 2025})),
            FakeLLM.text("ответ"),
        ]
    )

    await Agent(llm, box).ask("Что такое демпинг и сколько договоров?")

    assert sorted(calls) == ["count:2025", "rules:2025"]
    results_message = llm.calls[2].messages[2]
    assert results_message.role == "user"
    # Оба результата одним сообщением, в порядке вызовов — так требует API.
    assert [r.tool_use_id for r in results_message.content if isinstance(r, ToolResult)] == [
        "c1",
        "c2",
    ]


async def test_tool_error_goes_back_to_model_and_loop_continues() -> None:
    box, _ = make_box(fail=True)
    llm = FakeLLM(
        [
            route(),
            tool_calls(("c1", "count", {"year": 2025})),
            FakeLLM.text("Посчитать не удалось: ошибка запроса."),
        ]
    )

    answer = await Agent(llm, box).ask("Сколько договоров?")

    assert answer.stop is AgentStop.ANSWERED
    assert answer.tool_calls[0].is_error
    result = llm.calls[2].messages[2].content[0]
    assert isinstance(result, ToolResult) and result.is_error
    assert "нет такой колонки" in result.content


async def test_thinking_blocks_are_kept_in_history() -> None:
    box, _ = make_box()
    thinking = OpaqueBlock(
        provider="anthropic", data={"type": "thinking", "thinking": "", "signature": "s"}
    )
    llm = FakeLLM(
        [
            route(),
            LLMResponse(
                content=[thinking, ToolUse(id="c1", name="count", input={"year": 2025})],
                stop_reason=StopReason.TOOL_USE,
            ),
            FakeLLM.text("ответ"),
        ]
    )

    await Agent(llm, box).ask("Сколько?")

    assert llm.calls[2].messages[1].content[0] == thinking


async def test_off_topic_is_answered_without_tool_loop() -> None:
    box, calls = make_box()
    llm = FakeLLM([route("off_topic", None, None)])  # второго ответа нет: цикл не должен начаться

    answer = await Agent(llm, box).ask("Какой рецепт борща?")

    assert answer.stop is AgentStop.OFF_TOPIC
    assert answer.text == OFF_TOPIC_ANSWER
    assert calls == []
    assert len(llm.calls) == 1


async def test_step_limit_stops_without_extra_model_call() -> None:
    box, calls = make_box()
    looping = [tool_calls((f"c{i}", "count", {"year": 2025})) for i in range(3)]
    llm = FakeLLM([route(), *looping])  # четвёртого ответа нет — лишний вызов упадёт

    answer = await Agent(llm, box, max_steps=3).ask("Сколько?")

    assert answer.stop is AgentStop.STEP_LIMIT
    assert answer.steps == 3
    assert len(calls) == 3
    assert "3 шагов" in answer.text


async def test_timeout_stops_a_slow_tool() -> None:
    box, _ = make_box(delay_s=5)
    llm = FakeLLM([route(), tool_calls(("c1", "count", {"year": 2025}))])

    answer = await Agent(llm, box, timeout_s=0.2).ask("Сколько?")

    assert answer.stop is AgentStop.TIMEOUT
    assert answer.text.startswith("Не успел ответить")


@pytest.mark.parametrize(
    ("stop_reason", "expected"),
    [(StopReason.MAX_TOKENS, AgentStop.MAX_TOKENS), (StopReason.REFUSAL, AgentStop.REFUSAL)],
)
async def test_unfinished_answers_are_marked(stop_reason: StopReason, expected: AgentStop) -> None:
    box, _ = make_box()
    cut = LLMResponse(content=[TextBlock(text="В 2025 году")], stop_reason=stop_reason)
    llm = FakeLLM([route(), cut])

    answer = await Agent(llm, box).ask("Сколько?")

    assert answer.stop is expected
    assert answer.text.startswith("В 2025 году")


async def test_dialog_is_kept_for_evals() -> None:
    box, _ = make_box()
    llm = FakeLLM([route(), tool_calls(("c1", "count", {"year": 2025})), FakeLLM.text("42")])

    answer = await Agent(llm, box).ask("Сколько?")

    assert [m.role for m in answer.dialog] == ["user", "assistant", "user", "assistant"]
    assert answer.route is not None and answer.route.kind.value == "data"
