"""Клиент Claude API: перевод форматов и ошибок. Без сети — SDK подменён заглушкой.

Живой вызов — только в тесте с меткой live, и только если задан ключ.
"""

from __future__ import annotations

import os
from typing import Any, cast

import anthropic
import httpx2
import pytest
from anthropic.types import Message as APIMessage

from procurement.assistant.anthropic_llm import (
    AnthropicLLM,
    from_api_message,
    map_error,
    to_api_message,
    to_api_schema,
)
from procurement.assistant.errors import (
    LLMAuthError,
    LLMError,
    LLMRateLimitError,
    LLMRequestError,
    LLMResponseError,
    LLMUnavailableError,
)
from procurement.assistant.llm import build_llm_client
from procurement.assistant.models import (
    Message,
    OpaqueBlock,
    StopReason,
    ToolResult,
    ToolSpec,
    ToolUse,
)
from procurement.assistant.router import QuestionKind, route_question
from procurement.config import LLMKind, LLMSettings

SETTINGS = LLMSettings(
    kind=LLMKind.ANTHROPIC, api_key="sk-test", model="test-model", max_tokens=500
)
SQL_TOOL = ToolSpec(
    name="run_sql", description="SELECT в ClickHouse", input_schema={"type": "object"}
)
THINKING = {"type": "thinking", "thinking": "", "signature": "sig-abc"}


def api_message(content: list[dict[str, Any]], stop_reason: str = "end_turn") -> APIMessage:
    return APIMessage.model_validate(
        {
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "test-model",
            "content": content,
            "stop_reason": stop_reason,
            "stop_sequence": None,
            "usage": {"input_tokens": 12, "output_tokens": 5},
        }
    )


class StubMessages:
    def __init__(self, result: APIMessage | Exception) -> None:
        self.result = result
        self.params: dict[str, Any] = {}

    async def create(self, **params: Any) -> APIMessage:
        self.params = params
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class StubClient:
    def __init__(self, result: APIMessage | Exception) -> None:
        self.messages = StubMessages(result)
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def make_llm(result: APIMessage | Exception) -> tuple[AnthropicLLM, StubClient]:
    stub = StubClient(result)
    return AnthropicLLM(SETTINGS, client=cast(anthropic.AsyncAnthropic, stub)), stub


def status_error(cls: type[anthropic.APIStatusError], code: int) -> anthropic.APIStatusError:
    request = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
    return cls("ошибка", response=httpx2.Response(code, request=request), body=None)


# --- запрос ---------------------------------------------------------------


async def test_request_carries_model_limits_tools_and_schema() -> None:
    llm, stub = make_llm(api_message([{"type": "text", "text": "{}"}]))

    await llm.complete(
        [Message.user("вопрос")],
        system="ты маршрутизатор",
        tools=[SQL_TOOL],
        output_schema={"type": "object", "additionalProperties": False},
    )

    params = stub.messages.params
    assert params["model"] == "test-model"
    assert params["max_tokens"] == 500
    assert params["system"] == "ты маршрутизатор"
    assert params["tools"] == [
        {
            "name": "run_sql",
            "description": "SELECT в ClickHouse",
            "input_schema": {"type": "object"},
        }
    ]
    assert params["output_config"]["format"]["type"] == "json_schema"
    # temperature с 2026-10-21 даёт 400 на любой модели — его не шлём вовсе.
    assert "temperature" not in params


async def test_optional_params_are_not_sent_when_empty() -> None:
    llm, stub = make_llm(api_message([{"type": "text", "text": "ок"}]))

    await llm.complete([Message.user("вопрос")], max_tokens=64)

    assert stub.messages.params["max_tokens"] == 64
    assert {"system", "tools", "output_config"}.isdisjoint(stub.messages.params)


def test_schema_loses_constraints_api_does_not_support() -> None:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": {
            "year": {
                "anyOf": [{"type": "integer", "minimum": 2000, "maximum": 2100}, {"type": "null"}]
            },
            "reason": {"type": "string", "maxLength": 200, "description": "почему"},
        },
        "additionalProperties": False,
    }

    api = to_api_schema(schema)

    assert api["properties"]["year"]["anyOf"][0] == {"type": "integer"}
    assert api["properties"]["reason"] == {"type": "string", "description": "почему"}
    assert api["additionalProperties"] is False
    assert schema["properties"]["reason"]["maxLength"] == 200  # исходная схема не тронута


def test_dialog_with_tools_is_translated() -> None:
    assistant = Message(
        role="assistant",
        content=[
            OpaqueBlock(provider="anthropic", data=THINKING),
            ToolUse(id="t1", name="run_sql", input={"query": "SELECT 1"}),
        ],
    )
    tool_result = Message(
        role="user", content=[ToolResult(tool_use_id="t1", content="ошибка", is_error=True)]
    )

    assert to_api_message(assistant)["content"] == [
        THINKING,
        {"type": "tool_use", "id": "t1", "name": "run_sql", "input": {"query": "SELECT 1"}},
    ]
    assert to_api_message(tool_result)["content"] == [
        {"type": "tool_result", "tool_use_id": "t1", "content": "ошибка", "is_error": True}
    ]


def test_foreign_provider_block_is_refused() -> None:
    message = Message(role="assistant", content=[OpaqueBlock(provider="other", data={"x": 1})])

    with pytest.raises(LLMRequestError, match="other"):
        to_api_message(message)


# --- ответ ----------------------------------------------------------------


def test_text_response() -> None:
    response = from_api_message(api_message([{"type": "text", "text": "Готово"}]))

    assert response.text == "Готово"
    assert response.stop_reason is StopReason.END_TURN
    assert (response.usage.input_tokens, response.usage.output_tokens) == (12, 5)


def test_thinking_survives_round_trip() -> None:
    # Рассуждение надо вернуть модели в следующем ходе байт в байт.
    raw = api_message(
        [
            THINKING,
            {"type": "tool_use", "id": "t1", "name": "run_sql", "input": {"query": "SELECT 1"}},
        ],
        stop_reason="tool_use",
    )

    response = from_api_message(raw)
    replayed = to_api_message(response.as_message())

    assert response.stop_reason is StopReason.TOOL_USE
    assert replayed["content"][0] == THINKING
    assert response.tool_uses[0].input == {"query": "SELECT 1"}


def test_refusal_is_kept_as_stop_reason() -> None:
    response = from_api_message(api_message([], stop_reason="refusal"))

    assert response.stop_reason is StopReason.REFUSAL


def test_context_overflow_is_request_error() -> None:
    with pytest.raises(LLMRequestError, match="контекст"):
        from_api_message(api_message([], stop_reason="model_context_window_exceeded"))


@pytest.mark.parametrize("stop_reason", ["pause_turn", "stop_sequence"])
def test_unused_stop_reasons_are_response_errors(stop_reason: str) -> None:
    with pytest.raises(LLMResponseError, match=stop_reason):
        from_api_message(api_message([{"type": "text", "text": "..."}], stop_reason=stop_reason))


def test_inconsistent_response_becomes_response_error() -> None:
    with pytest.raises(LLMResponseError):
        from_api_message(api_message([{"type": "text", "text": "сейчас"}], stop_reason="tool_use"))


# --- ошибки ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (status_error(anthropic.AuthenticationError, 401), LLMAuthError),
        (status_error(anthropic.PermissionDeniedError, 403), LLMAuthError),
        (status_error(anthropic.RateLimitError, 429), LLMRateLimitError),
        (status_error(anthropic.InternalServerError, 500), LLMUnavailableError),
        # 529: проверено, OverloadedError — не наследник InternalServerError.
        (status_error(anthropic.OverloadedError, 529), LLMUnavailableError),
        (status_error(anthropic.BadRequestError, 400), LLMRequestError),
        (status_error(anthropic.NotFoundError, 404), LLMRequestError),
        (
            anthropic.APITimeoutError(
                request=httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
            ),
            LLMUnavailableError,
        ),
        (
            anthropic.APIConnectionError(
                request=httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
            ),
            LLMUnavailableError,
        ),
    ],
)
def test_sdk_errors_are_mapped(exc: anthropic.AnthropicError, expected: type[LLMError]) -> None:
    mapped = map_error(exc)

    assert type(mapped) is expected


async def test_complete_raises_our_error_with_cause() -> None:
    sdk_error = status_error(anthropic.RateLimitError, 429)
    llm, _ = make_llm(sdk_error)

    with pytest.raises(LLMRateLimitError) as exc_info:
        await llm.complete([Message.user("вопрос")])
    assert exc_info.value.__cause__ is sdk_error


async def test_close_closes_sdk_client() -> None:
    llm, stub = make_llm(api_message([]))

    await llm.aclose()

    assert stub.closed


# --- фабрика --------------------------------------------------------------


def test_factory_builds_anthropic_client() -> None:
    assert isinstance(build_llm_client(SETTINGS), AnthropicLLM)


def test_factory_refuses_fake_outside_tests() -> None:
    with pytest.raises(ValueError, match="PA_ASSISTANT__LLM__KIND=anthropic"):
        build_llm_client(LLMSettings())


# --- живой вызов ----------------------------------------------------------


@pytest.mark.live
@pytest.mark.skipif(
    not (
        os.environ.get("PA_ASSISTANT__LLM__API_KEY") and os.environ.get("PA_ASSISTANT__LLM__MODEL")
    ),
    reason="нужны PA_ASSISTANT__LLM__API_KEY и PA_ASSISTANT__LLM__MODEL",
)
async def test_live_router() -> None:
    settings = LLMSettings(
        kind=LLMKind.ANTHROPIC,
        api_key=os.environ["PA_ASSISTANT__LLM__API_KEY"],
        model=os.environ["PA_ASSISTANT__LLM__MODEL"],
    )
    llm = AnthropicLLM(settings)
    try:
        route = await route_question(llm, "Топ-5 заказчиков по сумме договоров за 2025 год")
    finally:
        await llm.aclose()

    assert route.kind is QuestionKind.DATA
    assert (route.year_from, route.year_to) == (2025, 2025)
