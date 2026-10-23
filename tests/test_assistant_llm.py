"""Слой модели ассистента: настройки, проверка ответа и поддельная модель."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from procurement.assistant.errors import (
    LLMAuthError,
    LLMError,
    LLMRateLimitError,
    LLMRequestError,
    LLMResponseError,
    LLMUnavailableError,
)
from procurement.assistant.fake import FakeLLM, FakeScriptError
from procurement.assistant.llm import LLMClient
from procurement.assistant.models import (
    LLMResponse,
    Message,
    StopReason,
    TextBlock,
    ToolResult,
    ToolSpec,
    ToolUse,
)
from procurement.config import LLMKind, LLMSettings, Settings

SQL_TOOL = ToolSpec(
    name="run_sql",
    description="Выполнить SELECT в ClickHouse",
    input_schema={"type": "object", "properties": {"query": {"type": "string"}}},
)


# --- настройки -----------------------------------------------------------


def test_default_llm_is_fake_and_needs_no_key() -> None:
    settings = Settings(_env_file=None)

    assert settings.assistant.llm.kind is LLMKind.FAKE
    assert settings.assistant.llm.api_key.get_secret_value() == ""


def test_llm_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PA_ASSISTANT__LLM__KIND", "anthropic")
    monkeypatch.setenv("PA_ASSISTANT__LLM__API_KEY", "sk-test-secret")
    monkeypatch.setenv("PA_ASSISTANT__LLM__MODEL", "some-model")
    monkeypatch.setenv("PA_ASSISTANT__LLM__MAX_TOKENS", "256")

    settings = Settings(_env_file=None)

    assert settings.assistant.llm.kind is LLMKind.ANTHROPIC
    assert settings.assistant.llm.model == "some-model"
    assert settings.assistant.llm.max_tokens == 256
    assert "sk-test-secret" not in repr(settings)


@pytest.mark.parametrize(
    ("api_key", "model", "missing"),
    [("", "some-model", "api_key"), ("sk-test", "", "model"), ("", "", "api_key, model")],
)
def test_real_llm_fails_at_start_without_key_or_model(
    api_key: str, model: str, missing: str
) -> None:
    with pytest.raises(ValidationError, match=missing):
        LLMSettings.model_validate({"kind": "anthropic", "api_key": api_key, "model": model})


# --- ошибки --------------------------------------------------------------


@pytest.mark.parametrize(
    ("error", "retryable"),
    [
        (LLMAuthError, False),
        (LLMRequestError, False),
        (LLMResponseError, False),
        (LLMRateLimitError, True),
        (LLMUnavailableError, True),
    ],
)
def test_only_transient_errors_are_retryable(error: type[LLMError], retryable: bool) -> None:
    assert issubclass(error, LLMError)
    assert error.retryable is retryable


# --- ответ модели --------------------------------------------------------


def test_response_text_joins_text_blocks_only() -> None:
    response = LLMResponse(
        content=[TextBlock(text="Топ-5 "), TextBlock(text="заказчиков")],
        stop_reason=StopReason.END_TURN,
    )

    assert response.text == "Топ-5 заказчиков"
    assert response.tool_uses == []


def test_tool_use_without_stop_reason_is_rejected() -> None:
    # Агент решает продолжать цикл по stop_reason: такой ответ потерял бы вызов.
    with pytest.raises(ValidationError, match="stop_reason=end_turn"):
        LLMResponse(
            content=[ToolUse(id="c1", name="run_sql", input={})],
            stop_reason=StopReason.END_TURN,
        )


def test_tool_stop_reason_without_tool_use_is_rejected() -> None:
    with pytest.raises(ValidationError, match="stop_reason=tool_use"):
        LLMResponse(content=[TextBlock(text="сейчас посчитаю")], stop_reason=StopReason.TOOL_USE)


def test_tool_result_in_model_response_is_rejected() -> None:
    with pytest.raises(ValidationError, match="ToolResult"):
        LLMResponse(
            content=[ToolResult(tool_use_id="c1", content="42")],
            stop_reason=StopReason.END_TURN,
        )


def test_blocks_survive_json_round_trip() -> None:
    # Диалоги будем писать в журнал и в отчёты evals: тип блока должен
    # восстанавливаться по полю type, а не теряться при разборе.
    message = Message(
        role="user",
        content=[TextBlock(text="вот результат"), ToolResult(tool_use_id="c1", content="42")],
    )

    restored = Message.model_validate_json(message.model_dump_json())

    assert restored == message
    assert isinstance(restored.content[1], ToolResult)


def test_tool_name_must_be_api_safe() -> None:
    with pytest.raises(ValidationError):
        ToolSpec(name="выполнить sql", description="", input_schema={})


# --- поддельная модель ---------------------------------------------------


def test_fake_satisfies_protocol() -> None:
    client: LLMClient = FakeLLM([])
    assert client is not None


async def test_fake_answers_in_script_order() -> None:
    llm = FakeLLM([FakeLLM.text("первый"), FakeLLM.text("второй")])

    first = await llm.complete([Message.user("вопрос")])
    second = await llm.complete([Message.user("ещё вопрос")])

    assert (first.text, second.text) == ("первый", "второй")
    assert llm.remaining == 0


async def test_fake_records_what_was_sent() -> None:
    llm = FakeLLM([FakeLLM.text("ок")])

    await llm.complete(
        [Message.user("сколько договоров?")], system="ты аналитик", tools=[SQL_TOOL], max_tokens=64
    )

    (call,) = llm.calls
    assert call.system == "ты аналитик"
    assert [t.name for t in call.tools] == ["run_sql"]
    assert call.max_tokens == 64
    assert call.messages[0].content == [TextBlock(text="сколько договоров?")]


async def test_fake_keeps_snapshot_of_dialog() -> None:
    # Агент дописывает в тот же список. Запись первого вызова не должна меняться.
    llm = FakeLLM([FakeLLM.tool_call("run_sql", {"query": "SELECT 1"}), FakeLLM.text("1")])
    dialog = [Message.user("посчитай")]

    response = await llm.complete(dialog, tools=[SQL_TOOL])
    dialog.append(response.as_message())
    dialog.append(Message(role="user", content=[ToolResult(tool_use_id="call_1", content="1")]))
    await llm.complete(dialog, tools=[SQL_TOOL])

    assert len(llm.calls[0].messages) == 1
    assert len(llm.calls[1].messages) == 3


async def test_fake_raises_scripted_error() -> None:
    llm = FakeLLM([LLMRateLimitError("429"), FakeLLM.text("ок")])

    with pytest.raises(LLMRateLimitError):
        await llm.complete([Message.user("вопрос")])
    assert (await llm.complete([Message.user("вопрос")])).text == "ок"


async def test_fake_fails_loudly_when_script_ends() -> None:
    llm = FakeLLM([FakeLLM.text("единственный")])
    await llm.complete([Message.user("раз")])

    with pytest.raises(FakeScriptError, match="вызов №2"):
        await llm.complete([Message.user("два")])


async def test_fake_script_error_is_not_llm_error() -> None:
    # Иначе обработчик ошибок модели в агенте проглотил бы сломанный тест.
    llm = FakeLLM([])

    with pytest.raises(FakeScriptError) as exc_info:
        await llm.complete([Message.user("вопрос")])
    assert not isinstance(exc_info.value, LLMError)


async def test_fake_refuses_tool_that_was_not_offered() -> None:
    llm = FakeLLM([FakeLLM.tool_call("drop_table", {})])

    with pytest.raises(FakeScriptError, match="drop_table"):
        await llm.complete([Message.user("удали всё")], tools=[SQL_TOOL])


async def test_fake_tool_call_is_valid_response() -> None:
    llm = FakeLLM([FakeLLM.tool_call("run_sql", {"query": "SELECT 1"}, id_="c7")])

    response = await llm.complete([Message.user("посчитай")], tools=[SQL_TOOL])

    assert response.stop_reason is StopReason.TOOL_USE
    (use,) = response.tool_uses
    assert (use.id, use.name, use.input) == ("c7", "run_sql", {"query": "SELECT 1"})


async def test_fake_close() -> None:
    llm = FakeLLM([])
    await llm.aclose()
    assert llm.closed
