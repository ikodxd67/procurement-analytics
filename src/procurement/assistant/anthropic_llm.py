"""Клиент Claude API за протоколом ``LLMClient``.

Здесь и только здесь живёт SDK ``anthropic``: перевод наших блоков в формат
API и обратно, и перевод исключений SDK в наши. Выше этого модуля никто не
знает ни про SDK, ни про коды ответов.

Повторы не пишем сами: SDK уже повторяет 429, 5xx и обрывы сети с
экспоненциальной паузой (``max_retries``). Наша ошибка поднимается, только
когда повторы кончились.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import anthropic
from anthropic.types import Message as APIMessage

from procurement.assistant.errors import (
    LLMAuthError,
    LLMError,
    LLMRateLimitError,
    LLMRequestError,
    LLMResponseError,
    LLMUnavailableError,
)
from procurement.assistant.models import (
    Block,
    LLMResponse,
    Message,
    OpaqueBlock,
    StopReason,
    TextBlock,
    ToolResult,
    ToolSpec,
    ToolUse,
    Usage,
)
from procurement.config import LLMSettings
from procurement.logging import get_logger

log = get_logger(__name__)

PROVIDER = "anthropic"

# stop_reason API -> наш. Остальные значения (stop_sequence, pause_turn) бывают
# только с функциями, которых мы не включаем: стоп-последовательности и
# серверные инструменты. Встретили — значит, что-то включили по ошибке.
_STOP_REASONS = {
    "end_turn": StopReason.END_TURN,
    "tool_use": StopReason.TOOL_USE,
    "max_tokens": StopReason.MAX_TOKENS,
    "refusal": StopReason.REFUSAL,
}


class AnthropicLLM:
    def __init__(
        self, settings: LLMSettings, *, client: anthropic.AsyncAnthropic | None = None
    ) -> None:
        self._settings = settings
        # client можно подменить в тестах; в бою создаём свой с ключом из
        # настроек. Ключ передаём явно, чтобы SDK не подхватил чужой
        # ANTHROPIC_API_KEY из окружения машины.
        self._client = client or anthropic.AsyncAnthropic(
            api_key=settings.api_key.get_secret_value(),
            timeout=settings.request_timeout_s,
            max_retries=settings.max_retries,
        )

    async def complete(
        self,
        messages: Sequence[Message],
        *,
        system: str | None = None,
        tools: Sequence[ToolSpec] = (),
        max_tokens: int | None = None,
        output_schema: dict[str, Any] | None = None,
    ) -> LLMResponse:
        params: dict[str, Any] = {
            "model": self._settings.model,
            "max_tokens": max_tokens or self._settings.max_tokens,
            "messages": [to_api_message(m) for m in messages],
        }
        if system:
            params["system"] = system
        if tools:
            params["tools"] = [to_api_tool(t) for t in tools]
        if output_schema is not None:
            params["output_config"] = {
                "format": {"type": "json_schema", "schema": to_api_schema(output_schema)}
            }

        try:
            raw = await self._client.messages.create(**params)
        except anthropic.AnthropicError as exc:
            raise map_error(exc) from exc

        response = from_api_message(raw)
        log.info(
            "llm.complete",
            model=raw.model,
            stop_reason=response.stop_reason.value,
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
            tool_uses=[u.name for u in response.tool_uses],
        )
        return response

    async def aclose(self) -> None:
        await self._client.close()


# --- перевод форматов -------------------------------------------------------


def to_api_block(block: Block) -> dict[str, Any]:
    match block:
        case TextBlock():
            return {"type": "text", "text": block.text}
        case ToolUse():
            return {"type": "tool_use", "id": block.id, "name": block.name, "input": block.input}
        case ToolResult():
            return {
                "type": "tool_result",
                "tool_use_id": block.tool_use_id,
                "content": block.content,
                "is_error": block.is_error,
            }
        case OpaqueBlock():
            if block.provider != PROVIDER:
                # Рассуждение другой модели Claude не прочтёт: формат и
                # подпись у каждого провайдера свои.
                msg = f"блок провайдера {block.provider!r} нельзя отправить в {PROVIDER}"
                raise LLMRequestError(msg)
            return dict(block.data)


def to_api_message(message: Message) -> dict[str, Any]:
    return {"role": message.role, "content": [to_api_block(b) for b in message.content]}


# Ограничения JSON Schema, которых structured output у Claude не поддерживает
# (документация Claude API, раздел JSON Schema Limitations). Их вырезаем из
# схемы для API, а проверяем у себя — pydantic-моделью ответа.
_UNSUPPORTED_SCHEMA_KEYS = frozenset(
    {
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
    }
)


def to_api_schema(schema: Any) -> Any:
    if isinstance(schema, dict):
        return {k: to_api_schema(v) for k, v in schema.items() if k not in _UNSUPPORTED_SCHEMA_KEYS}
    if isinstance(schema, list):
        return [to_api_schema(v) for v in schema]
    return schema


def to_api_tool(tool: ToolSpec) -> dict[str, Any]:
    return {"name": tool.name, "description": tool.description, "input_schema": tool.input_schema}


def from_api_message(raw: APIMessage) -> LLMResponse:
    if raw.stop_reason == "model_context_window_exceeded":
        # Диалог не влез в контекст модели. Продолжать нечем, и повтор
        # того же запроса упадёт так же — это ошибка запроса.
        msg = "диалог не помещается в контекст модели"
        raise LLMRequestError(msg)
    stop_reason = _STOP_REASONS.get(raw.stop_reason or "")
    if stop_reason is None:
        msg = f"неожиданный stop_reason={raw.stop_reason!r}"
        raise LLMResponseError(msg)

    content: list[Block] = []
    for block in raw.content:
        if block.type == "text":
            content.append(TextBlock(text=block.text))
        elif block.type == "tool_use":
            content.append(ToolUse(id=block.id, name=block.name, input=dict(block.input)))
        else:
            # thinking, redacted_thinking и всё, что появится потом: храним
            # как есть, чтобы вернуть модели в следующем запросе.
            content.append(OpaqueBlock(provider=PROVIDER, data=block.to_dict()))

    try:
        return LLMResponse(
            content=content,
            stop_reason=stop_reason,
            usage=Usage(input_tokens=raw.usage.input_tokens, output_tokens=raw.usage.output_tokens),
        )
    except ValueError as exc:
        raise LLMResponseError(str(exc)) from exc


def map_error(exc: anthropic.AnthropicError) -> LLMError:
    """Исключение SDK -> наше. Порядок важен: от частного к общему.

    Проверено на anthropic 2.1.0: OverloadedError (529) — наследник
    APIStatusError, а не InternalServerError, поэтому «сервер не справился»
    определяем по коду ответа, а не по классу.
    """
    text = str(exc)
    if isinstance(exc, anthropic.AuthenticationError | anthropic.PermissionDeniedError):
        return LLMAuthError(text)
    if isinstance(exc, anthropic.RateLimitError):
        return LLMRateLimitError(text)
    if isinstance(exc, anthropic.APIStatusError):
        if exc.status_code >= 500:
            return LLMUnavailableError(text)
        return LLMRequestError(text)
    # APITimeoutError — наследник APIConnectionError, его ловит эта же ветка.
    if isinstance(exc, anthropic.APIConnectionError):
        return LLMUnavailableError(text)
    if isinstance(exc, anthropic.APIResponseValidationError):
        return LLMResponseError(text)
    return LLMError(text)
