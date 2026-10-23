"""Протокол клиента модели.

Как ``ingest.source.Source``: узкий, один метод запроса и закрытие. Агент,
поиск и evals знают только его. Настоящий клиент — в ``assistant.anthropic_llm``,
поддельный — в ``assistant.fake``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from procurement.assistant.models import LLMResponse, Message, ToolSpec
from procurement.config import LLMKind, LLMSettings


class LLMClient(Protocol):
    async def complete(
        self,
        messages: Sequence[Message],
        *,
        system: str | None = None,
        tools: Sequence[ToolSpec] = (),
        max_tokens: int | None = None,
        output_schema: dict[str, Any] | None = None,
    ) -> LLMResponse:
        """Один ход модели по всему диалогу.

        Модель ничего не помнит между вызовами: ``messages`` каждый раз —
        весь диалог целиком. ``max_tokens=None`` — значение из настроек.
        ``output_schema`` — JSON Schema ответа (structured output): провайдер
        гарантирует, что текст ответа — JSON по этой схеме.
        """
        ...

    async def aclose(self) -> None: ...


def build_llm_client(settings: LLMSettings) -> LLMClient:
    """Клиент по настройкам — для приложения и CLI.

    Поддельную модель здесь не собрать: ей нужен сценарий ответов, и он есть
    только у теста. Поэтому kind=fake вне тестов — ошибка с подсказкой.
    """
    if settings.kind is LLMKind.ANTHROPIC:
        # Импорт внутри: модулю протокола не нужен SDK провайдера.
        from procurement.assistant.anthropic_llm import AnthropicLLM

        return AnthropicLLM(settings)
    msg = (
        "kind=fake годится только для тестов; для работы задай "
        "PA_ASSISTANT__LLM__KIND=anthropic, ключ и модель"
    )
    raise ValueError(msg)
