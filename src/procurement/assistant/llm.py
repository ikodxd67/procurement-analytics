"""Протокол клиента модели.

Как ``ingest.source.Source``: узкий, один метод запроса и закрытие. Агент,
поиск и evals знают только его. Настоящий клиент Claude API появится на
этапе 1, поддельный — в ``assistant.fake``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from procurement.assistant.models import LLMResponse, Message, ToolSpec


class LLMClient(Protocol):
    async def complete(
        self,
        messages: Sequence[Message],
        *,
        system: str | None = None,
        tools: Sequence[ToolSpec] = (),
        max_tokens: int | None = None,
    ) -> LLMResponse:
        """Один ход модели по всему диалогу.

        Модель ничего не помнит между вызовами: ``messages`` каждый раз —
        весь диалог целиком. ``max_tokens=None`` — значение из настроек.
        """
        ...

    async def aclose(self) -> None: ...
