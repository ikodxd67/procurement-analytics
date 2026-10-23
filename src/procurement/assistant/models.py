"""Сообщения, инструменты и ответ модели — общий язык для всех провайдеров.

Форма повторяет то, что есть у всех крупных API: диалог из сообщений, каждое
сообщение — список блоков. Блок бывает текстом, просьбой модели вызвать
инструмент (``ToolUse``) или нашим ответом на эту просьбу (``ToolResult``).

Инструменты заложены уже сейчас, хотя нужны с этапа 2: если добавить их потом,
поменяется сигнатура ``LLMClient.complete`` и всё, что её вызывает.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator


class TextBlock(BaseModel):
    type: Literal["text"] = "text"
    text: str


class ToolUse(BaseModel):
    """Модель просит вызвать инструмент ``name`` с аргументами ``input``.

    ``id`` связывает просьбу с ответом: в ``ToolResult`` мы возвращаем тот же id,
    иначе модель не поймёт, к какому вызову относится результат.
    """

    type: Literal["tool_use"] = "tool_use"
    id: str
    name: str
    input: dict[str, Any]


class ToolResult(BaseModel):
    """Наш ответ на ``ToolUse``. Ошибку инструмента тоже отдаём модели текстом
    (``is_error=True``), чтобы она могла исправить запрос, а не упасть."""

    type: Literal["tool_result"] = "tool_result"
    tool_use_id: str
    content: str
    is_error: bool = False


class OpaqueBlock(BaseModel):
    """Блок, который понимает только провайдер: например, рассуждение модели
    (thinking) у Claude. Мы его не читаем, но обязаны вернуть в следующем
    запросе без изменений, иначе модель потеряет ход своих рассуждений между
    вызовами инструментов. ``data`` — блок ровно в том виде, как его отдал API.
    """

    type: Literal["opaque"] = "opaque"
    provider: str
    data: dict[str, Any]


Block = Annotated[TextBlock | ToolUse | ToolResult | OpaqueBlock, Field(discriminator="type")]


class Message(BaseModel):
    role: Literal["user", "assistant"]
    content: list[Block]

    @classmethod
    def user(cls, text: str) -> Message:
        return cls(role="user", content=[TextBlock(text=text)])


class ToolSpec(BaseModel):
    """Описание инструмента для модели: что он делает и какие аргументы ждёт.

    ``input_schema`` — JSON Schema аргументов. Модель читает ``description``
    как инструкцию, поэтому от его текста зависит, позовёт ли она инструмент.
    """

    name: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    description: str
    input_schema: dict[str, Any]


class StopReason(StrEnum):
    """Почему модель закончила ответ. Агенту важно различать все четыре."""

    END_TURN = "end_turn"
    """Ответила полностью."""

    TOOL_USE = "tool_use"
    """Ждёт результата инструмента, диалог надо продолжить."""

    MAX_TOKENS = "max_tokens"
    """Упёрлась в лимит длины, ответ обрезан."""

    REFUSAL = "refusal"
    """Отказалась отвечать (сработала защита провайдера)."""


class Usage(BaseModel):
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)


class LLMResponse(BaseModel):
    content: list[Block]
    stop_reason: StopReason
    usage: Usage = Usage(input_tokens=0, output_tokens=0)

    @model_validator(mode="after")
    def _tool_use_matches_stop_reason(self) -> LLMResponse:
        # Агент решает, продолжать ли цикл, по stop_reason. Если он скажет
        # «инструмент», а блока ToolUse нет (или наоборот), цикл либо зависнет,
        # либо молча потеряет вызов. Такой ответ лучше не пропустить вовсе.
        has_tool_use = any(isinstance(b, ToolUse) for b in self.content)
        if has_tool_use != (self.stop_reason is StopReason.TOOL_USE):
            msg = f"stop_reason={self.stop_reason.value}, а блоков ToolUse: {len(self.tool_uses)}"
            raise ValueError(msg)
        if any(isinstance(b, ToolResult) for b in self.content):
            msg = "ToolResult в ответе модели: его пишем мы, а не она"
            raise ValueError(msg)
        return self

    @property
    def text(self) -> str:
        return "".join(b.text for b in self.content if isinstance(b, TextBlock))

    @property
    def tool_uses(self) -> list[ToolUse]:
        return [b for b in self.content if isinstance(b, ToolUse)]

    def as_message(self) -> Message:
        """Ответ как сообщение ассистента — чтобы положить его обратно в диалог."""
        return Message(role="assistant", content=list(self.content))
