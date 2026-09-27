"""Модели данных загрузчика.

Записи источника внутрь не разбираем: на первом этапе задача — доставить их
без потерь. Разбор полей по схеме придёт на этапе заливки в хранилища, когда
станет понятно, какие поля вообще нужны. Разбирать раньше времени — значит
терять данные, о существовании которых мы ещё не знаем.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

# Запись как её отдал источник. Ключи заранее не фиксируем.
RawRecord = dict[str, Any]


def _utcnow() -> datetime:
    return datetime.now(UTC)


class PageKind(StrEnum):
    """Три исхода одного запроса страницы, которые нельзя путать."""

    DATA = "data"
    """Есть записи и есть куда идти дальше."""

    EMPTY = "empty"
    """Записей нет, но курсор следующей страницы источник дал.

    Это не конец выдачи. Останавливаться тут — молча потерять хвост данных.
    """

    END = "end"
    """Курсора больше нет. Выдача кончилась."""


class Page(BaseModel):
    """Одна страница ответа источника."""

    entity: str
    items: list[RawRecord] = Field(default_factory=list)
    next_cursor: str | None = None
    total: int | None = None
    """Сколько записей всего, по мнению источника. Нужно для сверки на этапе 3."""
    fetched_at: datetime = Field(default_factory=_utcnow)

    @property
    def kind(self) -> PageKind:
        if self.next_cursor is None:
            return PageKind.END
        return PageKind.DATA if self.items else PageKind.EMPTY


class Batch(BaseModel):
    """Порция записей, уходящая по конвейеру от загрузки к обработке."""

    entity: str
    records: list[RawRecord]
    cursor_after: str | None
    """Курсор, который станет действительным ТОЛЬКО после успешной обработки батча.

    Хранить его здесь, а не в общей переменной, — способ не сохранить позицию
    раньше времени. Сохраняем после обработки, иначе при падении между
    получением и обработкой страница потеряется.
    """
    page_index: int
    is_final: bool = False


class CursorState(BaseModel):
    """Сохраняемая позиция по одной сущности. Переживает перезапуск."""

    entity: str
    cursor: str | None = None
    pages_done: int = 0
    records_done: int = 0
    finished: bool = False
    updated_at: datetime = Field(default_factory=_utcnow)

    @classmethod
    def fresh(cls, entity: str) -> CursorState:
        return cls(entity=entity)
