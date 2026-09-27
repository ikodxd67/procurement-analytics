"""Общая оснастка тестов."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from procurement.ingest.errors import SourceRequestError
from procurement.ingest.models import Page

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "goszakup"


@pytest.fixture
def fixtures_root() -> Path:
    return FIXTURES_ROOT


@pytest.fixture
def slept(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Убирает настоящие паузы между повторами и записывает запрошенные.

    Иначе тест на пять попыток с экспоненциальной паузой шёл бы полминуты и
    проверял бы терпение вместо расчёта.
    """
    recorded: list[float] = []

    async def fake_sleep(delay: float, *args: Any, **kwargs: Any) -> None:
        recorded.append(delay)

    monkeypatch.setattr("procurement.ingest.retry.asyncio.sleep", fake_sleep)
    return recorded


class StubSource:
    """Источник с предсказуемой выдачей.

    Курсор здесь — номер страницы, которую надо отдать. Так в тестах можно
    прямо утверждать, чему обязан быть равен сохранённый курсор, а не гадать.
    """

    def __init__(
        self,
        *,
        pages: int,
        per_page: int = 3,
        latency_s: float = 0.0,
        empty_pages: frozenset[int] = frozenset(),
        stuck_cursor: bool = False,
    ) -> None:
        self.pages = pages
        self.per_page = per_page
        self.latency_s = latency_s
        self.empty_pages = empty_pages
        self.stuck_cursor = stuck_cursor
        self.requested: list[str | None] = []
        self.closed = False

    async def fetch_page(self, entity: str, cursor: str | None) -> Page:
        self.requested.append(cursor)
        page_no = 0 if cursor is None else int(cursor)
        if page_no >= self.pages:
            msg = f"страницы {page_no} не существует"
            raise SourceRequestError(msg)

        if self.latency_s:
            await asyncio.sleep(self.latency_s)

        if page_no in self.empty_pages:
            items: list[dict[str, Any]] = []
        else:
            first = page_no * self.per_page
            items = [{"id": first + k, "page": page_no} for k in range(self.per_page)]

        if self.stuck_cursor:
            # Сломанный источник: всегда указывает на одну и ту же страницу.
            next_cursor: str | None = "1"
        else:
            next_cursor = None if page_no == self.pages - 1 else str(page_no + 1)

        return Page(
            entity=entity,
            items=items,
            next_cursor=next_cursor,
            total=self.pages * self.per_page,
        )

    async def aclose(self) -> None:
        self.closed = True


class RecordingHandler:
    """Обработчик батчей, который всё запоминает."""

    def __init__(self, *, latency_s: float = 0.0) -> None:
        self.latency_s = latency_s
        self.ids: list[int] = []
        self.pages: list[int] = []
        self.batches = 0

    async def __call__(self, batch: Any) -> None:
        if self.latency_s:
            await asyncio.sleep(self.latency_s)
        self.batches += 1
        self.pages.append(batch.page_index)
        self.ids.extend(record["id"] for record in batch.records)
