"""Инструмент агента ``search_docs``: найти пункты правил и описание данных."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from procurement.assistant.rag.embeddings import Embedder
from procurement.assistant.rag.search import hybrid_search
from procurement.assistant.tools import Tool

MAX_HITS = 8

DESCRIPTION = """\
Найти в документах ответ на вопрос «что это / как устроено / что требуется»: \
Правила осуществления государственных закупок РК (приказ МФ № 687 от \
09.10.2024, действующая редакция с adilet.zan.kz) и описание данных и \
показателей этой системы (таблицы, доля поставщика, индекс Херфиндаля и т. д.). \
Возвращает куски с заголовком «Глава › Параграф › пункт» и ссылкой. Отвечай \
только по найденному и называй пункт со ссылкой; пустой результат значит \
«в документах этого нет» — так и скажи, не додумывай. Цифр по закупкам здесь \
нет — для них срезы и run_sql.\
"""


class SearchDocsArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(
        min_length=2,
        max_length=500,
        description="Вопрос или ключевые слова по-русски, например «демпинговая цена в аукционе»",
    )
    limit: int = Field(default=4, ge=1, le=MAX_HITS, description="Сколько кусков вернуть")


def build_docs_tool(
    sessions: async_sessionmaker[AsyncSession], embedder: Embedder
) -> Tool[SearchDocsArgs]:
    async def run(args: SearchDocsArgs) -> list[dict[str, Any]]:
        hits = await hybrid_search(sessions, embedder, args.query, limit=args.limit)
        return [
            {"title": h.title, "url": h.url, "text": h.text, "score": round(h.score, 4)}
            for h in hits
        ]

    return Tool("search_docs", DESCRIPTION, SearchDocsArgs, run)
