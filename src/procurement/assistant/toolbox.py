"""Сборка набора инструментов агента из настроек."""

from __future__ import annotations

from clickhouse_connect.driver import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from procurement.api.queries import Marts
from procurement.assistant.rag.embeddings import Embedder
from procurement.assistant.rag.tool import build_docs_tool
from procurement.assistant.slices import build_slice_tools
from procurement.assistant.sql_tool import build_sql_tool
from procurement.assistant.tools import ToolBox
from procurement.config import Settings
from procurement.storage.clickhouse.client import build_client


async def build_readonly_client(settings: Settings) -> AsyncClient:
    """Клиент ClickHouse под пользователем агента, а не под администратором."""
    ro = settings.clickhouse.model_copy(
        update={"user": settings.assistant.sql.user, "password": settings.assistant.sql.password}
    )
    return await build_client(ro)


def build_toolbox(
    *,
    marts: Marts,
    readonly: AsyncClient,
    sessions: async_sessionmaker[AsyncSession],
    embedder: Embedder,
) -> ToolBox:
    # Срезы ходят обычным клиентом API: их запросы написаны нами и проверены.
    # run_sql — только клиентом только для чтения: его запрос пишет модель.
    return ToolBox(
        [
            *build_slice_tools(marts),
            build_sql_tool(readonly),
            build_docs_tool(sessions, embedder),
        ]
    )
