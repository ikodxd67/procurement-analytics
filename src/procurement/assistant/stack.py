"""Сборка ассистента целиком: модель, базы, инструменты — и их закрытие.

Одна сборка на всех, кто запускает агента: CLI вопроса, прогон evals и ручка
API (ей — ``open_agent`` поверх подключений приложения). AsyncExitStack: если
упадёт третье подключение, первые два всё равно закроются — в обратном порядке.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

from clickhouse_connect.driver import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from procurement.api.queries import Marts
from procurement.assistant.agent import Agent
from procurement.assistant.llm import build_llm_client
from procurement.assistant.rag.embeddings import Embedder, build_embedder
from procurement.assistant.toolbox import build_readonly_client, build_toolbox
from procurement.config import Settings
from procurement.storage.clickhouse.client import build_client
from procurement.storage.postgres.engine import build_engine, build_session_factory
from procurement.storage.postgres.models import EMBEDDING_DIM


@dataclass(frozen=True, slots=True)
class Assistant:
    agent: Agent
    readonly: AsyncClient
    """Клиент под пользователем агента — им же считается эталонный SQL в evals."""
    embedder: Embedder
    """Чтобы API мог загрузить модель эмбеддингов при старте, а не на первом вопросе."""


@asynccontextmanager
async def open_agent(
    settings: Settings,
    *,
    clickhouse: AsyncClient,
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncIterator[Assistant]:
    """Агент поверх уже открытых подключений — так его собирает API.

    У приложения свои клиент ClickHouse и пул Postgres на всё время жизни;
    второй комплект ради агента — лишние соединения к тем же базам.
    """
    async with AsyncExitStack() as stack:
        llm = build_llm_client(settings.assistant.llm)
        stack.push_async_callback(llm.aclose)
        readonly = await build_readonly_client(settings)
        stack.push_async_callback(readonly.close)
        embedder = build_embedder(settings.assistant.rag, dim=EMBEDDING_DIM)

        toolbox = build_toolbox(
            marts=Marts(clickhouse),
            readonly=readonly,
            sessions=sessions,
            embedder=embedder,
        )
        yield Assistant(agent=Agent(llm, toolbox), readonly=readonly, embedder=embedder)


@asynccontextmanager
async def open_assistant(settings: Settings) -> AsyncIterator[Assistant]:
    """Агент со своими подключениями — для CLI и evals."""
    async with AsyncExitStack() as stack:
        clickhouse = await build_client(settings.clickhouse)
        stack.push_async_callback(clickhouse.close)
        engine = build_engine(settings.postgres)
        stack.push_async_callback(engine.dispose)
        yield await stack.enter_async_context(
            open_agent(settings, clickhouse=clickhouse, sessions=build_session_factory(engine))
        )
