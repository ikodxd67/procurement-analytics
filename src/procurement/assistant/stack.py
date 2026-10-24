"""Сборка ассистента целиком: модель, базы, инструменты — и их закрытие.

Один контекстный менеджер на всех, кто запускает агента: CLI вопроса, прогон
evals, потом ручка API. AsyncExitStack: если упадёт третье подключение, первые
два всё равно закроются — в обратном порядке.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

from clickhouse_connect.driver import AsyncClient

from procurement.api.queries import Marts
from procurement.assistant.agent import Agent
from procurement.assistant.llm import build_llm_client
from procurement.assistant.rag.embeddings import build_embedder
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


@asynccontextmanager
async def open_assistant(settings: Settings) -> AsyncIterator[Assistant]:
    async with AsyncExitStack() as stack:
        llm = build_llm_client(settings.assistant.llm)
        stack.push_async_callback(llm.aclose)
        clickhouse = await build_client(settings.clickhouse)
        stack.push_async_callback(clickhouse.close)
        readonly = await build_readonly_client(settings)
        stack.push_async_callback(readonly.close)
        engine = build_engine(settings.postgres)
        stack.push_async_callback(engine.dispose)

        toolbox = build_toolbox(
            marts=Marts(clickhouse),
            readonly=readonly,
            sessions=build_session_factory(engine),
            embedder=build_embedder(settings.assistant.rag, dim=EMBEDDING_DIM),
        )
        yield Assistant(agent=Agent(llm, toolbox), readonly=readonly)
