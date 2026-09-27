"""Сборка зависимостей для заданий загрузки.

Отдельный модуль, чтобы DAG-и Airflow оставались тонкими. Вся логика живёт в
обычных функциях, которые запускаются и тестируются без оркестратора; DAG лишь
вызывает их. Иначе код нельзя ни прогнать локально, ни покрыть тестами, а
отладка превращается в чтение логов планировщика.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date

from clickhouse_connect.driver import AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from procurement.config import Settings, SourceKind, get_settings
from procurement.ingest.source import Source
from procurement.ingest.synthetic_source import SyntheticSource
from procurement.storage.clickhouse.client import build_client
from procurement.storage.clickhouse.ddl import apply_schema
from procurement.storage.postgres.engine import build_engine, build_session_factory

# Пока нет токена, месяц отдаёт синтетический источник. Когда токен появится,
# сюда встанет RestSource с фильтром по дате, а всё остальное не изменится —
# протокол Source один и тот же.
SourceFactory = Callable[[str, date], Source]

SYNTHETIC_RECORDS_PER_MONTH = 20_000


def default_source_factory(entity: str, month: date) -> Source:
    return SyntheticSource(month, records=SYNTHETIC_RECORDS_PER_MONTH, page_size=500)


@dataclass(slots=True)
class JobContext:
    settings: Settings
    clickhouse: AsyncClient
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    source_factory: SourceFactory


@asynccontextmanager
async def job_context(
    *,
    settings: Settings | None = None,
    source_factory: SourceFactory | None = None,
    ensure_schema: bool = True,
) -> AsyncIterator[JobContext]:
    resolved = settings or get_settings()
    client = await build_client(resolved.clickhouse)
    engine = build_engine(resolved.postgres)

    if ensure_schema:
        await apply_schema(client)

    factory = source_factory
    if factory is None:
        factory = (
            default_source_factory
            if resolved.source.kind is not SourceKind.REST
            else _rest_not_ready
        )

    try:
        yield JobContext(
            settings=resolved,
            clickhouse=client,
            engine=engine,
            sessions=build_session_factory(engine),
            source_factory=factory,
        )
    finally:
        await client.close()
        await engine.dispose()


def _rest_not_ready(entity: str, month: date) -> Source:
    msg = (
        "помесячная выкачка из REST требует фильтра по дате, а его параметры "
        "не проверены: токена goszakup пока нет. Используйте синтетический источник."
    )
    raise NotImplementedError(msg)
