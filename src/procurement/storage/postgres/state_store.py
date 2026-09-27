"""Состояние загрузки и журнал запусков в PostgreSQL.

Замена файловому хранилищу первого этапа. Протокол StateStore тот же, поэтому
конвейер подмены не замечает — ради этого он и делался узким.

Все чтения через select(). Старый стиль session.query() в проекте не
используется: он остался от версии 1.x, работает через отдельный путь внутри
SQLAlchemy и в асинхронном режиме не поддерживается вовсе.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from procurement.ingest.models import CursorState
from procurement.logging import get_logger
from procurement.storage.postgres.engine import session_scope
from procurement.storage.postgres.models import IngestRun, RunStatus, SyncState

log = get_logger(__name__)

DEFAULT_SOURCE = "goszakup_rest"


class PostgresStateStore:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        source: str = DEFAULT_SOURCE,
    ) -> None:
        self._factory = factory
        self._source = source

    async def load(self, entity: str) -> CursorState:
        async with self._factory() as session:
            # scalars().first(), а не scalar(): второй типизирован как Any и
            # тихо роняет проверку типов на всём, что дальше с ним делают.
            found = await session.scalars(
                select(SyncState).where(
                    SyncState.source == self._source,
                    SyncState.entity == entity,
                )
            )
            row = found.first()

        if row is None:
            return CursorState.fresh(entity)

        return CursorState(
            entity=row.entity,
            cursor=row.cursor,
            pages_done=row.pages_done,
            records_done=row.records_done,
            finished=row.finished,
            updated_at=row.updated_at,
        )

    async def save(self, state: CursorState) -> None:
        """Вставка или обновление одним запросом.

        Читать, проверять и потом писать — это три обращения и гонка между
        вторым и третьим. INSERT ... ON CONFLICT DO UPDATE выполняется как одна
        операция, и две параллельные записи не затрут друг друга наполовину.
        """
        now = datetime.now(UTC)
        statement = insert(SyncState).values(
            source=self._source,
            entity=state.entity,
            cursor=state.cursor,
            pages_done=state.pages_done,
            records_done=state.records_done,
            finished=state.finished,
            updated_at=now,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[SyncState.source, SyncState.entity],
            set_={
                "cursor": statement.excluded.cursor,
                "pages_done": statement.excluded.pages_done,
                "records_done": statement.excluded.records_done,
                "finished": statement.excluded.finished,
                "updated_at": statement.excluded.updated_at,
            },
        )

        async with session_scope(self._factory) as session:
            await session.execute(statement)

    async def get_watermark(self, entity: str) -> datetime | None:
        """До какой даты изменений данные уже загружены.

        None означает «ещё ни разу не синхронизировались» — инкрементальный
        прогон в этом случае возьмёт всё, что отдаст источник.
        """
        async with self._factory() as session:
            found = await session.scalars(
                select(SyncState.watermark).where(
                    SyncState.source == self._source,
                    SyncState.entity == entity,
                )
            )
            return found.first()

    async def set_watermark(self, entity: str, watermark: datetime) -> None:
        """Отметить, до какой даты изменений данные загружены.

        Вставка или обновление, а не просто UPDATE. Первая версия делала
        UPDATE, и это молча не работало: строки в sync_state ещё не было, ведь
        бэкфилл ведёт своё временное состояние и сюда ничего не пишет. UPDATE
        менял ноль строк, ошибки не возникало, водяной знак не сохранялся.
        Поймалось только повторным запуском синхронизации.
        """
        statement = insert(SyncState).values(
            source=self._source,
            entity=entity,
            watermark=watermark,
            updated_at=datetime.now(UTC),
        )
        statement = statement.on_conflict_do_update(
            index_elements=[SyncState.source, SyncState.entity],
            set_={
                "watermark": statement.excluded.watermark,
                "updated_at": statement.excluded.updated_at,
            },
        )
        async with session_scope(self._factory) as session:
            await session.execute(statement)


class RunJournal:
    """Записывает в журнал начало и исход каждого прогона."""

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        source: str = DEFAULT_SOURCE,
    ) -> None:
        self._factory = factory
        self._source = source

    async def start(self, entity: str) -> int:
        async with session_scope(self._factory) as session:
            run = IngestRun(source=self._source, entity=entity, status=RunStatus.RUNNING)
            session.add(run)
            await session.flush()
            run_id = run.id

        log.info("run.started", run_id=run_id, entity=entity)
        return run_id

    async def finish(
        self,
        run_id: int,
        *,
        status: RunStatus,
        pages: int = 0,
        records: int = 0,
        reason: str | None = None,
        error: str | None = None,
    ) -> None:
        async with session_scope(self._factory) as session:
            await session.execute(
                update(IngestRun)
                .where(IngestRun.id == run_id)
                .values(
                    status=status,
                    finished_at=datetime.now(UTC),
                    pages=pages,
                    records=records,
                    reason=reason,
                    # Текст ошибки обрезаем: в журнал нужен опознавательный
                    # признак, а полная трассировка живёт в логах.
                    error=error[:4000] if error else None,
                )
            )

        log.info("run.finished", run_id=run_id, status=status, pages=pages, records=records)

    async def last_successful(self, entity: str) -> IngestRun | None:
        async with self._factory() as session:
            found = await session.scalars(
                select(IngestRun)
                .where(
                    IngestRun.source == self._source,
                    IngestRun.entity == entity,
                    IngestRun.status == RunStatus.SUCCEEDED,
                )
                .order_by(IngestRun.started_at.desc())
                .limit(1)
            )
            return found.first()
