"""Тесты слоя PostgreSQL на настоящей базе в контейнере.

Почему не заглушки. Проверяется здесь именно то, чего заглушка не воспроизведёт:
что миграция Alembic накатывается на чистую базу, что INSERT ... ON CONFLICT
работает как задумано, что каскадное удаление настроено, что ограничения
CHECK действительно срабатывают. Мок скажет «да» на что угодно.

Тесты помечены как integration и не идут в обычном прогоне: им нужен Docker.
Запуск: make test-integration
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer

from procurement.ingest.models import CursorState
from procurement.storage.postgres.engine import build_session_factory, session_scope
from procurement.storage.postgres.models import (
    AppUser,
    IngestRun,
    RefClassifier,
    RunStatus,
    SavedFilter,
    SyncState,
)
from procurement.storage.postgres.state_store import PostgresStateStore, RunJournal

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def postgres_dsn() -> Iterator[str]:
    """Поднимает временный PostgreSQL и накатывает на него миграции.

    Модульная область видимости: контейнер стартует секунды, поднимать его на
    каждый тест — расточительство. Изоляция достигается тем, что каждый тест
    работает со своими ключами.
    """
    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as container:
        dsn = container.get_connection_url()

        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
        config.set_main_option("sqlalchemy.url", dsn)
        command.upgrade(config, "head")

        yield dsn


@pytest.fixture
async def engine(postgres_dsn: str) -> AsyncIterator[AsyncEngine]:
    from sqlalchemy.ext.asyncio import create_async_engine

    created = create_async_engine(postgres_dsn)
    yield created
    await created.dispose()


@pytest.fixture
def factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return build_session_factory(engine)


# --- миграции -----------------------------------------------------------------


async def test_migration_creates_all_tables(factory: async_sessionmaker[AsyncSession]) -> None:
    async with factory() as session:
        rows = await session.execute(
            sa.text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            )
        )
        tables = {row[0] for row in rows}

    assert {
        "alembic_version",
        "app_user",
        "ingest_run",
        "ref_classifier",
        "ref_status",
        "saved_filter",
        "sync_state",
    } <= tables


async def test_constraint_names_follow_convention(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    """Имена ограничений заданы соглашением, а не придуманы базой.

    Иначе Alembic не сможет сопоставить их с моделями и будет каждый раз
    предлагать удалить и создать заново.
    """
    async with factory() as session:
        rows = await session.execute(
            sa.text(
                "SELECT conname FROM pg_constraint c "
                "JOIN pg_class t ON t.oid = c.conrelid "
                "WHERE t.relname = 'saved_filter'"
            )
        )
        names = {row[0] for row in rows}

    assert "pk_saved_filter" in names
    assert "fk_saved_filter_user_id_app_user" in names
    assert "uq_saved_filter_user_id_name" in names


# --- состояние синхронизации ---------------------------------------------------


async def test_state_store_returns_fresh_when_empty(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    store = PostgresStateStore(factory, source="test_empty")

    state = await store.load("contracts")

    assert state.cursor is None
    assert state.pages_done == 0
    assert state.finished is False


async def test_state_store_roundtrip(factory: async_sessionmaker[AsyncSession]) -> None:
    store = PostgresStateStore(factory, source="test_roundtrip")

    await store.save(
        CursorState(entity="contracts", cursor="4996100", pages_done=3, records_done=150)
    )
    loaded = await store.load("contracts")

    assert loaded.cursor == "4996100"
    assert loaded.pages_done == 3
    assert loaded.records_done == 150


async def test_repeated_save_updates_instead_of_duplicating(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    """Проверка INSERT ... ON CONFLICT DO UPDATE.

    Без него второе сохранение упало бы на нарушении первичного ключа.
    """
    store = PostgresStateStore(factory, source="test_upsert")

    for page in range(1, 6):
        await store.save(
            CursorState(
                entity="contracts", cursor=str(page), pages_done=page, records_done=page * 50
            )
        )

    async with factory() as session:
        count = await session.scalar(
            sa.select(sa.func.count())
            .select_from(SyncState)
            .where(SyncState.source == "test_upsert")
        )

    assert count == 1
    final = await store.load("contracts")
    assert final.cursor == "5"
    assert final.pages_done == 5


async def test_entities_are_independent(factory: async_sessionmaker[AsyncSession]) -> None:
    store = PostgresStateStore(factory, source="test_entities")

    await store.save(CursorState(entity="contracts", cursor="111"))
    await store.save(CursorState(entity="lots", cursor="222"))

    assert (await store.load("contracts")).cursor == "111"
    assert (await store.load("lots")).cursor == "222"


async def test_watermark_is_stored_separately(factory: async_sessionmaker[AsyncSession]) -> None:
    store = PostgresStateStore(factory, source="test_watermark")
    await store.save(CursorState(entity="contracts", cursor="900"))

    moment = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    await store.set_watermark("contracts", moment)

    async with factory() as session:
        found = await session.scalars(
            sa.select(SyncState).where(
                SyncState.source == "test_watermark", SyncState.entity == "contracts"
            )
        )
        row = found.one()

    assert row.watermark == moment
    assert row.cursor == "900", "водяной знак не должен затирать курсор"


# --- журнал запусков -----------------------------------------------------------


async def test_run_journal_records_start_and_finish(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    journal = RunJournal(factory, source="test_journal")

    run_id = await journal.start("contracts")
    await journal.finish(run_id, status=RunStatus.SUCCEEDED, pages=12, records=600)

    async with factory() as session:
        found = await session.scalars(sa.select(IngestRun).where(IngestRun.id == run_id))
        run = found.one()

    assert run.status == RunStatus.SUCCEEDED
    assert run.pages == 12
    assert run.records == 600
    assert run.finished_at is not None
    assert run.finished_at >= run.started_at


async def test_last_successful_skips_failed_runs(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    journal = RunJournal(factory, source="test_last_ok")

    good = await journal.start("lots")
    await journal.finish(good, status=RunStatus.SUCCEEDED, pages=5, records=100)
    bad = await journal.start("lots")
    await journal.finish(bad, status=RunStatus.FAILED, error="boom")

    found = await journal.last_successful("lots")

    assert found is not None
    assert found.id == good


async def test_unknown_status_is_rejected_by_database(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    """Ограничение CHECK должно работать, а не просто существовать в коде."""
    with pytest.raises(IntegrityError):
        async with session_scope(factory) as session:
            session.add(IngestRun(source="test_check", entity="contracts", status="выдумка"))


# --- справочники и пользователи ------------------------------------------------


async def test_classifier_tree_can_be_walked_with_recursive_cte(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    """Дерево классификатора должно обходиться рекурсивным CTE.

    Ровно этот обход понадобится на четвёртом этапе для свёртки на любой
    уровень, поэтому проверяем сразу, что структура его выдерживает.
    """
    async with session_scope(factory) as session:
        session.add_all(
            [
                RefClassifier(code="10", parent_code=None, level=0, name_ru="Продукция"),
                RefClassifier(code="10.1", parent_code="10", level=1, name_ru="Бумага"),
                RefClassifier(code="10.1.1", parent_code="10.1", level=2, name_ru="Бумага А4"),
                RefClassifier(code="20", parent_code=None, level=0, name_ru="Услуги"),
            ]
        )

    async with factory() as session:
        rows = await session.execute(
            sa.text("""
                WITH RECURSIVE tree AS (
                    SELECT code, parent_code, level, name_ru
                    FROM ref_classifier WHERE code = '10'
                    UNION ALL
                    SELECT c.code, c.parent_code, c.level, c.name_ru
                    FROM ref_classifier c JOIN tree t ON c.parent_code = t.code
                )
                SELECT code FROM tree ORDER BY code
            """)
        )
        codes = [row[0] for row in rows]

    assert codes == ["10", "10.1", "10.1.1"], "ветка 20 в выборку попасть не должна"


async def test_self_parent_is_rejected(factory: async_sessionmaker[AsyncSession]) -> None:
    with pytest.raises(IntegrityError):
        async with session_scope(factory) as session:
            session.add(RefClassifier(code="99", parent_code="99", level=0, name_ru="сама себе"))


async def test_deleting_user_removes_their_filters(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    """Каскад настроен на стороне базы, а не только в ORM."""
    user_id = uuid.uuid4()

    async with session_scope(factory) as session:
        session.add(AppUser(id=user_id, email=f"{user_id}@example.test", display_name="Тест"))

    async with session_scope(factory) as session:
        session.add(
            SavedFilter(user_id=user_id, name="мой срез", payload={"customer_bin": "900140000101"})
        )

    async with session_scope(factory) as session:
        await session.execute(sa.delete(AppUser).where(AppUser.id == user_id))

    async with factory() as session:
        left = await session.scalar(
            sa.select(sa.func.count())
            .select_from(SavedFilter)
            .where(SavedFilter.user_id == user_id)
        )

    assert left == 0


async def test_same_filter_name_twice_for_one_user_is_rejected(
    factory: async_sessionmaker[AsyncSession],
) -> None:
    user_id = uuid.uuid4()
    async with session_scope(factory) as session:
        session.add(AppUser(id=user_id, email=f"{user_id}@example.test", display_name="Тест"))
    async with session_scope(factory) as session:
        session.add(SavedFilter(user_id=user_id, name="дубль", payload={}))

    with pytest.raises(IntegrityError):
        async with session_scope(factory) as session:
            session.add(SavedFilter(user_id=user_id, name="дубль", payload={}))


async def test_jsonb_payload_is_queryable(factory: async_sessionmaker[AsyncSession]) -> None:
    """JSONB выбран не ради вида: по нему должен работать поиск."""
    user_id = uuid.uuid4()
    async with session_scope(factory) as session:
        session.add(AppUser(id=user_id, email=f"{user_id}@example.test", display_name="Тест"))
    async with session_scope(factory) as session:
        session.add(
            SavedFilter(
                user_id=user_id,
                name="по заказчику",
                payload={"customer_bin": "900140000101", "years": [2025, 2026]},
            )
        )

    async with factory() as session:
        found = await session.scalars(
            sa.select(SavedFilter).where(
                SavedFilter.payload["customer_bin"].astext == "900140000101"
            )
        )
        rows = list(found)

    assert len(rows) == 1
    assert rows[0].payload["years"] == [2025, 2026]
