"""Тесты заданий загрузки на настоящих PostgreSQL и ClickHouse.

Проверяется главное требование этапа: повторный запуск за тот же период не
задваивает данные. Проверяется не рассуждением, а прогоном бэкфилла три раза
подряд и сравнением количества строк.

Второе по важности — что плохие данные не доезжают до боевой таблицы. Проверки
качества стоят между загрузкой и публикацией, и если они не прошли, партиция
не подменяется.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.community.postgres import PostgresContainer

from procurement.config import ClickHouseSettings, PostgresSettings, Settings
from procurement.ingest.models import Page
from procurement.ingest.synthetic_source import SyntheticSource
from procurement.jobs.backfill import backfill_month
from procurement.jobs.context import JobContext
from procurement.jobs.incremental import sync_incremental
from procurement.quality.checks import QualityFailedError, QualityGate
from procurement.storage.clickhouse.client import build_client
from procurement.storage.clickhouse.ddl import apply_schema
from procurement.storage.clickhouse.schema import CONTRACTS
from procurement.storage.postgres.engine import build_engine, build_session_factory
from procurement.storage.postgres.state_store import PostgresStateStore

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def stack() -> Iterator[tuple[PostgresSettings, ClickHouseSettings]]:
    postgres = PostgresContainer("postgres:16-alpine", driver="asyncpg")
    clickhouse = ClickHouseContainer(
        "clickhouse/clickhouse-server:24.8-alpine",
        port=8123,
        username="test",
        password="test",
        dbname="test",
    )
    with postgres, clickhouse:
        dsn = postgres.get_connection_url()
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
        config.set_main_option("sqlalchemy.url", dsn)
        command.upgrade(config, "head")

        from pydantic import SecretStr

        yield (
            PostgresSettings(dsn=dsn),
            ClickHouseSettings(
                host=clickhouse.get_container_host_ip(),
                port=int(clickhouse.get_exposed_port(8123)),
                database="test",
                user="test",
                password=SecretStr("test"),
            ),
        )


@pytest.fixture
async def context(
    stack: tuple[PostgresSettings, ClickHouseSettings],
) -> AsyncIterator[JobContext]:
    postgres_settings, clickhouse_settings = stack
    settings = Settings(postgres=postgres_settings, clickhouse=clickhouse_settings)

    client = await build_client(clickhouse_settings)
    await client.command("DROP TABLE IF EXISTS contracts")
    await client.command("DROP TABLE IF EXISTS lots")
    await apply_schema(client)

    engine = build_engine(postgres_settings)
    sessions = build_session_factory(engine)
    async with engine.begin() as connection:
        from sqlalchemy import text

        await connection.execute(text("TRUNCATE sync_state, ingest_run"))

    def factory(entity: str, month: date) -> Any:
        return SyntheticSource(month, records=1000, page_size=250)

    yield JobContext(
        settings=settings,
        clickhouse=client,
        engine=engine,
        sessions=sessions,
        source_factory=factory,
    )

    await client.close()
    await engine.dispose()


async def count_contracts(context: JobContext) -> tuple[int, int]:
    result = await context.clickhouse.query("SELECT count(), uniqExact(id) FROM contracts")
    row = result.result_rows[0]
    return int(row[0]), int(row[1])


# --- бэкфилл --------------------------------------------------------------------


async def test_backfill_loads_month_and_swaps_partition(context: JobContext) -> None:
    result = await backfill_month(context, entity="contracts", month=date(2024, 3, 1))

    assert result.records == 1000
    assert result.partitions == ["202403"]
    assert result.report.ok

    rows, distinct = await count_contracts(context)
    assert rows == 1000
    assert distinct == 1000


async def test_repeated_backfill_does_not_duplicate(context: JobContext) -> None:
    """Главное требование этапа, проверенное прогоном, а не рассуждением."""
    for _ in range(3):
        await backfill_month(context, entity="contracts", month=date(2024, 3, 1))

    rows, distinct = await count_contracts(context)

    assert rows == 1000, "три прогона одного месяца дали ровно один месяц"
    assert distinct == 1000


async def test_backfill_of_different_months_accumulates(context: JobContext) -> None:
    await backfill_month(context, entity="contracts", month=date(2024, 3, 1))
    await backfill_month(context, entity="contracts", month=date(2024, 4, 1))

    rows, _ = await count_contracts(context)
    partitions = await context.clickhouse.query(
        "SELECT DISTINCT partition FROM system.parts WHERE active AND table = 'contracts' "
        "ORDER BY partition"
    )

    assert rows == 2000
    assert [row[0] for row in partitions.result_rows] == ["202403", "202404"]


async def test_backfill_writes_to_the_run_journal(context: JobContext) -> None:
    from sqlalchemy import select

    from procurement.storage.postgres.models import IngestRun, RunStatus

    await backfill_month(context, entity="contracts", month=date(2024, 3, 1))

    async with context.sessions() as session:
        found = await session.scalars(select(IngestRun))
        runs = list(found)

    assert len(runs) == 1
    assert runs[0].status == RunStatus.SUCCEEDED
    assert runs[0].records == 1000
    assert runs[0].finished_at is not None


class BrokenSource:
    """Источник, отдающий записи без обязательных полей."""

    def __init__(self, records: int = 100) -> None:
        self._records = records

    async def fetch_page(self, entity: str, cursor: str | None) -> Page:
        items = [
            {
                "id": i,
                "customer_bin": "",  # обязательное поле пустое
                "supplier_biin": "",
                "contract_sum": -1000,  # отрицательная сумма
                "crdate": "2024-03-10 00:00:00",
                "last_update_date": "2024-03-11 00:00:00",
            }
            for i in range(self._records)
        ]
        return Page(entity=entity, items=items, next_cursor=None, total=self._records)

    async def aclose(self) -> None:
        return


async def test_bad_data_never_reaches_the_target_table(context: JobContext) -> None:
    """Проверки качества стоят между загрузкой и публикацией не для красоты."""
    context.source_factory = lambda entity, month: BrokenSource(100)

    with pytest.raises(QualityFailedError):
        await backfill_month(context, entity="contracts", month=date(2024, 3, 1))

    rows, _ = await count_contracts(context)
    assert rows == 0, "испорченный месяц не должен был доехать"


async def test_staging_table_is_cleaned_up_after_failure(context: JobContext) -> None:
    context.source_factory = lambda entity, month: BrokenSource(100)

    with pytest.raises(QualityFailedError):
        await backfill_month(context, entity="contracts", month=date(2024, 3, 1))

    left = await context.clickhouse.query(
        "SELECT count() FROM system.tables WHERE name LIKE 'contracts_staging_%'"
    )
    assert left.result_rows[0][0] == 0, "временная таблица должна убираться и после падения"


# --- проверки качества -----------------------------------------------------------


async def test_quality_gate_spots_duplicates(context: JobContext) -> None:
    """Дубль создаётся ДВУМЯ вставками, а не одной.

    Проверено на ClickHouse 24.8: строки с одинаковым ключом сортировки,
    пришедшие одной вставкой, схлопываются уже при формировании куска. Дубль
    возникает только между разными кусками — то есть между разными вставками.
    Первая версия теста делала одну вставку и ничего не ловила.
    """
    for total, updated in ((100, "2024-03-05"), (200, "2024-03-06")):
        await context.clickhouse.command(
            "INSERT INTO contracts (id, customer_bin, supplier_biin, contract_sum, crdate, "
            f"sign_date, last_update_date) VALUES "
            f"(1, '900140000101', '950140000111', {total}, '2024-03-01', "
            f"'2024-03-02', '{updated}')"
        )

    report = await QualityGate(context.clickhouse, CONTRACTS).run()
    duplicates = next(c for c in report.checks if c.name == "дубли по id")

    assert not duplicates.passed
    assert not report.ok


async def test_quality_gate_spots_count_mismatch(context: JobContext) -> None:
    await context.clickhouse.command(
        "INSERT INTO contracts (id, customer_bin, supplier_biin, contract_sum, crdate, "
        "sign_date, last_update_date) VALUES "
        "(1, '900140000101', '950140000111', 100, '2024-03-01', '2024-03-02', '2024-03-05')"
    )

    report = await QualityGate(context.clickhouse, CONTRACTS).run(api_total=500)
    drift = next(c for c in report.checks if c.name.startswith("сверка"))

    assert not drift.passed, "загрузили одну запись вместо пятисот — это потеря страниц"


async def test_quality_gate_passes_on_clean_data(context: JobContext) -> None:
    await backfill_month(context, entity="contracts", month=date(2024, 3, 1))

    report = await QualityGate(context.clickhouse, CONTRACTS).run(api_total=1000)

    assert report.ok
    assert report.warnings == []


# --- инкрементальная синхронизация -------------------------------------------------


async def test_incremental_sets_and_uses_watermark(context: JobContext) -> None:
    """Водяной знак должен сохраняться и на втором прогоне отсекать старое.

    Первая версия set_watermark делала UPDATE, а строки в sync_state ещё не
    было: знак молча не сохранялся, и каждый прогон публиковал всё заново.
    Поймалось ровно этим сценарием.
    """
    source = SyntheticSource(date(2024, 3, 1), records=500, page_size=250)
    first = await sync_incremental(context, entity="contracts", source=source)

    assert first.watermark_before is None
    assert first.published == 500
    assert first.watermark_after is not None

    stored = await PostgresStateStore(context.sessions).get_watermark("contracts")
    assert stored is not None, "знак обязан пережить прогон"

    again = SyntheticSource(date(2024, 3, 1), records=500, page_size=250)
    second = await sync_incremental(context, entity="contracts", source=again)

    assert second.watermark_before is not None
    assert second.published == 0, "те же записи публиковать повторно незачем"
    assert second.skipped_as_old == 500


async def test_incremental_publishes_only_newer_records(context: JobContext) -> None:
    await PostgresStateStore(context.sessions).set_watermark(
        "contracts", datetime(2024, 3, 15, tzinfo=UTC)
    )

    source = SyntheticSource(date(2024, 3, 1), records=500, page_size=250)
    result = await sync_incremental(context, entity="contracts", source=source)

    assert result.published < 500, "часть записей старее знака и публиковаться не должна"
    assert result.published + result.skipped_as_old == 500

    rows, _ = await count_contracts(context)
    assert rows == result.published
