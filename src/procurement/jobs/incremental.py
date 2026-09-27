"""Инкрементальная синхронизация по водяному знаку.

Почему не по курсору. Курсор годится для первичной выкачки: он отмечает, до
какого места в выдаче мы дошли. Но записи ревизируются — договор, изменившийся
вчера, лежит где-то в середине уже пройденного списка, и курсор о нём ничего не
скажет. Догонять изменения надо по дате последнего изменения, то есть по
водяному знаку.

Порядок такой же, как у бэкфилла, и это не совпадение: проверки качества должны
стоять между загрузкой и публикацией в обоих случаях.

1. прочитать водяной знак из sync_state;
2. выкачать во временную таблицу то, что отдал источник;
3. схлопнуть ревизии внутри неё;
4. прогнать проверки качества;
5. опубликовать только записи новее водяного знака;
6. сдвинуть водяной знак на максимум опубликованного.

Публикация здесь — обычный INSERT, а не подмена партиции: изменения приходят
вперемешку из любых месяцев, и какую партицию менять целиком, неизвестно.
Повторная вставка тех же записей результат не портит — ReplacingMergeTree
схлопнет их по ключу сортировки, — но до слияния запросы обязаны считаться с
дублями.

Водяной знак двигается **после** публикации, а не до. Порядок тот же, что с
курсором в загрузчике, и по той же причине: упасть между загрузкой и сдвигом
значит перезабрать данные повторно, а сдвинуть раньше — потерять их навсегда.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime

from procurement.ingest.limiter import AdaptiveLimiter
from procurement.ingest.pipeline import Pipeline
from procurement.ingest.retry import RetryPolicy
from procurement.ingest.source import Source
from procurement.ingest.state import InMemoryStateStore
from procurement.jobs.context import JobContext
from procurement.logging import get_logger
from procurement.quality.checks import QualityGate, QualityReport
from procurement.storage.clickhouse.schema import spec_for
from procurement.storage.clickhouse.writer import PartitionSwapLoader
from procurement.storage.postgres.models import RunStatus
from procurement.storage.postgres.state_store import PostgresStateStore, RunJournal

log = get_logger(__name__)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class IncrementalResult:
    entity: str
    fetched: int
    published: int
    watermark_before: datetime | None
    watermark_after: datetime | None
    report: QualityReport
    duration_s: float

    @property
    def skipped_as_old(self) -> int:
        return self.fetched - self.published


async def sync_incremental(
    context: JobContext,
    *,
    entity: str,
    source: Source,
    journal: bool = True,
) -> IncrementalResult:
    spec = spec_for(entity)
    started = time.perf_counter()

    state = PostgresStateStore(context.sessions)
    watermark = await state.get_watermark(entity)

    run_journal = RunJournal(context.sessions) if journal else None
    run_id = await run_journal.start(f"{entity}:incremental") if run_journal else None

    staging = PartitionSwapLoader(context.clickhouse, spec, suffix="incremental")
    await staging.prepare()

    try:
        limiter = AdaptiveLimiter(
            start=context.settings.http.concurrency_start,
            minimum=context.settings.http.concurrency_min,
            maximum=context.settings.http.concurrency_max,
        )
        pipeline = Pipeline(
            source=source,
            state=InMemoryStateStore(),
            limiter=limiter,
            handler=staging.writer(),
            retry_policy=RetryPolicy(
                max_attempts=context.settings.http.retry_max_attempts,
                base_delay_s=context.settings.http.retry_base_delay_s,
                max_delay_s=context.settings.http.retry_max_delay_s,
            ),
            queue_maxsize=context.settings.pipeline.queue_maxsize,
            batch_size=context.settings.pipeline.batch_size,
            workers=context.settings.pipeline.parser_workers,
        )

        async with limiter:
            outcome = await pipeline.run(entity, from_scratch=True)

        await context.clickhouse.command(f"OPTIMIZE TABLE {staging.staging_table} FINAL")

        gate = QualityGate(context.clickhouse, spec, table=staging.staging_table)
        report = await gate.run(api_total=outcome.records)
        log.info("incremental.quality", entity=entity, report=report.render())
        report.raise_if_failed()

        published, new_watermark = await _publish(context, spec.table, staging, watermark)

    except BaseException as error:
        await staging.cleanup()
        if run_journal and run_id is not None:
            await run_journal.finish(run_id, status=RunStatus.FAILED, error=str(error))
        raise
    else:
        await staging.cleanup()

    if new_watermark is not None:
        await state.set_watermark(entity, new_watermark)

    duration = time.perf_counter() - started
    if run_journal and run_id is not None:
        await run_journal.finish(
            run_id,
            status=RunStatus.SUCCEEDED,
            pages=outcome.pages,
            records=published,
            reason=f"водяной знак: {new_watermark}",
        )

    result = IncrementalResult(
        entity=entity,
        fetched=outcome.records,
        published=published,
        watermark_before=watermark,
        watermark_after=new_watermark,
        report=report,
        duration_s=duration,
    )
    log.info(
        "incremental.done",
        entity=entity,
        fetched=result.fetched,
        published=result.published,
        skipped=result.skipped_as_old,
        watermark=str(new_watermark),
        seconds=round(duration, 2),
    )
    return result


async def _publish(
    context: JobContext,
    target: str,
    staging: PartitionSwapLoader,
    watermark: datetime | None,
) -> tuple[int, datetime | None]:
    """Перелить из временной таблицы только записи новее водяного знака."""
    threshold = (watermark or EPOCH).strftime("%Y-%m-%d %H:%M:%S")

    counted = await context.clickhouse.query(
        f"SELECT count(), max(last_update_date) FROM {staging.staging_table} "  # noqa: S608
        "WHERE last_update_date > toDateTime({since:String})",
        parameters={"since": threshold},
    )
    published, newest = counted.result_rows[0]
    published = int(published)

    if published == 0:
        return 0, watermark

    await context.clickhouse.command(
        f"INSERT INTO {target} SELECT * FROM {staging.staging_table} "  # noqa: S608
        "WHERE last_update_date > toDateTime({since:String})",
        parameters={"since": threshold},
    )

    newest_dt = newest if isinstance(newest, datetime) else None
    if newest_dt is not None and newest_dt.tzinfo is None:
        newest_dt = newest_dt.replace(tzinfo=UTC)
    return published, newest_dt
