"""Помесячный бэкфилл с идемпотентной подменой партиции.

Порядок шагов и причина каждого:

1. создать временную таблицу той же структуры;
2. выкачать месяц целиком в неё;
3. схлопнуть ревизии внутри неё;
4. прогнать проверки качества — **до** того, как данные увидит кто-либо;
5. только при успехе подменить партицию в боевой таблице одним действием;
6. убрать временную таблицу.

Идемпотентность держится на пятом шаге. `ALTER TABLE ... REPLACE PARTITION`
заменяет партицию целиком: сколько раз ни запусти бэкфилл за апрель, в боевой
таблице будет ровно один апрель. Это сильнее, чем полагаться на схлопывание
ReplacingMergeTree — там дубли исчезают когда-нибудь, а здесь их не возникает
вовсе.

Обратная сторона того же свойства: партиция заменяется **целиком**, поэтому
месяц обязан быть выкачан полностью. Упасть на середине и подменить — значит
потерять остаток. Отсюда порядок: сначала собрать всё, проверить, и только
потом публиковать.

Отдельно: бэкфилл работает со своим временным состоянием, а не с общим
`sync_state`. Иначе он сдвинул бы курсор инкрементальной синхронизации, и та
после него пропустила бы свежие изменения.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date

from procurement.ingest.limiter import AdaptiveLimiter
from procurement.ingest.pipeline import Pipeline
from procurement.ingest.retry import RetryPolicy
from procurement.ingest.state import InMemoryStateStore
from procurement.jobs.context import JobContext
from procurement.logging import get_logger
from procurement.quality.checks import QualityGate, QualityReport
from procurement.storage.clickhouse.schema import spec_for
from procurement.storage.clickhouse.writer import PartitionSwapLoader
from procurement.storage.postgres.models import RunStatus
from procurement.storage.postgres.state_store import RunJournal

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class BackfillResult:
    entity: str
    month: date
    pages: int
    records: int
    partitions: list[str]
    report: QualityReport
    duration_s: float

    @property
    def rows_per_second(self) -> float:
        return 0.0 if self.duration_s == 0 else self.records / self.duration_s


async def backfill_month(
    context: JobContext,
    *,
    entity: str,
    month: date,
    journal: bool = True,
) -> BackfillResult:
    spec = spec_for(entity)
    month = month.replace(day=1)
    suffix = f"{month:%Y%m}"
    started = time.perf_counter()

    run_journal = RunJournal(context.sessions) if journal else None
    run_id = await run_journal.start(f"{entity}:backfill:{suffix}") if run_journal else None

    loader = PartitionSwapLoader(context.clickhouse, spec, suffix=suffix)
    await loader.prepare()

    try:
        source = context.source_factory(entity, month)
        limiter = AdaptiveLimiter(
            start=context.settings.http.concurrency_start,
            minimum=context.settings.http.concurrency_min,
            maximum=context.settings.http.concurrency_max,
        )
        pipeline = Pipeline(
            source=source,
            # Своё состояние, не общее: бэкфилл не должен трогать курсор
            # инкрементальной синхронизации.
            state=InMemoryStateStore(),
            limiter=limiter,
            handler=loader.writer(),
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

        if not outcome.finished:
            msg = (
                f"месяц {suffix} выкачан не полностью ({outcome.reason}). "
                "Подменять партицию нельзя: потеряем остаток"
            )
            raise RuntimeError(msg)

        # Схлопываем ревизии до проверок, иначе проверка на дубли поймает их же.
        await context.clickhouse.command(f"OPTIMIZE TABLE {loader.staging_table} FINAL")

        gate = QualityGate(context.clickhouse, spec, table=loader.staging_table)
        report = await gate.run(api_total=outcome.records)
        log.info("backfill.quality", entity=entity, month=suffix, report=report.render())
        report.raise_if_failed()

        partitions = await loader.commit()

    except BaseException as error:
        await loader.cleanup()
        if run_journal and run_id is not None:
            await run_journal.finish(run_id, status=RunStatus.FAILED, error=str(error))
        raise
    else:
        await loader.cleanup()

    duration = time.perf_counter() - started
    if run_journal and run_id is not None:
        await run_journal.finish(
            run_id,
            status=RunStatus.SUCCEEDED,
            pages=outcome.pages,
            records=outcome.records,
            reason=f"партиции: {', '.join(partitions)}",
        )

    result = BackfillResult(
        entity=entity,
        month=month,
        pages=outcome.pages,
        records=outcome.records,
        partitions=partitions,
        report=report,
        duration_s=duration,
    )
    log.info(
        "backfill.done",
        entity=entity,
        month=suffix,
        records=result.records,
        partitions=partitions,
        seconds=round(duration, 2),
        rows_per_second=round(result.rows_per_second),
    )
    return result
