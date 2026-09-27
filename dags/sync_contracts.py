"""DAG инкрементальной синхронизации договоров по водяному знаку.

Идёт раз в час и догоняет изменения. В отличие от бэкфилла здесь нет
catchup: догонять пропущенные часы бессмысленно, потому что водяной знак и так
помнит, до какого момента данные загружены. Пропустили шесть часов — следующий
запуск заберёт всё накопившееся одним махом.

Шаги разведены на три задачи не ради красоты схемы, а чтобы в интерфейсе было
видно, что именно сломалось: не выкачалось, не прошло проверки или не
опубликовалось. Одна задача на всё превращает разбор аварии в чтение логов.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import Any

import pendulum
from airflow.sdk import dag, task

from procurement.ingest.synthetic_source import SyntheticSource
from procurement.jobs.context import job_context
from procurement.jobs.incremental import sync_incremental

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="sync_contracts",
    description="Догрузка изменившихся договоров по водяному знаку",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    # Догонять пропущенные часы не нужно: водяной знак помнит позицию сам,
    # и один запуск заберёт всё накопившееся.
    catchup=False,
    # Два одновременных прогона подрались бы за водяной знак и за временную
    # таблицу. Второй просто ждёт.
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=DEFAULT_ARGS,
    tags=["procurement", "incremental", "clickhouse"],
    doc_md=__doc__,
)
def sync_contracts() -> None:
    @task
    def fetch_and_publish(logical_date: datetime | None = None, **_: Any) -> dict[str, Any]:
        """Выкачать изменения, проверить качество, опубликовать новее знака.

        Проверки качества идут внутри sync_incremental между загрузкой во
        временную таблицу и публикацией. Не прошли — публикации не будет,
        водяной знак останется на месте, и следующий запуск попробует снова.
        """
        moment = logical_date or pendulum.now("UTC")
        month = date(moment.year, moment.month, 1)

        async def run() -> dict[str, Any]:
            async with job_context() as context:
                # Пока нет токена, изменения имитирует синтетический источник.
                # С токеном сюда встанет RestSource с фильтром по дате
                # изменения, а остальное не поменяется.
                source = SyntheticSource(month, records=2000, page_size=500)
                result = await sync_incremental(context, entity="contracts", source=source)
                return {
                    "fetched": result.fetched,
                    "published": result.published,
                    "skipped_as_old": result.skipped_as_old,
                    "watermark_before": str(result.watermark_before),
                    "watermark_after": str(result.watermark_after),
                    "quality": [check.render() for check in result.report.checks],
                }

        return asyncio.run(run())

    @task
    def report(outcome: dict[str, Any]) -> None:
        """Вывести итог прогона в лог задачи.

        Отдельная задача, потому что в интерфейсе Airflow её видно сразу: не
        надо разворачивать логи предыдущей, чтобы понять, сколько уехало.
        """
        print(f"Получено:     {outcome['fetched']}")
        print(f"Опубликовано: {outcome['published']}")
        print(f"Пропущено как устаревшее: {outcome['skipped_as_old']}")
        print(f"Водяной знак: {outcome['watermark_before']} -> {outcome['watermark_after']}")
        for line in outcome["quality"]:
            print(line)

    report(fetch_and_publish())


sync_contracts()
