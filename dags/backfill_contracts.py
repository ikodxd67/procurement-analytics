"""DAG помесячного бэкфилла договоров.

Одно расписание — один месяц. Airflow сам держит очередь запусков по месяцам,
сам повторяет упавшие и сам показывает, какие месяцы уже сделаны. Писать цикл
по месяцам внутри одной задачи было бы ошибкой: упал бы двадцатый месяц —
пришлось бы перезапускать все тридцать шесть.

Логики здесь нет намеренно. Задача вызывает backfill_month из
procurement.jobs.backfill — ту же функцию, что запускается из командной строки.
Так её можно отладить и покрыть тестами без планировщика.

Идемпотентность обеспечивается не Airflow, а самой загрузкой: месяц собирается
во временную таблицу и попадает в боевую через ALTER TABLE ... REPLACE
PARTITION. Повторный запуск за тот же месяц даёт тот же результат.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import Any

import pendulum
from airflow.sdk import dag, task

from procurement.jobs.backfill import backfill_month
from procurement.jobs.context import job_context

DEFAULT_ARGS = {
    "retries": 3,
    # Пауза между попытками растёт: если источник лежит, долбить его каждые
    # тридцать секунд бессмысленно и невежливо.
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="backfill_contracts",
    description="Помесячная выкачка договоров с подменой партиции",
    schedule="@monthly",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=True,
    # Месяцы грузятся по одному. Причина не в осторожности, а в ресурсах:
    # каждый запуск держит временную таблицу в ClickHouse, и десяток
    # параллельных съест память без всякой пользы — узкое место всё равно
    # в источнике.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["procurement", "backfill", "clickhouse"],
    doc_md=__doc__,
)
def backfill_contracts() -> None:
    @task
    def load_month(data_interval_start: datetime | None = None, **_: Any) -> dict[str, Any]:
        """Выкачать один месяц и подменить им партицию.

        Месяц берётся из data_interval_start, а не из «сегодня». Это то, что
        отличает воспроизводимый бэкфилл от невоспроизводимого: перезапуск
        запуска за март 2023 года всегда грузит март 2023 года, когда бы он ни
        случился.
        """
        if data_interval_start is None:
            msg = "Airflow не передал начало интервала — запуск без расписания?"
            raise ValueError(msg)

        month = date(data_interval_start.year, data_interval_start.month, 1)

        async def run() -> dict[str, Any]:
            async with job_context() as context:
                result = await backfill_month(context, entity="contracts", month=month)
                return {
                    "month": f"{month:%Y-%m}",
                    "records": result.records,
                    "pages": result.pages,
                    "partitions": result.partitions,
                    "seconds": round(result.duration_s, 1),
                    "rows_per_second": round(result.rows_per_second),
                    "quality": [check.render() for check in result.report.checks],
                }

        return asyncio.run(run())

    load_month()


backfill_contracts()
