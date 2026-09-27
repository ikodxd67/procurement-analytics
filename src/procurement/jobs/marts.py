"""Пересчёт витрин.

Витрина пересчитывается заданием, а не материализованным представлением.
Причина техническая: представление срабатывает на INSERT в исходную таблицу, а
бэкфилл кладёт данные через ALTER TABLE ... REPLACE PARTITION — это ALTER, и
представление его не увидит. Отдельный шаг после загрузки надёжнее и виден в
Airflow как самостоятельная задача.

Пересчёт идемпотентен по тому же приёму, что и сама загрузка: месяц собирается
во временную таблицу и подменяет партицию целиком.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date

from procurement.jobs.context import JobContext
from procurement.logging import get_logger
from procurement.storage.clickhouse.schema import safe_identifier, safe_partition

log = get_logger(__name__)

MART_TABLE = "mart_contracts_monthly"


@dataclass(frozen=True, slots=True)
class MartRefreshResult:
    table: str
    months: list[str]
    rows: int
    duration_s: float


async def refresh_monthly_mart(
    context: JobContext, *, month: date | None = None
) -> MartRefreshResult:
    """Пересчитать витрину: один месяц или всё целиком.

    Указан месяц — подменяется только его партиция. Это то, что вызывается
    после бэкфилла: пересчитывать три года ради одного изменившегося месяца
    незачем.
    """
    started = time.perf_counter()
    staging = safe_identifier(f"{MART_TABLE}_staging", what="имя временной таблицы")

    await context.clickhouse.command(f"DROP TABLE IF EXISTS {staging}")
    await context.clickhouse.command(f"CREATE TABLE {staging} AS {MART_TABLE}")

    if month is None:
        where = ""
        params: dict[str, object] = {}
    else:
        where = "WHERE toStartOfMonth(crdate) = {month:Date}"
        params = {"month": month.replace(day=1)}

    try:
        # Имена таблиц — константы этого модуля, прошедшие safe_identifier;
        # where собирается из двух литералов, пользовательского ввода в тексте
        # запроса нет. Месяц передаётся параметром.
        insert_sql = (
            f"INSERT INTO {staging} "  # noqa: S608
            "SELECT toStartOfMonth(crdate) AS month, customer_bin, "
            "count() AS contracts, sum(contract_sum) AS total "
            f"FROM contracts FINAL {where} "
            "GROUP BY month, customer_bin"
        )
        await context.clickhouse.command(insert_sql, parameters=params)

        partitions = await context.clickhouse.query(
            "SELECT DISTINCT partition FROM system.parts "
            "WHERE active AND table = {staging:String} AND database = currentDatabase()",
            parameters={"staging": staging},
        )
        months = [str(row[0]) for row in partitions.result_rows]

        for partition in months:
            # Имя партиции пришло из system.parts, то есть от самого
            # ClickHouse, но проверяем и его: подстановка в текст запроса
            # обязана быть проверенной независимо от источника.
            safe_partition(partition)
            await context.clickhouse.command(
                f"ALTER TABLE {MART_TABLE} REPLACE PARTITION '{partition}' FROM {staging}"
            )

        counted = await context.clickhouse.query(
            f"SELECT count() FROM {MART_TABLE}"  # noqa: S608
        )
        rows = int(counted.result_rows[0][0])
    finally:
        await context.clickhouse.command(f"DROP TABLE IF EXISTS {staging}")

    duration = time.perf_counter() - started
    log.info(
        "mart.refreshed",
        table=MART_TABLE,
        partitions=months,
        rows=rows,
        seconds=round(duration, 2),
    )
    return MartRefreshResult(table=MART_TABLE, months=months, rows=rows, duration_s=duration)
