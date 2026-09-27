"""Замер аналитических запросов: PostgreSQL против ClickHouse.

Скрипт делает три вещи для каждой пары запросов:

1. выполняет обе версии несколько раз и берёт медиану;
2. сверяет результаты между базами — без этого сравнение скорости
   бессмысленно, потому что быстрее всего работает неправильный ответ;
3. выводит таблицу в markdown для документа с замерами.

Первый прогон каждого запроса не учитывается: он прогревает кэши, и медиана с
ним показывала бы не запрос, а состояние диска.

Запуск (нужны поднятые контейнеры и загруженные данные):
    .venv/Scripts/python scripts/load_postgres_facts.py
    .venv/Scripts/python scripts/bench_sql.py
"""

from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg
import clickhouse_connect
from clickhouse_connect.driver import Client

from procurement.config import get_settings

SQL_ROOT = Path(__file__).resolve().parent.parent / "sql"
REPEATS = 5
TOLERANCE = 1e-6


@dataclass(frozen=True, slots=True)
class Query:
    name: str
    title: str
    postgres_args: tuple[Any, ...] = ()
    clickhouse_params: dict[str, Any] | None = None
    compare: bool = True


QUERIES = (
    Query(
        "01_classifier_rollup",
        "Свёртка по дереву классификатора (рекурсивный CTE)",
        postgres_args=(1,),
        clickhouse_params={"target_level": 1},
    ),
    Query("02_supplier_share", "Доля поставщика в закупках заказчика"),
    Query("03_price_deviation", "Отклонение цены лота от медианы позиции"),
    Query("04_period_lag", "Помесячная динамика через LAG"),
    Query("05_hhi", "Концентрация закупок: индекс Херфиндаля"),
    Query("06_gaps_islands", "Периоды непрерывной активности (gaps and islands)"),
)


@dataclass(frozen=True, slots=True)
class Measurement:
    query: str
    postgres_ms: float
    clickhouse_ms: float
    rows_pg: int
    rows_ch: int
    verdict: str

    @property
    def ratio(self) -> float:
        return 0.0 if self.clickhouse_ms == 0 else self.postgres_ms / self.clickhouse_ms


def read_sql(dialect: str, name: str) -> str:
    return (SQL_ROOT / dialect / f"{name}.sql").read_text(encoding="utf-8")


def normalise(value: Any) -> Any:
    """Привести значение к виду, сравнимому между базами.

    Числа сводятся к float: PostgreSQL отдаёт numeric как Decimal, ClickHouse —
    Decimal или Float64 в зависимости от выражения. Даты сводятся к строке:
    одна база отдаёт date, другая datetime той же полуночи.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, int | float):
        return float(value)
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    return str(value)


def rows_match(left: list[tuple[Any, ...]], right: list[tuple[Any, ...]]) -> str:
    if len(left) != len(right):
        return f"РАЗНОЕ ЧИСЛО СТРОК: {len(left)} и {len(right)}"

    for index, (row_a, row_b) in enumerate(zip(left, right, strict=True)):
        if len(row_a) != len(row_b):
            return f"строка {index}: разное число колонок"
        for column, (a, b) in enumerate(zip(row_a, row_b, strict=True)):
            na, nb = normalise(a), normalise(b)
            if isinstance(na, float) and isinstance(nb, float):
                scale = max(abs(na), abs(nb), 1.0)
                if abs(na - nb) / scale > TOLERANCE:
                    return f"строка {index}, колонка {column}: {na} против {nb}"
            elif na != nb:
                return f"строка {index}, колонка {column}: {na!r} против {nb!r}"
    return "совпадает"


async def run_postgres(
    connection: asyncpg.Connection, sql: str, args: tuple[Any, ...]
) -> tuple[list[tuple[Any, ...]], float]:
    durations: list[float] = []
    rows: list[Any] = []
    for _ in range(REPEATS + 1):
        started = time.perf_counter()
        rows = await connection.fetch(sql, *args)
        durations.append((time.perf_counter() - started) * 1000)
    # Первый прогон отбрасываем: он прогревает кэши.
    return [tuple(r.values()) for r in rows], statistics.median(durations[1:])


def run_clickhouse(
    client: Client, sql: str, params: dict[str, Any] | None
) -> tuple[list[tuple[Any, ...]], float]:
    durations: list[float] = []
    rows: list[Any] = []
    for _ in range(REPEATS + 1):
        started = time.perf_counter()
        rows = client.query(sql, parameters=params or {}).result_rows
        durations.append((time.perf_counter() - started) * 1000)
    return [tuple(r) for r in rows], statistics.median(durations[1:])


async def main() -> None:
    settings = get_settings()
    dsn = settings.postgres.dsn.replace("postgresql+asyncpg://", "postgresql://")
    connection: asyncpg.Connection = await asyncpg.connect(dsn)
    client = clickhouse_connect.get_client(
        host=settings.clickhouse.host,
        port=settings.clickhouse.port,
        database=settings.clickhouse.database,
    )

    results: list[Measurement] = []
    try:
        for query in QUERIES:
            print(f"\n{query.title}")
            pg_rows, pg_ms = await run_postgres(
                connection, read_sql("postgres", query.name), query.postgres_args
            )
            print(f"  PostgreSQL: {pg_ms:8.1f} мс, строк {len(pg_rows)}")

            ch_rows, ch_ms = run_clickhouse(
                client, read_sql("clickhouse", query.name), query.clickhouse_params
            )
            print(f"  ClickHouse: {ch_ms:8.1f} мс, строк {len(ch_rows)}")

            verdict = rows_match(pg_rows, ch_rows) if query.compare else "не сверялось"
            print(f"  результаты: {verdict}")

            results.append(
                Measurement(
                    query=query.title,
                    postgres_ms=pg_ms,
                    clickhouse_ms=ch_ms,
                    rows_pg=len(pg_rows),
                    rows_ch=len(ch_rows),
                    verdict=verdict,
                )
            )
    finally:
        await connection.close()
        client.close()

    print("\n\n## Замеры\n")
    print("| Запрос | PostgreSQL | ClickHouse | Отношение | Результаты |")
    print("|---|---|---|---|---|")
    for measurement in results:
        faster = (
            f"CH быстрее в {measurement.ratio:.1f}x"
            if measurement.ratio >= 1
            else f"PG быстрее в {1 / measurement.ratio:.1f}x"
        )
        print(
            f"| {measurement.query} | {measurement.postgres_ms:.0f} мс | "
            f"{measurement.clickhouse_ms:.0f} мс | {faster} | {measurement.verdict} |"
        )


if __name__ == "__main__":
    asyncio.run(main())
