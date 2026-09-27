"""Эксперимент: как ключ сортировки влияет на скорость запросов в ClickHouse.

Что делает скрипт:
1. создаёт базу bench и одну таблицу-источник с синтетическими договорами;
2. раскладывает одни и те же данные в несколько таблиц, отличающихся только
   ключом ORDER BY;
3. гоняет типовые аналитические запросы и снимает замеры из system.query_log.

Данные синтетические, генерируются детерминированно средствами самого
ClickHouse: гнать двадцать миллионов строк через Python бессмысленно, узким
местом станет Python, а не то, что мы измеряем.

Запуск:
    .venv/Scripts/python scripts/bench_order_by.py
"""

from __future__ import annotations

import argparse
import statistics
import time
import uuid
from dataclasses import dataclass

import clickhouse_connect
from clickhouse_connect.driver import Client

DB = "bench"
SOURCE = f"{DB}.contracts_source"

# Варианты ключа сортировки. Во всех обязан присутствовать id: ReplacingMergeTree
# схлопывает дубли по полному ключу сортировки, и без id в ключе две ревизии
# одного договора считались бы разными строками.
#
# Изменяемых полей (статус, суммы) в ключе быть не должно по той же причине:
# при ревизии поле меняется, ключ становится другим, и старая строка останется
# жить рядом с новой.
VARIANTS: dict[str, str] = {
    "A: (customer_bin, supplier_biin, id)": "(customer_bin, supplier_biin, id)",
    "B: (supplier_biin, customer_bin, id)": "(supplier_biin, customer_bin, id)",
    "C: (crdate, id)": "(crdate, id)",
    "D: (id)": "(id)",
}

COLUMNS = """
    id                     UInt64,
    contract_number        String,
    trd_buy_number_anno    String,
    supplier_biin          String,
    customer_bin           String,
    contract_sum           Decimal(18, 2),
    contract_sum_wnds      Decimal(18, 2),
    fakt_sum               Decimal(18, 2),
    ref_contract_status_id UInt16,
    crdate                 DateTime,
    sign_date              DateTime,
    last_update_date       DateTime
"""

GENERATE = """
SELECT
    number + 1                                                                   AS id,
    toString(cityHash64(number, 2) % 900 + 1)                                    AS contract_number,
    concat(toString(400000 + cityHash64(number, 4) % 99999), '-1')               AS trd_buy_number_anno,
    concat('95', leftPad(toString(cityHash64(number, 7) % 150000), 10, '0'))     AS supplier_biin,
    concat('9', leftPad(toString(cityHash64(number) % 60000), 11, '0'))          AS customer_bin,
    toDecimal64(cityHash64(number, 11) % 2000000000 / 100, 2)                    AS contract_sum,
    toDecimal64(cityHash64(number, 12) % 2200000000 / 100, 2)                    AS contract_sum_wnds,
    toDecimal64(cityHash64(number, 13) % 2000000000 / 100, 2)                    AS fakt_sum,
    toUInt16(200 + cityHash64(number, 5) % 4)                                    AS ref_contract_status_id,
    toDateTime('2022-01-01 00:00:00') + toIntervalSecond(cityHash64(number, 3) % 149000000) AS crdate,
    toDateTime('2022-01-01 00:00:00') + toIntervalSecond(cityHash64(number, 3) % 149000000 + 86400) AS sign_date,
    toDateTime('2026-01-01 00:00:00') + toIntervalSecond(cityHash64(number, 9) % 20000000)  AS last_update_date
FROM numbers({rows})
"""

# Каждая ревизия — та же запись с более свежим last_update_date. Именно их
# должен схлопнуть ReplacingMergeTree.
REVISIONS = """
SELECT
    number + 1                                                                   AS id,
    toString(cityHash64(number, 2) % 900 + 1)                                    AS contract_number,
    concat(toString(400000 + cityHash64(number, 4) % 99999), '-1')               AS trd_buy_number_anno,
    concat('95', leftPad(toString(cityHash64(number, 7) % 150000), 10, '0'))     AS supplier_biin,
    concat('9', leftPad(toString(cityHash64(number) % 60000), 11, '0'))          AS customer_bin,
    toDecimal64(cityHash64(number, 21) % 2000000000 / 100, 2)                    AS contract_sum,
    toDecimal64(cityHash64(number, 22) % 2200000000 / 100, 2)                    AS contract_sum_wnds,
    toDecimal64(cityHash64(number, 23) % 2000000000 / 100, 2)                    AS fakt_sum,
    toUInt16(210 + cityHash64(number, 25) % 3)                                   AS ref_contract_status_id,
    toDateTime('2022-01-01 00:00:00') + toIntervalSecond(cityHash64(number, 3) % 149000000) AS crdate,
    toDateTime('2022-01-01 00:00:00') + toIntervalSecond(cityHash64(number, 3) % 149000000 + 86400) AS sign_date,
    toDateTime('2026-06-01 00:00:00') + toIntervalSecond(cityHash64(number, 9) % 5000000)   AS last_update_date
FROM numbers({rows})
"""

QUERIES: dict[str, str] = {
    "Q1 поставщики одного заказчика": """
        SELECT supplier_biin, count() AS contracts, sum(contract_sum) AS total
        FROM {table}
        WHERE customer_bin = '900000033810'
        GROUP BY supplier_biin ORDER BY total DESC LIMIT 20
    """,
    "Q2 заказчики одного поставщика": """
        SELECT customer_bin, count() AS contracts, sum(contract_sum) AS total
        FROM {table}
        WHERE supplier_biin = '950000001231'
        GROUP BY customer_bin ORDER BY total DESC LIMIT 20
    """,
    "Q3 помесячная динамика за год": """
        SELECT toYYYYMM(crdate) AS month, count() AS contracts, sum(contract_sum) AS total
        FROM {table}
        WHERE crdate >= '2024-01-01' AND crdate < '2025-01-01'
        GROUP BY month ORDER BY month
    """,
    "Q4 всё по всем заказчикам": """
        SELECT customer_bin, sum(contract_sum) AS total
        FROM {table}
        GROUP BY customer_bin ORDER BY total DESC LIMIT 20
    """,
}


@dataclass(frozen=True, slots=True)
class Measurement:
    variant: str
    query: str
    median_ms: float
    read_rows: int
    read_mb: float


def table_name(variant: str) -> str:
    letter = variant.split(":", 1)[0].strip().lower()
    return f"{DB}.contracts_{letter}"


def prepare_source(client: Client, rows: int, revisions: int) -> None:
    print(f"Готовлю источник: {rows:,} записей + {revisions:,} ревизий".replace(",", " "))
    client.command(f"DROP DATABASE IF EXISTS {DB}")
    client.command(f"CREATE DATABASE {DB}")
    client.command(f"CREATE TABLE {SOURCE} ({COLUMNS}) ENGINE = MergeTree ORDER BY tuple()")

    started = time.perf_counter()
    client.command(f"INSERT INTO {SOURCE} {GENERATE.format(rows=rows)}")
    client.command(f"INSERT INTO {SOURCE} {REVISIONS.format(rows=revisions)}")
    total = client.query(f"SELECT count() FROM {SOURCE}").result_rows[0][0]
    print(
        f"  готово за {time.perf_counter() - started:.1f} с, строк в источнике: {total:,}".replace(
            ",", " "
        )
    )


def build_variant(client: Client, variant: str, order_by: str) -> tuple[int, float]:
    table = table_name(variant)
    client.command(f"DROP TABLE IF EXISTS {table}")
    client.command(
        f"CREATE TABLE {table} ({COLUMNS}) "
        f"ENGINE = ReplacingMergeTree(last_update_date) "
        f"PARTITION BY toYYYYMM(crdate) "
        f"ORDER BY {order_by}"
    )
    started = time.perf_counter()
    client.command(f"INSERT INTO {table} SELECT * FROM {SOURCE}")
    # OPTIMIZE FINAL нужен, чтобы сравнивать одинаково слитые таблицы. В бою
    # так делать нельзя — почему, разобрано в девлоге.
    client.command(f"OPTIMIZE TABLE {table} FINAL")
    build_s = time.perf_counter() - started

    size_mb = client.query(
        "SELECT round(sum(bytes_on_disk) / 1024 / 1024, 1) FROM system.parts "
        f"WHERE active AND database = '{DB}' AND table = '{table.split('.')[1]}'"
    ).result_rows[0][0]
    rows = client.query(f"SELECT count() FROM {table}").result_rows[0][0]
    print(
        f"  {variant:<40} собрана за {build_s:6.1f} с, "
        f"строк после схлопывания: {rows:>10,}, на диске: {size_mb} МБ".replace(",", " ")
    )
    return rows, float(size_mb)


def measure(client: Client, variant: str, name: str, sql: str, repeats: int) -> Measurement:
    table = table_name(variant)
    body = sql.format(table=table)
    marker = uuid.uuid4().hex
    durations: list[float] = []

    for _ in range(repeats):
        client.query(f"-- bench:{marker}\n{body}")

    client.command("SYSTEM FLUSH LOGS")
    rows = client.query(
        "SELECT query_duration_ms, read_rows, read_bytes FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query LIKE '%bench:{marker}%' "
        "AND query NOT LIKE '%system.query_log%'"
    ).result_rows

    durations = [float(r[0]) for r in rows]
    read_rows = max(int(r[1]) for r in rows)
    read_bytes = max(int(r[2]) for r in rows)

    return Measurement(
        variant=variant,
        query=name,
        median_ms=statistics.median(durations),
        read_rows=read_rows,
        read_mb=round(read_bytes / 1024 / 1024, 1),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000_000)
    parser.add_argument("--revisions", type=int, default=2_000_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8123)
    args = parser.parse_args()

    client = clickhouse_connect.get_client(host=args.host, port=args.port)

    prepare_source(client, args.rows, args.revisions)

    print("\nСобираю варианты:")
    shapes: dict[str, tuple[int, float]] = {}
    for variant, order_by in VARIANTS.items():
        shapes[variant] = build_variant(client, variant, order_by)

    print(f"\nЗамеряю запросы (медиана из {args.repeats} прогонов, кэши прогреты):\n")
    results: list[Measurement] = []
    for variant in VARIANTS:
        for name, sql in QUERIES.items():
            results.append(measure(client, variant, name, sql, args.repeats))
            print(f"  {variant:<40} {name:<32} {results[-1].median_ms:8.1f} мс")

    print("\n\n## Результат\n")
    print("| Запрос | " + " | ".join(VARIANTS) + " |")
    print("|---" * (len(VARIANTS) + 1) + "|")
    for name in QUERIES:
        cells = []
        for variant in VARIANTS:
            m = next(r for r in results if r.variant == variant and r.query == name)
            cells.append(f"{m.median_ms:.0f} мс / {m.read_rows / 1e6:.1f} млн строк")
        print(f"| {name} | " + " | ".join(cells) + " |")

    print("\n| Вариант | Строк после схлопывания | На диске |")
    print("|---|---|---|")
    for variant, (rows, size_mb) in shapes.items():
        print(f"| {variant} | {rows:,} | {size_mb} МБ |".replace(",", " "))


if __name__ == "__main__":
    main()
