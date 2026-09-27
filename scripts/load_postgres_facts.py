"""Копия фактов в PostgreSQL для сравнения запросов.

Зачем она нужна. Архитектура проекта кладёт факты в ClickHouse, и это
обосновано. Но утверждение «в ClickHouse быстрее» без цифр — просто мнение.
Чтобы сравнить, те же самые данные должны лежать в обеих базах.

Схема называется bench и существует только ради замеров. Боевой код в неё не
ходит.

Данные не генерируются заново, а переливаются из ClickHouse. Так исключается
возражение «а данные точно одинаковые»: они буквально одни и те же.

Индексы в PostgreSQL созданы намеренно и по делу. Сравнивать колоночную базу с
неиндексированной строчной нечестно: любой инженер, положив эти данные в
PostgreSQL, индексы бы создал. Сравнение имеет смысл только если каждой базе
дали то, что ей полагается.

Запуск:
    .venv/Scripts/python scripts/load_postgres_facts.py
"""

from __future__ import annotations

import asyncio
import time

import asyncpg
import clickhouse_connect

from procurement.config import get_settings

CHUNK = 100_000

CONTRACTS_DDL = """
CREATE TABLE bench.contracts (
    id                     bigint PRIMARY KEY,
    contract_number        text        NOT NULL,
    trd_buy_number_anno    text        NOT NULL,
    supplier_biin          text        NOT NULL,
    customer_bin           text        NOT NULL,
    contract_sum           numeric(18,2) NOT NULL,
    contract_sum_wnds      numeric(18,2) NOT NULL,
    fakt_sum               numeric(18,2) NOT NULL,
    ref_contract_status_id smallint    NOT NULL,
    crdate                 timestamp   NOT NULL,
    sign_date              timestamp   NOT NULL,
    last_update_date       timestamp   NOT NULL
)
"""

LOTS_DDL = """
CREATE TABLE bench.lots (
    id                  bigint PRIMARY KEY,
    lot_number          text        NOT NULL,
    ref_lot_status_id   smallint    NOT NULL,
    customer_bin        text        NOT NULL,
    trd_buy_number_anno text        NOT NULL,
    name_ru             text        NOT NULL,
    enstru_code         text        NOT NULL,
    count               numeric(18,3) NOT NULL,
    amount              numeric(18,2) NOT NULL,
    last_update_date    timestamp   NOT NULL
)
"""

INDEXES = (
    "CREATE INDEX ix_bench_contracts_customer ON bench.contracts (customer_bin)",
    "CREATE INDEX ix_bench_contracts_supplier ON bench.contracts (supplier_biin)",
    "CREATE INDEX ix_bench_contracts_crdate ON bench.contracts (crdate)",
    "CREATE INDEX ix_bench_contracts_customer_crdate ON bench.contracts (customer_bin, crdate)",
    "CREATE INDEX ix_bench_lots_enstru ON bench.lots (enstru_code)",
    "CREATE INDEX ix_bench_lots_customer ON bench.lots (customer_bin)",
)

CONTRACT_COLUMNS = (
    "id, contract_number, trd_buy_number_anno, supplier_biin, customer_bin, "
    "contract_sum, contract_sum_wnds, fakt_sum, ref_contract_status_id, "
    "crdate, sign_date, last_update_date"
)
LOT_COLUMNS = (
    "id, lot_number, ref_lot_status_id, customer_bin, trd_buy_number_anno, "
    "name_ru, enstru_code, count, amount, last_update_date"
)
CLASSIFIER_COLUMNS = "code, parent_code, level, name_ru"

CLASSIFIER_DDL = """
CREATE TABLE bench.ref_classifier (
    code        text PRIMARY KEY,
    parent_code text,
    level       smallint NOT NULL,
    name_ru     text     NOT NULL
)
"""


def raw_dsn() -> str:
    """asyncpg не понимает префикс +asyncpg из строки SQLAlchemy."""
    return get_settings().postgres.dsn.replace("postgresql+asyncpg://", "postgresql://")


async def copy_table(
    connection: asyncpg.Connection,
    clickhouse: clickhouse_connect.driver.Client,
    *,
    source: str,
    target: str,
    columns: str,
) -> int:
    column_list = [c.strip() for c in columns.split(",")]
    total = 0
    started = time.perf_counter()

    # Потоком по блокам, а не одним SELECT: два миллиона строк целиком в
    # память класть незачем, и COPY всё равно работает порциями.
    with clickhouse.query_row_block_stream(f"SELECT {columns} FROM {source}") as stream:
        for block in stream:
            await connection.copy_records_to_table(
                target.split(".")[1],
                schema_name=target.split(".")[0],
                records=block,
                columns=column_list,
            )
            total += len(block)

    print(f"  {target}: {total:,} строк за {time.perf_counter() - started:.1f} с".replace(",", " "))
    return total


async def main() -> None:
    settings = get_settings()
    clickhouse = clickhouse_connect.get_client(
        host=settings.clickhouse.host,
        port=settings.clickhouse.port,
        database=settings.clickhouse.database,
    )
    connection: asyncpg.Connection = await asyncpg.connect(raw_dsn())

    try:
        print("Готовлю схему bench")
        await connection.execute("DROP SCHEMA IF EXISTS bench CASCADE")
        await connection.execute("CREATE SCHEMA bench")
        await connection.execute(CONTRACTS_DDL)
        await connection.execute(LOTS_DDL)
        await connection.execute(CLASSIFIER_DDL)

        print("Переливаю данные из ClickHouse")
        await copy_table(
            connection,
            clickhouse,
            source="contracts FINAL",
            target="bench.contracts",
            columns=CONTRACT_COLUMNS,
        )
        await copy_table(
            connection,
            clickhouse,
            source="lots FINAL",
            target="bench.lots",
            columns=LOT_COLUMNS,
        )
        await copy_table(
            connection,
            clickhouse,
            source="ref_classifier",
            target="bench.ref_classifier",
            columns=CLASSIFIER_COLUMNS,
        )

        print("Строю индексы")
        started = time.perf_counter()
        for statement in INDEXES:
            await connection.execute(statement)
        print(f"  готово за {time.perf_counter() - started:.1f} с")

        # Без свежей статистики планировщик PostgreSQL выбирает планы вслепую.
        # Замер на таблице без ANALYZE измерял бы не базу, а невезение.
        print("Обновляю статистику (ANALYZE)")
        started = time.perf_counter()
        for table in ("bench.contracts", "bench.lots", "bench.ref_classifier"):
            await connection.execute(f"ANALYZE {table}")
        print(f"  готово за {time.perf_counter() - started:.1f} с")

        size = await connection.fetchval(
            "SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'bench' AND c.relkind = 'r'"
        )
        print(f"Схема bench занимает {size} (с индексами)")
    finally:
        await connection.close()
        clickhouse.close()


if __name__ == "__main__":
    asyncio.run(main())
