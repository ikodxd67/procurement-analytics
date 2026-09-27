"""Демонстрация ReplacingMergeTree: когда схлопываются дубли и чего стоит FINAL.

Скрипт показывает три вещи по шагам:
1. сразу после вставки ревизии в таблице лежат ОБЕ версии записи;
2. FINAL показывает правильный ответ, но не меняет то, что на диске;
3. сколько FINAL стоит на двадцати миллионах строк.

Запуск (нужны поднятые контейнеры):
    .venv/Scripts/python scripts/demo_final.py
"""

from __future__ import annotations

import statistics
import time

import clickhouse_connect
from clickhouse_connect.driver import Client

DEMO_DB = "final_demo"
BENCH_TABLE = "bench.contracts_a"

ROW_TEMPLATE = (
    "({id}, '1', '400001-1', '950000000001', '900000000001', "
    "{total}, {total}, 0, {status}, '2026-01-10 00:00:00', "
    "'2026-01-11 00:00:00', '{updated}')"
)


def demo_small(client: Client) -> None:
    print("=" * 70)
    print("ЧАСТЬ 1. Две версии одного договора")
    print("=" * 70)

    client.command(f"DROP DATABASE IF EXISTS {DEMO_DB}")
    client.command(f"CREATE DATABASE {DEMO_DB}")
    client.command(f"""
        CREATE TABLE {DEMO_DB}.contracts
        (
            id UInt64, contract_number String, trd_buy_number_anno String,
            supplier_biin String, customer_bin String,
            contract_sum Decimal(18,2), contract_sum_wnds Decimal(18,2),
            fakt_sum Decimal(18,2), ref_contract_status_id UInt16,
            crdate DateTime, sign_date DateTime, last_update_date DateTime
        )
        ENGINE = ReplacingMergeTree(last_update_date)
        PARTITION BY toYYYYMM(crdate)
        ORDER BY (customer_bin, supplier_biin, id)
    """)

    first = ROW_TEMPLATE.format(id=1, total=100000, status=200, updated="2026-01-15 09:00:00")
    revised = ROW_TEMPLATE.format(id=1, total=250000, status=230, updated="2026-02-20 14:30:00")

    # Две отдельные вставки — именно так это происходит в жизни: первая при
    # первичной выкачке, вторая когда загрузчик увидел изменившийся договор.
    client.command(f"INSERT INTO {DEMO_DB}.contracts VALUES {first}")
    client.command(f"INSERT INTO {DEMO_DB}.contracts VALUES {revised}")

    plain = client.query(f"SELECT count() FROM {DEMO_DB}.contracts").result_rows[0][0]
    final = client.query(f"SELECT count() FROM {DEMO_DB}.contracts FINAL").result_rows[0][0]
    parts = client.query(
        "SELECT count() FROM system.parts WHERE active "
        f"AND database = '{DEMO_DB}' AND table = 'contracts'"
    ).result_rows[0][0]

    print(f"  строк без FINAL : {plain}   <- на диске лежат обе версии")
    print(f"  строк с FINAL   : {final}   <- движок склеил их на лету, при чтении")
    print(f"  кусков на диске : {parts}   <- каждая вставка создала свой кусок")

    sums = client.query(
        f"SELECT contract_sum, ref_contract_status_id FROM {DEMO_DB}.contracts ORDER BY last_update_date"
    ).result_rows
    final_sum = client.query(
        f"SELECT contract_sum, ref_contract_status_id FROM {DEMO_DB}.contracts FINAL"
    ).result_rows
    print(f"  без FINAL видно : {sums}")
    print(f"  с FINAL видно   : {final_sum}   <- осталась версия с более свежей датой")

    print("\n  А теперь сумма без FINAL — главная ловушка:")
    wrong = client.query(f"SELECT sum(contract_sum) FROM {DEMO_DB}.contracts").result_rows[0][0]
    right = client.query(f"SELECT sum(contract_sum) FROM {DEMO_DB}.contracts FINAL").result_rows[0][
        0
    ]
    print(f"  SELECT sum(...) без FINAL : {wrong}  <- сложились обе версии, цифра ложная")
    print(f"  SELECT sum(...) с FINAL   : {right}  <- верно")

    print("\n  Принудительное слияние (OPTIMIZE TABLE ... FINAL):")
    client.command(f"OPTIMIZE TABLE {DEMO_DB}.contracts FINAL")
    after = client.query(f"SELECT count() FROM {DEMO_DB}.contracts").result_rows[0][0]
    parts_after = client.query(
        "SELECT count() FROM system.parts WHERE active "
        f"AND database = '{DEMO_DB}' AND table = 'contracts'"
    ).result_rows[0][0]
    print(f"  строк без FINAL : {after}   <- теперь дубль убран физически")
    print(f"  кусков на диске : {parts_after}")


def timed(client: Client, sql: str, repeats: int = 5) -> float:
    runs = []
    for _ in range(repeats):
        started = time.perf_counter()
        client.query(sql)
        runs.append((time.perf_counter() - started) * 1000)
    return statistics.median(runs)


def demo_cost(client: Client) -> None:
    print()
    print("=" * 70)
    print("ЧАСТЬ 2. Чего стоит FINAL на 20 млн строк")
    print("=" * 70)

    exists = client.query(
        "SELECT count() FROM system.tables WHERE database = 'bench' AND name = 'contracts_a'"
    ).result_rows[0][0]
    if not exists:
        print("  Таблицы bench.contracts_a нет. Сначала: python scripts/bench_order_by.py")
        return

    # Доливаем ревизии, чтобы куски были неслитыми — так выглядит таблица в
    # проде сразу после загрузки, до того как фоновые слияния догонят.
    client.command("""
        INSERT INTO bench.contracts_a
        SELECT id, contract_number, trd_buy_number_anno, supplier_biin, customer_bin,
               contract_sum * 2, contract_sum_wnds * 2, fakt_sum,
               230, crdate, sign_date, last_update_date + toIntervalDay(30)
        FROM bench.contracts_a
        WHERE id <= 2000000
    """)

    rows_plain = client.query("SELECT count() FROM bench.contracts_a").result_rows[0][0]
    rows_final = client.query("SELECT count() FROM bench.contracts_a FINAL").result_rows[0][0]
    parts = client.query(
        "SELECT count() FROM system.parts WHERE active "
        "AND database = 'bench' AND table = 'contracts_a'"
    ).result_rows[0][0]

    print(f"  строк на диске        : {rows_plain:,}".replace(",", " "))
    print(f"  строк логически       : {rows_final:,}".replace(",", " "))
    print(f"  разница (дубли)       : {rows_plain - rows_final:,}".replace(",", " "))
    print(f"  активных кусков       : {parts}")

    q = "SELECT customer_bin, sum(contract_sum) FROM bench.contracts_a{final} GROUP BY customer_bin ORDER BY 2 DESC LIMIT 20"
    plain_ms = timed(client, q.format(final=""))
    final_ms = timed(client, q.format(final=" FINAL"))

    print()
    print(f"  запрос без FINAL      : {plain_ms:7.0f} мс   (ответ неверный, дубли сложены)")
    print(f"  запрос с FINAL        : {final_ms:7.0f} мс   (ответ верный)")
    print(f"  цена корректности     : x{final_ms / plain_ms:.1f}")


def main() -> None:
    client = clickhouse_connect.get_client(host="localhost", port=8123)
    demo_small(client)
    demo_cost(client)


if __name__ == "__main__":
    main()
