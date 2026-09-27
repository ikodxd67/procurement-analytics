"""Тесты правильности аналитических запросов.

Скорость меряет scripts/bench_sql.py, а здесь проверяется, что запросы дают
верный ответ. Набор данных крошечный и подобран так, чтобы результат считался
руками: иначе тест проверял бы, что запрос выдаёт то же, что выдавал вчера, а
не то, что должен.

Запросы берутся из тех же файлов в sql/, которые идут в замеры и в
документацию. Копии в тесте не было бы смысла: разошлись бы через неделю.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
from clickhouse_connect.driver import AsyncClient
from pydantic import SecretStr
from testcontainers.community.clickhouse import ClickHouseContainer

from procurement.config import ClickHouseSettings
from procurement.storage.clickhouse.client import build_client
from procurement.storage.clickhouse.ddl import apply_schema

pytestmark = pytest.mark.integration

SQL_ROOT = Path(__file__).resolve().parent.parent / "sql" / "clickhouse"


def sql(name: str) -> str:
    return (SQL_ROOT / name).read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def clickhouse_settings() -> Iterator[ClickHouseSettings]:
    container = ClickHouseContainer(
        "clickhouse/clickhouse-server:24.8-alpine",
        port=8123,
        username="test",
        password="test",
        dbname="test",
    )
    with container:
        yield ClickHouseSettings(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(8123)),
            database="test",
            user="test",
            password=SecretStr("test"),
        )


@pytest.fixture
async def client(clickhouse_settings: ClickHouseSettings) -> AsyncIterator[AsyncClient]:
    created = await build_client(clickhouse_settings)
    for table in ("contracts", "lots", "ref_classifier"):
        await created.command(f"DROP TABLE IF EXISTS {table}")
    await apply_schema(created)
    await created.command("""
        CREATE TABLE ref_classifier
        (code String, parent_code String, level UInt8, name_ru String)
        ENGINE = MergeTree ORDER BY code
    """)
    yield created
    await created.close()


async def add_contract(
    client: AsyncClient,
    *,
    contract_id: int,
    customer: str,
    supplier: str,
    total: int,
    crdate: str,
) -> None:
    await client.command(
        "INSERT INTO contracts (id, contract_number, trd_buy_number_anno, supplier_biin, "
        "customer_bin, contract_sum, contract_sum_wnds, fakt_sum, ref_contract_status_id, "
        f"crdate, sign_date, last_update_date) VALUES ({contract_id}, '1', '1-1', "
        f"'{supplier}', '{customer}', {total}, {total}, 0, 230, '{crdate}', "
        f"'{crdate}', '{crdate}')"
    )


# --- доля поставщика --------------------------------------------------------------


async def test_supplier_share_sums_to_hundred(client: AsyncClient) -> None:
    """У заказчика два поставщика на 750 и 250. Доли обязаны быть 75 и 25."""
    await add_contract(
        client, contract_id=1, customer="C1", supplier="S1", total=500, crdate="2024-01-10"
    )
    await add_contract(
        client, contract_id=2, customer="C1", supplier="S1", total=250, crdate="2024-02-10"
    )
    await add_contract(
        client, contract_id=3, customer="C1", supplier="S2", total=250, crdate="2024-01-15"
    )

    rows = (await client.query(sql("02_supplier_share.sql"))).result_rows
    shares = {row[1]: (row[2], float(row[3]), float(row[5])) for row in rows}

    assert shares["S1"] == (2, 750.0, 75.0)
    assert shares["S2"] == (1, 250.0, 25.0)
    assert sum(value[2] for value in shares.values()) == pytest.approx(100.0)


async def test_supplier_share_separates_customers(client: AsyncClient) -> None:
    """Окно разделено по заказчику: доли одного не влияют на доли другого."""
    await add_contract(
        client, contract_id=1, customer="C1", supplier="S1", total=100, crdate="2024-01-10"
    )
    await add_contract(
        client, contract_id=2, customer="C2", supplier="S1", total=900, crdate="2024-01-10"
    )

    rows = (await client.query(sql("02_supplier_share.sql"))).result_rows
    by_customer = {row[0]: float(row[5]) for row in rows}

    assert by_customer["C1"] == 100.0, "единственный поставщик заказчика — это 100%"
    assert by_customer["C2"] == 100.0


# --- gaps and islands ---------------------------------------------------------------


async def test_gaps_and_islands_splits_on_a_missing_month(client: AsyncClient) -> None:
    """Январь, февраль, пропуск марта, апрель, май.

    Ожидаем два отрезка: два месяца и два месяца, а не один из четырёх.
    """
    months = ["2024-01-10", "2024-02-10", "2024-04-10", "2024-05-10"]
    for index, day in enumerate(months, start=1):
        await add_contract(
            client, contract_id=index, customer="C1", supplier="S1", total=100, crdate=day
        )

    rows = (await client.query(sql("06_gaps_islands.sql"))).result_rows

    assert len(rows) == 2, f"ожидали два отрезка, получили {len(rows)}"
    lengths = sorted(int(row[3]) for row in rows)
    assert lengths == [2, 2]


async def test_gaps_and_islands_keeps_a_continuous_run_whole(client: AsyncClient) -> None:
    for index, month in enumerate(range(1, 6), start=1):
        await add_contract(
            client,
            contract_id=index,
            customer="C1",
            supplier="S1",
            total=100,
            crdate=f"2024-{month:02d}-10",
        )

    rows = (await client.query(sql("06_gaps_islands.sql"))).result_rows

    assert len(rows) == 1
    assert int(rows[0][3]) == 5


async def test_gaps_and_islands_crosses_the_year_boundary(client: AsyncClient) -> None:
    """Ноябрь, декабрь, январь — один отрезок, а не два.

    Именно здесь ломается наивный вариант, считающий разницу по номеру месяца
    внутри года: с декабря на январь номер падает с 12 на 1.
    """
    for index, day in enumerate(["2024-11-10", "2024-12-10", "2025-01-10"], start=1):
        await add_contract(
            client, contract_id=index, customer="C1", supplier="S1", total=100, crdate=day
        )

    rows = (await client.query(sql("06_gaps_islands.sql"))).result_rows

    assert len(rows) == 1, "граница года не должна разрывать отрезок"
    assert int(rows[0][3]) == 3


async def test_gaps_and_islands_separates_suppliers(client: AsyncClient) -> None:
    await add_contract(
        client, contract_id=1, customer="C1", supplier="S1", total=100, crdate="2024-01-10"
    )
    await add_contract(
        client, contract_id=2, customer="C1", supplier="S2", total=100, crdate="2024-02-10"
    )

    rows = (await client.query(sql("06_gaps_islands.sql"))).result_rows

    assert len(rows) == 2, "разные поставщики не склеиваются в один отрезок"
    assert all(int(row[3]) == 1 for row in rows)


# --- свёртка по классификатору --------------------------------------------------------


async def seed_tree(client: AsyncClient) -> None:
    nodes: list[list[Any]] = [
        ["01", "", 0, "Раздел один"],
        ["01.1", "01", 1, "Группа 01.1"],
        ["01.1.1", "01.1", 2, "Подгруппа 01.1.1"],
        ["01.1.1.001", "01.1.1", 3, "Позиция 001"],
        ["01.1.1.002", "01.1.1", 3, "Позиция 002"],
        ["02", "", 0, "Раздел два"],
        ["02.1", "02", 1, "Группа 02.1"],
        ["02.1.1", "02.1", 2, "Подгруппа 02.1.1"],
        ["02.1.1.001", "02.1.1", 3, "Позиция 003"],
    ]
    await client.insert(
        "ref_classifier", nodes, column_names=["code", "parent_code", "level", "name_ru"]
    )


async def add_lot(client: AsyncClient, *, lot_id: int, code: str, amount: int) -> None:
    await client.command(
        "INSERT INTO lots (id, lot_number, ref_lot_status_id, customer_bin, "
        "trd_buy_number_anno, name_ru, enstru_code, count, amount, last_update_date) "
        f"VALUES ({lot_id}, 'L{lot_id}', 230, 'C1', '1-1', 'нечто', '{code}', 1, "
        f"{amount}, '2024-01-10')"
    )


async def test_classifier_rollup_climbs_to_the_requested_level(client: AsyncClient) -> None:
    """Два листа одного раздела должны сложиться, лист другого — остаться отдельно."""
    await seed_tree(client)
    await add_lot(client, lot_id=1, code="01.1.1.001", amount=100)
    await add_lot(client, lot_id=2, code="01.1.1.002", amount=200)
    await add_lot(client, lot_id=3, code="02.1.1.001", amount=50)

    rows = (
        await client.query(sql("01_classifier_rollup.sql"), parameters={"target_level": 0})
    ).result_rows
    totals = {row[0]: float(row[3]) for row in rows}

    assert totals == {"01": 300.0, "02": 50.0}


async def test_classifier_rollup_to_a_deeper_level(client: AsyncClient) -> None:
    await seed_tree(client)
    await add_lot(client, lot_id=1, code="01.1.1.001", amount=100)
    await add_lot(client, lot_id=2, code="02.1.1.001", amount=50)

    rows = (
        await client.query(sql("01_classifier_rollup.sql"), parameters={"target_level": 2})
    ).result_rows
    totals = {row[0]: float(row[3]) for row in rows}

    assert totals == {"01.1.1": 100.0, "02.1.1": 50.0}


async def test_classifier_rollup_at_leaf_level_changes_nothing(client: AsyncClient) -> None:
    await seed_tree(client)
    await add_lot(client, lot_id=1, code="01.1.1.001", amount=100)
    await add_lot(client, lot_id=2, code="01.1.1.002", amount=200)

    rows = (
        await client.query(sql("01_classifier_rollup.sql"), parameters={"target_level": 3})
    ).result_rows
    totals = {row[0]: float(row[3]) for row in rows}

    assert totals == {"01.1.1.001": 100.0, "01.1.1.002": 200.0}


# --- LAG ------------------------------------------------------------------------------


async def test_lag_gives_null_for_the_first_period(client: AsyncClient) -> None:
    """У первого месяца предыдущего нет, и это должно быть NULL, а не ноль.

    Ноль означал бы «в прошлом месяце закупок не было», что неправда: прошлого
    месяца в данных просто нет. Первая версия запроса для ClickHouse возвращала
    здесь ноль, и сравнение с PostgreSQL разошлось именно на этом.
    """
    await add_contract(
        client, contract_id=1, customer="C1", supplier="S1", total=100, crdate="2024-01-10"
    )
    await add_contract(
        client, contract_id=2, customer="C1", supplier="S1", total=150, crdate="2024-02-10"
    )

    rows = (await client.query(sql("04_period_lag.sql"))).result_rows

    assert len(rows) == 2
    assert rows[0][3] is None, "у первого месяца предыдущего нет"
    assert float(rows[1][3]) == 100.0
    assert float(rows[1][5]) == 50.0, "рост со 100 до 150 — это плюс 50 процентов"
