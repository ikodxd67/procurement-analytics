"""Тесты хранилища фактов на настоящем ClickHouse в контейнере.

Проверяется поведение движка, а не наш код вокруг него. Это осмысленно: вся
схема построена на конкретных обещаниях ReplacingMergeTree, и если они не
выполняются так, как мы думаем, ошибка будет тихой — цифры в отчётах просто
окажутся неверными.

Ключевое, что тут закреплено: без FINAL сумма считается по всем версиям
записи, включая устаревшие. Этот тест не даст однажды «оптимизировать» запрос
выкидыванием FINAL.

Тесты помечены как integration, им нужен Docker.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import pytest
from clickhouse_connect.driver import AsyncClient
from pydantic import SecretStr
from testcontainers.community.clickhouse import ClickHouseContainer

from procurement.config import ClickHouseSettings
from procurement.storage.clickhouse.client import build_client
from procurement.storage.clickhouse.ddl import apply_schema

pytestmark = pytest.mark.integration

CONTRACT_COLUMNS = (
    "id, contract_number, trd_buy_number_anno, supplier_biin, customer_bin, "
    "contract_sum, contract_sum_wnds, fakt_sum, ref_contract_status_id, "
    "crdate, sign_date, last_update_date"
)


def contract_values(
    *,
    contract_id: int,
    total: int,
    status: int,
    updated: str,
    customer: str = "900140000101",
    supplier: str = "950140000111",
    crdate: str = "2026-01-10 00:00:00",
) -> str:
    return (
        f"({contract_id}, '1', '400001-1', '{supplier}', '{customer}', "
        f"{total}, {total}, 0, {status}, '{crdate}', '{crdate}', '{updated}')"
    )


@pytest.fixture(scope="module")
def clickhouse_settings() -> Iterator[ClickHouseSettings]:
    # port=8123 задан явно: по умолчанию контейнер публикует 9000, родной
    # протокол, а clickhouse-connect ходит по HTTP. Логин и пароль тоже
    # задаём сами — иначе библиотека сгенерирует свои, а мы о них не узнаем.
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
    await created.command("DROP TABLE IF EXISTS contracts")
    await created.command("DROP TABLE IF EXISTS lots")
    await apply_schema(created)
    yield created
    await created.close()


# --- схема ---------------------------------------------------------------------


async def test_schema_uses_expected_engine_and_keys(client: AsyncClient) -> None:
    ddl = (await client.query("SHOW CREATE TABLE contracts")).result_rows[0][0]

    assert "ReplacingMergeTree(last_update_date)" in ddl
    assert "PARTITION BY toYYYYMM(crdate)" in ddl
    assert "ORDER BY (customer_bin, supplier_biin, id)" in ddl


async def test_money_columns_are_decimal_not_float(client: AsyncClient) -> None:
    """Деньги во Float64 накапливают ошибку округления. Для сумм это недопустимо."""
    rows = (
        await client.query(
            "SELECT name, type FROM system.columns WHERE table = 'contracts' AND name LIKE '%sum%'"
        )
    ).result_rows

    assert rows, "колонки с суммами должны существовать"
    for name, column_type in rows:
        assert column_type.startswith("Decimal"), f"{name} имеет тип {column_type}"


async def test_lots_are_not_partitioned_by_a_mutable_date(client: AsyncClient) -> None:
    """У лота нет стабильной даты, поэтому по last_update_date партиционировать нельзя.

    Ревизия уехала бы в другую партицию и никогда не схлопнулась бы с
    оригиналом: дедупликация работает только внутри партиции.
    """
    ddl = (await client.query("SHOW CREATE TABLE lots")).result_rows[0][0]

    assert "PARTITION BY intDiv(id, 5000000)" in ddl
    assert "last_update_date" not in ddl.split("PARTITION BY")[1].split("ORDER BY")[0]


# --- поведение ReplacingMergeTree ----------------------------------------------


async def test_revision_does_not_replace_the_row_immediately(client: AsyncClient) -> None:
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(contract_id=1, total=100000, status=200, updated="2026-01-15 09:00:00")
    )
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(contract_id=1, total=250000, status=230, updated="2026-02-20 14:30:00")
    )

    plain = (await client.query("SELECT count() FROM contracts")).result_rows[0][0]
    final = (await client.query("SELECT count() FROM contracts FINAL")).result_rows[0][0]

    assert plain == 2, "обе версии лежат на диске до слияния"
    assert final == 1, "FINAL склеивает их при чтении"


async def test_sum_without_final_is_wrong(client: AsyncClient) -> None:
    """Главная ловушка движка, ради которой этот тест и существует."""
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(contract_id=1, total=100000, status=200, updated="2026-01-15 09:00:00")
    )
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(contract_id=1, total=250000, status=230, updated="2026-02-20 14:30:00")
    )

    naive = (await client.query("SELECT sum(contract_sum) FROM contracts")).result_rows[0][0]
    correct = (await client.query("SELECT sum(contract_sum) FROM contracts FINAL")).result_rows[0][
        0
    ]

    assert naive == 350000, "без FINAL складываются обе версии — цифра ложная"
    assert correct == 250000, "с FINAL остаётся только свежая версия"


async def test_latest_version_wins_regardless_of_insert_order(client: AsyncClient) -> None:
    """Побеждает бóльшая версия, а не та, что вставлена последней."""
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(contract_id=1, total=250000, status=230, updated="2026-02-20 14:30:00")
    )
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(contract_id=1, total=100000, status=200, updated="2026-01-15 09:00:00")
    )

    rows = (
        await client.query("SELECT contract_sum, ref_contract_status_id FROM contracts FINAL")
    ).result_rows

    assert rows == [(250000, 230)], "устаревшая версия не должна перезаписать свежую"


async def test_optimize_final_collapses_duplicates_physically(client: AsyncClient) -> None:
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(contract_id=1, total=100000, status=200, updated="2026-01-15 09:00:00")
    )
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(contract_id=1, total=250000, status=230, updated="2026-02-20 14:30:00")
    )

    await client.command("OPTIMIZE TABLE contracts FINAL")

    plain = (await client.query("SELECT count() FROM contracts")).result_rows[0][0]
    assert plain == 1, "после принудительного слияния дубль убран с диска"


async def test_duplicates_in_different_partitions_never_collapse_on_disk(
    client: AsyncClient,
) -> None:
    """Почему ключ партиционирования обязан быть неизменным.

    Здесь crdate у двух версий одной записи разный — как если бы мы
    партиционировали по дате изменения. Проверено на ClickHouse 24.8:

    * физическое слияние идёт ТОЛЬКО внутри партиции, поэтому даже
      принудительный OPTIMIZE FINAL дубль не убирает — на диске он остаётся
      навсегда;
    * SELECT ... FINAL при этом ответ даёт верный, потому что настройка
      do_not_merge_across_partitions_select_final по умолчанию равна 0 и
      склейка при чтении идёт через партиции тоже.

    То есть беда не в неверных цифрах, а в том, что таблица растёт без предела
    и каждый запрос навсегда обязан платить за FINAL: схлопнуть эти дубли
    физически невозможно в принципе.
    """
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(
            contract_id=7,
            total=100000,
            status=200,
            updated="2026-01-15 09:00:00",
            crdate="2026-01-10 00:00:00",
        )
    )
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(
            contract_id=7,
            total=250000,
            status=230,
            updated="2026-02-20 14:30:00",
            crdate="2026-03-10 00:00:00",
        )
    )

    await client.command("OPTIMIZE TABLE contracts FINAL")

    on_disk = (await client.query("SELECT count() FROM contracts")).result_rows[0][0]
    logical = (await client.query("SELECT count() FROM contracts FINAL")).result_rows[0][0]
    parts = (
        await client.query("SELECT count() FROM system.parts WHERE active AND table = 'contracts'")
    ).result_rows[0][0]

    assert on_disk == 2, "через границу партиций физическое слияние невозможно"
    assert parts == 2, "две партиции — два куска, слить их движок не станет"
    assert logical == 1, "но при чтении FINAL склеивает и через партиции"


async def test_different_keys_are_not_confused(client: AsyncClient) -> None:
    await client.command(
        f"INSERT INTO contracts ({CONTRACT_COLUMNS}) VALUES "
        + contract_values(contract_id=1, total=100000, status=200, updated="2026-01-15 09:00:00")
        + ", "
        + contract_values(contract_id=2, total=300000, status=200, updated="2026-01-15 09:00:00")
    )

    await client.command("OPTIMIZE TABLE contracts FINAL")

    total = (await client.query("SELECT count() FROM contracts FINAL")).result_rows[0][0]
    assert total == 2, "разные договоры схлопываться не должны"
