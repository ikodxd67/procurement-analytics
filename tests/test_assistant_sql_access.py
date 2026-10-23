"""Пользователь агента на настоящем ClickHouse: что база ему не даст.

Это главный тест безопасности run_sql. Текстовую проверку SELECT здесь
специально обходим — вызываем клиент напрямую, мимо ``check_select``: защита
обязана держаться на правах и закреплённых лимитах, даже если проверка текста
пропустит что-то лишнее.

Тесты помечены как integration, им нужен Docker.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import pytest
from clickhouse_connect.driver import AsyncClient
from clickhouse_connect.driver.exceptions import ClickHouseError
from pydantic import SecretStr
from testcontainers.community.clickhouse import ClickHouseContainer

from procurement.assistant.sql_access import apply_readonly_user
from procurement.assistant.sql_tool import run_select
from procurement.assistant.tools import ToolError
from procurement.config import ClickHouseSettings
from procurement.storage.clickhouse.client import build_client
from procurement.storage.clickhouse.ddl import apply_schema

pytestmark = pytest.mark.integration

RO_USER = "assistant_ro"
RO_PASSWORD = "integration-test-password"


@pytest.fixture(scope="module")
def admin_settings() -> Iterator[ClickHouseSettings]:
    container = ClickHouseContainer(
        "clickhouse/clickhouse-server:24.8-alpine",
        port=8123,
        username="test",
        password="test",
        dbname="test",
    )
    # Без этого пользователь из окружения не может создавать других
    # пользователей — так же включено в docker-compose.yml.
    container.with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
    with container:
        yield ClickHouseSettings(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(8123)),
            database="test",
            user="test",
            password=SecretStr("test"),
        )


@pytest.fixture(scope="module")
async def readonly(admin_settings: ClickHouseSettings) -> AsyncIterator[AsyncClient]:
    admin = await build_client(admin_settings)
    await apply_schema(admin)
    await admin.command(
        "CREATE TABLE IF NOT EXISTS ref_classifier "
        "(code String, parent_code String, level UInt8, name_ru String) "
        "ENGINE = MergeTree ORDER BY code"
    )
    # Таблица, которой нет в списке разрешённых: агент не должен её видеть.
    await admin.command(
        "CREATE TABLE IF NOT EXISTS sync_secret (token String) ENGINE = MergeTree ORDER BY token"
    )
    await admin.command("INSERT INTO sync_secret VALUES ('не для агента')")
    await admin.command(
        "INSERT INTO contracts (id, customer_bin, supplier_biin, contract_sum, crdate, "
        "sign_date, last_update_date) VALUES (1, '900140000101', '950140000111', 100, "
        "'2025-01-10 00:00:00', '2025-01-10 00:00:00', '2025-01-10 00:00:00')"
    )
    await apply_readonly_user(admin, user=RO_USER, password=RO_PASSWORD, database="test")
    await admin.close()

    ro = await build_client(
        admin_settings.model_copy(update={"user": RO_USER, "password": SecretStr(RO_PASSWORD)})
    )
    yield ro
    await ro.close()


async def test_readonly_user_can_read_analytics(readonly: AsyncClient) -> None:
    out = await run_select(readonly, "SELECT count() AS n FROM contracts FINAL")

    assert out == {"columns": ["n"], "rows": [[1]], "truncated": False}


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO contracts (id) VALUES (2)",
        "ALTER TABLE contracts DELETE WHERE 1",
        "DROP TABLE contracts",
        "TRUNCATE TABLE contracts",
        "CREATE TABLE evil (x UInt8) ENGINE = Memory",
    ],
)
async def test_readonly_user_cannot_change_anything(readonly: AsyncClient, statement: str) -> None:
    with pytest.raises(ClickHouseError):
        await readonly.command(statement)


async def test_readonly_user_cannot_read_other_tables(readonly: AsyncClient) -> None:
    with pytest.raises(ClickHouseError):
        await readonly.query("SELECT token FROM sync_secret")


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM url('http://example.com', 'CSV', 'x String')",
        "SELECT * FROM file('/etc/passwd', 'LineAsString')",
        "SELECT * FROM remote('127.0.0.1', 'test', 'sync_secret')",
    ],
)
async def test_readonly_user_cannot_use_table_functions(readonly: AsyncClient, query: str) -> None:
    with pytest.raises(ClickHouseError):
        await readonly.query(query)


@pytest.mark.parametrize(
    "setting", ["max_execution_time = 3600", "readonly = 0", "max_result_rows = 0"]
)
async def test_query_cannot_lift_pinned_limits(readonly: AsyncClient, setting: str) -> None:
    with pytest.raises(ClickHouseError):
        await readonly.query(f"SELECT 1 SETTINGS {setting}")


async def test_long_result_is_cut_by_database_not_an_error(readonly: AsyncClient) -> None:
    # max_result_rows=1000 с break: база обрывает выдачу, а не падает.
    out = await run_select(readonly, "SELECT number FROM numbers(100000)")

    assert out["truncated"] is True


async def test_database_error_reaches_model_as_text(readonly: AsyncClient) -> None:
    with pytest.raises(ToolError, match="ClickHouse"):
        await run_select(readonly, "SELECT no_such_column FROM contracts")
