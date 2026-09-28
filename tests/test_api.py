"""Тесты API на настоящих базах в контейнерах.

Запросы идут в приложение через ASGI, минуя сеть: проверяется поведение
обработчиков, а не работа петли loopback.

Набор данных крошечный и подобран так, чтобы ответы считались руками. Половина
тестов — про то, что происходит при неверном запросе: это та часть API, которую
обычно не проверяют, а ломается она чаще остальных.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.community.postgres import PostgresContainer

from procurement.api.app import build_app
from procurement.config import ClickHouseSettings, PostgresSettings, Settings
from procurement.storage.clickhouse.client import build_client
from procurement.storage.clickhouse.ddl import apply_schema

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CUSTOMER = "900140000101"
OTHER_CUSTOMER = "900140000202"
SUPPLIER_BIG = "950140000111"
SUPPLIER_SMALL = "950140000222"
UNKNOWN_BIN = "999999999999"

PERIOD = {"date_from": "2024-01-01", "date_to": "2024-12-31"}


@pytest.fixture(scope="module")
def settings() -> Iterator[Settings]:
    postgres = PostgresContainer("postgres:16-alpine", driver="asyncpg")
    clickhouse = ClickHouseContainer(
        "clickhouse/clickhouse-server:24.8-alpine",
        port=8123,
        username="test",
        password="test",
        dbname="test",
    )
    with postgres, clickhouse:
        dsn = postgres.get_connection_url()
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
        config.set_main_option("sqlalchemy.url", dsn)
        command.upgrade(config, "head")

        yield Settings(
            postgres=PostgresSettings(dsn=dsn),
            clickhouse=ClickHouseSettings(
                host=clickhouse.get_container_host_ip(),
                port=int(clickhouse.get_exposed_port(8123)),
                database="test",
                user="test",
                password=SecretStr("test"),
            ),
        )


async def seed(settings: Settings) -> None:
    client = await build_client(settings.clickhouse)
    for table in ("contracts", "lots", "ref_classifier", "mart_contracts_monthly"):
        await client.command(f"DROP TABLE IF EXISTS {table}")
    await apply_schema(client)
    await client.command("""
        CREATE TABLE ref_classifier
        (code String, parent_code String, level UInt8, name_ru String)
        ENGINE = MergeTree ORDER BY code
    """)

    # Заказчик CUSTOMER: 750 у одного поставщика и 250 у другого. Доли 75 и 25.
    contracts = [
        (1, CUSTOMER, SUPPLIER_BIG, 500, "2024-01-10"),
        (2, CUSTOMER, SUPPLIER_BIG, 250, "2024-02-10"),
        (3, CUSTOMER, SUPPLIER_SMALL, 250, "2024-03-10"),
        (4, OTHER_CUSTOMER, SUPPLIER_BIG, 100, "2024-01-20"),
    ]
    values = ", ".join(
        f"({i}, '1', '1-1', '{sup}', '{cus}', {total}, {total}, 0, 230, '{day}', '{day}', '{day}')"
        for i, cus, sup, total, day in contracts
    )
    await client.command(
        "INSERT INTO contracts (id, contract_number, trd_buy_number_anno, supplier_biin, "
        f"customer_bin, contract_sum, contract_sum_wnds, fakt_sum, ref_contract_status_id, "
        f"crdate, sign_date, last_update_date) VALUES {values}"
    )

    await client.insert(
        "ref_classifier",
        [
            ["01", "", 0, "Раздел один"],
            ["01.1", "01", 1, "Группа 01.1"],
            ["01.1.1", "01.1", 2, "Подгруппа"],
            ["01.1.1.001", "01.1.1", 3, "Позиция"],
        ],
        column_names=["code", "parent_code", "level", "name_ru"],
    )
    await client.command(
        "INSERT INTO lots (id, lot_number, ref_lot_status_id, customer_bin, "
        "trd_buy_number_anno, name_ru, enstru_code, count, amount, last_update_date) VALUES "
        "(1, 'L1', 230, '" + CUSTOMER + "', '1-1', 'нечто', '01.1.1.001', 1, 100, '2024-05-10')"
    )
    await client.command("""
        INSERT INTO mart_contracts_monthly
        SELECT toStartOfMonth(crdate), customer_bin, count(), sum(contract_sum)
        FROM contracts FINAL GROUP BY 1, 2
    """)
    await client.close()


@pytest.fixture
async def client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    await seed(settings)
    app = build_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as http,
    ):
        yield http


# --- состояние ---------------------------------------------------------------


async def test_live_does_not_touch_storages(client: httpx.AsyncClient) -> None:
    """Проба живости отвечает про процесс и только про него."""
    response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "checks": {}}


async def test_ready_reports_every_dependency(client: httpx.AsyncClient) -> None:
    response = await client.get("/health/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["checks"] == {"clickhouse": "ok", "postgres": "ok"}


# --- содержательные ответы -----------------------------------------------------


async def test_supplier_shares_add_up(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/v1/customers/{CUSTOMER}/suppliers", params=PERIOD)

    assert response.status_code == 200
    items = response.json()["items"]
    shares = {item["supplier_biin"]: item for item in items}

    assert shares[SUPPLIER_BIG]["contracts"] == 2
    assert shares[SUPPLIER_BIG]["share_pct"] == 75.0
    assert shares[SUPPLIER_SMALL]["share_pct"] == 25.0
    assert sum(item["share_pct"] for item in items) == pytest.approx(100.0)


async def test_other_customers_do_not_leak_into_shares(client: httpx.AsyncClient) -> None:
    """Договор другого заказчика не должен влиять на доли этого."""
    response = await client.get(f"/v1/customers/{CUSTOMER}/suppliers", params=PERIOD)
    total = sum(float(item["total"]) for item in response.json()["items"])

    assert total == 1000.0, "у этого заказчика ровно 1000, чужие 100 сюда не попадают"


async def test_monthly_first_period_has_no_previous(client: httpx.AsyncClient) -> None:
    response = await client.get("/v1/contracts/monthly", params=PERIOD)

    assert response.status_code == 200
    points = response.json()
    assert points[0]["prev_total"] is None, "у первого месяца предыдущего нет"
    assert points[0]["change_pct"] is None
    assert points[1]["prev_total"] is not None


async def test_monthly_filtered_by_supplier_leaves_the_mart(client: httpx.AsyncClient) -> None:
    """С фильтром по поставщику витрина не годится — в ней нет поставщика."""
    response = await client.get(
        "/v1/contracts/monthly", params={**PERIOD, "supplier_biin": SUPPLIER_SMALL}
    )

    assert response.status_code == 200
    points = response.json()
    assert len(points) == 1, "у этого поставщика договор только в марте"
    assert float(points[0]["total"]) == 250.0


async def test_categories_roll_up_to_the_requested_level(client: httpx.AsyncClient) -> None:
    response = await client.get("/v1/categories", params={**PERIOD, "level": 0})

    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["code"] == "01"
    assert items[0]["level"] == 0


async def test_activity_returns_one_island(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/v1/suppliers/{SUPPLIER_BIG}/activity", params=PERIOD)

    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["months_in_row"] == 2, "январь и февраль подряд"


# --- пагинация -----------------------------------------------------------------


async def test_has_more_is_true_when_something_is_left(client: httpx.AsyncClient) -> None:
    response = await client.get(
        f"/v1/customers/{CUSTOMER}/suppliers", params={**PERIOD, "limit": 1}
    )

    body = response.json()
    assert body["meta"]["returned"] == 1
    assert body["meta"]["has_more"] is True, "поставщиков двое, отдали одного"


async def test_has_more_is_false_on_the_last_page(client: httpx.AsyncClient) -> None:
    response = await client.get(
        f"/v1/customers/{CUSTOMER}/suppliers", params={**PERIOD, "limit": 1, "offset": 1}
    )

    body = response.json()
    assert body["meta"]["returned"] == 1
    assert body["meta"]["has_more"] is False


async def test_offset_past_the_end_gives_an_empty_page(client: httpx.AsyncClient) -> None:
    """Сдвиг за пределы выборки — не ошибка, а пустая страница."""
    response = await client.get(
        f"/v1/customers/{CUSTOMER}/suppliers", params={**PERIOD, "offset": 100}
    )

    assert response.status_code == 200
    assert response.json()["items"] == []


# --- проверка входа -------------------------------------------------------------


@pytest.mark.parametrize("value", ["12345", "abcdefghijkl", "9001400001011", ""])
async def test_malformed_bin_is_rejected(client: httpx.AsyncClient, value: str) -> None:
    response = await client.get(f"/v1/customers/{value}/suppliers", params=PERIOD)

    assert response.status_code in (404, 422), "или маршрут не найден, или БИН не прошёл проверку"


async def test_validation_error_uses_the_common_envelope(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/v1/customers/{CUSTOMER}/suppliers", params={"limit": 0})

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "validation_failed"
    assert body["error"]["details"], "в подробностях должно быть сказано, какое поле не так"


@pytest.mark.parametrize(("field", "value"), [("limit", 0), ("limit", 100000), ("offset", -1)])
async def test_paging_bounds_are_enforced(
    client: httpx.AsyncClient, field: str, value: int
) -> None:
    response = await client.get(
        f"/v1/customers/{CUSTOMER}/suppliers", params={**PERIOD, field: value}
    )

    assert response.status_code == 422


async def test_reversed_period_is_a_bad_request(client: httpx.AsyncClient) -> None:
    response = await client.get(
        f"/v1/customers/{CUSTOMER}/suppliers",
        params={"date_from": "2025-01-01", "date_to": "2024-01-01"},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "bad_request"


async def test_too_wide_period_is_rejected(client: httpx.AsyncClient) -> None:
    """Иначе date_from=1900 превращается в способ устроить полный перебор."""
    response = await client.get(
        f"/v1/customers/{CUSTOMER}/suppliers",
        params={"date_from": "1900-01-01", "date_to": "2026-01-01"},
    )

    assert response.status_code == 400


async def test_level_out_of_range_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.get("/v1/categories", params={**PERIOD, "level": 9})

    assert response.status_code == 422


# --- не найдено ------------------------------------------------------------------


async def test_unknown_customer_is_not_found(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/v1/customers/{UNKNOWN_BIN}/suppliers", params=PERIOD)

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


async def test_known_customer_with_no_data_in_period_is_not_a_404(
    client: httpx.AsyncClient,
) -> None:
    """Опечатка в БИН и тихий период — разные вещи, и отвечать надо по-разному."""
    response = await client.get(
        f"/v1/customers/{CUSTOMER}/suppliers",
        params={"date_from": "2020-01-01", "date_to": "2020-12-31"},
    )

    assert response.status_code == 200
    assert response.json()["items"] == []


async def test_unknown_route_uses_the_common_envelope(client: httpx.AsyncClient) -> None:
    response = await client.get("/v1/nothing-here")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"


# --- описание API ------------------------------------------------------------------


async def test_openapi_describes_every_route(client: httpx.AsyncClient) -> None:
    response = await client.get("/openapi.json")

    assert response.status_code == 200
    spec: dict[str, Any] = response.json()
    assert "/v1/customers/{customer_bin}/suppliers" in spec["paths"]
    assert "/health/ready" in spec["paths"]
