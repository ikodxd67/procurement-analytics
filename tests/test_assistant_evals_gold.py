"""Эталонный SQL набора на настоящем ClickHouse — под пользователем агента.

Данные маленькие, и правильный ответ на каждый вопрос посчитан руками в
EXPECTED. Так проверяется не только «запрос выполняется», но и «запрос считает
то, что написано в вопросе»: эталон с ошибкой хуже, чем никакой — он будет
ставить двойки верным ответам.

Тесты помечены как integration, им нужен Docker.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import datetime
from decimal import Decimal

import pytest
from clickhouse_connect.driver import AsyncClient
from pydantic import SecretStr
from testcontainers.community.clickhouse import ClickHouseContainer

from procurement.assistant.evals.dataset import Kind, load
from procurement.assistant.evals.gold import gold_value
from procurement.assistant.sql_access import apply_readonly_user
from procurement.config import ClickHouseSettings
from procurement.storage.clickhouse.client import build_client
from procurement.storage.clickhouse.ddl import apply_schema

pytestmark = pytest.mark.integration

RO_USER = "assistant_ro"
RO_PASSWORD = "integration-test-password"

# (id, заказчик, поставщик, сумма, дата)
CONTRACTS = [
    # 2023: два разных заказчика
    (1, "C1", "S0", 50, "2023-05-10"),
    (2, "C1", "S0", 60, "2023-06-10"),
    (3, "C2", "S0", 70, "2023-07-10"),
    # 2024: три договора в первом квартале и один в апреле
    (10, "C3", "S1", 100, "2024-01-15"),
    (11, "C3", "S1", 200, "2024-02-15"),
    (12, "C3", "S1", 300, "2024-03-15"),
    (13, "C3", "S1", 400, "2024-04-15"),
    # 2025: два в марте, один в декабре
    (20, "X", "S1", 10, "2025-03-05"),
    (21, "X", "S2", 20, "2025-03-06"),
    (22, "X", "S3", 30, "2025-12-01"),
    # 2025: заказчик H — доли 40/10/10/10/30 % → HHI 1600+100+100+100+900 = 2800
    (30, "H", "H1", 400, "2025-06-01"),
    (31, "H", "H2", 100, "2025-06-02"),
    (32, "H", "H3", 100, "2025-06-03"),
    (33, "H", "H4", 100, "2025-06-04"),
    (34, "H", "H5", 300, "2025-06-05"),
    # 2025: заказчик E — пять равных долей по 20 % → HHI 5·400 = 2000
    (40, "E", "E1", 100, "2025-07-01"),
    (41, "E", "E2", 100, "2025-07-02"),
    (42, "E", "E3", 100, "2025-07-03"),
    (43, "E", "E4", 100, "2025-07-04"),
    (44, "E", "E5", 100, "2025-07-05"),
]

EXPECTED = {
    "data-count-2024": 4,
    "data-sum-2025": 10 + 20 + 30 + 1000 + 500,
    "data-count-march-2025": 2,
    "data-sum-december-2025": 30,
    "data-count-q1-2024": 3,
    "data-customers-2023": 2,
    "data-suppliers-2025": 3 + 5 + 5,  # S1–S3, H1–H5, E1–E5
    "data-avg-2024": 250,
    "data-max-2024": 400,
    "data-hhi-max-2025": 2800,  # у X три поставщика — не проходит порог «не меньше пяти»
    "mixed-hhi": 2800,
    "mixed-methods-and-count": 13,
}


@pytest.fixture(scope="module")
def admin_settings() -> Iterator[ClickHouseSettings]:
    container = ClickHouseContainer(
        "clickhouse/clickhouse-server:24.8-alpine",
        port=8123,
        username="test",
        password="test",
        dbname="test",
    )
    container.with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
    with container:
        yield ClickHouseSettings(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(8123)),
            database="test",
            user="test",
            password=SecretStr("test"),
        )


@pytest.fixture
async def readonly(admin_settings: ClickHouseSettings) -> AsyncIterator[AsyncClient]:
    # Клиент на каждый тест: асинхронный клиент привязан к циклу событий
    # (грабли этапа 2, test_assistant_sql_access.py).
    admin = await build_client(admin_settings)
    await apply_schema(admin)
    await admin.command(
        "CREATE TABLE IF NOT EXISTS ref_classifier "
        "(code String, parent_code String, level UInt8, name_ru String) "
        "ENGINE = MergeTree ORDER BY code"
    )
    await admin.command("TRUNCATE TABLE contracts")
    await admin.insert(
        "contracts",
        [
            [
                cid,
                f"N{cid}",
                "",
                supplier,
                customer,
                Decimal(total),
                Decimal(total),
                Decimal(0),
                1,
                datetime.fromisoformat(day),
                datetime.fromisoformat(day),
                datetime.fromisoformat(day),
            ]
            for cid, customer, supplier, total, day in CONTRACTS
        ],
        column_names=[
            "id",
            "contract_number",
            "trd_buy_number_anno",
            "supplier_biin",
            "customer_bin",
            "contract_sum",
            "contract_sum_wnds",
            "fakt_sum",
            "ref_contract_status_id",
            "crdate",
            "sign_date",
            "last_update_date",
        ],
    )
    await apply_readonly_user(admin, user=RO_USER, password=RO_PASSWORD, database="test")
    await admin.close()

    ro = await build_client(
        admin_settings.model_copy(update={"user": RO_USER, "password": SecretStr(RO_PASSWORD)})
    )
    yield ro
    await ro.close()


def test_every_data_question_has_a_hand_computed_answer() -> None:
    ids = {q.id for q in load().of_kind(Kind.DATA, Kind.MIXED)}

    assert ids == set(EXPECTED)


async def test_gold_sql_computes_the_hand_computed_answers(readonly: AsyncClient) -> None:
    got = {q.id: await gold_value(readonly, q) for q in load().of_kind(Kind.DATA, Kind.MIXED)}

    assert got == pytest.approx(EXPECTED)
