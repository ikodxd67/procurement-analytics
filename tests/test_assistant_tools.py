"""Инструменты агента без баз: ToolBox, срезы на заглушке витрин, проверка SQL, права."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any, cast

import pytest
from clickhouse_connect.driver import AsyncClient
from clickhouse_connect.driver.exceptions import DatabaseError
from pydantic import BaseModel, ConfigDict, Field

from procurement.api.errors import BadRequestError, NotFoundError
from procurement.api.queries import MAX_PERIOD_DAYS, Marts, Paging, Period
from procurement.assistant.models import ToolUse
from procurement.assistant.slices import MAX_TOOL_LIMIT, build_slice_tools
from procurement.assistant.sql_access import ALLOWED_TABLES, LIMITS, readonly_user_statements
from procurement.assistant.sql_tool import (
    MAX_ROWS_TO_MODEL,
    build_sql_tool,
    check_select,
    run_select,
)
from procurement.assistant.toolbox import build_toolbox
from procurement.assistant.tools import MAX_RESULT_CHARS, Tool, ToolBox, ToolError, to_tool_content

CUSTOMER = "900140000101"


def use(name: str, **args: Any) -> ToolUse:
    return ToolUse(id="c1", name=name, input=args)


# --- общий период ----------------------------------------------------------


def test_period_defaults_to_last_year() -> None:
    period = Period.resolve(None, None, today=date(2025, 6, 30))

    assert period == Period(date_from=date(2024, 6, 30), date_to=date(2025, 6, 30))


def test_period_rejects_reversed_and_too_wide() -> None:
    with pytest.raises(BadRequestError, match="позже"):
        Period.resolve(date(2025, 2, 1), date(2025, 1, 1))
    with pytest.raises(BadRequestError, match=str(MAX_PERIOD_DAYS)):
        Period.resolve(date(2010, 1, 1), date(2025, 1, 1))


# --- ToolBox ---------------------------------------------------------------


class EchoArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    n: int = Field(ge=1)


def echo_box(run: Any) -> ToolBox:
    return ToolBox([Tool("echo", "вернуть n", EchoArgs, run)])


async def test_toolbox_returns_json_result() -> None:
    async def run(a: EchoArgs) -> dict[str, Any]:
        return {"n": a.n, "sum": Decimal("10.50"), "day": date(2025, 1, 2)}

    result = await echo_box(run).execute(use("echo", n=3))

    assert not result.is_error
    assert result.tool_use_id == "c1"
    # Деньги строкой, без потери копеек; дата ISO.
    assert json.loads(result.content) == {"n": 3, "sum": "10.50", "day": "2025-01-02"}


async def test_unknown_tool_is_error_for_model() -> None:
    result = await echo_box(None).execute(use("drop_everything"))

    assert result.is_error
    assert "drop_everything" in result.content
    assert "echo" in result.content


@pytest.mark.parametrize(
    ("args", "field"), [({"n": 0}, "n"), ({}, "n"), ({"n": 1, "extra": True}, "extra")]
)
async def test_bad_arguments_are_error_for_model(args: dict[str, Any], field: str) -> None:
    result = await echo_box(None).execute(use("echo", **args))

    assert result.is_error
    assert field in result.content
    assert "https://" not in result.content  # без ссылок pydantic: это токены впустую


@pytest.mark.parametrize("error", [ToolError("нужен SELECT"), NotFoundError("БИН не найден")])
async def test_expected_failures_go_to_model(error: Exception) -> None:
    async def run(a: EchoArgs) -> None:
        raise error

    result = await echo_box(run).execute(use("echo", n=1))

    assert result.is_error
    assert result.content == str(error)


async def test_bugs_in_our_code_are_not_hidden_from_us() -> None:
    # Ошибку нашего кода модель не исправит. Если отдать её текстом, агент
    # будет вежливо крутиться, а мы ничего не узнаем.
    async def run(a: EchoArgs) -> None:
        raise KeyError("опечатка в коде")

    with pytest.raises(KeyError):
        await echo_box(run).execute(use("echo", n=1))


def test_duplicate_tool_names_are_rejected() -> None:
    tool = Tool("echo", "", EchoArgs, None)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="дважды"):
        ToolBox([tool, tool])


def test_long_result_is_cut_with_a_hint() -> None:
    text = to_tool_content([{"x": "a" * 100} for _ in range(200)])

    assert len(text) < MAX_RESULT_CHARS + 200
    assert "обрезано" in text


# --- срезы на заглушке витрин ----------------------------------------------


class StubMarts:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    async def supplier_shares(
        self, customer_bin: str, period: Period, paging: Paging
    ) -> tuple[list[tuple[Any, ...]], bool]:
        self.calls.append(("supplier_shares", (customer_bin, period, paging)))
        if customer_bin == "000000000000":
            return [], False
        return [("950140000111", 3, Decimal("1500.00"), 75.0)], False

    async def customer_exists(self, customer_bin: str) -> bool:
        return False


def slice_box(marts: StubMarts) -> ToolBox:
    return ToolBox(build_slice_tools(cast(Marts, marts)))


def test_slice_tools_cover_every_api_slice() -> None:
    names = {t.name for t in build_slice_tools(cast(Marts, StubMarts()))}

    assert names == {
        "customer_suppliers",
        "supplier_customers",
        "category_rollup",
        "contracts_monthly",
        "price_outliers",
        "customer_concentration",
        "supplier_activity",
    }


def test_slice_schemas_forbid_extra_fields_and_cap_limit() -> None:
    for tool in build_slice_tools(cast(Marts, StubMarts())):
        schema = tool.spec.input_schema
        assert schema["additionalProperties"] is False, tool.name
        if "limit" in schema["properties"]:
            assert schema["properties"]["limit"]["maximum"] == MAX_TOOL_LIMIT


async def test_slice_goes_through_the_api_function() -> None:
    marts = StubMarts()

    result = await slice_box(marts).execute(
        use(
            "customer_suppliers",
            customer_bin=CUSTOMER,
            date_from="2025-01-01",
            date_to="2025-12-31",
            limit=5,
        )
    )

    assert not result.is_error
    page = json.loads(result.content)
    assert page["items"] == [
        {"supplier_biin": "950140000111", "contracts": 3, "total": "1500.00", "share_pct": 75.0}
    ]
    _, (bin_, period, paging) = marts.calls[0]
    assert bin_ == CUSTOMER
    assert period == Period(date_from=date(2025, 1, 1), date_to=date(2025, 12, 31))
    assert paging == Paging(limit=5, offset=0)


async def test_slice_unknown_customer_is_404_text_for_model() -> None:
    result = await slice_box(StubMarts()).execute(
        use("customer_suppliers", customer_bin="000000000000")
    )

    assert result.is_error
    assert "не найден" in result.content


@pytest.mark.parametrize(
    "args",
    [
        {"customer_bin": "12345"},  # не 12 цифр
        {"customer_bin": CUSTOMER, "limit": MAX_TOOL_LIMIT + 1},
        {"customer_bin": CUSTOMER, "date_from": "2025-02-01", "date_to": "2025-01-01"},
        {"customer_bin": CUSTOMER, "date_from": "2010-01-01", "date_to": "2025-01-01"},
    ],
)
async def test_slice_rejects_what_api_rejects(args: dict[str, Any]) -> None:
    marts = StubMarts()

    result = await slice_box(marts).execute(use("customer_suppliers", **args))

    assert result.is_error
    assert marts.calls == []  # до базы не дошло


# --- проверка текста SQL ---------------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        "SELECT count() FROM contracts FINAL",
        "select 1;",
        "  WITH t AS (SELECT 1) SELECT * FROM t",
        "-- топ заказчиков\nSELECT customer_bin FROM contracts",
        "/* комментарий */ SELECT 1",
    ],
)
def test_select_is_accepted(query: str) -> None:
    assert check_select(query) == query.strip().rstrip(";").strip()


@pytest.mark.parametrize(
    ("query", "why"),
    [
        ("DROP TABLE contracts", "только SELECT"),
        ("INSERT INTO contracts SELECT * FROM contracts", "только SELECT"),
        ("/* SELECT */ ALTER TABLE contracts DELETE WHERE 1", "только SELECT"),
        ("SELECT 1; DROP TABLE contracts", "один запрос"),
        ("SELECTED", "только SELECT"),
    ],
)
def test_non_select_is_rejected(query: str, why: str) -> None:
    with pytest.raises(ToolError, match=why):
        check_select(query)


class StubResult:
    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.column_names = columns
        self.result_rows = rows


class StubClickHouse:
    def __init__(self, result: StubResult | Exception) -> None:
        self.result = result
        self.queries: list[str] = []

    async def query(self, sql: str) -> StubResult:
        self.queries.append(sql)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


async def test_run_select_cuts_rows_and_says_so() -> None:
    rows = [(i, Decimal(i)) for i in range(MAX_ROWS_TO_MODEL + 5)]
    client = StubClickHouse(StubResult(["n", "total"], rows))

    out = await run_select(cast(AsyncClient, client), "SELECT n, total FROM t;")

    assert out["columns"] == ["n", "total"]
    assert len(out["rows"]) == MAX_ROWS_TO_MODEL
    assert out["truncated"] is True
    assert client.queries == ["SELECT n, total FROM t"]


async def test_clickhouse_error_goes_to_model_short() -> None:
    error = DatabaseError("Code: 47. Unknown identifier 'totl'\nстек вызова\nещё строки")
    box = ToolBox([build_sql_tool(cast(AsyncClient, StubClickHouse(error)))])

    result = await box.execute(use("run_sql", query="SELECT totl FROM contracts"))

    assert result.is_error
    assert result.content == "ClickHouse: Code: 47. Unknown identifier 'totl'"


async def test_rejected_sql_never_reaches_database() -> None:
    client = StubClickHouse(StubResult([], []))
    box = ToolBox([build_sql_tool(cast(AsyncClient, client))])

    result = await box.execute(use("run_sql", query="DROP TABLE contracts"))

    assert result.is_error
    assert client.queries == []


def test_full_toolbox_has_slices_and_sql() -> None:
    box = build_toolbox(
        marts=cast(Marts, StubMarts()),
        readonly=cast(AsyncClient, StubClickHouse(StubResult([], []))),
    )

    names = [s.name for s in box.specs]
    assert names == sorted(names)
    assert "run_sql" in names
    assert len(names) == 8


# --- пользователь только для чтения ----------------------------------------


def test_readonly_statements_pin_every_limit_and_grant_only_select() -> None:
    statements = readonly_user_statements("assistant_ro", "local-only-password", "procurement")
    profile, user, *grants = statements

    for name in LIMITS:
        assert f"{name} = " in profile
    assert profile.count("CONST") == len(LIMITS)
    assert "SETTINGS PROFILE 'assistant_readonly'" in user
    assert grants == [f"GRANT SELECT ON procurement.{t} TO assistant_ro" for t in ALLOWED_TABLES]


@pytest.mark.parametrize(
    ("user", "password", "database"),
    [
        ("assistant_ro", "short", "procurement"),
        ("assistant_ro", "pass' SETTINGS readonly = 0 --", "procurement"),
        ("ro; DROP USER default", "local-only-password", "procurement"),
        ("assistant_ro", "local-only-password", "procurement.contracts"),
    ],
)
def test_readonly_statements_refuse_unsafe_names(user: str, password: str, database: str) -> None:
    with pytest.raises(ValueError):
        readonly_user_statements(user, password, database)
