"""Тесты приведения типов, защиты идентификаторов и разбивки по месяцам.

Юнит-тесты, без баз. Проверяется то, что легко ломается незаметно: пустое поле,
дробная сумма, неизвестный формат даты и граница декабря.
"""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from procurement.jobs.cli import months_between, parse_month
from procurement.storage.clickhouse.schema import (
    CONTRACTS,
    LOTS,
    UNKNOWN_DATE,
    as_datetime,
    as_decimal,
    as_uint,
    safe_identifier,
    spec_for,
)

# --- приведение типов ----------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(5, 5), ("7", 7), (None, 0), ("", 0), ("мусор", 0), (-3, 0), (2.9, 2)],
)
def test_as_uint(raw: object, expected: int) -> None:
    assert as_uint(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (100, Decimal("100")),
        ("250000.50", Decimal("250000.50")),
        (None, Decimal(0)),
        ("", Decimal(0)),
        ("не число", Decimal(0)),
    ],
)
def test_as_decimal(raw: object, expected: Decimal) -> None:
    assert as_decimal(raw) == expected


def test_as_decimal_does_not_inherit_float_noise() -> None:
    """Decimal(0.1) даёт 0.1000000000000000055..., Decimal("0.1") — ровно 0.1.

    Для денег разница принципиальна, поэтому в коде идёт приведение через str.
    """
    assert as_decimal(0.1) == Decimal("0.1")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-01-10 10:30:00", datetime(2026, 1, 10, 10, 30, tzinfo=UTC)),
        ("2026-01-10T10:30:00", datetime(2026, 1, 10, 10, 30, tzinfo=UTC)),
        ("2026-01-10", datetime(2026, 1, 10, tzinfo=UTC)),
        (None, UNKNOWN_DATE),
        ("", UNKNOWN_DATE),
        ("10.01.2026", UNKNOWN_DATE),
        # Ниже — то, что появилось вместе с переходом на fromisoformat.
        # Дробные доли секунды прежняя версия не принимала и отдавала
        # UNKNOWN_DATE, то есть теряла настоящую дату.
        ("2026-01-10 10:30:00.123456", datetime(2026, 1, 10, 10, 30, 0, 123456, tzinfo=UTC)),
        # Смещение переводится в UTC, а не затирается. Затереть означало бы
        # сдвинуть время на величину смещения и не заметить этого.
        ("2026-01-10T15:30:00+05:00", datetime(2026, 1, 10, 10, 30, tzinfo=UTC)),
        ("2026-01-10T10:30:00Z", datetime(2026, 1, 10, 10, 30, tzinfo=UTC)),
    ],
)
def test_as_datetime(raw: object, expected: datetime) -> None:
    assert as_datetime(raw) == expected


def test_as_datetime_keeps_the_moment_across_offsets() -> None:
    """Одно и то же мгновение, записанное в разных поясах, даёт одно значение.

    Проверка не про формат, а про смысл: ReplacingMergeTree выбирает версию
    записи по last_update_date, и сдвиг на пять часов из-за пояса означал бы
    выбор не той версии.
    """
    assert as_datetime("2026-01-10T15:30:00+05:00") == as_datetime("2026-01-10 10:30:00")


def test_missing_field_does_not_lose_the_record() -> None:
    """Запись с дырами должна дойти до хранилища, а не исчезнуть.

    Пропуск поймают проверки качества — это лучше, чем тихо потерять запись.
    """
    row = CONTRACTS.to_row({"id": 42})

    assert len(row) == len(CONTRACTS.columns)
    assert row[0] == 42
    assert row[CONTRACTS.column_names.index("customer_bin")] == ""
    assert row[CONTRACTS.column_names.index("contract_sum")] == Decimal(0)
    assert row[CONTRACTS.column_names.index("crdate")] == UNKNOWN_DATE


def test_row_order_matches_column_names() -> None:
    record = {"id": 1, "customer_bin": "900140000101", "amount": "1500.25"}
    row = LOTS.to_row(record)
    pairs = dict(zip(LOTS.column_names, row, strict=True))

    assert pairs["id"] == 1
    assert pairs["customer_bin"] == "900140000101"
    assert pairs["amount"] == Decimal("1500.25")


def test_unknown_entity_is_rejected() -> None:
    with pytest.raises(ValueError, match="нет описания"):
        spec_for("самолёты")


# --- защита идентификаторов ------------------------------------------------------


@pytest.mark.parametrize("name", ["contracts", "contracts_staging_202401", "_tmp1"])
def test_safe_identifier_accepts_normal_names(name: str) -> None:
    assert safe_identifier(name) == name


@pytest.mark.parametrize(
    "name",
    ["contracts; DROP TABLE lots", "таблица", "1abc", "a-b", "", "a b", "a'b"],
)
def test_safe_identifier_rejects_everything_else(name: str) -> None:
    with pytest.raises(ValueError, match="безопасное имя"):
        safe_identifier(name)


# --- разбивка по месяцам ---------------------------------------------------------


def test_months_between_single_month() -> None:
    assert months_between(date(2024, 5, 1), date(2024, 5, 1)) == [date(2024, 5, 1)]


def test_months_between_crosses_december() -> None:
    """Первая версия зацикливалась ровно здесь.

    divmod на декабре возвращал тот же месяц, цикл не двигался, и трёхлетний
    бэкфилл падал с MemoryError. Дымовой прогон январь-февраль этого не ловил.
    """
    assert months_between(date(2024, 11, 1), date(2025, 2, 1)) == [
        date(2024, 11, 1),
        date(2024, 12, 1),
        date(2025, 1, 1),
        date(2025, 2, 1),
    ]


def test_months_between_three_years() -> None:
    months = months_between(date(2023, 1, 1), date(2025, 12, 1))

    assert len(months) == 36
    assert months[0] == date(2023, 1, 1)
    assert months[-1] == date(2025, 12, 1)
    assert len(set(months)) == 36, "повторов быть не должно"


def test_months_between_empty_when_end_before_start() -> None:
    assert months_between(date(2025, 3, 1), date(2025, 1, 1)) == []


@pytest.mark.parametrize("value", ["2024-13", "abcd", "2024", "", "2024/01"])
def test_parse_month_rejects_garbage(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        parse_month(value)


def test_parse_month_accepts_normal_form() -> None:
    assert parse_month("2024-07") == date(2024, 7, 1)
