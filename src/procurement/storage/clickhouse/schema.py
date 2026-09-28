"""Описание сущностей: какие колонки и как приводить к ним сырые значения.

Источник отдаёт JSON, где всё либо строка, либо число, либо null. ClickHouse
ждёт DateTime, Decimal и UInt. Между ними нужен слой приведения, и он должен
быть предсказуемым: молча уронить запись из-за пустого поля хуже, чем записать
её с заметным значением по умолчанию.

Принцип такой. Отсутствующее значение не повод потерять запись целиком — она
проходит дальше, а факт пропуска ловят проверки качества (см. procurement.
quality). Так плохие данные видны в отчёте, а не исчезают бесследно.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from procurement.ingest.models import RawRecord

# Значение для отсутствующей даты. Не None: crdate входит в ключ
# партиционирования, а он не может быть пустым. Записи с этой датой попадут в
# партицию 197001 — они не потеряются и будут сразу заметны.
UNKNOWN_DATE = datetime(1970, 1, 1, tzinfo=UTC)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_identifier(value: str, *, what: str = "идентификатор") -> str:
    """Проверить, что строка годится как имя таблицы или колонки.

    Имена таблиц и колонок нельзя передать параметром запроса — их приходится
    подставлять в текст. Значит подставлять можно только проверенное. Сейчас
    все имена приходят из констант этого модуля, но суффикс временной таблицы
    при бэкфилле собирается из параметров запуска DAG, и вот он уже способен
    принести что угодно.
    """
    if not _IDENTIFIER.match(value):
        msg = f"{what} {value!r} не похож на безопасное имя"
        raise ValueError(msg)
    return value


_DATE_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d")


_PARTITION = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def safe_partition(value: str) -> str:
    """Проверить имя партиции перед подстановкой в ALTER TABLE.

    Отдельная проверка, а не safe_identifier: имена партиций бывают чисто
    числовыми (202403), а идентификатор обязан начинаться с буквы.

    Значение приходит из system.parts, то есть от самого ClickHouse, и всё
    равно проверяется. Подстановка в текст запроса обязана быть проверенной
    независимо от источника: сегодня источник свой, завтра кто-нибудь передаст
    имя партиции параметром задачи.
    """
    if not _PARTITION.match(value):
        msg = f"имя партиции {value!r} не похоже на безопасное"
        raise ValueError(msg)
    return value


def as_uint(value: Any) -> int:
    if value is None or value == "":
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def as_decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal(0)
    try:
        # Через str, а не напрямую из float: Decimal(0.1) даёт
        # 0.1000000000000000055511151231257827, Decimal("0.1") — ровно 0.1.
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


def as_datetime(value: Any) -> datetime:
    """Привести значение даты из источника к datetime в UTC.

    Сначала fromisoformat, и только если он не справился — разбор по списку
    форматов. Порядок выбран замером: на строке "2024-06-11 10:00:00"
    strptime стоит 4.45 мкс, fromisoformat — 0.43 мкс, в десять раз меньше.
    Разница не косметическая: профилировщик показал, что на разбор дат уходила
    половина процессорного времени загрузчика.

    Причина разрыва в том, что strptime разбирает формат заново при каждом
    вызове и по дороге спрашивает локаль, а fromisoformat — код на C под
    заранее известную форму строки.

    Со смещением поступаем осмысленно: наивное время считаем UTC, время со
    смещением переводим в UTC. Прежняя версия такие строки не принимала вовсе
    и молча отдавала UNKNOWN_DATE.
    """
    if not value:
        return UNKNOWN_DATE
    text = str(value).strip()

    try:
        parsed: datetime | None = datetime.fromisoformat(text)
    except ValueError:
        parsed = _by_format(text)

    if parsed is None:
        return UNKNOWN_DATE
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _by_format(text: str) -> datetime | None:
    """Запасной разбор по списку форматов.

    Сейчас fromisoformat покрывает все формы из _DATE_FORMATS, так что сюда
    попадает только то, что источник ещё не присылал. Оставлено как место, куда
    добавлять новые формы: там, где они появятся, цена strptime уже неважна.
    """
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    """Имя колонки в ClickHouse."""

    source_key: str
    """Ключ в сырой записи. Обычно совпадает с именем, но не обязан."""

    convert: Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class EntitySpec:
    entity: str
    table: str
    columns: tuple[Column, ...]
    id_column: str
    version_column: str
    partition_expression: str
    """Как посчитать имя партиции для записи. Нужно идемпотентному загрузчику."""

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def to_row(self, record: RawRecord) -> list[Any]:
        return [column.convert(record.get(column.source_key)) for column in self.columns]

    def to_rows(self, records: list[RawRecord]) -> list[list[Any]]:
        return [self.to_row(record) for record in records]


CONTRACTS = EntitySpec(
    entity="contracts",
    table="contracts",
    id_column="id",
    version_column="last_update_date",
    partition_expression="toYYYYMM(crdate)",
    columns=(
        Column("id", "id", as_uint),
        Column("contract_number", "contract_number", as_str),
        Column("trd_buy_number_anno", "trd_buy_number_anno", as_str),
        Column("supplier_biin", "supplier_biin", as_str),
        Column("customer_bin", "customer_bin", as_str),
        Column("contract_sum", "contract_sum", as_decimal),
        Column("contract_sum_wnds", "contract_sum_wnds", as_decimal),
        Column("fakt_sum", "fakt_sum", as_decimal),
        Column("ref_contract_status_id", "ref_contract_status_id", as_uint),
        Column("crdate", "crdate", as_datetime),
        Column("sign_date", "sign_date", as_datetime),
        Column("last_update_date", "last_update_date", as_datetime),
    ),
)

LOTS = EntitySpec(
    entity="lots",
    table="lots",
    id_column="id",
    version_column="last_update_date",
    partition_expression="intDiv(id, 5000000)",
    columns=(
        Column("id", "id", as_uint),
        Column("lot_number", "lot_number", as_str),
        Column("ref_lot_status_id", "ref_lot_status_id", as_uint),
        Column("customer_bin", "customer_bin", as_str),
        Column("trd_buy_number_anno", "trd_buy_number_anno", as_str),
        Column("name_ru", "name_ru", as_str),
        Column("enstru_code", "enstru_code", as_str),
        Column("count", "count", as_decimal),
        Column("amount", "amount", as_decimal),
        Column("last_update_date", "last_update_date", as_datetime),
    ),
)

SPECS: dict[str, EntitySpec] = {CONTRACTS.entity: CONTRACTS, LOTS.entity: LOTS}


def spec_for(entity: str) -> EntitySpec:
    try:
        return SPECS[entity]
    except KeyError:
        known = ", ".join(sorted(SPECS))
        msg = f"нет описания для сущности {entity!r}, известны: {known}"
        raise ValueError(msg) from None
