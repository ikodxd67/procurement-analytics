"""Проверки качества между загрузкой и заливкой в боевую таблицу.

Почему проверки написаны на SQL, а не на Python. Накопить множество
идентификаторов для поиска дублей в памяти — это гигабайты на миллионах
записей, и всё ради того, что ClickHouse считает одним `uniqExact`. Данные уже
лежат в таблице, считать по ним надо там же.

Порядок в пайплайне такой: выкачали месяц во временную таблицу, прогнали
проверки, и только если они прошли — подменили партицию в боевой. Плохой месяц
до пользователей не доезжает.

Два уровня строгости. **fail** останавливает заливку: данные заведомо
испорчены. **warn** пропускает, но остаётся в журнале: показатель ушёл, но не
настолько, чтобы отказываться от данных совсем. Разница важна — пайплайн,
который падает от каждого подозрительного числа, быстро отключают.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from clickhouse_connect.driver import AsyncClient

from procurement.logging import get_logger
from procurement.storage.clickhouse.schema import UNKNOWN_DATE, EntitySpec, safe_identifier

log = get_logger(__name__)


class Severity(StrEnum):
    WARN = "warn"
    FAIL = "fail"


class QualityFailedError(RuntimeError):
    """Хотя бы одна проверка уровня fail не прошла."""


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    passed: bool
    severity: Severity
    observed: str
    limit: str
    comment: str = ""

    def render(self) -> str:
        mark = "ok  " if self.passed else ("FAIL" if self.severity is Severity.FAIL else "warn")
        body = f"[{mark}] {self.name}: {self.observed} (допустимо {self.limit})"
        return f"{body} {self.comment}".rstrip()


@dataclass(frozen=True, slots=True)
class QualityReport:
    entity: str
    table: str
    rows: int
    checks: list[CheckResult]

    @property
    def failures(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed and c.severity is Severity.FAIL]

    @property
    def warnings(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed and c.severity is Severity.WARN]

    @property
    def ok(self) -> bool:
        return not self.failures

    def render(self) -> str:
        head = f"Качество {self.entity} в {self.table}: {self.rows} строк"
        return "\n".join([head, *(c.render() for c in self.checks)])

    def raise_if_failed(self) -> None:
        if self.ok:
            return
        details = "; ".join(c.render() for c in self.failures)
        msg = f"проверки качества не прошли для {self.entity}: {details}"
        raise QualityFailedError(msg)


@dataclass(frozen=True, slots=True)
class Rules:
    """Пороги проверок.

    Значения подобраны как разумные стартовые и должны уточняться по реальным
    данным. Держать их здесь, а не разбросанными по запросам, — чтобы можно
    было обсудить одним взглядом.
    """

    required_fields: tuple[str, ...] = ()
    """Поля, пустота которых делает запись бесполезной."""

    max_null_share: float = 0.05
    """Доля пустых значений в обязательном поле, выше которой это fail."""

    warn_null_share: float = 0.01

    amount_field: str | None = None
    max_amount: float = 1e13
    """Десять триллионов тенге на один договор — заведомо мусор."""

    max_out_of_range_share: float = 0.0
    """Сумм вне диапазона быть не должно вовсе."""

    max_unknown_date_share: float = 0.01
    """Доля записей, у которых не разобралась дата."""

    count_tolerance: float = 0.01
    """Допустимое расхождение с числом записей, заявленным источником."""


RULES: dict[str, Rules] = {
    "contracts": Rules(
        required_fields=("customer_bin", "supplier_biin"),
        amount_field="contract_sum",
    ),
    "lots": Rules(
        required_fields=("customer_bin", "lot_number"),
        amount_field="amount",
    ),
}


def rules_for(entity: str) -> Rules:
    return RULES.get(entity, Rules())


def _share(part: int, total: int) -> float:
    return 0.0 if total == 0 else part / total


class QualityGate:
    def __init__(
        self,
        client: AsyncClient,
        spec: EntitySpec,
        *,
        table: str | None = None,
        rules: Rules | None = None,
    ) -> None:
        self._client = client
        self._spec = spec
        self._table = safe_identifier(table or spec.table, what="имя таблицы")
        self._rules = rules or rules_for(spec.entity)

    async def run(self, *, api_total: int | None = None) -> QualityReport:
        checks: list[CheckResult] = []

        rows, distinct_ids = await self._counts()
        checks.append(self._check_not_empty(rows))
        checks.append(self._check_duplicates(rows, distinct_ids))

        for name in self._rules.required_fields:
            checks.append(await self._check_null_share(name, rows))

        if self._rules.amount_field:
            checks.append(await self._check_amount_range(rows))

        checks.append(await self._check_unknown_dates(rows))

        if api_total is not None:
            checks.append(self._check_count_against_api(rows, api_total))

        report = QualityReport(
            entity=self._spec.entity, table=self._table, rows=rows, checks=checks
        )
        log.info(
            "quality.report",
            entity=self._spec.entity,
            table=self._table,
            rows=rows,
            failures=len(report.failures),
            warnings=len(report.warnings),
        )
        return report

    # --- отдельные проверки ---------------------------------------------------

    async def _counts(self) -> tuple[int, int]:
        # Здесь и ниже имена подставляются в текст запроса: параметром их
        # передать нельзя. Все они проверены safe_identifier при создании.
        result = await self._client.query(
            f"SELECT count(), uniqExact({self._spec.id_column}) FROM {self._table}"  # noqa: S608
        )
        row = result.result_rows[0]
        return int(row[0]), int(row[1])

    @staticmethod
    def _check_not_empty(rows: int) -> CheckResult:
        return CheckResult(
            name="таблица не пуста",
            passed=rows > 0,
            severity=Severity.FAIL,
            observed=f"{rows} строк",
            limit="больше нуля",
            comment="" if rows else "загрузка не принесла ни одной записи",
        )

    def _check_duplicates(self, rows: int, distinct_ids: int) -> CheckResult:
        duplicates = rows - distinct_ids
        return CheckResult(
            name="дубли по id",
            passed=duplicates == 0,
            severity=Severity.FAIL,
            observed=f"{duplicates}",
            limit="0",
            comment=(
                ""
                if duplicates == 0
                else "ревизии не схлопнулись: проверь OPTIMIZE перед подменой партиции"
            ),
        )

    async def _check_null_share(self, column: str, rows: int) -> CheckResult:
        column = safe_identifier(column, what="имя колонки")
        result = await self._client.query(
            f"SELECT countIf(empty(toString({column}))) FROM {self._table}"  # noqa: S608
        )
        empties = int(result.result_rows[0][0])
        share = _share(empties, rows)
        severity = Severity.FAIL if share > self._rules.max_null_share else Severity.WARN
        limit = (
            self._rules.max_null_share if severity is Severity.FAIL else self._rules.warn_null_share
        )
        return CheckResult(
            name=f"пустых в {column}",
            passed=share <= limit,
            severity=severity,
            observed=f"{share:.2%} ({empties} из {rows})",
            limit=f"{limit:.2%}",
        )

    async def _check_amount_range(self, rows: int) -> CheckResult:
        column = self._rules.amount_field
        result = await self._client.query(
            f"SELECT countIf({column} < 0 OR {column} > {self._rules.max_amount}), "  # noqa: S608
            f"min({column}), max({column}) FROM {self._table}"
        )
        bad, low, high = result.result_rows[0]
        share = _share(int(bad), rows)
        return CheckResult(
            name=f"{column} вне диапазона",
            passed=share <= self._rules.max_out_of_range_share,
            severity=Severity.FAIL,
            observed=f"{bad} записей, min={low}, max={high}",
            limit=f"{self._rules.max_out_of_range_share:.2%}",
            comment="отрицательная сумма или запредельная величина",
        )

    async def _check_unknown_dates(self, rows: int) -> CheckResult:
        column = self._spec.version_column
        stamp = UNKNOWN_DATE.strftime("%Y-%m-%d %H:%M:%S")
        result = await self._client.query(
            f"SELECT countIf({column} = toDateTime('{stamp}')) FROM {self._table}"  # noqa: S608
        )
        unknown = int(result.result_rows[0][0])
        share = _share(unknown, rows)
        return CheckResult(
            name=f"не разобрана дата {column}",
            passed=share <= self._rules.max_unknown_date_share,
            severity=Severity.WARN,
            observed=f"{share:.2%} ({unknown} из {rows})",
            limit=f"{self._rules.max_unknown_date_share:.2%}",
            comment="такие записи попадают в партицию 197001 и видны отдельно",
        )

    def _check_count_against_api(self, rows: int, api_total: int) -> CheckResult:
        if api_total <= 0:
            return CheckResult(
                name="сверка с числом записей у источника",
                passed=True,
                severity=Severity.WARN,
                observed="источник не сообщил total",
                limit="—",
            )
        drift = abs(rows - api_total) / api_total
        return CheckResult(
            name="сверка с числом записей у источника",
            passed=drift <= self._rules.count_tolerance,
            severity=Severity.FAIL,
            observed=f"загружено {rows}, источник заявил {api_total}, расхождение {drift:.2%}",
            limit=f"{self._rules.count_tolerance:.2%}",
            comment="потеря страниц при загрузке или изменение данных во время выкачки",
        )
