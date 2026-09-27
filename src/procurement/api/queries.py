"""Запросы витрин к ClickHouse.

Все значения, пришедшие от клиента, передаются типизированными параметрами
`{name:Type}`. Подстановки в текст запроса нет нигде: это не только про
инъекции, но и про типы — ClickHouse проверит, что в UInt8 пришло число, до
того как запрос дойдёт до данных.

Про FINAL. Факты лежат в ReplacingMergeTree, и без FINAL агрегаты посчитались
бы по всем версиям записи, включая устаревшие. Здесь FINAL обходится дёшево,
потому что бэкфилл кладёт партиции уже схлопнутыми через REPLACE PARTITION, и
кусков мало. Но это свойство текущей загрузки, а не гарантия: инкрементальная
синхронизация добавляет новые куски, и с ростом их числа цена FINAL вырастет.
Измерения задержек — в docs/bench/api_latency.md, и там же видно, где витрина
уже нужна.

Лимит выборки всегда запрашивается на единицу больше нужного. Так узнаём, есть
ли следующая страница, не выполняя COUNT по всей выборке — на миллионах строк
такой COUNT стоил бы дороже самой страницы.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from clickhouse_connect.driver import AsyncClient
from clickhouse_connect.driver.exceptions import ClickHouseError

from procurement.api.errors import StorageUnavailableError
from procurement.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class Period:
    date_from: date
    date_to: date


@dataclass(frozen=True, slots=True)
class Paging:
    limit: int
    offset: int


# Два полных запроса, а не общий хвост со склейкой: собирать текст запроса из
# кусков во время выполнения — привычка, которая однажды приведёт к подстановке
# туда чужой строки. Хвост продублирован сознательно.

MONTHLY_FROM_MART = """
WITH monthly AS (
    SELECT month, sum(contracts) AS contracts, sum(total) AS total
    FROM mart_contracts_monthly
    WHERE month >= toStartOfMonth({date_from:Date})
      AND month <= toStartOfMonth({date_to:Date})
      AND ({customer_bin:String} = '' OR customer_bin = {customer_bin:String})
    GROUP BY month
)
SELECT
    month,
    contracts,
    total,
    prev_total,
    round(100.0 * toFloat64(total - prev_total) / nullif(toFloat64(prev_total), 0), 1)
        AS change_pct
FROM (
    SELECT
        month, contracts, total,
        lagInFrame(toNullable(total), 1, NULL) OVER (
            ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS prev_total
    FROM monthly
)
ORDER BY month
"""

MONTHLY_FROM_FACTS = """
WITH monthly AS (
    SELECT
        toStartOfMonth(crdate) AS month,
        count()                AS contracts,
        sum(contract_sum)      AS total
    FROM contracts FINAL
    WHERE crdate >= {date_from:Date}
      AND crdate < addDays({date_to:Date}, 1)
      AND supplier_biin = {supplier_biin:String}
      AND ({customer_bin:String} = '' OR customer_bin = {customer_bin:String})
    GROUP BY month
)
SELECT
    month,
    contracts,
    total,
    prev_total,
    round(100.0 * toFloat64(total - prev_total) / nullif(toFloat64(prev_total), 0), 1)
        AS change_pct
FROM (
    SELECT
        month, contracts, total,
        lagInFrame(toNullable(total), 1, NULL) OVER (
            ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS prev_total
    FROM monthly
)
ORDER BY month
"""


class Marts:
    def __init__(self, client: AsyncClient) -> None:
        self._client = client

    async def _fetch(self, sql: str, params: dict[str, Any]) -> list[tuple[Any, ...]]:
        try:
            result = await self._client.query(sql, parameters=params)
        except ClickHouseError as error:
            # Наружу не пробрасываем: в тексте ошибки ClickHouse бывает сам
            # запрос целиком. Клиенту — 503, подробности в лог.
            log.error("marts.query_failed", error=str(error)[:500])
            msg = "Хранилище аналитики недоступно"
            raise StorageUnavailableError(msg) from error
        return [tuple(row) for row in result.result_rows]

    @staticmethod
    def _split(rows: list[tuple[Any, ...]], paging: Paging) -> tuple[list[tuple[Any, ...]], bool]:
        """Отрезать лишнюю запись, по которой определяли наличие следующей страницы."""
        has_more = len(rows) > paging.limit
        return rows[: paging.limit], has_more

    # --- поставщики одного заказчика ------------------------------------------

    async def supplier_shares(
        self, customer_bin: str, period: Period, paging: Paging
    ) -> tuple[list[tuple[Any, ...]], bool]:
        rows = await self._fetch(
            """
            SELECT
                supplier_biin,
                count()           AS contracts,
                sum(contract_sum) AS total,
                round(
                    100.0 * toFloat64(sum(contract_sum))
                    / toFloat64(sum(sum(contract_sum)) OVER ()),
                    2
                ) AS share_pct
            FROM contracts FINAL
            WHERE customer_bin = {customer_bin:String}
              AND crdate >= {date_from:Date}
              AND crdate < addDays({date_to:Date}, 1)
            GROUP BY supplier_biin
            ORDER BY total DESC, supplier_biin
            LIMIT {limit:UInt32} OFFSET {offset:UInt32}
            """,
            {
                "customer_bin": customer_bin,
                "date_from": period.date_from,
                "date_to": period.date_to,
                "limit": paging.limit + 1,
                "offset": paging.offset,
            },
        )
        return self._split(rows, paging)

    # --- заказчики одного поставщика ------------------------------------------

    async def customer_shares(
        self, supplier_biin: str, period: Period, paging: Paging
    ) -> tuple[list[tuple[Any, ...]], bool]:
        rows = await self._fetch(
            """
            SELECT
                customer_bin,
                count()           AS contracts,
                sum(contract_sum) AS total,
                round(
                    100.0 * toFloat64(sum(contract_sum))
                    / toFloat64(sum(sum(contract_sum)) OVER ()),
                    2
                ) AS share_pct
            FROM contracts FINAL
            WHERE supplier_biin = {supplier_biin:String}
              AND crdate >= {date_from:Date}
              AND crdate < addDays({date_to:Date}, 1)
            GROUP BY customer_bin
            ORDER BY total DESC, customer_bin
            LIMIT {limit:UInt32} OFFSET {offset:UInt32}
            """,
            {
                "supplier_biin": supplier_biin,
                "date_from": period.date_from,
                "date_to": period.date_to,
                "limit": paging.limit + 1,
                "offset": paging.offset,
            },
        )
        return self._split(rows, paging)

    # --- свёртка по классификатору --------------------------------------------

    async def category_rollup(
        self, level: int, period: Period, paging: Paging
    ) -> tuple[list[tuple[Any, ...]], bool]:
        rows = await self._fetch(
            """
            WITH RECURSIVE up AS (
                SELECT code AS leaf_code, code, parent_code, level
                FROM ref_classifier
                WHERE level = 3
                UNION ALL
                SELECT u.leaf_code, parent.code, parent.parent_code, parent.level
                FROM up AS u
                JOIN ref_classifier AS parent ON parent.code = u.parent_code
            ),
            mapping AS (
                SELECT leaf_code, code AS rollup_code FROM up WHERE level = {level:UInt8}
            )
            SELECT
                m.rollup_code,
                node.name_ru,
                node.level,
                count()                 AS lots,
                sum(l.amount)           AS total_amount,
                round(avg(l.amount), 2) AS avg_amount
            FROM lots AS l FINAL
            JOIN mapping AS m           ON m.leaf_code = l.enstru_code
            JOIN ref_classifier AS node ON node.code = m.rollup_code
            WHERE l.last_update_date >= {date_from:Date}
              AND l.last_update_date < addDays({date_to:Date}, 1)
            GROUP BY m.rollup_code, node.name_ru, node.level
            ORDER BY total_amount DESC, m.rollup_code
            LIMIT {limit:UInt32} OFFSET {offset:UInt32}
            """,
            {
                "level": level,
                "date_from": period.date_from,
                "date_to": period.date_to,
                "limit": paging.limit + 1,
                "offset": paging.offset,
            },
        )
        return self._split(rows, paging)

    # --- помесячная динамика ----------------------------------------------------

    async def monthly(
        self,
        period: Period,
        *,
        customer_bin: str | None = None,
        supplier_biin: str | None = None,
    ) -> list[tuple[Any, ...]]:
        """Динамика по месяцам.

        Без пагинации намеренно: диапазон ограничен периодом, а период — не
        больше нескольких лет. Страницы здесь были бы неудобством без пользы.

        Источник выбирается по набору фильтров. Без фильтра по поставщику
        хватает витрины mart_contracts_monthly: 216 тысяч строк вместо 1.8
        миллиона. Поставщика в витрине нет — добавлять его значило бы вернуть
        почти исходный объём, потому что пара «заказчик + поставщик» на этих
        данных почти уникальна. С фильтром по поставщику идём в факты.
        """
        if supplier_biin:
            return await self._monthly_from_facts(period, customer_bin, supplier_biin)
        return await self._monthly_from_mart(period, customer_bin)

    async def _monthly_from_mart(
        self, period: Period, customer_bin: str | None
    ) -> list[tuple[Any, ...]]:
        return await self._fetch(
            MONTHLY_FROM_MART,
            {
                "date_from": period.date_from,
                "date_to": period.date_to,
                "customer_bin": customer_bin or "",
            },
        )

    async def _monthly_from_facts(
        self, period: Period, customer_bin: str | None, supplier_biin: str
    ) -> list[tuple[Any, ...]]:
        return await self._fetch(
            MONTHLY_FROM_FACTS,
            {
                "date_from": period.date_from,
                "date_to": period.date_to,
                "customer_bin": customer_bin or "",
                "supplier_biin": supplier_biin,
            },
        )

    # --- отклонение цены от медианы ----------------------------------------------

    async def price_outliers(
        self, period: Period, paging: Paging, *, min_lots: int, enstru_code: str | None = None
    ) -> tuple[list[tuple[Any, ...]], bool]:
        rows = await self._fetch(
            """
            WITH unit_prices AS (
                SELECT id, enstru_code, toFloat64(amount) / toFloat64(count) AS unit_price
                FROM lots FINAL
                WHERE count > 0 AND amount > 0
                  AND last_update_date >= {date_from:Date}
                  AND last_update_date < addDays({date_to:Date}, 1)
                  AND ({enstru_code:String} = '' OR enstru_code = {enstru_code:String})
            ),
            position_stats AS (
                SELECT
                    enstru_code,
                    quantileExactInclusive(0.5)(unit_price) AS median_price,
                    count() AS lots_in_position
                FROM unit_prices
                GROUP BY enstru_code
                HAVING count() >= {min_lots:UInt32}
            )
            SELECT
                u.id,
                u.enstru_code,
                round(u.unit_price, 2),
                round(s.median_price, 2),
                s.lots_in_position,
                round(100.0 * (u.unit_price - s.median_price) / s.median_price, 1)
            FROM unit_prices AS u
            JOIN position_stats AS s ON s.enstru_code = u.enstru_code
            ORDER BY abs(u.unit_price - s.median_price) / s.median_price DESC, u.id
            LIMIT {limit:UInt32} OFFSET {offset:UInt32}
            """,
            {
                "date_from": period.date_from,
                "date_to": period.date_to,
                "enstru_code": enstru_code or "",
                "min_lots": min_lots,
                "limit": paging.limit + 1,
                "offset": paging.offset,
            },
        )
        return self._split(rows, paging)

    # --- концентрация закупок ------------------------------------------------------

    async def concentration(
        self, period: Period, paging: Paging, *, min_suppliers: int
    ) -> tuple[list[tuple[Any, ...]], bool]:
        rows = await self._fetch(
            """
            WITH per_supplier AS (
                SELECT customer_bin, supplier_biin, sum(contract_sum) AS total
                FROM contracts FINAL
                WHERE crdate >= {date_from:Date}
                  AND crdate < addDays({date_to:Date}, 1)
                GROUP BY customer_bin, supplier_biin
            ),
            shares AS (
                SELECT
                    customer_bin,
                    100.0 * toFloat64(total)
                        / toFloat64(sum(total) OVER (PARTITION BY customer_bin)) AS share_pct,
                    row_number() OVER (PARTITION BY customer_bin ORDER BY total DESC) AS position
                FROM per_supplier
            ),
            cumulative AS (
                SELECT
                    customer_bin, position, share_pct,
                    sum(share_pct) OVER (
                        PARTITION BY customer_bin ORDER BY position
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cum_share_pct
                FROM shares
            )
            SELECT
                customer_bin,
                count()                           AS suppliers,
                toUInt32(round(sum(share_pct * share_pct))) AS hhi,
                round(max(share_pct), 1)          AS top1_share_pct,
                if(count() >= 3, round(maxIf(cum_share_pct, position = 3), 1), NULL)
                    AS top3_share_pct
            FROM cumulative
            GROUP BY customer_bin
            HAVING count() >= {min_suppliers:UInt32}
            ORDER BY hhi DESC, customer_bin
            LIMIT {limit:UInt32} OFFSET {offset:UInt32}
            """,
            {
                "date_from": period.date_from,
                "date_to": period.date_to,
                "min_suppliers": min_suppliers,
                "limit": paging.limit + 1,
                "offset": paging.offset,
            },
        )
        return self._split(rows, paging)

    # --- периоды активности поставщика -----------------------------------------------

    async def supplier_activity(
        self, supplier_biin: str, period: Period, paging: Paging
    ) -> tuple[list[tuple[Any, ...]], bool]:
        rows = await self._fetch(
            """
            WITH active_months AS (
                SELECT supplier_biin, toStartOfMonth(crdate) AS month
                FROM contracts FINAL
                WHERE supplier_biin = {supplier_biin:String}
                  AND crdate >= {date_from:Date}
                  AND crdate < addDays({date_to:Date}, 1)
                GROUP BY supplier_biin, month
            ),
            numbered AS (
                SELECT
                    supplier_biin, month,
                    toInt32(toYear(month) * 12 + toMonth(month))
                        - toInt32(row_number() OVER (PARTITION BY supplier_biin ORDER BY month))
                      AS island_key
                FROM active_months
            )
            SELECT supplier_biin, min(month), max(month), count()
            FROM numbered
            GROUP BY supplier_biin, island_key
            ORDER BY min(month)
            LIMIT {limit:UInt32} OFFSET {offset:UInt32}
            """,
            {
                "supplier_biin": supplier_biin,
                "date_from": period.date_from,
                "date_to": period.date_to,
                "limit": paging.limit + 1,
                "offset": paging.offset,
            },
        )
        return self._split(rows, paging)

    # --- существование сущности ---------------------------------------------------------

    async def customer_exists(self, customer_bin: str) -> bool:
        rows = await self._fetch(
            "SELECT 1 FROM contracts WHERE customer_bin = {value:String} LIMIT 1",
            {"value": customer_bin},
        )
        return bool(rows)

    async def supplier_exists(self, supplier_biin: str) -> bool:
        rows = await self._fetch(
            "SELECT 1 FROM contracts WHERE supplier_biin = {value:String} LIMIT 1",
            {"value": supplier_biin},
        )
        return bool(rows)

    async def ping(self) -> None:
        await self._fetch("SELECT 1", {})
