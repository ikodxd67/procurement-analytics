"""Инструмент ``run_sql``: свободный SELECT к ClickHouse, когда готового среза нет.

Выполняется под пользователем только для чтения (``sql_access``). Проверка
текста здесь — не защита, а вежливость: модель сразу получает понятное «нужен
SELECT», а не ошибку прав от базы. Даже если проверку обойти, база не даст
ни писать, ни читать чужое.
"""

from __future__ import annotations

import re
from typing import Any

from clickhouse_connect.driver import AsyncClient
from clickhouse_connect.driver.exceptions import ClickHouseError
from pydantic import BaseModel, ConfigDict, Field

from procurement.assistant.sql_access import ALLOWED_TABLES
from procurement.assistant.tools import Tool, ToolError

# Строк результата, которые увидит модель. База отдаёт до 1000 (лимит
# профиля), дальше режем сами и честно пишем, что обрезали.
MAX_ROWS_TO_MODEL = 50

_LEADING_COMMENTS = re.compile(r"^(\s+|--[^\n]*\n?|/\*.*?\*/)*", re.S)
_STARTS_WITH_SELECT = re.compile(r"^(select|with)\b", re.I)

# S608 — ложное срабатывание: это описание инструмента для модели, а не запрос.
DESCRIPTION = f"""\
Выполнить один SELECT в ClickHouse (диалект ClickHouse), если ни один готовый \
срез не подходит. Только чтение; лимит 10 секунд и {MAX_ROWS_TO_MODEL} строк \
результата — агрегируй в запросе, а не выгружай строки.

Таблицы: {", ".join(ALLOWED_TABLES)}.
- contracts — договоры: id, contract_number, supplier_biin, customer_bin, \
contract_sum Decimal(18,2), fakt_sum, ref_contract_status_id, crdate DateTime \
(дата создания), sign_date, last_update_date. Движок ReplacingMergeTree: \
пиши FROM contracts FINAL, иначе посчитаются устаревшие версии записей.
- lots — лоты: id, lot_number, customer_bin, name_ru, enstru_code (код ЕНС ТРУ), \
count Decimal(18,3), amount Decimal(18,2), last_update_date. Тоже FROM lots FINAL.
- mart_contracts_monthly — витрина: month Date, customer_bin, contracts, total.
- ref_classifier — классификатор: code, parent_code, level (0–3), name_ru.\
"""  # noqa: S608


class RunSqlArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(min_length=1, max_length=4000, description="Один SELECT или WITH … SELECT")


def check_select(query: str) -> str:
    """Один запрос на чтение. Возвращает текст без завершающей точки с запятой."""
    body = query.strip().rstrip(";").strip()
    if ";" in body:
        msg = "нужен один запрос: точка с запятой внутри запрещена"
        raise ToolError(msg)
    if not _STARTS_WITH_SELECT.match(_LEADING_COMMENTS.sub("", body, count=1)):
        msg = "разрешён только SELECT (или WITH … SELECT)"
        raise ToolError(msg)
    return body


async def run_select(client: AsyncClient, query: str) -> dict[str, Any]:
    sql = check_select(query)
    try:
        result = await client.query(sql)
    except ClickHouseError as exc:
        # Текст ошибки модели нужен: по нему она исправит запрос. Запрос она
        # написала сама, так что в тексте нет ничего, чего она не знает.
        first_line = str(exc).strip().splitlines()[0] if str(exc).strip() else "ошибка"
        msg = f"ClickHouse: {first_line[:500]}"
        raise ToolError(msg) from exc

    rows = [list(row) for row in result.result_rows]
    return {
        "columns": list(result.column_names),
        "rows": rows[:MAX_ROWS_TO_MODEL],
        "truncated": len(rows) > MAX_ROWS_TO_MODEL,
    }


def build_sql_tool(client: AsyncClient) -> Tool[RunSqlArgs]:
    async def run(args: RunSqlArgs) -> dict[str, Any]:
        return await run_select(client, args.query)

    return Tool("run_sql", DESCRIPTION, RunSqlArgs, run)
