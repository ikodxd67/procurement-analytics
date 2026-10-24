"""Эталонный SQL: правильный ответ на вопрос по данным считается, а не хранится.

Хранить число нельзя: синтетику можно перегенерировать, и эталон тихо
устареет. Хранить запрос — можно, и тогда эталон всегда про те данные, на
которых идёт прогон. Выполняется тем же путём, что ``run_sql`` агента: под
пользователем только для чтения и через ту же проверку SELECT — эталону не
положено больше прав, чем агенту.
"""

from __future__ import annotations

from decimal import Decimal

from clickhouse_connect.driver import AsyncClient

from procurement.assistant.evals.dataset import Question
from procurement.assistant.sql_tool import run_select


class GoldError(Exception):
    """Эталонный запрос не дал одного числа — набор вопросов надо чинить."""


async def gold_value(readonly: AsyncClient, question: Question) -> float:
    if question.gold_sql is None:
        msg = f"{question.id}: у вопроса нет gold_sql"
        raise GoldError(msg)
    out = await run_select(readonly, question.gold_sql)
    rows = out["rows"]
    if len(rows) != 1 or len(rows[0]) != 1:
        msg = f"{question.id}: эталон должен вернуть одно значение, получено {rows[:3]}"
        raise GoldError(msg)
    value = rows[0][0]
    if isinstance(value, bool) or not isinstance(value, int | float | Decimal):
        msg = f"{question.id}: эталон вернул не число: {value!r}"
        raise GoldError(msg)
    return float(value)
