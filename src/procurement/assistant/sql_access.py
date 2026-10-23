"""Пользователь ClickHouse только для чтения — для инструмента ``run_sql``.

**Защита SQL агента держится на базе, а не на промпте и не на разборе текста.**
Промпт модель может проигнорировать, а текстовую проверку можно обойти
(комментарием, регистром, вложенным запросом). Поэтому запросы агента идут под
отдельным пользователем, которому база сама не даст сделать лишнего:

- права только ``SELECT`` и только на четыре таблицы аналитики. Без
  ``CREATE TEMPORARY TABLE`` недоступны табличные функции ``url``, ``file``,
  ``s3``, ``remote`` — а через них можно было бы читать файлы сервера и ходить
  в сеть (документация ClickHouse, раздел Table functions → Permissions);
- профиль настроек с лимитами ``CONST``: запрос не может поднять их своим
  ``SETTINGS max_execution_time = ...`` (документация, Constraints on settings);
- ``readonly = 2``: менять данные и схему нельзя, менять незакреплённые
  настройки можно. Уровень 1 запретил бы менять любые настройки, и запрос
  упал бы, если драйвер передаст свою. Передаёт ли clickhouse-connect
  настройки сам — не проверял; лимиты всё равно закреплены через CONST.

Текстовая проверка в ``sql_tool`` остаётся, но только ради понятной ошибки для
модели, а не как защита.
"""

from __future__ import annotations

import re

from clickhouse_connect.driver import AsyncClient

from procurement.logging import get_logger

log = get_logger(__name__)

PROFILE = "assistant_readonly"

# Таблицы, которые агенту можно читать. Состояние загрузки, служебные таблицы и
# всё, что появится потом, сюда не попадает само — только явной правкой.
ALLOWED_TABLES: tuple[str, ...] = ("contracts", "lots", "mart_contracts_monthly", "ref_classifier")

# Лимиты на один запрос агента. Время и память — чтобы один неудачный JOIN не
# положил базу для API. Строки результата — с режимом break: база отдаёт
# первые строки вместо ошибки (по документации может вернуть чуть больше —
# break срабатывает на границе блока; точную обрезку делает sql_tool).
LIMITS: dict[str, int | str] = {
    "readonly": 2,
    "max_execution_time": 10,
    "max_memory_usage": 1_000_000_000,
    "max_rows_to_read": 50_000_000,
    "max_result_rows": 1000,
    "result_overflow_mode": "break",
}

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
# Пароль подставляется в текст CREATE USER: параметры запроса ClickHouse в DDL
# не работают. Поэтому набор символов узкий, и кавычка в него не входит.
_PASSWORD = re.compile(r"^[A-Za-z0-9_\-]{12,128}$")


def readonly_user_statements(user: str, password: str, database: str) -> list[str]:
    for name, value in (("user", user), ("database", database)):
        if not _IDENTIFIER.match(value):
            msg = f"{name}={value!r} — не имя ClickHouse"
            raise ValueError(msg)
    if not _PASSWORD.match(password):
        msg = "пароль: 12–128 символов из букв, цифр, _ и -"
        raise ValueError(msg)

    limits = ", ".join(
        f"{name} = {value if isinstance(value, int) else repr(value)} CONST"
        for name, value in LIMITS.items()
    )
    statements = [
        # OR REPLACE: повторный запуск обновляет лимиты и пароль, а не падает.
        f"CREATE SETTINGS PROFILE OR REPLACE {PROFILE} SETTINGS {limits}",
        f"CREATE USER OR REPLACE {user} IDENTIFIED WITH sha256_password BY '{password}' "
        f"SETTINGS PROFILE '{PROFILE}' DEFAULT DATABASE {database}",
    ]
    statements += [f"GRANT SELECT ON {database}.{table} TO {user}" for table in ALLOWED_TABLES]
    return statements


async def apply_readonly_user(
    admin: AsyncClient, *, user: str, password: str, database: str
) -> None:
    """Создать или обновить пользователя. Нужны права администратора."""
    for statement in readonly_user_statements(user, password, database):
        await admin.command(statement)
    log.info("assistant.readonly_user", user=user, tables=list(ALLOWED_TABLES))
