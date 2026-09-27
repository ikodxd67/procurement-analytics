"""Окружение Alembic.

Отличия от шаблона по умолчанию, каждое намеренное:

1. Адрес базы берётся из настроек приложения, а не из alembic.ini. Иначе он
   оказался бы записан в двух местах, и однажды они разойдутся. Заодно пароль
   не попадает в файл, который лежит в репозитории.
2. target_metadata указывает на наши модели — без этого автогенерация
   миграций не работает вовсе.
3. compare_type и compare_server_default включены: без них Alembic не заметит
   смену типа колонки или значения по умолчанию и молча пропустит изменение.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from procurement.config import get_settings
from procurement.storage.postgres.base import Base
from procurement.storage.postgres.models import (  # noqa: F401  # нужны для автогенерации
    AppUser,
    IngestRun,
    RefClassifier,
    RefStatus,
    SavedFilter,
    SyncState,
)

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Адрес берётся из настроек, но только если его не задали снаружи. Тесты на
# testcontainers поднимают свою временную базу и подставляют её адрес через
# Config перед вызовом upgrade — без этой проверки миграция уехала бы в базу
# разработчика и снесла его данные.
if not config.get_main_option("sqlalchemy.url", None):
    config.set_main_option("sqlalchemy.url", get_settings().postgres.dsn)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        # NullPool: миграция — разовая операция, пул соединений ей не нужен и
        # только мешает корректно закрыться.
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
