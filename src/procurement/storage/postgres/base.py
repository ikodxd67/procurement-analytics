"""Базовый класс моделей и соглашение об именах ограничений.

Зачем соглашение об именах. Если его не задать, PostgreSQL придумывает имена
индексов и ограничений сам, и Alembic при автогенерации не может сопоставить
то, что в базе, с тем, что в коде: он видит ограничение без известного имени и
предлагает его удалить и создать заново. Соглашение делает имена
предсказуемыми с обеих сторон.

Второе следствие важнее: именованное ограничение можно снять в миграции по
имени. Безымянное придётся искать запросом к системным таблицам.
"""

from __future__ import annotations

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
