"""Создание асинхронного движка и фабрики сессий."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from procurement.config import PostgresSettings


def build_engine(settings: PostgresSettings) -> AsyncEngine:
    return create_async_engine(
        settings.dsn,
        echo=settings.echo,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        # Проверять соединение перед выдачей из пула. Без этого первое
        # обращение после простоя натыкается на соединение, которое сервер уже
        # закрыл, и падает вместо того, чтобы переподключиться.
        pool_pre_ping=True,
    )


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        engine,
        # Не сбрасывать объекты после commit. Иначе обращение к любому полю
        # после фиксации отправит в базу новый запрос, а в асинхронном коде
        # ленивая подгрузка вне сессии просто упадёт.
        expire_on_commit=False,
    )


@asynccontextmanager
async def session_scope(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Сессия с транзакцией: успех — commit, исключение — rollback."""
    async with factory() as session:
        try:
            yield session
        except BaseException:
            await session.rollback()
            raise
        else:
            await session.commit()
