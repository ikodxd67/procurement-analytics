"""Подключение к ClickHouse."""

from __future__ import annotations

from clickhouse_connect import get_async_client
from clickhouse_connect.driver import AsyncClient

from procurement.config import ClickHouseSettings


async def build_client(settings: ClickHouseSettings, *, database: str | None = None) -> AsyncClient:
    """Асинхронный клиент поверх HTTP-интерфейса (порт 8123).

    Почему HTTP, а не родной протокол на 9000: он проще в проксировании и
    отладке (запрос видно обычным curl), а разница в скорости заметна только
    при выгрузке очень больших результатов. Мы гоняем агрегаты, а не выгружаем
    миллионы строк в приложение.
    """
    return await get_async_client(
        host=settings.host,
        port=settings.port,
        database=database if database is not None else settings.database,
        username=settings.user,
        password=settings.password.get_secret_value(),
    )
