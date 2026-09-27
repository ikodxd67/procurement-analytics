"""Сборка приложения FastAPI."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from procurement.api.errors import install_error_handlers
from procurement.api.routers import analytics, health
from procurement.config import Settings, get_settings
from procurement.logging import configure_logging, get_logger
from procurement.storage.clickhouse.client import build_client
from procurement.storage.postgres.engine import build_engine, build_session_factory

log = get_logger(__name__)

DESCRIPTION = """
Срезы по данным госзакупок: доли поставщиков, свёртка по классификатору,
помесячная динамика, отклонения цен, концентрация закупок, периоды активности.

Показатели описывают распределение сумм и сроки. Выводов о добросовестности
участников из них не следует, и API таких выводов не делает.
"""


def build_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or get_settings()
    configure_logging(resolved.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Подключения создаются один раз на всё время жизни приложения.
        # Создавать их на запрос значило бы делать рукопожатия заново
        # несколько раз в секунду.
        app.state.clickhouse = await build_client(resolved.clickhouse)
        engine = build_engine(resolved.postgres)
        app.state.engine = engine
        app.state.sessions = build_session_factory(engine)
        log.info("api.started", env=resolved.env)
        try:
            yield
        finally:
            await app.state.clickhouse.close()
            await engine.dispose()
            log.info("api.stopped")

    app = FastAPI(
        title="procurement-analytics",
        description=DESCRIPTION,
        version="0.1.0",
        lifespan=lifespan,
    )
    install_error_handlers(app)
    app.include_router(health.router)
    app.include_router(analytics.router)
    return app


app = build_app()
