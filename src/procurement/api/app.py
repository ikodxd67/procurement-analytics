"""Сборка приложения FastAPI."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI

from procurement.api.errors import install_error_handlers
from procurement.api.ratelimit import SlidingWindowLimiter
from procurement.api.routers import analytics, assistant, health
from procurement.api.routers.assistant import WINDOW_S, AskEndpoint
from procurement.assistant.service import AnswerCache, AskService
from procurement.assistant.stack import open_agent
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

POST /v1/ask — вопрос ИИ-ассистенту (включается PA_ASSISTANT__API__ENABLED):
ответ с найденными пунктами правил и выполненным SQL.
"""


def build_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or get_settings()
    configure_logging(resolved.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Подключения создаются один раз на всё время жизни приложения.
        # Создавать их на запрос значило бы делать рукопожатия заново
        # несколько раз в секунду. AsyncExitStack закрывает всё в обратном
        # порядке, даже если упало открытие следующего.
        async with AsyncExitStack() as stack:
            app.state.clickhouse = await build_client(resolved.clickhouse)
            stack.push_async_callback(app.state.clickhouse.close)
            engine = build_engine(resolved.postgres)
            stack.push_async_callback(engine.dispose)
            app.state.engine = engine
            app.state.sessions = build_session_factory(engine)
            app.state.ask = None
            if resolved.assistant.api.enabled:
                app.state.ask = await stack.enter_async_context(_open_ask(resolved, app))
            log.info("api.started", env=resolved.env, assistant=app.state.ask is not None)
            yield
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
    app.include_router(assistant.router)
    return app


@asynccontextmanager
async def _open_ask(settings: Settings, app: FastAPI) -> AsyncIterator[AskEndpoint]:
    api = settings.assistant.api
    async with open_agent(
        settings, clickhouse=app.state.clickhouse, sessions=app.state.sessions
    ) as opened:
        # Модель эмбеддингов (гигабайт) грузится здесь, а не на первом
        # вопросе: иначе первый клиент ждал бы её загрузки лишние секунды.
        await opened.embedder.embed_query("прогрев")
        yield AskEndpoint(
            service=AskService(
                opened.agent,
                AnswerCache(ttl_s=api.cache_ttl_s, max_entries=api.cache_max_entries),
            ),
            model=settings.assistant.llm.model,
            per_client=SlidingWindowLimiter(api.per_client_per_minute, WINDOW_S),
            total=SlidingWindowLimiter(api.total_per_minute, WINDOW_S),
        )


app = build_app()
