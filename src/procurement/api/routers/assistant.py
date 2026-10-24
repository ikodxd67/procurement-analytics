"""Вопрос ассистенту: POST /v1/ask.

POST, а не GET: вопрос — текст до тысячи символов, в адресе ему не место
(длина URL ограничена, а адреса оседают в логах прокси). Кэш от этого не
страдает — он свой, по нормализованному тексту вопроса.

Ответ — не только текст. Клиенту (и проверяющему) нужно видеть, на чём он
построен: какие пункты правил нашлись, какой SQL написала модель и сколько
стоил проход. Ответ без источников проверить нельзя, а значит, и доверять ему.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from fastapi import APIRouter, Depends, Request

from procurement.api.errors import (
    AssistantDisabledError,
    ErrorResponse,
    ModelFailedError,
    ModelUnavailableError,
    TooManyRequestsError,
)
from procurement.api.ratelimit import SlidingWindowLimiter
from procurement.api.schemas import (
    AskRequest,
    AskResponse,
    AskSource,
    AskSql,
    AskToolCall,
    AskUsage,
)
from procurement.assistant.errors import LLMError
from procurement.assistant.pricing import cost_usd
from procurement.assistant.service import Asked, AskService, sources, sql_runs
from procurement.logging import get_logger

log = get_logger(__name__)

WINDOW_S = 60.0


@dataclass(frozen=True, slots=True)
class AskEndpoint:
    """Всё, что нужно ручке; собирается при старте приложения (app.lifespan)."""

    service: AskService
    model: str
    per_client: SlidingWindowLimiter
    total: SlidingWindowLimiter

    def check_limits(self, client: str) -> None:
        # Сначала лимит клиента: если один клиент упёрся в свой, его лишние
        # запросы не должны съедать общий лимит у остальных.
        wait = self.per_client.retry_after(client)
        if wait is not None:
            msg = f"Слишком много вопросов с вашего адреса. Повторите через {wait} с"
            raise TooManyRequestsError(msg, retry_after_s=wait)
        wait = self.total.retry_after("*")
        if wait is not None:
            msg = f"Ассистент сейчас загружен. Повторите через {wait} с"
            raise TooManyRequestsError(msg, retry_after_s=wait)


def get_ask_endpoint(request: Request) -> AskEndpoint:
    endpoint: AskEndpoint | None = getattr(request.app.state, "ask", None)
    if endpoint is None:
        msg = "Ассистент выключен на этом стенде (PA_ASSISTANT__API__ENABLED)"
        raise AssistantDisabledError(msg)
    return endpoint


def client_key(request: Request) -> str:
    # Адрес соединения. За обратным прокси это адрес прокси — тогда uvicorn
    # запускают с --proxy-headers и списком доверенных адресов, и сюда придёт
    # адрес из X-Forwarded-For. Самим читать заголовок нельзя: его подделает
    # любой клиент и обойдёт лимит.
    return request.client.host if request.client else "unknown"


router = APIRouter(
    prefix="/v1",
    tags=["ассистент"],
    responses={
        422: {"model": ErrorResponse, "description": "Вопрос пустой или слишком длинный"},
        429: {"model": ErrorResponse, "description": "Превышен лимит; ждать Retry-After секунд"},
        502: {"model": ErrorResponse, "description": "Модель вернула ошибку"},
        503: {"model": ErrorResponse, "description": "Ассистент выключен или модель недоступна"},
    },
)


@router.post("/ask", summary="Вопрос ассистенту по закупкам")
async def ask(
    body: AskRequest,
    request: Request,
    endpoint: Annotated[AskEndpoint, Depends(get_ask_endpoint)],
) -> AskResponse:
    """Ответ агента: текст, найденные пункты правил, выполненный SQL, расход.

    Цифры — из синтетических данных стенда. Агент только читает: SQL идёт под
    пользователем ClickHouse с правом SELECT на четыре таблицы.
    """
    endpoint.check_limits(client_key(request))
    try:
        asked = await endpoint.service.ask(body.question)
    except LLMError as exc:
        log.warning("ask.llm_error", error=type(exc).__name__, retryable=exc.retryable)
        if exc.retryable:
            msg = "Модель сейчас недоступна, повторите позже"
            raise ModelUnavailableError(msg) from exc
        msg = "Модель вернула ошибку"
        raise ModelFailedError(msg) from exc
    return to_response(asked, endpoint.model)


def to_response(asked: Asked, model: str) -> AskResponse:
    answer = asked.answer
    return AskResponse(
        answer=answer.text,
        stop=answer.stop.value,
        kind=answer.route.kind.value if answer.route else None,
        sources=[AskSource(title=s.title, url=s.url, cited=s.cited) for s in sources(answer)],
        sql=[AskSql(query=s.query, ok=s.ok) for s in sql_runs(answer)],
        tool_calls=[
            AskToolCall(name=c.name, ok=not c.is_error, duration_ms=c.duration_ms)
            for c in answer.tool_calls
        ],
        steps=answer.steps,
        usage=AskUsage(
            model=model,
            input_tokens=answer.input_tokens,
            output_tokens=answer.output_tokens,
            cost_usd=cost_usd(model, answer.input_tokens, answer.output_tokens),
        ),
        cached=asked.cached,
    )
