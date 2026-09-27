"""Ошибки API и единый формат ответа при сбое.

Зачем свой формат. FastAPI по умолчанию отдаёт `{"detail": ...}`, причём в
разных случаях detail — то строка, то список объектов от валидатора. Клиенту,
который хочет отличить «сам виноват» от «сервис лежит», приходится гадать.
Единый конверт с кодом решает это: код машиночитаем, сообщение для человека.

Коды нарочно не совпадают с HTTP-статусами. Статус говорит категорию, код —
конкретную причину внутри неё.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from procurement.logging import get_logger

log = get_logger(__name__)


class ErrorBody(BaseModel):
    code: str = Field(description="Машиночитаемый код причины")
    message: str = Field(description="Объяснение для человека")
    details: list[dict[str, Any]] | None = Field(
        default=None, description="Подробности, если причина составная"
    )


class ErrorResponse(BaseModel):
    error: ErrorBody


class ApiError(Exception):
    """Базовая ошибка, которую обработчик превращает в ответ."""

    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    code: str = "internal_error"

    def __init__(self, message: str, *, details: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class NotFoundError(ApiError):
    status_code = status.HTTP_404_NOT_FOUND
    code = "not_found"


class BadRequestError(ApiError):
    status_code = status.HTTP_400_BAD_REQUEST
    code = "bad_request"


class StorageUnavailableError(ApiError):
    """Хранилище не ответило.

    Отдельный класс и статус 503, а не 500: 500 означает «мы сломались», а 503
    — «попробуйте позже». Для клиента с повторами разница принципиальна.
    """

    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    code = "storage_unavailable"


def _body(code: str, message: str, details: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return ErrorResponse(error=ErrorBody(code=code, message=message, details=details)).model_dump(
        exclude_none=True
    )


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle_api_error(_: Request, error: ApiError) -> JSONResponse:
        if error.status_code >= status.HTTP_500_INTERNAL_SERVER_ERROR:
            log.error("api.error", code=error.code, message=error.message)
        return JSONResponse(
            status_code=error.status_code,
            content=_body(error.code, error.message, error.details),
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation(_: Request, error: RequestValidationError) -> JSONResponse:
        # Приводим вывод валидатора к тому же конверту. Поле input убираем:
        # в него попадает то, что прислал клиент, и в логах это лишнее.
        details = [
            {
                "where": ".".join(str(part) for part in item.get("loc", ())),
                "problem": item.get("msg", ""),
            }
            for item in error.errors()
        ]
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=_body("validation_failed", "Параметры запроса не прошли проверку", details),
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http(_: Request, error: StarletteHTTPException) -> JSONResponse:
        codes = {404: "not_found", 405: "method_not_allowed"}
        return JSONResponse(
            status_code=error.status_code,
            content=_body(codes.get(error.status_code, "http_error"), str(error.detail)),
        )

    @app.exception_handler(Exception)
    async def handle_unexpected(_: Request, error: Exception) -> JSONResponse:
        # Наружу текст исключения не отдаём: в нём бывают куски запросов и
        # адреса внутренних сервисов. В лог — полностью, клиенту — код.
        log.exception("api.unhandled", error=type(error).__name__)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=_body("internal_error", "Внутренняя ошибка сервиса"),
        )
