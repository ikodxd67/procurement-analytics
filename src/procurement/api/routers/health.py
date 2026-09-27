"""Пробы состояния сервиса.

Две отдельные ручки, а не одна, и это не формальность. Kubernetes использует
их по-разному:

* liveness отвечает на вопрос «процесс жив или его надо перезапустить».
  Зависимости он не проверяет намеренно: если лежит ClickHouse, перезапуск
  нашего процесса ничего не исправит, а только добавит суеты.
* readiness отвечает «можно ли слать сюда запросы прямо сейчас». Вот здесь
  зависимости проверять надо: без ClickHouse отвечать по существу нечем, и
  трафик лучше увести на другой экземпляр.

Перепутать их — классическая ошибка: liveness с проверкой базы приводит к
тому, что при недоступной базе Kubernetes начинает перезапускать все здоровые
экземпляры приложения по кругу.
"""

from __future__ import annotations

from fastapi import APIRouter, Response, status
from sqlalchemy import text

from procurement.api.deps import MartsDep, SessionsDep
from procurement.api.schemas import HealthStatus
from procurement.logging import get_logger

log = get_logger(__name__)

router = APIRouter(prefix="/health", tags=["состояние"])


@router.get("/live", summary="Процесс жив")
async def live() -> HealthStatus:
    return HealthStatus(status="ok")


@router.get("/ready", summary="Готов принимать запросы")
async def ready(marts: MartsDep, sessions: SessionsDep, response: Response) -> HealthStatus:
    checks: dict[str, str] = {}

    try:
        await marts.ping()
        checks["clickhouse"] = "ok"
    except Exception as error:
        checks["clickhouse"] = type(error).__name__

    try:
        async with sessions() as session:
            await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as error:
        checks["postgres"] = type(error).__name__

    healthy = all(value == "ok" for value in checks.values())
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        log.warning("health.not_ready", checks=checks)

    return HealthStatus(status="ok" if healthy else "degraded", checks=checks)
