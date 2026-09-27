"""Замер задержек ручек API на боевом объёме данных.

Запросы идут в приложение напрямую через ASGI, без сетевого слоя. Так меряется
то, что нас интересует — работа с хранилищем и сборка ответа, — а не скорость
петли loopback.

Первый прогон каждой ручки отбрасывается: он прогревает кэши ClickHouse.

Запуск (нужны поднятые контейнеры и загруженные данные):
    .venv/Scripts/python scripts/bench_api.py
"""

from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass
from typing import Any

import httpx

from procurement.api.app import build_app

REPEATS = 7
PERIOD = {"date_from": "2023-01-01", "date_to": "2025-12-31"}


@dataclass(frozen=True, slots=True)
class Call:
    title: str
    path: str
    params: dict[str, Any]


CALLS = (
    Call("Проба готовности", "/health/ready", {}),
    Call(
        "Поставщики заказчика",
        "/v1/customers/900000000072/suppliers",
        {**PERIOD, "limit": 20},
    ),
    Call(
        "Заказчики поставщика",
        "/v1/suppliers/950000000001/customers",
        {**PERIOD, "limit": 20},
    ),
    Call("Свёртка по разделам", "/v1/categories", {**PERIOD, "level": 0, "limit": 20}),
    Call("Свёртка по группам", "/v1/categories", {**PERIOD, "level": 1, "limit": 20}),
    Call("Помесячная динамика", "/v1/contracts/monthly", dict(PERIOD)),
    Call(
        "Помесячная по одному заказчику",
        "/v1/contracts/monthly",
        {**PERIOD, "customer_bin": "900000000072"},
    ),
    Call(
        "Отклонения цен",
        "/v1/lots/price-outliers",
        {**PERIOD, "limit": 20, "min_lots": 30},
    ),
    Call(
        "Концентрация закупок",
        "/v1/customers/concentration",
        {**PERIOD, "limit": 20, "min_suppliers": 5},
    ),
    Call(
        "Активность поставщика",
        "/v1/suppliers/950000000001/activity",
        {**PERIOD, "limit": 20},
    ),
)


@dataclass(frozen=True, slots=True)
class Result:
    title: str
    median_ms: float
    p95_ms: float
    status: int
    items: int


async def measure(client: httpx.AsyncClient, call: Call) -> Result:
    durations: list[float] = []
    payload: Any = None
    status = 0

    for _ in range(REPEATS + 1):
        started = time.perf_counter()
        response = await client.get(call.path, params=call.params)
        durations.append((time.perf_counter() - started) * 1000)
        status = response.status_code
        payload = response.json()

    warm = sorted(durations[1:])
    items = 0
    if isinstance(payload, dict) and "items" in payload:
        items = len(payload["items"])
    elif isinstance(payload, list):
        items = len(payload)

    return Result(
        title=call.title,
        median_ms=statistics.median(warm),
        p95_ms=warm[int(len(warm) * 0.95) - 1],
        status=status,
        items=items,
    )


async def main() -> None:
    app = build_app()
    transport = httpx.ASGITransport(app=app)

    results: list[Result] = []
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://bench", timeout=120) as client,
    ):
        for call in CALLS:
            result = await measure(client, call)
            results.append(result)
            print(
                f"  {result.title:<32} {result.median_ms:8.1f} мс  "
                f"(p95 {result.p95_ms:7.1f})  статус {result.status}, записей {result.items}"
            )

    print("\n\n| Ручка | Медиана | p95 | Записей |")
    print("|---|---|---|---|")
    for result in results:
        print(
            f"| {result.title} | {result.median_ms:.0f} мс | "
            f"{result.p95_ms:.0f} мс | {result.items} |"
        )


if __name__ == "__main__":
    asyncio.run(main())
