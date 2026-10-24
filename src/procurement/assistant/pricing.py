"""Цена вопроса в долларах — для отчёта evals и для ответа ручки /ask."""

from __future__ import annotations

# Цена за 1 млн токенов, $ (вход, выход): таблица моделей документации Claude
# API, кэш от 2026-10-06. У Haiku 5.5 — для промптов до 100 тыс. токенов; наши
# короче. Неизвестная модель — цена не считается, а не выдумывается.
PRICES_PER_MTOK: dict[str, tuple[float, float]] = {
    "claude-haiku-5-5": (0.10, 0.50),
    "claude-sonnet-5-5": (2.00, 10.00),
    "claude-opus-5-5": (4.00, 20.00),
}


def cost_usd(model: str, input_tokens: int, output_tokens: int) -> float | None:
    price = PRICES_PER_MTOK.get(model)
    if price is None:
        return None
    return (input_tokens * price[0] + output_tokens * price[1]) / 1_000_000
