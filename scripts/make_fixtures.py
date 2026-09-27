"""Генератор фикстур ответов API.

Данные синтетические. Форма ответа повторяет документированную для REST v3
goszakup.gov.kz (ключи total, limit, next_page, items и набор полей записи),
значения выдуманы и никого не описывают.

Генератор детерминированный: один и тот же запуск даёт один и тот же набор,
поэтому фикстуры можно держать в репозитории и пересоздавать при изменении
формы ответа.

Запуск:
    .venv/Scripts/python scripts/make_fixtures.py
"""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "goszakup"

SEED = 20260927
PAGES = 3
PER_PAGE = 5

# Синтетические БИН: двенадцать цифр, начинаются с 9, каких в реальном
# реестре не выдают. Так исключается совпадение с настоящей организацией.
CUSTOMER_BINS = ["900140000101", "900240000202", "900340000303"]
SUPPLIER_BINS = ["950140000111", "950240000222", "950340000333", "950440000444"]

LOT_NAMES = [
    "Бумага офисная А4",
    "Услуги по техническому обслуживанию",
    "Картридж лазерный",
    "Мебель офисная",
    "Программное обеспечение",
]


def contract_record(rng: random.Random, record_id: int) -> dict[str, Any]:
    return {
        "id": record_id,
        "contract_number": f"{rng.randint(1, 400)}",
        "trd_buy_number_anno": f"{rng.randint(400000, 499999)}-1",
        "supplier_biin": rng.choice(SUPPLIER_BINS),
        "customer_bin": rng.choice(CUSTOMER_BINS),
        "contract_sum": rng.randrange(50_000, 20_000_000, 1000),
        "contract_sum_wnds": rng.randrange(50_000, 22_000_000, 1000),
        "ref_contract_status_id": rng.choice([200, 210, 220, 230]),
        "fin_year": rng.choice([2024, 2025, 2026]),
        "crdate": f"2026-0{rng.randint(1, 9)}-1{rng.randint(0, 9)} 10:00:00",
        "last_update_date": f"2026-0{rng.randint(1, 9)}-2{rng.randint(0, 8)} 12:00:00",
    }


def lot_record(rng: random.Random, record_id: int) -> dict[str, Any]:
    amount = rng.randrange(10_000, 5_000_000, 500)
    return {
        "id": record_id,
        "lot_number": f"{rng.randint(4000000, 4999999)}-ОИ{rng.randint(1, 9)}",
        "ref_lot_status_id": rng.choice([210, 220, 230, 240]),
        "customer_bin": rng.choice(CUSTOMER_BINS),
        "trd_buy_number_anno": f"{rng.randint(400000, 499999)}-1",
        "name_ru": rng.choice(LOT_NAMES),
        "count": rng.randint(1, 500),
        "amount": amount,
        "last_update_date": f"2026-0{rng.randint(1, 9)}-2{rng.randint(0, 8)} 12:00:00",
    }


def build_entity(entity: str, path: str, first_id: int) -> None:
    rng = random.Random(SEED)  # noqa: S311  # генерация фикстур, не криптография
    folder = ROOT / entity
    folder.mkdir(parents=True, exist_ok=True)

    for old in folder.glob("page_*.json"):
        old.unlink()

    make = contract_record if entity == "contracts" else lot_record
    total = PAGES * PER_PAGE
    record_id = first_id

    for page_no in range(1, PAGES + 1):
        items = []
        for _ in range(PER_PAGE):
            items.append(make(rng, record_id))
            record_id += 1

        last_id = items[-1]["id"]
        is_last_page = page_no == PAGES

        payload: dict[str, Any] = {
            "total": total,
            "limit": PER_PAGE,
            # На последней странице источник ссылку не отдаёт — именно
            # отсутствие next_page, а не пустой список items, означает конец.
            "next_page": None if is_last_page else f"{path}?page=next&search_after={last_id}",
            "items": items,
        }

        target = folder / f"page_{page_no:03d}.json"
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"  {target.relative_to(ROOT.parent.parent.parent)}  ({len(items)} записей)")


def main() -> None:
    print("Генерация фикстур (данные синтетические):")
    build_entity("contracts", "/contract", first_id=4_998_000)
    build_entity("lots", "/lots", first_id=836_800)

    readme = ROOT / "README.md"
    readme.write_text(
        "# Фикстуры ответов goszakup\n\n"
        "**Данные синтетические.** Форма ответа повторяет документированную для\n"
        "REST v3 (`total`, `limit`, `next_page`, `items`), значения выдуманы\n"
        "детерминированным генератором и не описывают ни одну реальную\n"
        "организацию, закупку или договор. БИН начинаются с 9, такие в\n"
        "действительности не выдаются.\n\n"
        "Пересоздать: `.venv/Scripts/python scripts/make_fixtures.py`\n\n"
        "Цепочка страниц строится через `next_page`: последняя страница\n"
        "отдаёт `null`, и именно это означает конец выдачи.\n",
        encoding="utf-8",
    )
    print(f"  {readme.name}")


if __name__ == "__main__":
    main()
