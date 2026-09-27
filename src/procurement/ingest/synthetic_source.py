"""Синтетический источник: генерирует правдоподобную выдачу за заданный месяц.

Зачем он есть. Токен goszakup ещё не выдан, а бэкфилл за три года прогнать надо
по-настоящему: без этого нельзя ни измерить пропускную способность, ни
проверить, что повторный запуск не задваивает данные. Фикстур на это не хватит —
их пятнадцать записей.

Данные **синтетические**. Набор полей повторяет схему API (проверено
2026-09-27), значения выдуманы детерминированным генератором и не описывают ни
одну реальную организацию или закупку. БИН начинаются с девятки — такие в
действительности не выдаются.

Источник реализует тот же протокол, что и сетевой, и подставляется вместо него
без единой правки в конвейере. Когда появится токен, месяц будет отдавать
RestSource с фильтром по дате, а всё остальное останется как есть.
"""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, date, datetime, timedelta

from procurement.ingest.errors import SourceRequestError
from procurement.ingest.models import Page, RawRecord

CUSTOMERS = 6000
SUPPLIERS = 15000

LOT_NAMES = (
    "Бумага офисная А4",
    "Услуги по техническому обслуживанию",
    "Картридж лазерный",
    "Мебель офисная",
    "Программное обеспечение",
    "Услуги охраны",
    "Топливо дизельное",
)


def month_bounds(month: date) -> tuple[datetime, datetime]:
    start = datetime(month.year, month.month, 1, tzinfo=UTC)
    end = datetime(month.year + (month.month // 12), (month.month % 12) + 1, 1, tzinfo=UTC)
    return start, end


class SyntheticSource:
    """Отдаёт записи одного месяца страницами, как это делал бы настоящий API."""

    def __init__(
        self,
        month: date,
        *,
        records: int,
        page_size: int = 500,
        latency_s: float = 0.0,
        seed: int = 20260927,
    ) -> None:
        self._month = month.replace(day=1)
        self._records = records
        self._page_size = page_size
        self._latency_s = latency_s
        self._seed = seed
        self._start, self._end = month_bounds(self._month)
        self._span_seconds = int((self._end - self._start).total_seconds())
        # Сдвиг идентификаторов по месяцу: записи разных месяцев не должны
        # пересекаться по id, иначе подмена партиции затрёт чужие данные.
        self._id_base = (self._month.year * 12 + self._month.month) * 10_000_000

    @property
    def total(self) -> int:
        return self._records

    async def fetch_page(self, entity: str, cursor: str | None) -> Page:
        offset = 0 if cursor is None else int(cursor)
        if offset > self._records:
            msg = f"смещение {offset} за пределами месяца {self._month:%Y-%m}"
            raise SourceRequestError(msg)

        if self._latency_s:
            await asyncio.sleep(self._latency_s)

        take = min(self._page_size, self._records - offset)
        items = [self._record(entity, offset + i) for i in range(take)]

        next_offset = offset + take
        next_cursor = str(next_offset) if next_offset < self._records else None

        return Page(entity=entity, items=items, next_cursor=next_cursor, total=self._records)

    def _record(self, entity: str, index: int) -> RawRecord:
        # Свой генератор на запись: выдача не зависит от порядка обращений,
        # значит одна и та же страница всегда одинакова. Для проверки
        # идемпотентности это обязательное свойство.
        rng = random.Random((self._seed, self._id_base, index).__hash__())  # noqa: S311
        record_id = self._id_base + index
        created = self._start + timedelta(seconds=rng.randrange(self._span_seconds))
        updated = created + timedelta(days=rng.randrange(1, 120))
        customer = f"9{rng.randrange(CUSTOMERS):011d}"

        if entity == "lots":
            return {
                "id": record_id,
                "lot_number": f"{record_id}-ОИ{rng.randint(1, 9)}",
                "ref_lot_status_id": rng.choice([210, 220, 230, 240]),
                "customer_bin": customer,
                "trd_buy_number_anno": f"{rng.randrange(400000, 500000)}-1",
                "name_ru": rng.choice(LOT_NAMES),
                "count": rng.randint(1, 500),
                "amount": rng.randrange(10_000, 5_000_000, 500),
                "last_update_date": updated.strftime("%Y-%m-%d %H:%M:%S"),
            }

        total = rng.randrange(50_000, 20_000_000, 1000)
        return {
            "id": record_id,
            "contract_number": str(rng.randint(1, 900)),
            "trd_buy_number_anno": f"{rng.randrange(400000, 500000)}-1",
            "supplier_biin": f"95{rng.randrange(SUPPLIERS):010d}",
            "customer_bin": customer,
            "contract_sum": total,
            "contract_sum_wnds": int(total * 1.12),
            "fakt_sum": rng.randrange(0, total + 1, 1000),
            "ref_contract_status_id": rng.choice([200, 210, 220, 230]),
            "crdate": created.strftime("%Y-%m-%d %H:%M:%S"),
            "sign_date": (created + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "last_update_date": updated.strftime("%Y-%m-%d %H:%M:%S"),
        }

    async def aclose(self) -> None:
        return
