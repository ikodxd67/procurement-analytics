"""Стенд для профилирования загрузчика.

Зачем отдельный стенд, а не бэкфилл из командной строки. Бэкфилл тянет на себе
временную таблицу, проверки качества и подмену партиции — всё это полезно в
бою и мешает мерить. Здесь остаётся только конвейер: источник, очередь,
обработчик.

Два источника, и разница между ними принципиальна:

  synthetic — тот же генератор, что в бэкфилле. Он **придумывает** записи:
              random, strftime, форматирование строк. Настоящий REST-источник
              ничего такого не делает, поэтому профиль под этим источником
              показывает работу стенда, а не работу загрузчика.

  replay    — страницы сериализованы в JSON один раз, до замера, и при каждом
              обращении разбираются orjson-ом. Это то, чем занят настоящий
              источник за вычетом сети, и профилировать надо именно его.

Два приёмника:

  clickhouse — настоящая вставка во временную таблицу;
  null       — записи выбрасываются. Разница между приёмниками показывает,
               сколько стоит хранилище, а сколько — сам конвейер.

Запуск:
    .venv/Scripts/python scripts/profile_loader.py --records 50000
    .venv/Scripts/python scripts/profile_loader.py --sink null --debug
    .venv/Scripts/py-spy record -o docs/bench/loader.svg -- \
        .venv/Scripts/python scripts/profile_loader.py --records 50000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from procurement.config import get_settings
from procurement.ingest.limiter import AdaptiveLimiter
from procurement.ingest.models import Batch, Page
from procurement.ingest.pipeline import Pipeline
from procurement.ingest.retry import RetryPolicy
from procurement.ingest.source import page_from_payload
from procurement.ingest.state import InMemoryStateStore
from procurement.ingest.synthetic_source import SyntheticSource
from procurement.storage.clickhouse.client import build_client
from procurement.storage.clickhouse.ddl import apply_schema
from procurement.storage.clickhouse.schema import spec_for
from procurement.storage.clickhouse.writer import PartitionSwapLoader


class ReplaySource:
    """Отдаёт заранее сериализованные страницы, разбирая их на каждом обращении.

    Сериализация происходит в __init__, то есть до замера. В замер попадает
    ровно то, что делает боевой источник после получения ответа: orjson.loads
    и сборка Page.
    """

    def __init__(self, entity: str, month: date, records: int, page_size: int) -> None:
        generator = SyntheticSource(month, records=records, page_size=page_size)
        self._page_size = page_size
        self._pages: list[bytes] = []

        for offset in range(0, records, page_size):
            take = min(page_size, records - offset)
            items = [generator._record(entity, offset + i) for i in range(take)]
            next_offset = offset + take
            payload = {
                "total": records,
                "limit": page_size,
                "items": items,
                "next_page": (
                    None if next_offset >= records else f"/x?page=next&search_after={next_offset}"
                ),
            }
            self._pages.append(json.dumps(payload, ensure_ascii=False).encode("utf-8"))

        self.payload_bytes = sum(len(p) for p in self._pages)

    async def fetch_page(self, entity: str, cursor: str | None) -> Page:
        index = 0 if cursor is None else int(cursor) // self._page_size
        if index >= len(self._pages):
            index = len(self._pages) - 1
        return page_from_payload(entity, orjson.loads(self._pages[index]))

    async def aclose(self) -> None:
        return None


async def null_handler(batch: Batch) -> None:
    return None


async def run(args: argparse.Namespace) -> None:
    settings = get_settings()
    entity = args.entity
    month = date(2024, 6, 1)

    if args.source == "replay":
        built = time.perf_counter()
        source = ReplaySource(entity, month, args.records, args.page_size)
        print(
            f"подготовка: {args.records} записей, {source.payload_bytes / 1024 / 1024:.1f} МиБ "
            f"JSON за {time.perf_counter() - built:.1f} с"
        )
    else:
        source = SyntheticSource(month, records=args.records, page_size=args.page_size)  # type: ignore[assignment]

    client = None
    loader = None
    if args.sink == "clickhouse":
        client = await build_client(settings.clickhouse)
        await apply_schema(client)
        loader = PartitionSwapLoader(client, spec_for(entity), suffix="profile")
        await loader.prepare()
        handler = loader.writer()
    else:
        handler = null_handler  # type: ignore[assignment]

    limiter = AdaptiveLimiter(
        start=settings.http.concurrency_start,
        minimum=settings.http.concurrency_min,
        maximum=settings.http.concurrency_max,
    )
    pipeline = Pipeline(
        source=source,
        state=InMemoryStateStore(),
        limiter=limiter,
        handler=handler,
        retry_policy=RetryPolicy(max_attempts=3, base_delay_s=0.1, max_delay_s=1.0),
        queue_maxsize=args.queue,
        batch_size=args.batch_size,
        workers=args.workers,
    )

    started = time.perf_counter()
    async with limiter:
        outcome = await pipeline.run(entity, from_scratch=True)
    elapsed = time.perf_counter() - started

    if loader is not None:
        await loader.cleanup()
    if client is not None:
        await client.close()

    print(
        f"источник={args.source} приёмник={args.sink} батч={args.batch_size} "
        f"воркеров={args.workers} очередь={args.queue}"
    )
    print(
        f"  {outcome.records} записей, {outcome.pages} страниц, "
        f"{elapsed:.2f} с, {outcome.records / elapsed:.0f} строк/с"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entity", default="contracts", choices=("contracts", "lots"))
    parser.add_argument("--source", default="replay", choices=("replay", "synthetic"))
    parser.add_argument("--sink", default="clickhouse", choices=("clickhouse", "null"))
    parser.add_argument("--records", type=int, default=50_000)
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--queue", type=int, default=32)
    parser.add_argument(
        "--debug",
        action="store_true",
        help="режим отладки asyncio: ругается на обработчики, занявшие цикл дольше порога",
    )
    parser.add_argument("--slow-callback-s", type=float, default=0.1)
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

        async def wrapped() -> None:
            asyncio.get_running_loop().slow_callback_duration = args.slow_callback_s
            await run(args)

        asyncio.run(wrapped(), debug=True)
    else:
        asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    sys.exit(main())
