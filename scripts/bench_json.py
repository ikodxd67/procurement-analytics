"""Замер разбора JSON: orjson против json из стандартной библиотеки.

Зачем. Загрузчик разбирает ответ источника на каждой странице, и это
единственная заметная вычислительная работа в его горячем пути. Библиотека
выбрана в самом начале проекта, но выбор без цифр — это не выбор, а привычка.

Что меряется:
  loads  — разбор байтов ответа в структуры Python;
  dumps  — обратная сборка (нужна фикстурам и выгрузке в jsonl).

Отдельная строка на .text: httpx отдаёт байты в .content и декодированную
строку в .text. json умеет обе, orjson — обе тоже, но декодирование в str
стоит отдельного прохода по буферу, и это видно в цифрах.

Данные берутся из того же генератора, что и весь синтетический поток, так что
форма записей совпадает с боевой. Договоры — чистый ASCII, лоты содержат
кириллицу в названии: для json это принципиальная разница из-за ensure_ascii.

Запуск:
    .venv/Scripts/python scripts/bench_json.py
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from procurement.ingest.synthetic_source import SyntheticSource

RUNS = 7
MIN_SECONDS = 0.5


def build_page(entity: str, size: int) -> dict[str, Any]:
    source = SyntheticSource(month=date(2024, 6, 1), records=size, page_size=size)
    items = [source._record(entity, i) for i in range(size)]
    return {
        "total": size,
        "limit": size,
        "next_page": f"/{entity}?page=next&search_after={items[-1]['id']}",
        "items": items,
    }


def measure(fn: Callable[[], Any]) -> tuple[float, int]:
    """Медиана из RUNS прогонов. Внутри прогона — столько повторов, чтобы
    набрать MIN_SECONDS: одиночный вызов на миллисекундах меряется шумом.
    """
    repeats = 1
    while True:
        start = time.perf_counter()
        for _ in range(repeats):
            fn()
        if time.perf_counter() - start >= MIN_SECONDS:
            break
        repeats *= 4

    samples = []
    for _ in range(RUNS):
        start = time.perf_counter()
        for _ in range(repeats):
            fn()
        samples.append((time.perf_counter() - start) / repeats)
    return statistics.median(samples), repeats


def report(label: str, seconds: float, payload_bytes: int, records: int) -> None:
    per_record_us = seconds / records * 1e6
    mb_s = payload_bytes / seconds / 1024 / 1024
    print(f"{label:<44} {seconds * 1000:8.3f} мс {mb_s:8.1f} МБ/с {per_record_us:7.2f} мкс/зап")


def main() -> None:
    print(f"python {sys.version.split()[0]}, orjson {orjson.__version__}")
    print(f"медиана из {RUNS} прогонов\n")

    for entity in ("contracts", "lots"):
        for size in (50, 500):
            page = build_page(entity, size)
            raw_bytes = json.dumps(page, ensure_ascii=False).encode("utf-8")
            raw_text = raw_bytes.decode("utf-8")
            kb = len(raw_bytes) / 1024

            print(f"=== {entity}, страница {size} записей, {kb:.1f} КиБ ===")

            t, _ = measure(lambda v=(raw_bytes): json.loads(v))
            report("json.loads(bytes)", t, len(raw_bytes), size)
            t, _ = measure(lambda v=(raw_text): json.loads(v))
            report("json.loads(str)", t, len(raw_bytes), size)
            t, _ = measure(lambda v=(raw_bytes): orjson.loads(v))
            report("orjson.loads(bytes)", t, len(raw_bytes), size)
            t, _ = measure(lambda v=(raw_text): orjson.loads(v))
            report("orjson.loads(str)", t, len(raw_bytes), size)

            t, _ = measure(lambda v=(page,): json.dumps(v[0], ensure_ascii=False).encode("utf-8"))
            report("json.dumps + encode (ensure_ascii=False)", t, len(raw_bytes), size)
            t, _ = measure(lambda v=(page): json.dumps(v).encode("utf-8"))
            report("json.dumps + encode (ensure_ascii=True)", t, len(raw_bytes), size)
            t, _ = measure(lambda v=(page): orjson.dumps(v))
            report("orjson.dumps", t, len(raw_bytes), size)
            print()


if __name__ == "__main__":
    main()
