"""Свод по срезам py-spy: кто сколько занял процессорного времени.

py-spy с ``--format raw`` пишет «схлопнутые стеки» (folded stacks): одна
строка — один стек вызовов, кадры через точку с запятой, в конце число
попаданий. Формат придуман для рисования пламенных графиков, но читать его
скриптом удобнее, чем разглядывать SVG: можно посчитать доли по областям.

Считается две вещи:

  по областям — сколько срезов содержат нужный кадр где-либо в стеке. Так
                видно суммарную цену куска кода вместе со всем, что он зовёт;

  по листьям  — сколько срезов пришлось ровно на этот кадр как на последний.
                Так видно, где процессор стоял на самом деле.

Отдельно отбрасываются срезы подготовки стенда: profile_loader сериализует
страницы в JSON до замера, эта работа попадает в профиль и к загрузчику
отношения не имеет.

Запуск:
    .venv/Scripts/python scripts/fold_summary.py docs/bench/loader_folded_after.txt
"""

from __future__ import annotations

import collections
import sys
from pathlib import Path

# Кадр, по которому узнаётся подготовка стенда.
PREP_FRAME = "__init__ (profile_loader.py"

AREAS = (
    ("разбор дат (as_datetime)", "as_datetime"),
    ("сборка строк для ClickHouse (to_rows)", "to_rows"),
    ("драйвер ClickHouse: сеть и сериализация", "clickhouse_connect"),
    ("разбор ответа (orjson)", "orjson"),
    ("модели pydantic (Page/Batch)", "pydantic"),
    ("служебный код asyncio", "asyncio\\"),
)

TOP_LEAVES = 20


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2

    path = Path(sys.argv[1])
    total = 0
    prep = 0
    leaves: collections.Counter[str] = collections.Counter()
    areas: collections.Counter[str] = collections.Counter()

    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        stack, _, count = line.rpartition(" ")
        samples = int(count)
        # Первые кадры — заголовки процессов, они не код.
        frames = [f for f in stack.split(";") if not f.startswith("process ")]
        if not frames:
            continue

        joined = ";".join(frames)
        if PREP_FRAME in joined:
            prep += samples
            continue

        total += samples
        leaves[frames[-1]] += samples
        for label, needle in AREAS:
            if needle in joined:
                areas[label] += samples

    if total == 0:
        print("в файле нет срезов конвейера")
        return 1

    grand = total + prep
    print(f"срезов всего: {grand}")
    print(f"  подготовка стенда: {prep} ({prep * 100 / grand:.1f}%)")
    print(f"  прогон конвейера : {total} ({total * 100 / grand:.1f}%)")

    print("\n=== доли внутри прогона, по областям ===")
    for label, samples in areas.most_common():
        print(f"{samples * 100 / total:6.1f}%  {samples:5d}  {label}")

    print(f"\n=== топ-{TOP_LEAVES} листьев ===")
    for frame, samples in leaves.most_common(TOP_LEAVES):
        print(f"{samples * 100 / total:6.1f}%  {samples:5d}  {frame}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
