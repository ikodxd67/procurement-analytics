"""Снимок кусков правил для evals: фиксированная редакция в репозитории.

Зачем снимок, а не свежий текст с adilet на каждый прогон:
- эталоны размечены по конкретной редакции — новая редакция может сдвинуть
  пункты, и метрика упадёт не из-за поиска, а из-за документа;
- CI не должен зависеть от того, отвечает ли чужой сайт.

Текст правил — официальный документ, по статье 8 Закона РК «Об авторском праве
и смежных правах» он не объект авторского права; держать его в репозитории можно.
Обновление: ``python -m procurement.assistant.evals.cli snapshot`` — после этого
надо перепроверить якоря эталонов (тест это делает).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from procurement.assistant.rag.adilet import AdiletDocument, chunk_rules
from procurement.assistant.rag.chunks import DocChunk

SNAPSHOT_DIR = Path(__file__).resolve().parents[4] / "evals" / "data"


def snapshot_path(ngr: str) -> Path:
    return SNAPSHOT_DIR / f"adilet_{ngr}.json"


def write_snapshot(document: AdiletDocument, path: Path | None = None) -> Path:
    target = path or snapshot_path(document.ngr)
    chunks = chunk_rules(document)
    payload: dict[str, Any] = {
        "ngr": document.ngr,
        "title": document.title,
        "version": document.version,
        "chunks": [
            {"anchor": c.anchor, "url": c.url, "title": c.title, "text": c.text} for c in chunks
        ],
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    # Один кусок — одна строка: диф снимка после новой редакции читается по пунктам.
    lines = json.dumps({k: v for k, v in payload.items() if k != "chunks"}, ensure_ascii=False)
    body = ",\n".join(json.dumps(c, ensure_ascii=False) for c in payload["chunks"])
    target.write_text(lines[:-1] + ', "chunks": [\n' + body + "\n]}\n", encoding="utf-8")
    return target


def read_snapshot(ngr: str, path: Path | None = None) -> tuple[str, list[DocChunk]]:
    """(версия редакции, куски) из снимка."""
    data = json.loads((path or snapshot_path(ngr)).read_text(encoding="utf-8"))
    source = f"adilet:{data['ngr']}"
    chunks = [
        DocChunk(source=source, anchor=c["anchor"], url=c["url"], title=c["title"], text=c["text"])
        for c in data["chunks"]
    ]
    return str(data["version"]), chunks
