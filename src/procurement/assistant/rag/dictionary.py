"""Описание данных проекта как источник для поиска: раздел «##» — один кусок."""

from __future__ import annotations

import re
from importlib.resources import files

from procurement.assistant.rag.chunks import DocChunk, split_long

SOURCE = "project:data_dictionary"
# Файл лежит в пакете, а не в docs/: так он есть и в контейнере Airflow, куда
# ставится пакет, а не весь репозиторий.
FILE = "data_dictionary.md"
URL = "src/procurement/assistant/rag/data_dictionary.md"


def load_text() -> str:
    return files("procurement.assistant.rag").joinpath(FILE).read_text(encoding="utf-8")


def _slug(heading: str) -> str:
    return re.sub(r"[^\w]+", "-", heading.lower()).strip("-")[:100]


def chunk_dictionary(text: str) -> list[DocChunk]:
    chunks: list[DocChunk] = []
    for section in re.split(r"^## ", text, flags=re.M)[1:]:
        heading, _, body = section.partition("\n")
        body = " ".join(body.split())  # абзацы markdown — одна строка
        if not body:
            continue
        slug = _slug(heading)
        parts = split_long(body)
        for index, part in enumerate(parts):
            chunks.append(
                DocChunk(
                    source=SOURCE,
                    anchor=slug if index == 0 else f"{slug}-{index + 1}",
                    url=f"{URL}#{slug}",
                    title=f"Описание данных › {heading.strip()}",
                    text=part,
                )
            )
    return chunks
