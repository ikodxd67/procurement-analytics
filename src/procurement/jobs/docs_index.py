"""Пересборка индекса документов ассистента: правила с adilet и описание данных.

Логика здесь, а не в DAG, — как у остальных заданий: её запускают и Airflow,
и командная строка (``python -m procurement.jobs.cli docs-index``).
"""

from __future__ import annotations

import hashlib

import httpx

from procurement.assistant.rag.adilet import chunk_rules, fetch_document
from procurement.assistant.rag.dictionary import SOURCE, chunk_dictionary, load_text
from procurement.assistant.rag.embeddings import Embedder, build_embedder
from procurement.assistant.rag.index import IndexResult, replace_source
from procurement.jobs.context import JobContext
from procurement.storage.postgres.models import EMBEDDING_DIM


async def rebuild_docs_index(
    context: JobContext, *, embedder: Embedder | None = None
) -> list[IndexResult]:
    settings = context.settings
    rag = settings.assistant.rag
    resolved = embedder or build_embedder(rag, dim=EMBEDDING_DIM)

    results: list[IndexResult] = []
    timeout = httpx.Timeout(
        settings.http.request_timeout_s, connect=settings.http.connect_timeout_s
    )
    headers = {"User-Agent": settings.http.user_agent}
    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        for ngr in rag.adilet_documents:
            document = await fetch_document(client, rag.adilet_api_url, ngr)
            results.append(
                await replace_source(
                    context.sessions,
                    resolved,
                    source=f"adilet:{ngr}",
                    version=document.version,
                    chunks=chunk_rules(document),
                )
            )

    text = load_text()
    results.append(
        await replace_source(
            context.sessions,
            resolved,
            source=SOURCE,
            # Своей даты редакции у описания нет — версией служит хэш текста.
            version=hashlib.sha256(text.encode("utf-8")).hexdigest()[:16],
            chunks=chunk_dictionary(text),
        )
    )
    return results
