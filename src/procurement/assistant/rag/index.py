"""Запись кусков в ``doc_chunk``: пересборка источника целиком, идемпотентно.

Пересборка источника = привести таблицу к новому списку кусков:
- кусок с тем же ключом и тем же хэшем не трогаем — эмбеддинг не пересчитываем
  (это и делает еженедельную пересборку дешёвой: правила меняются по пунктам);
- изменённый или новый — считаем эмбеддинг и пишем (INSERT … ON CONFLICT);
- кусок, которого в новой редакции нет, удаляем.
Всё в одной транзакции: поиск никогда не видит наполовину обновлённый источник.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from procurement.assistant.rag.chunks import DocChunk
from procurement.assistant.rag.embeddings import Embedder
from procurement.logging import get_logger
from procurement.storage.postgres.models import EMBEDDING_DIM

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class IndexResult:
    source: str
    written: int
    unchanged: int
    deleted: int


def to_pgvector(vector: Sequence[float]) -> str:
    """Вектор в текстовый вид pgvector '[0.1,0.2,…]' — для CAST(:v AS vector).

    Текст, а не бинарный кодек драйвера: один и тот же запрос работает через
    SQLAlchemy без регистрации типа на каждом соединении.
    """
    return "[" + ",".join(f"{v:.7g}" for v in vector) + "]"


_UPSERT = text(
    """
    INSERT INTO doc_chunk
        (source, anchor, url, title, text, content_hash, embedding, embedding_model,
         source_version)
    VALUES
        (:source, :anchor, :url, :title, :text, :content_hash, CAST(:embedding AS vector),
         :embedding_model, :source_version)
    ON CONFLICT (source, anchor) DO UPDATE SET
        url = EXCLUDED.url,
        title = EXCLUDED.title,
        text = EXCLUDED.text,
        content_hash = EXCLUDED.content_hash,
        embedding = EXCLUDED.embedding,
        embedding_model = EXCLUDED.embedding_model,
        source_version = EXCLUDED.source_version,
        updated_at = now()
    """
)


async def replace_source(
    sessions: async_sessionmaker[AsyncSession],
    embedder: Embedder,
    *,
    source: str,
    version: str,
    chunks: Sequence[DocChunk],
) -> IndexResult:
    if embedder.dim != EMBEDDING_DIM:
        # Колонка vector(768) не примет другой размер, но ошибка базы на
        # середине пересборки хуже, чем внятный отказ до начала.
        msg = f"модель {embedder.model_name} даёт вектор {embedder.dim}, колонка — {EMBEDDING_DIM}"
        raise ValueError(msg)
    if any(c.source != source for c in chunks):
        msg = f"в списке есть куски не из источника {source}"
        raise ValueError(msg)
    anchors = [c.anchor for c in chunks]
    if len(set(anchors)) != len(anchors):
        msg = f"{source}: якоря кусков повторяются"
        raise ValueError(msg)

    async with sessions() as session, session.begin():
        rows = await session.execute(
            text("SELECT anchor, content_hash FROM doc_chunk WHERE source = :source"),
            {"source": source},
        )
        # Без Result.tuples(): в SQLAlchemy 2.1 он устарел (строка и так
        # распаковывается как кортеж) — поймано в CI, локально стояла 2.0.
        existing: dict[str, str] = {row.anchor: row.content_hash for row in rows}

        changed = [
            c for c in chunks if existing.get(c.anchor) != c.content_hash(embedder.model_name)
        ]
        vectors = await embedder.embed_documents([c.embed_text for c in changed])
        if changed:
            await session.execute(
                _UPSERT,
                [
                    {
                        "source": c.source,
                        "anchor": c.anchor,
                        "url": c.url,
                        "title": c.title,
                        "text": c.text,
                        "content_hash": c.content_hash(embedder.model_name),
                        "embedding": to_pgvector(v),
                        "embedding_model": embedder.model_name,
                        "source_version": version,
                    }
                    for c, v in zip(changed, vectors, strict=True)
                ],
            )

        stale = sorted(set(existing) - set(anchors))
        if stale:
            await session.execute(
                text("DELETE FROM doc_chunk WHERE source = :source AND anchor = ANY(:anchors)"),
                {"source": source, "anchors": stale},
            )

    result = IndexResult(
        source=source,
        written=len(changed),
        unchanged=len(chunks) - len(changed),
        deleted=len(stale),
    )
    log.info("rag.index", **result.__dict__)
    return result
