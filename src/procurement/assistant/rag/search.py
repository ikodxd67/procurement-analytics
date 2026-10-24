"""Гибридный поиск по ``doc_chunk``: по словам и по смыслу, слияние рангов (RRF).

Почему два поиска, а не один:
- **по словам** (полнотекстовый поиск PostgreSQL, конфигурация russian) точно
  находит номера, термины и редкие слова: «демпинговая цена», «пункт 588»;
- **по смыслу** (векторы, pgvector) находит ответ, сказанный другими словами:
  «можно ли участвовать вдвоём» → «временные объединения юридических лиц».

Слияние — Reciprocal Rank Fusion: у куска счёт ``1/(k + ранг)`` из каждого
списка, где он есть, счета складываются. Ранги, а не сырые оценки: расстояние
косинуса и ts_rank_cd несопоставимы по шкале, а ранги — сопоставимы. k = 60 —
общепринятое значение, его называют по статье Cormack и др. (SIGIR 2009); по
первоисточнику я его не сверял, подбирать будем на evals.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from procurement.assistant.rag.embeddings import Embedder
from procurement.assistant.rag.index import to_pgvector

RRF_K = 60
# Сколько кандидатов берёт каждый из двух поисков до слияния.
CANDIDATES = 20
# Кусок, найденный только по смыслу и дальше этого расстояния, отбрасываем: на
# любой вопрос векторный поиск вернёт «ближайшее», даже если оно ни при чём.
# Замер evals (CI, 2026-10-24, mpnet): у эталонов медиана 0.264, худший 0.420;
# у вопросов без ответа лучший кусок — 0.242. Распределения пересекаются:
# отличить «ответа нет» расстоянием нельзя, это делает модель по смыслу. Порог
# только срезает явный мусор: 0.5 — худший эталон плюс запас. Прежние 0.6 не
# срезали ничего.
MAX_SEMANTIC_DISTANCE = 0.5


class SearchMode(StrEnum):
    """Какие поиски участвуют. Агенту нужен hybrid; остальные — для evals,
    чтобы измерить вклад каждого поиска, а не поверить на слово."""

    HYBRID = "hybrid"
    LEXICAL = "lexical"
    SEMANTIC = "semantic"


@dataclass(frozen=True, slots=True)
class Hit:
    source: str
    anchor: str
    title: str
    url: str
    text: str
    score: float
    distance: float | None
    """Косинусное расстояние, если кусок нашёлся по смыслу."""

    lexical_rank: int | None
    semantic_rank: int | None


_HYBRID = text(
    """
    WITH semantic AS (
        SELECT id, distance, row_number() OVER (ORDER BY distance) AS rnk
        FROM (
            -- Внутренний запрос в виде «ORDER BY расстояние LIMIT n» — ровно
            -- тот вид, в котором PostgreSQL берёт индекс HNSW.
            SELECT id, embedding <=> CAST(:embedding AS vector) AS distance
            FROM doc_chunk
            WHERE embedding_model = :model AND :use_semantic
            ORDER BY embedding <=> CAST(:embedding AS vector)
            LIMIT :candidates
        ) nearest
    ),
    lexical AS (
        SELECT id, row_number() OVER (ORDER BY rank DESC, id) AS rnk
        FROM (
            SELECT id, ts_rank_cd(tsv, query) AS rank
            FROM doc_chunk, websearch_to_tsquery('russian', :query) AS query
            WHERE tsv @@ query AND :use_lexical
            ORDER BY rank DESC, id
            LIMIT :candidates
        ) matched
    )
    SELECT
        c.source, c.anchor, c.title, c.url, c.text,
        coalesce(1.0 / (:rrf_k + s.rnk), 0) + coalesce(1.0 / (:rrf_k + l.rnk), 0) AS score,
        s.distance, l.rnk AS lexical_rank, s.rnk AS semantic_rank
    FROM semantic s
    FULL JOIN lexical l ON l.id = s.id
    JOIN doc_chunk c ON c.id = coalesce(s.id, l.id)
    WHERE l.id IS NOT NULL OR s.distance <= :max_distance
    ORDER BY score DESC, c.id
    LIMIT :limit
    """
)


async def hybrid_search(
    sessions: async_sessionmaker[AsyncSession],
    embedder: Embedder,
    query: str,
    *,
    limit: int = 5,
    max_distance: float = MAX_SEMANTIC_DISTANCE,
    mode: SearchMode = SearchMode.HYBRID,
) -> list[Hit]:
    vector = await embedder.embed_query(query)
    async with sessions() as session:
        rows = await session.execute(
            _HYBRID,
            {
                "embedding": to_pgvector(vector),
                "model": embedder.model_name,
                "query": query,
                "candidates": CANDIDATES,
                "rrf_k": RRF_K,
                "max_distance": max_distance,
                "limit": limit,
                "use_lexical": mode is not SearchMode.SEMANTIC,
                "use_semantic": mode is not SearchMode.LEXICAL,
            },
        )
        return [
            Hit(
                source=r.source,
                anchor=r.anchor,
                title=r.title,
                url=r.url,
                text=r.text,
                score=float(r.score),
                distance=None if r.distance is None else float(r.distance),
                lexical_rank=r.lexical_rank,
                semantic_rank=r.semantic_rank,
            )
            for r in rows
        ]
