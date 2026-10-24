"""Оценка поиска по документам — без языковой модели.

Метрики (на вопросах с эталонными кусками):
- **hit@1 / hit@5** — доля вопросов, где эталонный кусок первый / в первой пятёрке;
- **MRR@10** (mean reciprocal rank) — среднее 1/место первого эталонного куска,
  0, если его нет в первых десяти. Чувствительнее hit@k: место 2 лучше места 5;
- **отказ** — на вопросах без ответа доля пустых результатов. Поиск, который
  на всё находит «что-то», заставляет модель додумывать.

Отдельно — расстояния: у эталонных кусков и у лучших кусков на вопросах без
ответа. По ним подбирается порог MAX_SEMANTIC_DISTANCE — замером, а не на глаз.
"""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from procurement.assistant.evals.dataset import Dataset, Kind, Question
from procurement.assistant.rag.chunks import DocChunk
from procurement.assistant.rag.embeddings import Embedder
from procurement.assistant.rag.index import replace_source, to_pgvector
from procurement.assistant.rag.search import MAX_SEMANTIC_DISTANCE, SearchMode, hybrid_search

TOP_K = 10
# Без отсечения — чтобы увидеть настоящие расстояния, а не обрезанные порогом.
NO_CUTOFF = 2.0


@dataclass
class QuestionResult:
    question_id: str
    rank: int | None
    """Место первого эталонного куска, 1..TOP_K, или None."""

    hits: int
    top: list[str]
    """Первые три «source#anchor» — для разбора промахов."""


@dataclass
class ModeReport:
    mode: SearchMode
    results: list[QuestionResult] = field(default_factory=list)
    abstained: int = 0
    no_answer_total: int = 0

    def _ranks(self) -> list[int | None]:
        return [r.rank for r in self.results]

    def hit_at(self, k: int) -> float:
        ranks = self._ranks()
        return sum(1 for r in ranks if r is not None and r <= k) / len(ranks) if ranks else 0.0

    @property
    def mrr(self) -> float:
        ranks = self._ranks()
        return sum(1 / r for r in ranks if r is not None) / len(ranks) if ranks else 0.0

    @property
    def abstention(self) -> float:
        return self.abstained / self.no_answer_total if self.no_answer_total else 0.0


@dataclass
class DistanceReport:
    gold: list[float]
    """Расстояние до эталонного куска (лучшего из эталонов) на вопросах с ответом."""

    no_answer_best: list[float]
    """Расстояние до ближайшего куска на вопросах без ответа."""

    def separating_threshold(self) -> float | None:
        """Порог посередине между худшим эталоном и лучшим «ложным», если они не пересекаются."""
        if not self.gold or not self.no_answer_best:
            return None
        worst_gold, best_false = max(self.gold), min(self.no_answer_best)
        return (worst_gold + best_false) / 2 if worst_gold < best_false else None


@dataclass
class RetrievalReport:
    model: str
    modes: dict[SearchMode, ModeReport]
    distances: DistanceReport
    max_distance: float

    def render(self) -> str:
        lines = [
            f"# Поиск по документам: {self.model}",
            "",
            f"Порог расстояния: {self.max_distance}. Вопросов с эталоном: "
            f"{len(next(iter(self.modes.values())).results)}, без ответа: "
            f"{next(iter(self.modes.values())).no_answer_total}.",
            "",
            "| Режим | hit@1 | hit@5 | MRR@10 | отказ на «нет ответа» |",
            "|---|---|---|---|---|",
        ]
        for mode, rep in self.modes.items():
            lines.append(
                f"| {mode.value} | {rep.hit_at(1):.2f} | {rep.hit_at(5):.2f} | "
                f"{rep.mrr:.2f} | {rep.abstained}/{rep.no_answer_total} |"
            )
        d = self.distances
        lines += ["", "## Расстояния (поиск по смыслу без порога)", ""]
        if d.gold:
            lines.append(
                f"- эталонные куски: медиана {statistics.median(d.gold):.3f}, "
                f"худший {max(d.gold):.3f} (n={len(d.gold)})"
            )
        if d.no_answer_best:
            lines.append(
                f"- ближайший кусок на вопросах без ответа: лучший {min(d.no_answer_best):.3f} "
                f"(n={len(d.no_answer_best)})"
            )
        threshold = d.separating_threshold()
        lines.append(
            f"- разделяющий порог: {threshold:.3f}"
            if threshold is not None
            else "- разделяющего порога нет: распределения пересекаются"
        )
        hybrid = self.modes.get(SearchMode.HYBRID)
        if hybrid is not None:
            misses = [r for r in hybrid.results if r.rank is None or r.rank > 5]
            lines += ["", f"## Промахи hybrid (эталон не в первой пятёрке): {len(misses)}", ""]
            lines += [f"- `{r.question_id}`: место {r.rank}, найдено {r.top}" for r in misses]
        return "\n".join(lines) + "\n"


async def index_corpus(
    sessions: async_sessionmaker[AsyncSession],
    embedder: Embedder,
    sources: Sequence[tuple[str, str, Sequence[DocChunk]]],
) -> None:
    """Проиндексировать (источник, версия, куски) — той же функцией, что в бою."""
    for source, version, chunks in sources:
        await replace_source(sessions, embedder, source=source, version=version, chunks=chunks)


def _key(source: str, anchor: str) -> str:
    return f"{source}#{anchor}"


async def _rank(
    sessions: async_sessionmaker[AsyncSession],
    embedder: Embedder,
    question: Question,
    mode: SearchMode,
    max_distance: float,
) -> QuestionResult:
    hits = await hybrid_search(
        sessions, embedder, question.text, limit=TOP_K, mode=mode, max_distance=max_distance
    )
    keys = [_key(h.source, h.anchor) for h in hits]
    gold = set(question.gold_anchors)
    rank = next((i for i, key in enumerate(keys, 1) if key in gold), None)
    return QuestionResult(question.id, rank, len(hits), keys[:3])


_GOLD_DISTANCE = text(
    """
    SELECT min(embedding <=> CAST(:embedding AS vector))
    FROM doc_chunk
    WHERE embedding_model = :model AND source || '#' || anchor = ANY(:keys)
    """
)


async def _gold_distance(
    sessions: async_sessionmaker[AsyncSession], embedder: Embedder, question: Question
) -> float | None:
    vector = await embedder.embed_query(question.text)
    async with sessions() as session:
        value = (
            await session.execute(
                _GOLD_DISTANCE,
                {
                    "embedding": to_pgvector(vector),
                    "model": embedder.model_name,
                    "keys": list(question.gold_anchors),
                },
            )
        ).scalar()
    return None if value is None else float(value)


async def evaluate_retrieval(
    sessions: async_sessionmaker[AsyncSession],
    embedder: Embedder,
    dataset: Dataset,
    *,
    max_distance: float = MAX_SEMANTIC_DISTANCE,
) -> RetrievalReport:
    with_gold = dataset.of_kind(Kind.DOCS, Kind.MIXED)
    no_answer = dataset.of_kind(Kind.NO_ANSWER)

    modes: dict[SearchMode, ModeReport] = {}
    for mode in SearchMode:
        report = ModeReport(mode=mode, no_answer_total=len(no_answer))
        for q in with_gold:
            report.results.append(await _rank(sessions, embedder, q, mode, max_distance))
        for q in no_answer:
            result = await _rank(sessions, embedder, q, mode, max_distance)
            report.abstained += result.hits == 0
        modes[mode] = report

    # Расстояние до эталона — прямым запросом, а не из выдачи поиска: выдача
    # ограничена кандидатами (CANDIDATES), и эталон дальше них выпал бы из
    # замера — как раз самые плохие случаи, по которым и ставится порог.
    gold_distances: list[float] = []
    for q in with_gold:
        distance = await _gold_distance(sessions, embedder, q)
        if distance is not None:
            gold_distances.append(distance)
    no_answer_best: list[float] = []
    for q in no_answer:
        hits = await hybrid_search(
            sessions, embedder, q.text, limit=1, mode=SearchMode.SEMANTIC, max_distance=NO_CUTOFF
        )
        if hits and hits[0].distance is not None:
            no_answer_best.append(hits[0].distance)

    return RetrievalReport(
        model=embedder.model_name,
        modes=modes,
        distances=DistanceReport(gold=gold_distances, no_answer_best=no_answer_best),
        max_distance=max_distance,
    )
