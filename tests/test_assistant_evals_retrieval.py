"""Качество поиска по документам: настоящая модель эмбеддингов, настоящий pgvector.

Без языковой модели — поэтому бесплатно и идёт в CI на каждый пуш (задача
evals). Индексируется снимок правил из репозитория и описание данных; вопросы
и эталоны — evals/questions.toml. Отчёт пишется в evals/reports/retrieval-ci.md
и в сводку задачи CI.

Нижние границы метрик (MIN_*) поставлены по первому замеру в CI (b259884), с
запасом на порядок при равных оценках, — чтобы регресс поиска ронял сборку,
а не проходил тихо.

Помечены evals: нужны Docker и модель (~1.1 ГБ, кэшируется).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.community.postgres import PostgresContainer

from procurement.assistant.evals.dataset import load
from procurement.assistant.evals.retrieval import RetrievalReport, evaluate_retrieval, index_corpus
from procurement.assistant.evals.snapshot import SNAPSHOT_DIR, read_snapshot
from procurement.assistant.rag.dictionary import SOURCE, chunk_dictionary, load_text
from procurement.assistant.rag.embeddings import FastEmbedder
from procurement.assistant.rag.search import SearchMode
from procurement.config import get_settings
from procurement.storage.postgres.engine import build_session_factory
from tests.conftest import POSTGRES_IMAGE

pytestmark = pytest.mark.evals

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORT = SNAPSHOT_DIR.parent / "reports" / "retrieval-ci.md"
NGR = "V2400035238"
MIN_HYBRID_HIT5 = 0.84
MIN_HYBRID_MRR = 0.73


@pytest.fixture(scope="module")
def postgres_dsn() -> Iterator[str]:
    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as container:
        dsn = container.get_connection_url()
        config = Config(str(PROJECT_ROOT / "alembic.ini"))
        config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
        config.set_main_option("sqlalchemy.url", dsn)
        command.upgrade(config, "head")
        yield dsn


@pytest.fixture
async def sessions(postgres_dsn: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(postgres_dsn)
    yield build_session_factory(engine)
    await engine.dispose()


async def test_retrieval_quality(sessions: async_sessionmaker[AsyncSession]) -> None:
    embedder = FastEmbedder(get_settings().assistant.rag)
    version, rules = read_snapshot(NGR)
    await index_corpus(
        sessions,
        embedder,
        [(f"adilet:{NGR}", version, rules), (SOURCE, "eval", chunk_dictionary(load_text()))],
    )

    report: RetrievalReport = await evaluate_retrieval(sessions, embedder, load())

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(report.render(), encoding="utf-8")
    print(report.render())

    hybrid = report.modes[SearchMode.HYBRID]
    lexical = report.modes[SearchMode.LEXICAL]
    semantic = report.modes[SearchMode.SEMANTIC]
    # Первый замер (CI, b259884): hybrid hit@5 0.89, MRR 0.78; semantic 0.83 / 0.67;
    # lexical 0.31 / 0.31. Границы — с запасом ~0.05 на равные оценки.
    assert hybrid.hit_at(5) >= MIN_HYBRID_HIT5, report.render()
    assert hybrid.mrr >= MIN_HYBRID_MRR, report.render()
    # Ради чего гибрид: слияние обязано быть не хуже каждого поиска по отдельности.
    assert hybrid.mrr >= max(lexical.mrr, semantic.mrr), report.render()
