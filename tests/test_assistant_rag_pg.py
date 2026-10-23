"""Индекс и гибридный поиск на настоящем PostgreSQL с pgvector.

Эмбеддинги — HashEmbedder: смысла он не понимает, но тексты с общими основами
слов у него ближе. Этого хватает, чтобы проверить SQL, слияние рангов, порог
расстояния и пересборку, не скачивая гигабайт модели в CI. Качество поиска с
настоящей моделью меряется на evals (этап 5), не здесь.

Тесты помечены как integration, им нужен Docker.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from testcontainers.community.postgres import PostgresContainer

from procurement.assistant.rag.chunks import DocChunk
from procurement.assistant.rag.embeddings import HashEmbedder
from procurement.assistant.rag.index import replace_source
from procurement.assistant.rag.search import hybrid_search
from procurement.storage.postgres.engine import build_session_factory
from procurement.storage.postgres.models import EMBEDDING_DIM
from tests.conftest import POSTGRES_IMAGE

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE = "adilet:TEST"


def chunk(anchor: str, title: str, text: str, source: str = SOURCE) -> DocChunk:
    return DocChunk(
        source=source, anchor=anchor, url=f"https://x/#{anchor}", title=title, text=text
    )


RULES = [
    chunk(
        "z1",
        "Глава 5. Порядок определения демпинговой цены › п. 85",
        "85. Цена заявки признаётся демпинговой, если она ниже сметной более чем на два процента.",
    ),
    chunk(
        "z2",
        "Глава 12. Порядок осуществления закупок способом аукциона › п. 300",
        "300. Аукцион проводится на веб-портале с понижением стартовой цены.",
    ),
    chunk(
        "z3",
        "Глава 19. Порядок обжалования › п. 588",
        "588. Жалоба на действия заказчика подаётся в течение пяти рабочих дней.",
    ),
    chunk(
        "z4",
        "Глава 6. Временные объединения юридических лиц › п. 90",
        "90. Юридические лица вправе участвовать в закупках как временное объединение.",
    ),
]


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
    factory = build_session_factory(engine)
    async with factory() as session, session.begin():
        await session.execute(sa.text("TRUNCATE doc_chunk"))
    yield factory
    await engine.dispose()


EMBEDDER = HashEmbedder(EMBEDDING_DIM)


async def index(
    sessions: async_sessionmaker[AsyncSession], chunks: list[DocChunk]
) -> tuple[int, int, int]:
    result = await replace_source(sessions, EMBEDDER, source=SOURCE, version="v1", chunks=chunks)
    return result.written, result.unchanged, result.deleted


async def fetch_column(sessions: async_sessionmaker[AsyncSession], sql: str) -> list[str]:
    async with sessions() as session:
        rows = await session.execute(sa.text(sql))
        return [str(row[0]) for row in rows]


# --- пересборка ------------------------------------------------------------


async def test_rebuild_is_idempotent_and_only_touches_changes(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    assert await index(sessions, RULES) == (4, 0, 0)
    # Второй прогон без изменений не пересчитывает ни одного эмбеддинга.
    assert await index(sessions, RULES) == (0, 4, 0)

    edited = [*RULES[:3], chunk("z4", RULES[3].title, RULES[3].text + " Изменено.")]
    assert await index(sessions, edited) == (1, 3, 0)

    # Пункт исчез из редакции — исчезает из индекса.
    assert await index(sessions, edited[:2]) == (0, 2, 2)
    anchors = await fetch_column(sessions, "SELECT anchor FROM doc_chunk ORDER BY anchor")
    assert anchors == ["z1", "z2"]


async def test_other_sources_are_left_alone(sessions: async_sessionmaker[AsyncSession]) -> None:
    other = [chunk("d1", "Описание данных › HHI", "Индекс Херфиндаля.", source="project:dd")]
    await replace_source(sessions, EMBEDDER, source="project:dd", version="v", chunks=other)

    await index(sessions, [])  # источник правил опустел

    sources = await fetch_column(sessions, "SELECT DISTINCT source FROM doc_chunk")
    assert sources == ["project:dd"]


async def test_full_text_column_is_computed_by_database(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await index(sessions, RULES)

    found = await fetch_column(
        sessions,
        "SELECT anchor FROM doc_chunk "
        "WHERE tsv @@ websearch_to_tsquery('russian', 'демпинговых ценах')",
    )
    # Стемминг russian: «демпинговых ценах» находит «демпинговой … цена».
    assert found == ["z1"]


async def test_refuses_wrong_vector_size(sessions: async_sessionmaker[AsyncSession]) -> None:
    with pytest.raises(ValueError, match="768"):
        await replace_source(sessions, HashEmbedder(16), source=SOURCE, version="v", chunks=RULES)


# --- поиск -----------------------------------------------------------------


async def test_hybrid_search_ranks_the_matching_point_first(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await index(sessions, RULES)

    hits = await hybrid_search(sessions, EMBEDDER, "демпинговая цена")

    assert hits[0].url.endswith("#z1")
    # Нашёлся обоими способами — у такого куска счёт выше.
    assert hits[0].lexical_rank == 1 and hits[0].semantic_rank == 1
    assert hits == sorted(hits, key=lambda h: h.score, reverse=True)


async def test_search_by_words_from_the_chapter_title(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    # В тексте п. 588 нет слова «обжалование» — оно в заголовке главы, который
    # входит и в полнотекстовый индекс, и в эмбеддинг.
    await index(sessions, RULES)

    hits = await hybrid_search(sessions, EMBEDDER, "обжалование")

    assert hits and hits[0].url.endswith("#z3")


async def test_unrelated_question_finds_nothing(sessions: async_sessionmaker[AsyncSession]) -> None:
    await index(sessions, RULES)

    hits = await hybrid_search(sessions, EMBEDDER, "рецепт борща со сметаной")

    assert hits == []


async def test_vectors_of_another_model_are_not_mixed_in(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    await index(sessions, RULES)
    other_model = HashEmbedder(EMBEDDING_DIM, model_name="another-model")

    hits = await hybrid_search(sessions, other_model, "демпинговая цена")

    # Векторы чужой модели в поиск по смыслу не идут; находит только по словам.
    assert [h.semantic_rank for h in hits] == [None] * len(hits)
    assert hits[0].lexical_rank == 1
