"""Поиск по документам без базы: нарезка правил, описание данных, эмбеддинги, инструмент.

HTML ниже — уменьшенная копия устройства настоящего документа V2400035238 с
adilet. В нём все странности, найденные на настоящем тексте: заголовок главы без
id, пункт без id, сноски между абзацами, номер пункта без точки, исключённый
пункт только в сноске, приложение после основного текста.
"""

from __future__ import annotations

import json
import math
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from procurement.assistant.models import ToolUse
from procurement.assistant.rag.adilet import (
    AdiletDocument,
    AdiletError,
    chunk_rules,
    parse_api_response,
)
from procurement.assistant.rag.chunks import MAX_CHUNK_CHARS, DocChunk, split_long
from procurement.assistant.rag.dictionary import SOURCE, chunk_dictionary, load_text
from procurement.assistant.rag.embeddings import HashEmbedder, build_embedder
from procurement.assistant.rag.index import to_pgvector
from procurement.assistant.rag.tool import MAX_HITS, build_docs_tool
from procurement.assistant.tools import ToolBox
from procurement.config import EmbedderKind, RagSettings

NOTE = (
    '<span class="note">&nbsp; Сноска. Пункт {n} - в редакции приказа </span>'
    '<a href="#">№ 297</a><br>'
)

RULES_HTML = f"""
<p id="z5">&nbsp;&nbsp; В соответствии с Законом ПРИКАЗЫВАЮ:</p>
<p align="right"><table><tr><td>Приложение 1 к приказу</td></tr></table></p>
<h3 id="z15"> Правила осуществления государственных закупок</h3>
<p id="z16">&nbsp;&nbsp; 1. Настоящие Правила определяют порядок закупок.</p>
{NOTE.format(n=1)}
<h3 id="z18"> Глава 1. Общие положения</h3>
<p id="z19">&nbsp;&nbsp; 3. Используются понятия:</p>
<p id="z20">&nbsp;&nbsp; 1) стартовая цена – цена до аукциона;</p>
<p id="z21">&nbsp;&nbsp; 2) веб-портал – единая платформа закупок.</p>
<h3> Глава 3. Порядок определения организатора</h3>
<h3 id="z110"> Параграф 1. Порядок определения организатора</h3>
<p id="z111">&nbsp;&nbsp; 31. Организатор определяется заказчиком.</p>
<p id="z268">&nbsp;&nbsp; <a name="z7604"></a>106 Сумма обеспечения округляется до тенге.</p>
<span class="note">&nbsp; <a name="z1024"></a>464. Исключен приказом от 08.05.2026 </span>
<p>&nbsp;&nbsp; 461. Заявки с нулём баллов не допускаются.</p>
<p id="z300">&nbsp;&nbsp; 2 процента — порог демпинга по этому пункту.</p>
<p align="right"><table><tr><td>Приложение 1 к Правилам</td></tr></table></p>
<h3 id="z7609"> Годовой план государственных закупок</h3>
<p id="z7610">&nbsp;&nbsp; 1. Форма плана.</p>
"""

DOC = AdiletDocument(ngr="V2400035238", title="Правила", version="2026-09-16", html=RULES_HTML)


def by_title(chunks: list[DocChunk]) -> dict[str, DocChunk]:
    return {c.title: c for c in chunks}


# --- нарезка правил --------------------------------------------------------


def test_rules_become_one_chunk_per_point_with_path() -> None:
    chunks = by_title(chunk_rules(DOC))

    point3 = chunks["Глава 1. Общие положения › п. 3"]
    # Подпункты едут со своим пунктом, а не отдельными кусками.
    assert point3.text.splitlines() == [
        "3. Используются понятия:",
        "1) стартовая цена – цена до аукциона;",
        "2) веб-портал – единая платформа закупок.",
    ]
    assert point3.url == "https://adilet.zan.kz/rus/docs/V2400035238#z19"
    assert point3.source == "adilet:V2400035238"


def test_chapter_heading_without_id_is_not_lost() -> None:
    # На adilet у «Главы 3» нет id; первая версия приписала её пункты «Главе 2».
    titles = list(by_title(chunk_rules(DOC)))

    assert (
        "Глава 3. Порядок определения организатора › "
        "Параграф 1. Порядок определения организатора › п. 31"
    ) in titles


def test_point_without_id_links_to_nearest_anchor() -> None:
    chunk = next(c for c in chunk_rules(DOC) if c.text.startswith("461."))

    assert chunk.url.endswith("#z268")
    assert chunk.anchor == "z268+п. 461"


def test_point_number_without_dot_is_still_a_point() -> None:
    labels = [c.title.rsplit("› ", 1)[-1] for c in chunk_rules(DOC)]

    assert "п. 106" in labels
    # «2 процента…» — не пункт 2, а продолжение пункта 461.
    assert "п. 2" not in labels


def test_notes_preamble_and_appendices_are_not_indexed() -> None:
    texts = " ".join(c.text for c in chunk_rules(DOC))

    assert "Сноска" not in texts
    assert "464" not in texts  # исключённый пункт живёт только в сноске
    assert "ПРИКАЗЫВАЮ" not in texts  # текст приказа до самих правил
    assert "Форма плана" not in texts  # приложение


def test_anchors_are_unique() -> None:
    anchors = [c.anchor for c in chunk_rules(DOC)]

    assert len(anchors) == len(set(anchors))


def test_long_point_is_split_into_parts_in_order() -> None:
    long_point = "<p id='z1'></p>".join(
        f'<p id="z{900 + i}">&nbsp; {i}) подпункт номер {i} о порядке проведения закупок.</p>'
        for i in range(1, 60)
    )
    html = RULES_HTML.replace(
        '<h3 id="z18">',
        f'<p id="z899">&nbsp; 2. Длинный пункт.</p>{long_point}<h3 id="z18">',
    )
    chunks = [c for c in chunk_rules(AdiletDocument("V1", "t", "v", html)) if "п. 2 " in c.title]

    assert len(chunks) > 1
    assert chunks[0].text.startswith("2. Длинный пункт.")
    assert all(len(c.text) <= MAX_CHUNK_CHARS for c in chunks)
    assert [c.title.endswith(f"(часть {i})") for i, c in enumerate(chunks, 1)] == [True] * len(
        chunks
    )
    assert chunks[1].anchor == "z899-2"
    assert {c.url for c in chunks} == {"https://adilet.zan.kz/rus/docs/V1#z899"}


# --- split_long ------------------------------------------------------------


def test_short_text_is_not_split() -> None:
    assert split_long("коротко") == ["коротко"]


def test_split_keeps_order_when_a_sentence_is_too_long() -> None:
    # Регресс: «55.» уезжал во вторую часть, потому что длинное предложение
    # писалось раньше накопленного начала.
    text = "55. Требование. " + "очень длинное предложение " * 80

    parts = split_long(text, limit=300)

    assert parts[0].startswith("55. Требование.")
    assert "".join(parts).replace(" ", "") == text.strip().replace(" ", "")


def test_split_does_not_cut_after_numbers() -> None:
    text = ("Согласно пунктам 1. и 2. настоящих Правил закупка проводится. " * 3).strip()

    parts = split_long(text, limit=130)

    assert all(not p.rstrip().endswith(("1.", "2.")) for p in parts)


# --- ответ API adilet ------------------------------------------------------


def test_api_response_is_parsed() -> None:
    doc = parse_api_response(
        "V2400035238",
        {
            "document": {
                "text_content": "<p>x</p>",
                "is_actual": True,
                "title_ru": "Правила",
                "updated_at": "2026-09-16",
            }
        },
    )

    assert (doc.title, doc.version, doc.html) == ("Правила", "2026-09-16", "<p>x</p>")


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"document": {"is_actual": True}},
        {"document": {"text_content": "<p>x</p>", "is_actual": False}},
        [],
    ],
)
def test_bad_or_repealed_document_is_refused(payload: Any) -> None:
    # Утративший силу акт в индексе опасен: ассистент процитирует отменённое.
    with pytest.raises(AdiletError):
        parse_api_response("V1", payload)


def test_html_without_rules_heading_is_refused() -> None:
    with pytest.raises(AdiletError, match="заголовок"):
        chunk_rules(AdiletDocument("V1", "t", "v", "<p id='z1'>1. Не те правила.</p>"))


# --- описание данных -------------------------------------------------------


def test_data_dictionary_ships_with_package_and_chunks_by_section() -> None:
    chunks = chunk_dictionary(load_text())

    titles = [c.title for c in chunks]
    assert "Описание данных › Индекс Херфиндаля (HHI) — концентрация закупок заказчика" in titles
    assert all(c.source == SOURCE for c in chunks)
    assert len({c.anchor for c in chunks}) == len(chunks)
    # Оговорка о синтетических данных обязана попасть в индекс.
    assert any("синтетических" in c.text for c in chunks)


# --- эмбеддинги ------------------------------------------------------------


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b, strict=True))


async def test_hash_embedder_is_deterministic_and_normalised() -> None:
    embedder = HashEmbedder(64)

    (a,) = await embedder.embed_documents(["демпинговая цена конкурса"])
    b = await embedder.embed_query("демпинговая цена конкурса")

    assert a == b
    assert math.isclose(sum(v * v for v in a), 1.0)


async def test_hash_embedder_puts_shared_stems_closer() -> None:
    embedder = HashEmbedder(256)
    query = await embedder.embed_query("закупки способом аукциона")
    near, far = await embedder.embed_documents(
        ["Порядок закупок способом аукциона", "Обжалование действий заказчика"]
    )

    assert cosine(query, near) > cosine(query, far)


def test_settings_pick_embedder() -> None:
    assert isinstance(build_embedder(RagSettings(embedder=EmbedderKind.HASH), dim=8), HashEmbedder)


def test_content_hash_changes_with_model_and_text() -> None:
    chunk = DocChunk("s", "a", "u", "t", "текст")

    assert chunk.content_hash("m1") != chunk.content_hash("m2")
    assert chunk.content_hash("m1") != DocChunk("s", "a", "u", "t", "текст!").content_hash("m1")


def test_embed_text_carries_the_path() -> None:
    chunk = DocChunk("s", "a", "u", "Глава 8 › п. 113", "Секретарь комиссии…")

    assert chunk.embed_text == "Глава 8 › п. 113\nСекретарь комиссии…"


def test_pgvector_literal() -> None:
    assert to_pgvector([0.5, -1.0, 1e-9]) == "[0.5,-1,1e-09]"


# --- инструмент ------------------------------------------------------------


async def test_search_docs_validates_arguments_before_database() -> None:
    # sessions=None: если до базы дошло, тест упадёт с AttributeError.
    box = ToolBox([build_docs_tool(cast(async_sessionmaker[AsyncSession], None), HashEmbedder(8))])

    too_many = await box.execute(
        ToolUse(id="c1", name="search_docs", input={"query": "аукцион", "limit": MAX_HITS + 1})
    )
    too_short = await box.execute(ToolUse(id="c2", name="search_docs", input={"query": "a"}))

    assert too_many.is_error and "limit" in too_many.content
    assert too_short.is_error and "query" in too_short.content


def test_search_docs_description_tells_model_to_cite_and_not_invent() -> None:
    spec = build_docs_tool(cast(async_sessionmaker[AsyncSession], None), HashEmbedder(8)).spec

    assert "ссылк" in spec.description
    assert "не додумывай" in spec.description
    assert json.dumps(spec.input_schema)  # схема сериализуется для API
