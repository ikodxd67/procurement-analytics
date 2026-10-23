"""Правила госзакупок с adilet.zan.kz: загрузка и нарезка по пунктам.

Сайт — одностраничное приложение, HTML страницы без текста. Текст отдаёт его
же API: ``/api/documents/by-ngr/<номер>?language=rus`` — JSON, в поле
``document.text_content`` HTML акта (проверено 2026-10-23 по запросам страницы).

Использовать текст можно: по статье 8 Закона РК «Об авторском праве и смежных
правах» официальные документы не являются объектами авторского права.

Устройство HTML (проверено на V2400035238): главы и параграфы — ``<h3 id>``,
абзацы — ``<p id="zNNN">``, сноски об изменениях — отдельные
``<span class="note">`` между абзацами. Приложения (формы и шаблоны) начинаются
с таблицы «Приложение N к Правилам» — их не индексируем, они засоряют поиск.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Any

import httpx

from procurement.assistant.rag.chunks import DocChunk, split_long

# id бывает не у всех: у заголовка «Главы 3» его нет, у пунктов 461, 464, 466
# тоже (проверено на V2400035238). Без поблажки пункты главы 3 достались бы
# «Главе 2», а три пункта пропали бы вовсе. Берём p с id или совсем без
# атрибутов; p с class или align — служебные (сноски, обёртки таблиц).
_ELEMENT = re.compile(
    r"<h3[^>]*>(?P<heading>.*?)</h3>"
    r'|<p(?: id="(?P<anchor>z\d+)"[^>]*)?>(?P<para>.*?)</p>',
    re.S,
)
_TAG = re.compile(r"<[^>]+>")
_SPACES = re.compile(r"[ \t ]+")
# Номер пункта: «12.», «12-1.» (пункты, добавленные поправками).
# Бывает и без точки: в источнике «106 Сумма обеспечения…» (опечатка на
# adilet, 2026-10-23). Без точки — только если дальше заглавная буква, чтобы
# не принять за пункт абзац вида «2 процента…».
_POINT = re.compile(r"^(\d+(?:-\d+)?)(?:\.\s|\s(?=[А-ЯЁA-Z]))")


class AdiletError(Exception):
    """Документ не получен или он не тот, что ждали."""


@dataclass(frozen=True, slots=True)
class AdiletDocument:
    ngr: str
    title: str
    version: str
    """Дата последнего обновления текста на adilet — версия для индекса."""

    html: str


async def fetch_document(client: httpx.AsyncClient, url_template: str, ngr: str) -> AdiletDocument:
    response = await client.get(url_template.format(ngr=ngr))
    if response.status_code != httpx.codes.OK:
        msg = f"adilet {ngr}: HTTP {response.status_code}"
        raise AdiletError(msg)
    return parse_api_response(ngr, response.json())


def parse_api_response(ngr: str, payload: Any) -> AdiletDocument:
    doc = payload.get("document") if isinstance(payload, dict) else None
    if not isinstance(doc, dict) or not doc.get("text_content"):
        msg = f"adilet {ngr}: в ответе нет document.text_content"
        raise AdiletError(msg)
    if doc.get("is_actual") is not True:
        # Утративший силу акт в поиске опасен: ассистент процитирует отменённое
        # правило со ссылкой, и ссылка будет выглядеть убедительно.
        msg = f"adilet {ngr}: документ не действует (is_actual={doc.get('is_actual')!r})"
        raise AdiletError(msg)
    return AdiletDocument(
        ngr=ngr,
        title=str(doc.get("title_ru") or ngr),
        version=str(doc.get("updated_at") or ""),
        html=str(doc["text_content"]),
    )


def _clean(fragment: str) -> str:
    text = html.unescape(_TAG.sub(" ", fragment))
    return _SPACES.sub(" ", text).strip()


def rules_body(document_html: str) -> str:
    """Тело правил: от заголовка «Правила …» до первой таблицы приложения."""
    start = document_html.find("Правила осуществления государственных закупок</h3>")
    if start < 0:
        msg = "не найден заголовок «Правила осуществления государственных закупок»"
        raise AdiletError(msg)
    end = document_html.find("<table", start)
    return document_html[start : end if end > 0 else len(document_html)]


def chunk_rules(document: AdiletDocument) -> list[DocChunk]:
    """Один кусок — один пункт с подпунктами; длинный пункт режется на части.

    Заголовок куска — путь «Глава › Параграф › п. N»: в тексте пункта часто нет
    слов, по которым его ищут («конкурс», «аукцион»), они есть в главе.
    """
    source = f"adilet:{document.ngr}"
    base_url = f"https://adilet.zan.kz/rus/docs/{document.ngr}"
    chapter = paragraph = ""
    last_anchor = ""
    # (ключ куска, якорь для ссылки, заголовок, абзацы)
    points: list[tuple[str, str, str, list[str]]] = []

    for element in _ELEMENT.finditer(rules_body(document.html)):
        heading = element.group("heading")
        text = _clean(heading if heading is not None else element.group("para"))
        if not text:
            continue
        if heading is not None:
            if text.startswith("Глава"):
                chapter, paragraph = text, ""
            elif text.startswith("Параграф"):
                paragraph = text
            continue
        own_anchor = element.group("anchor")
        if own_anchor:
            last_anchor = own_anchor
        match = _POINT.match(text)
        if match or not points:
            label = f"п. {match.group(1)}" if match else "вводная часть"
            title = " › ".join(part for part in (chapter, paragraph, label) if part)
            # У абзаца без id ссылка ведёт на ближайший якорь выше, а ключ
            # делается из него и номера пункта — чтобы остаться уникальным.
            key = own_anchor or f"{last_anchor}+{label}"
            points.append((key, last_anchor, title, [text]))
        else:
            # Подпункт «1)», «2)» или продолжение — к своему пункту.
            points[-1][3].append(text)

    chunks: list[DocChunk] = []
    for key, link_anchor, title, paragraphs in points:
        parts = split_long("\n".join(paragraphs))
        for index, part in enumerate(parts):
            part_anchor = key if index == 0 else f"{key}-{index + 1}"
            part_title = title if len(parts) == 1 else f"{title} (часть {index + 1})"
            chunks.append(
                DocChunk(
                    source=source,
                    anchor=part_anchor,
                    url=f"{base_url}#{link_anchor}",
                    title=part_title,
                    text=part,
                )
            )
    return chunks
