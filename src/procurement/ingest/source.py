"""Протокол источника и разбор ответа, общий для всех реализаций.

Протокол намеренно узкий: один метод получения страницы и закрытие. Всё, что
выше по конвейеру, видит только его и не догадывается, откуда взялись данные —
из сети или из файла на диске.
"""

from __future__ import annotations

from typing import Any, Protocol
from urllib.parse import parse_qs, urlsplit

from procurement.ingest.errors import SourceResponseError
from procurement.ingest.models import Page

# Имя сущности у нас -> путь в API. Проверено 2026-09-27: у договоров путь в
# единственном числе, у лотов во множественном. Несогласованность источника,
# прятать её надо здесь и один раз.
ENTITY_PATHS: dict[str, str] = {
    "contracts": "/contract",
    "lots": "/lots",
}


class Source(Protocol):
    async def fetch_page(self, entity: str, cursor: str | None) -> Page: ...

    async def aclose(self) -> None: ...


def resolve_path(entity: str) -> str:
    try:
        return ENTITY_PATHS[entity]
    except KeyError:
        known = ", ".join(sorted(ENTITY_PATHS))
        msg = f"неизвестная сущность {entity!r}, известны: {known}"
        raise ValueError(msg) from None


def extract_cursor(next_page: str | None) -> str | None:
    """Достать значение search_after из ссылки next_page.

    Источник отдаёт относительный путь вида
    ``/contract?page=next&search_after=4996100``.

    Хранить как курсор именно число, а не ссылку целиком, — сознательное
    решение. Курсор попадёт в базу состояния и переживёт смену транспорта;
    ссылка же привязана к конкретному REST-адресу и к выбранному limit. Если
    завтра limit поменяется, сохранённая ссылка продолжит тянуть страницы
    старого размера.
    """
    if not next_page:
        return None

    query = parse_qs(urlsplit(next_page).query)
    values = query.get("search_after")
    if not values or not values[0]:
        # Ссылка есть, а курсора в ней нет — источник ведёт себя не по
        # документации. Молча считать это концом данных опасно: так теряется
        # весь хвост. Лучше громко упасть.
        msg = f"в next_page нет параметра search_after: {next_page!r}"
        raise SourceResponseError(msg)
    return values[0]


def page_from_payload(entity: str, payload: Any) -> Page:
    """Превратить разобранный JSON в модель Page.

    Общая функция для сетевого источника и для источника на фикстурах: так
    фикстуры проверяют тот же код разбора, что работает в бою.
    """
    if not isinstance(payload, dict):
        msg = f"ожидался объект JSON, получен {type(payload).__name__}"
        raise SourceResponseError(msg)

    items = payload.get("items")
    if items is None:
        msg = "в ответе нет ключа items"
        raise SourceResponseError(msg)
    if not isinstance(items, list):
        msg = f"items должен быть списком, получен {type(items).__name__}"
        raise SourceResponseError(msg)

    total = payload.get("total")
    if total is not None and not isinstance(total, int):
        total = None

    return Page(
        entity=entity,
        items=items,
        next_cursor=extract_cursor(payload.get("next_page")),
        total=total,
    )
