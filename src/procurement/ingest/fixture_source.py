"""Источник, читающий заранее записанные ответы с диска.

Нужен, пока нет токена: весь конвейер, отмена, сохранение курсора и тесты
пишутся и проверяются без единого запроса в сеть. Переключение — настройкой
PA_SOURCE__KIND, код конвейера об этом не знает.

Файлы лежат как ``<root>/<сущность>/page_001.json`` и содержат ответ ровно в
том виде, в каком его отдаёт API. Разбираются они той же функцией
page_from_payload, что и настоящие ответы, — иначе тесты проверяли бы код,
которого в бою нет.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import orjson

from procurement.ingest.errors import SourceRequestError, SourceResponseError
from procurement.ingest.models import Page
from procurement.ingest.source import page_from_payload
from procurement.logging import get_logger

log = get_logger(__name__)


class FixtureSource:
    def __init__(self, root: Path, *, latency_s: float = 0.0) -> None:
        self._root = root
        # Искусственная задержка: без неё все страницы приходят мгновенно, и
        # проверить поведение очереди под нагрузкой невозможно.
        self._latency_s = latency_s
        self._cache: dict[str, dict[str | None, Page]] = {}

    async def fetch_page(self, entity: str, cursor: str | None) -> Page:
        index = await self._index(entity)

        if cursor not in index:
            known = len(index)
            msg = f"фикстур для сущности {entity!r} с курсором {cursor!r} нет (страниц: {known})"
            raise SourceRequestError(msg)

        if self._latency_s:
            await asyncio.sleep(self._latency_s)

        page = index[cursor]
        log.debug(
            "fixture.page",
            entity=entity,
            cursor=cursor,
            items=len(page.items),
            kind=page.kind.value,
        )
        return page

    async def _index(self, entity: str) -> dict[str | None, Page]:
        cached = self._cache.get(entity)
        if cached is not None:
            return cached

        index = await asyncio.to_thread(self._build_index, entity)
        self._cache[entity] = index
        return index

    def _build_index(self, entity: str) -> dict[str | None, Page]:
        """Собрать соответствие «курсор запроса -> страница ответа».

        Первый файл отвечает на запрос без курсора. Каждый следующий отвечает
        на тот курсор, который предыдущая страница указала в next_page. Так
        цепочка строится из самих данных, а не из имён файлов, и фикстура
        остаётся честной моделью курсорной выдачи.
        """
        folder = self._root / entity
        if not folder.is_dir():
            msg = f"папка с фикстурами не найдена: {folder}"
            raise SourceRequestError(msg)

        files = sorted(folder.glob("page_*.json"))
        if not files:
            msg = f"в {folder} нет файлов page_*.json"
            raise SourceRequestError(msg)

        index: dict[str | None, Page] = {}
        expected_cursor: str | None = None

        for path in files:
            try:
                payload = orjson.loads(path.read_bytes())
            except orjson.JSONDecodeError as err:
                msg = f"фикстура {path.name} не разбирается как JSON"
                raise SourceResponseError(msg) from err

            page = page_from_payload(entity, payload)
            index[expected_cursor] = page
            expected_cursor = page.next_cursor

            if expected_cursor is None:
                # Дошли до страницы без курсора — это конец выдачи. Файлы
                # после неё недостижимы, и это ошибка в наборе фикстур.
                break

        if expected_cursor is not None:
            msg = (
                f"набор фикстур для {entity!r} обрывается: последняя страница "
                f"указывает на курсор {expected_cursor!r}, а файла для него нет"
            )
            raise SourceResponseError(msg)

        return index

    async def aclose(self) -> None:
        """Закрывать нечего, метод есть ради соответствия протоколу."""
        return
