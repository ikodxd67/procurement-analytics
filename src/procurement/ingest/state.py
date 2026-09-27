"""Хранение позиции загрузки между запусками.

На первом этапе это JSON-файл. На втором переедет в PostgreSQL, и интерфейс
StateStore для этого специально узкий: два метода, никаких намёков на файлы.
Код конвейера переезда не заметит.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Protocol

from procurement.ingest.models import CursorState
from procurement.logging import get_logger

log = get_logger(__name__)


class StateStore(Protocol):
    async def load(self, entity: str) -> CursorState: ...

    async def save(self, state: CursorState) -> None: ...


class InMemoryStateStore:
    """Для тестов: то же поведение, но без диска."""

    def __init__(self) -> None:
        self._data: dict[str, CursorState] = {}

    async def load(self, entity: str) -> CursorState:
        return self._data.get(entity) or CursorState.fresh(entity)

    async def save(self, state: CursorState) -> None:
        self._data[state.entity] = state


class FileStateStore:
    """JSON-файл со словарём: сущность -> состояние."""

    def __init__(self, path: Path) -> None:
        self._path = path
        # Два одновременных сохранения перетёрли бы друг друга. Блокировка
        # нужна даже в однопоточном asyncio: между чтением файла и его
        # записью есть await, а значит и возможность переключения задач.
        self._lock = asyncio.Lock()

    async def load(self, entity: str) -> CursorState:
        # to_thread, а не прямое чтение: обращение к диску блокирует поток
        # целиком, и пока оно идёт, весь событийный цикл стоит. На одном
        # маленьком файле это незаметно, но привычка должна быть правильной —
        # asyncio debug mode на этапе 6 такие вызовы как раз и ловит.
        raw = await asyncio.to_thread(self._read_all)
        payload = raw.get(entity)
        if payload is None:
            return CursorState.fresh(entity)
        return CursorState.model_validate(payload)

    async def save(self, state: CursorState) -> None:
        async with self._lock:
            await asyncio.to_thread(self._write_one, state)
        log.debug(
            "state.saved",
            entity=state.entity,
            cursor=state.cursor,
            pages=state.pages_done,
            records=state.records_done,
        )

    # --- синхронная часть, выполняется в отдельном потоке ----------------------

    def _read_all(self) -> dict[str, Any]:
        if not self._path.exists():
            return {}
        try:
            data: Any = json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            # Файл побился — лучше начать с нуля, чем упасть. Потеря позиции
            # приведёт к повторной загрузке, а записи идемпотентны по id.
            log.warning("state.corrupted", path=str(self._path))
            return {}
        return data if isinstance(data, dict) else {}

    def _write_one(self, state: CursorState) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = self._read_all()
        data[state.entity] = state.model_dump(mode="json")

        # Атомарная замена. Пишем во временный файл рядом с целевым, затем
        # os.replace подменяет его одним неделимым действием. Если процесс
        # умрёт на середине, целевой файл останется прежним, а не обрубленным.
        # Временный файл обязан лежать в той же папке: os.replace атомарен
        # только в пределах одной файловой системы.
        fd, tmp_name = tempfile.mkstemp(dir=self._path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=2)
                fh.flush()
                # Без fsync данные могут остаться в кэше ОС и пропасть при
                # выключении питания. Замена файла тогда произойдёт, а
                # содержимого в нём не будет.
                os.fsync(fh.fileno())
            os.replace(tmp_name, self._path)
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise
