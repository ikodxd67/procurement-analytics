"""Тесты хранилища позиции загрузки."""

from __future__ import annotations

import json
from pathlib import Path

from procurement.ingest.models import CursorState
from procurement.ingest.state import FileStateStore, InMemoryStateStore


async def test_missing_file_gives_fresh_state(tmp_path: Path) -> None:
    store = FileStateStore(tmp_path / "state.json")

    state = await store.load("contracts")

    assert state.entity == "contracts"
    assert state.cursor is None
    assert state.pages_done == 0
    assert state.finished is False


async def test_roundtrip(tmp_path: Path) -> None:
    store = FileStateStore(tmp_path / "state.json")
    saved = CursorState(entity="contracts", cursor="4996100", pages_done=3, records_done=150)

    await store.save(saved)
    loaded = await store.load("contracts")

    assert loaded.cursor == "4996100"
    assert loaded.pages_done == 3
    assert loaded.records_done == 150


async def test_entities_do_not_overwrite_each_other(tmp_path: Path) -> None:
    store = FileStateStore(tmp_path / "state.json")

    await store.save(CursorState(entity="contracts", cursor="111"))
    await store.save(CursorState(entity="lots", cursor="222"))

    assert (await store.load("contracts")).cursor == "111"
    assert (await store.load("lots")).cursor == "222"


async def test_corrupted_file_does_not_crash(tmp_path: Path) -> None:
    """Битый файл состояния — повод начать заново, а не упасть.

    Потеря позиции приводит к повторной загрузке, а записи идемпотентны по id.
    Падение на старте не приводит ни к чему хорошему.
    """
    path = tmp_path / "state.json"
    path.write_text("{это не json", encoding="utf-8")
    store = FileStateStore(path)

    state = await store.load("contracts")

    assert state.cursor is None


async def test_save_leaves_no_temporary_files(tmp_path: Path) -> None:
    """Атомарная запись создаёт временный файл, но обязана его убрать."""
    store = FileStateStore(tmp_path / "state.json")

    await store.save(CursorState(entity="contracts", cursor="1"))
    await store.save(CursorState(entity="contracts", cursor="2"))

    leftovers = [p.name for p in tmp_path.iterdir() if p.suffix == ".tmp"]
    assert leftovers == []
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]


async def test_written_file_is_readable_json(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = FileStateStore(path)

    await store.save(CursorState(entity="contracts", cursor="4996100", pages_done=2))

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["contracts"]["cursor"] == "4996100"
    assert payload["contracts"]["pages_done"] == 2


async def test_in_memory_store_behaves_the_same() -> None:
    """Заглушка для тестов обязана вести себя как настоящее хранилище."""
    store = InMemoryStateStore()

    assert (await store.load("contracts")).cursor is None

    await store.save(CursorState(entity="contracts", cursor="42"))

    assert (await store.load("contracts")).cursor == "42"
