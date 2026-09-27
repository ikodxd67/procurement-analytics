"""Тесты конвейера.

Главные здесь — на отмену и таймаут. Проверяется не то, что код не упал, а
инвариант: сохранённый курсор всегда указывает ровно на границу обработанных
данных. Ни вперёд (иначе при перезапуске потеряем страницу), ни слишком
назад больше, чем на один прогон.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from procurement.ingest.fixture_source import FixtureSource
from procurement.ingest.limiter import AdaptiveLimiter
from procurement.ingest.models import Batch, CursorState
from procurement.ingest.pipeline import CursorLoopError, Pipeline
from procurement.ingest.state import InMemoryStateStore
from tests.conftest import RecordingHandler, StubSource


def make_limiter(concurrency: int = 2) -> AdaptiveLimiter:
    return AdaptiveLimiter(start=concurrency, minimum=1, maximum=concurrency, quiet_period_s=60.0)


def make_pipeline(
    source: Any,
    handler: Any,
    store: Any,
    *,
    workers: int = 1,
    batch_size: int = 100,
    queue_maxsize: int = 8,
    limiter: AdaptiveLimiter | None = None,
) -> Pipeline:
    return Pipeline(
        source=source,
        state=store,
        limiter=limiter or make_limiter(),
        handler=handler,
        workers=workers,
        batch_size=batch_size,
        queue_maxsize=queue_maxsize,
    )


# --- нормальный проход ---------------------------------------------------------


async def test_full_run_over_fixtures(fixtures_root: Path) -> None:
    source = FixtureSource(fixtures_root)
    handler = RecordingHandler()
    store = InMemoryStateStore()
    limiter = make_limiter()
    pipeline = make_pipeline(source, handler, store, limiter=limiter)

    async with limiter:
        result = await pipeline.run("contracts")

    assert result.pages == 3
    assert result.records == 15
    assert result.finished is True
    assert len(handler.ids) == 15
    assert len(set(handler.ids)) == 15, "дублей быть не должно"


async def test_full_run_over_stub() -> None:
    source = StubSource(pages=5, per_page=4)
    handler = RecordingHandler()
    store = InMemoryStateStore()
    limiter = make_limiter()
    pipeline = make_pipeline(source, handler, store, limiter=limiter)

    async with limiter:
        result = await pipeline.run("contracts")

    assert result.pages == 5
    assert result.records == 20
    assert result.finished is True
    assert handler.ids == list(range(20))

    saved = await store.load("contracts")
    assert saved.finished is True
    assert saved.cursor is None


async def test_already_finished_entity_is_skipped() -> None:
    source = StubSource(pages=5)
    store = InMemoryStateStore()
    await store.save(CursorState(entity="contracts", cursor=None, pages_done=5, finished=True))
    handler = RecordingHandler()
    limiter = make_limiter()
    pipeline = make_pipeline(source, handler, store, limiter=limiter)

    async with limiter:
        result = await pipeline.run("contracts")

    assert result.pages == 0
    assert source.requested == [], "к источнику обращаться незачем"


async def test_max_pages_stops_early_and_saves_position() -> None:
    source = StubSource(pages=10, per_page=2)
    handler = RecordingHandler()
    store = InMemoryStateStore()
    limiter = make_limiter()
    pipeline = make_pipeline(source, handler, store, limiter=limiter)

    async with limiter:
        result = await pipeline.run("contracts", max_pages=3)

    assert result.pages == 3
    assert result.finished is False
    saved = await store.load("contracts")
    assert saved.cursor == "3", "курсор указывает на следующую неотданную страницу"


async def test_empty_page_in_the_middle_does_not_stop_the_run() -> None:
    """Страница без записей, но с курсором — не конец выдачи."""
    source = StubSource(pages=5, per_page=2, empty_pages=frozenset({2}))
    handler = RecordingHandler()
    store = InMemoryStateStore()
    limiter = make_limiter()
    pipeline = make_pipeline(source, handler, store, limiter=limiter)

    async with limiter:
        result = await pipeline.run("contracts")

    assert result.pages == 5, "пустую страницу надо пройти насквозь"
    assert result.records == 8, "четыре непустые страницы по две записи"
    assert result.finished is True


async def test_repeated_cursor_raises_instead_of_looping_forever() -> None:
    source = StubSource(pages=5, stuck_cursor=True)
    handler = RecordingHandler()
    store = InMemoryStateStore()
    limiter = make_limiter()
    pipeline = make_pipeline(source, handler, store, limiter=limiter)

    async with limiter:
        with pytest.raises(CursorLoopError):
            await pipeline.run("contracts")


# --- отмена --------------------------------------------------------------------


async def test_cancellation_keeps_cursor_consistent_with_processed_data() -> None:
    """Отмена на середине: курсор обязан совпасть с обработанным объёмом.

    Если сохранить его раньше обработки, при перезапуске страница будет
    пропущена и данные потеряются молча.

    Момент отмены выбирается по событию, а не по часам: на Windows пауза
    короче 15.6 мс не выполняется вовсе, если циклу есть чем заняться, и тест
    на таймерах проверял бы везение.
    """
    source = StubSource(pages=20, per_page=2)
    store = InMemoryStateStore()
    handled: list[int] = []
    enough = asyncio.Event()

    async def handler(batch: Batch) -> None:
        handled.append(batch.page_index)
        if len(handled) >= 4:
            enough.set()

    limiter = make_limiter()
    pipeline = make_pipeline(source, handler, store, limiter=limiter, queue_maxsize=2)

    async with limiter:
        task = asyncio.create_task(pipeline.run("contracts"))
        await enough.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    saved = await store.load("contracts")

    assert saved.pages_done >= 4, "что-то успели обработать"
    assert saved.pages_done < 20, "и не всё — иначе тест ничего не проверяет"
    assert saved.pages_done == len(handled), "курсор ровно по обработанным страницам"
    assert saved.records_done == len(handled) * 2
    assert saved.cursor == str(saved.pages_done)
    assert saved.finished is False


async def test_restart_after_cancellation_finishes_without_duplicates() -> None:
    store = InMemoryStateStore()
    first_ids: list[int] = []
    enough = asyncio.Event()

    async def first_handler(batch: Batch) -> None:
        first_ids.extend(r["id"] for r in batch.records)
        if len(first_ids) >= 6:
            enough.set()

    limiter = make_limiter()
    async with limiter:
        pipeline = make_pipeline(
            StubSource(pages=12, per_page=2),
            first_handler,
            store,
            limiter=limiter,
            queue_maxsize=2,
        )
        task = asyncio.create_task(pipeline.run("contracts"))
        await enough.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    after_cancel = await store.load("contracts")
    assert after_cancel.finished is False
    assert after_cancel.pages_done < 12

    second = RecordingHandler()
    limiter2 = make_limiter()
    async with limiter2:
        resumed = make_pipeline(StubSource(pages=12, per_page=2), second, store, limiter=limiter2)
        result = await resumed.run("contracts")

    assert result.finished is True

    all_ids = first_ids + second.ids
    assert sorted(all_ids) == list(range(24)), "вместе два прогона дали ровно все записи"
    assert len(set(all_ids)) == len(all_ids), "и ни одной дважды"


async def test_batch_in_flight_is_finished_before_shutdown() -> None:
    """Батч, уже отданный обработчику, обязан дописаться, а не оборваться."""
    started = asyncio.Event()
    completed: list[int] = []

    async def slow_handler(batch: Batch) -> None:
        started.set()
        await asyncio.sleep(0.15)
        completed.append(batch.page_index)

    source = StubSource(pages=6, per_page=2)
    store = InMemoryStateStore()
    limiter = make_limiter()
    pipeline = make_pipeline(source, slow_handler, store, limiter=limiter, queue_maxsize=1)

    async with limiter:
        task = asyncio.create_task(pipeline.run("contracts"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert completed, "обработчик должен был досчитать начатый батч"
    saved = await store.load("contracts")
    assert saved.pages_done == len(completed)


async def test_stop_event_gives_graceful_stop_with_result() -> None:
    """Мягкая остановка (так работает SIGTERM) возвращает результат, а не отмену."""
    source = StubSource(pages=30, per_page=2)
    store = InMemoryStateStore()
    stop = asyncio.Event()
    handled: list[int] = []

    async def handler(batch: Batch) -> None:
        handled.append(batch.page_index)
        if len(handled) == 3:
            stop.set()

    limiter = make_limiter()
    pipeline = make_pipeline(source, handler, store, limiter=limiter, queue_maxsize=2)

    async with limiter:
        result = await pipeline.run("contracts", stop=stop)

    assert result.finished is False
    assert result.reason == "получен сигнал остановки"
    assert result.pages >= 3
    assert result.pages < 30

    saved = await store.load("contracts")
    assert saved.pages_done == len(handled)


# --- таймаут -------------------------------------------------------------------


async def test_overall_timeout_stops_run_and_keeps_position() -> None:
    # Задержка заведомо больше разрешения часов событийного цикла (15.6 мс на
    # Windows), иначе sleep внутри источника просто не состоится.
    source = StubSource(pages=100, per_page=2, latency_s=0.05)
    handler = RecordingHandler()
    store = InMemoryStateStore()
    limiter = make_limiter()
    pipeline = make_pipeline(source, handler, store, limiter=limiter)

    async with limiter:
        result = await pipeline.run("contracts", overall_timeout_s=0.3)

    assert result.finished is False
    assert result.reason == "исчерпан общий бюджет времени"
    assert result.pages > 0

    saved = await store.load("contracts")
    assert saved.pages_done == len(handler.pages)
    assert saved.cursor == str(saved.pages_done)


# --- порядок фиксации курсора --------------------------------------------------


class OrderSpyStore(InMemoryStateStore):
    """Хранилище, которое на каждом сохранении снимает слепок обработанного."""

    def __init__(self, handled: set[int]) -> None:
        super().__init__()
        self._handled = handled
        self.snapshots: list[tuple[int, frozenset[int]]] = []

    async def save(self, state: CursorState) -> None:
        self.snapshots.append((state.pages_done, frozenset(self._handled)))
        await super().save(state)


async def test_cursor_never_runs_ahead_of_processed_pages() -> None:
    """С несколькими обработчиками страницы заканчиваются не по порядку.

    Наивная фиксация сохранила бы курсор пятой страницы, пока четвёртая ещё в
    работе. Проверяем инвариант: в момент любого сохранения все страницы до
    сохранённой границы уже обработаны.
    """
    handled: set[int] = set()

    async def uneven_handler(batch: Batch) -> None:
        # Чётные страницы обрабатываются дольше нечётных — порядок завершения
        # гарантированно разойдётся с порядком поступления.
        await asyncio.sleep(0.02 if batch.page_index % 2 == 0 else 0.001)
        handled.add(batch.page_index)

    source = StubSource(pages=12, per_page=2)
    store = OrderSpyStore(handled)
    limiter = make_limiter(4)
    pipeline = make_pipeline(
        source, uneven_handler, store, workers=4, queue_maxsize=8, limiter=limiter
    )

    async with limiter:
        result = await pipeline.run("contracts")

    assert result.pages == 12
    assert store.snapshots, "сохранения должны были происходить"
    for pages_done, handled_then in store.snapshots:
        expected = set(range(pages_done))
        assert expected <= handled_then, (
            f"курсор ушёл на {pages_done} страниц, но обработаны только {sorted(handled_then)}"
        )


# --- backpressure ---------------------------------------------------------------


async def test_queue_limit_holds_producer_back() -> None:
    """Предел очереди обязан тормозить загрузку, а не копить страницы в памяти.

    Держим первый батч в обработчике и считаем, сколько страниц producer успел
    забрать. При очереди на 2 и одном обработчике это ровно четыре: одна в
    работе, две в очереди, на пятой producer встаёт.
    """
    release = asyncio.Event()

    async def blocking_handler(batch: Batch) -> None:
        await release.wait()

    source = StubSource(pages=50, per_page=1)
    store = InMemoryStateStore()
    limiter = make_limiter()
    pipeline = make_pipeline(
        source, blocking_handler, store, workers=1, queue_maxsize=2, batch_size=100, limiter=limiter
    )

    async with limiter:
        task = asyncio.create_task(pipeline.run("contracts"))
        await asyncio.sleep(0.1)

        fetched_while_blocked = len(source.requested)

        release.set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert fetched_while_blocked == 4, (
        f"producer забежал вперёд на {fetched_while_blocked} страниц — очередь не держит"
    )
