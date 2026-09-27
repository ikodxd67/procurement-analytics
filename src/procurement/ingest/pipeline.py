"""Конвейер загрузки: producer -> очередь -> consumers.

Устройство и обоснования.

Почему внутри одной сущности нет конкурентности. Курсорная пагинация
последовательна по определению: адрес следующей страницы известен только из
текущего ответа. Параллелить нечего. Конкурентность появляется между
сущностями и между месяцами бэкфилла на этапе 3 — там лимитер и пригодится.

Зачем тогда очередь. Она разделяет две разные по природе работы: producer
занят сетью, consumers будут заняты записью в базу. Пока consumer пишет,
producer уже тянет следующую страницу.

Зачем очереди предел размера. Без него producer выкачает всю сущность в
оперативную память, если consumers отстают: миллионы записей в списке. Предел
превращает отставание consumers в естественное торможение producer — он просто
встанет на queue.put, пока не освободится место. Это и называется backpressure.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial

from procurement.ingest.limiter import AdaptiveLimiter
from procurement.ingest.models import Batch, CursorState, PageKind, RawRecord
from procurement.ingest.retry import RetryPolicy, run_with_retry
from procurement.ingest.source import Source
from procurement.ingest.state import StateStore
from procurement.logging import get_logger

log = get_logger(__name__)

BatchHandler = Callable[[Batch], Awaitable[None]]

# Маркер конца очереди. Отдельный объект, а не None: None — законное значение
# и может однажды оказаться в данных.
_STOP = object()


class CursorLoopError(RuntimeError):
    """Источник вернул курсор, который уже встречался.

    Защита от бесконечного цикла: без неё сломанная пагинация крутила бы одни
    и те же страницы, пока не кончится место на диске.
    """


@dataclass(frozen=True, slots=True)
class IngestResult:
    entity: str
    pages: int
    records: int
    finished: bool
    cursor: str | None
    reason: str

    @property
    def stopped_early(self) -> bool:
        return not self.finished


def _chunked(items: Sequence[RawRecord], size: int) -> Iterator[list[RawRecord]]:
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


class OrderedCommitter:
    """Двигает сохранённый курсор строго по порядку страниц.

    Зачем это нужно. Consumers работают параллельно, и батч пятой страницы
    может закончиться раньше батча четвёртой. Если сохранить курсор сразу по
    факту завершения, он укажет за пределы того, что реально обработано, — при
    падении четвёртая страница потеряется навсегда.

    Поэтому фиксация идёт по порядку: курсор страницы N сохраняется только
    когда обработаны все батчи страниц до N включительно. Сохранённый курсор
    всегда означает «всё до этого места гарантированно обработано».
    """

    def __init__(self, store: StateStore, state: CursorState) -> None:
        self._store = store
        self._state = state
        self._lock = asyncio.Lock()
        self._remaining: dict[int, int] = {}
        self._info: dict[int, tuple[str | None, int, bool]] = {}
        self._next_to_commit = 0

    @property
    def state(self) -> CursorState:
        return self._state

    def plan(
        self,
        page_index: int,
        batches: int,
        cursor_after: str | None,
        records: int,
        *,
        is_final: bool,
    ) -> None:
        """Объявить, из скольких батчей состоит страница.

        Метод синхронный и блокировки не берёт намеренно: producer вызывает
        его до того, как первый батч страницы попадёт в очередь, то есть до
        того, как хоть один consumer сможет о нём узнать. Точки переключения
        задач между объявлением и постановкой в очередь нет.
        """
        self._remaining[page_index] = batches
        self._info[page_index] = (cursor_after, records, is_final)

    async def batch_done(self, page_index: int) -> None:
        async with self._lock:
            self._remaining[page_index] -= 1
            if self._remaining[page_index] > 0:
                return
            await self._advance()

    async def flush_empty_pages(self) -> None:
        """Страница без записей батчей не порождает, сдвинуть её надо вручную."""
        async with self._lock:
            await self._advance()

    async def _advance(self) -> None:
        moved = False
        while self._remaining.get(self._next_to_commit) == 0:
            cursor_after, records, is_final = self._info.pop(self._next_to_commit)
            del self._remaining[self._next_to_commit]
            self._state = self._state.model_copy(
                update={
                    "cursor": cursor_after,
                    "pages_done": self._state.pages_done + 1,
                    "records_done": self._state.records_done + records,
                    "finished": is_final,
                    "updated_at": datetime.now(UTC),
                }
            )
            self._next_to_commit += 1
            moved = True

        if moved:
            await self._store.save(self._state)


class Pipeline:
    def __init__(
        self,
        *,
        source: Source,
        state: StateStore,
        limiter: AdaptiveLimiter,
        handler: BatchHandler,
        retry_policy: RetryPolicy | None = None,
        queue_maxsize: int = 32,
        batch_size: int = 500,
        workers: int = 2,
    ) -> None:
        self._source = source
        self._state = state
        self._limiter = limiter
        self._handler = handler
        self._policy = retry_policy or RetryPolicy()
        self._queue_maxsize = queue_maxsize
        self._batch_size = batch_size
        self._workers = workers

    async def run(
        self,
        entity: str,
        *,
        max_pages: int | None = None,
        overall_timeout_s: float | None = None,
        from_scratch: bool = False,
        stop: asyncio.Event | None = None,
    ) -> IngestResult:
        """Прогнать загрузку одной сущности.

        Событие stop можно передать снаружи: на него вешается обработчик
        сигнала, и тогда SIGTERM приводит к мягкой остановке с сохранённой
        позицией, а не к обрыву. Без него создаётся внутреннее.
        """
        initial = CursorState.fresh(entity) if from_scratch else await self._state.load(entity)
        committer = OrderedCommitter(self._state, initial)

        queue: asyncio.Queue[Batch | object] = asyncio.Queue(maxsize=self._queue_maxsize)
        stop = stop if stop is not None else asyncio.Event()
        reason_box: list[str] = ["выдача закончилась"]

        producer = asyncio.create_task(
            self._produce(entity, queue, committer, stop, max_pages, reason_box),
            name=f"producer-{entity}",
        )
        consumers = [
            asyncio.create_task(self._consume(queue, committer), name=f"consumer-{entity}-{i}")
            for i in range(self._workers)
        ]

        try:
            try:
                if overall_timeout_s is None:
                    await producer
                else:
                    # Общий бюджет на весь прогон. Таймаут отдельного запроса
                    # задан в httpx и этот бюджет не заменяет: тысяча быстрых
                    # запросов уложится в лимит каждого и превысит общий.
                    async with asyncio.timeout(overall_timeout_s):
                        await producer
            except TimeoutError:
                reason_box[0] = "исчерпан общий бюджет времени"
                log.warning("pipeline.timeout", entity=entity, budget_s=overall_timeout_s)
        finally:
            # Корректная остановка защищена от повторной отмены. Если отменить
            # её на середине, батч, уже отданный обработчику, оборвётся,
            # а курсор останется указывать на необработанные данные.
            shutdown = asyncio.ensure_future(self._shutdown(stop, producer, queue, consumers))
            try:
                await asyncio.shield(shutdown)
            except asyncio.CancelledError:
                # Отмена пришла во время самой остановки. Дожидаемся её
                # завершения и только потом пропускаем отмену дальше.
                await shutdown
                raise

        final = committer.state
        result = IngestResult(
            entity=entity,
            pages=final.pages_done - initial.pages_done,
            records=final.records_done - initial.records_done,
            finished=final.finished,
            cursor=final.cursor,
            reason=reason_box[0],
        )
        log.info(
            "pipeline.done",
            entity=entity,
            pages=result.pages,
            records=result.records,
            finished=result.finished,
            reason=result.reason,
        )
        return result

    # --- producer ------------------------------------------------------------

    async def _produce(
        self,
        entity: str,
        queue: asyncio.Queue[Batch | object],
        committer: OrderedCommitter,
        stop: asyncio.Event,
        max_pages: int | None,
        reason_box: list[str],
    ) -> None:
        cursor = committer.state.cursor
        if committer.state.finished:
            reason_box[0] = "сущность уже загружена полностью"
            return

        seen: set[str] = set()
        page_index = 0
        pages_this_run = 0

        while True:
            if stop.is_set():
                reason_box[0] = "получен сигнал остановки"
                return
            if max_pages is not None and pages_this_run >= max_pages:
                reason_box[0] = f"достигнут предел в {max_pages} страниц"
                return

            async with self._limiter.slot():
                page = await run_with_retry(
                    partial(self._source.fetch_page, entity, cursor),
                    policy=self._policy,
                    feedback=self._limiter,
                    what=f"{entity}:{cursor}",
                )
            pages_this_run += 1

            next_cursor = page.next_cursor
            if next_cursor is not None and (next_cursor in seen or next_cursor == cursor):
                msg = f"курсор {next_cursor!r} повторился на странице {page_index}"
                raise CursorLoopError(msg)
            if next_cursor is not None:
                seen.add(next_cursor)

            is_final = page.kind is PageKind.END
            chunks = list(_chunked(page.items, self._batch_size))

            committer.plan(
                page_index,
                len(chunks),
                next_cursor,
                len(page.items),
                is_final=is_final,
            )

            if not chunks:
                # Пустая страница посреди выдачи. Записей нет, но курсор есть —
                # это не конец. Двигаем позицию и идём дальше.
                await committer.flush_empty_pages()
            else:
                last = len(chunks) - 1
                for i, chunk in enumerate(chunks):
                    await queue.put(
                        Batch(
                            entity=entity,
                            records=chunk,
                            cursor_after=next_cursor if i == last else None,
                            page_index=page_index,
                            is_final=is_final and i == last,
                        )
                    )

            page_index += 1
            if is_final:
                return
            cursor = next_cursor

    # --- consumers -----------------------------------------------------------

    async def _consume(
        self,
        queue: asyncio.Queue[Batch | object],
        committer: OrderedCommitter,
    ) -> None:
        while True:
            item = await queue.get()
            try:
                if item is _STOP:
                    return
                assert isinstance(item, Batch)
                await self._handle_protected(item, committer)
            finally:
                queue.task_done()

    async def _handle_protected(self, batch: Batch, committer: OrderedCommitter) -> None:
        """Обработать батч так, чтобы отмена не оборвала его на середине.

        asyncio.shield защищает вложенную задачу от отмены снаружи, но само
        ожидание при этом обрывается сразу. Поэтому после отмены задачу нужно
        дождаться явно — иначе выйдем раньше, чем она допишет, и курсор
        сдвинется на необработанные данные.

        Защищается только один батч. Щит на весь конвейер сделал бы остановку
        невозможной: процесс отказывался бы завершаться до конца всей выдачи.
        """
        task = asyncio.ensure_future(self._handle(batch, committer))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _handle(self, batch: Batch, committer: OrderedCommitter) -> None:
        await self._handler(batch)
        await committer.batch_done(batch.page_index)

    # --- остановка -----------------------------------------------------------

    async def _shutdown(
        self,
        stop: asyncio.Event,
        producer: asyncio.Task[None],
        queue: asyncio.Queue[Batch | object],
        consumers: list[asyncio.Task[None]],
    ) -> None:
        # Producer больше не начинает новых страниц, но текущую дописывает —
        # это и есть требование «дописать начатое и сохранить позицию».
        stop.set()
        with suppress(asyncio.CancelledError):
            await producer

        # Producer точно закончил, очередь больше не пополняется. Значит место
        # под маркеры освободится и put не зависнет навсегда.
        for _ in consumers:
            await queue.put(_STOP)

        await asyncio.gather(*consumers)
