"""Точка входа загрузчика.

    python -m procurement.ingest.cli --entity contracts --max-pages 3

По умолчанию источник — фикстуры на диске. Чтобы пойти в настоящий API, нужен
токен и PA_SOURCE__KIND=rest.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from pathlib import Path

import orjson

from procurement.config import Settings, SourceKind, get_settings
from procurement.ingest.fixture_source import FixtureSource
from procurement.ingest.limiter import AdaptiveLimiter
from procurement.ingest.models import Batch
from procurement.ingest.pipeline import Pipeline
from procurement.ingest.rest_source import RestSource
from procurement.ingest.retry import RetryPolicy
from procurement.ingest.source import ENTITY_PATHS, Source
from procurement.ingest.state import FileStateStore, StateStore
from procurement.logging import configure_logging, get_logger
from procurement.storage.postgres.engine import build_engine, build_session_factory
from procurement.storage.postgres.state_store import PostgresStateStore

log = get_logger(__name__)

# Коды возврата различаются намеренно: по ним внешний оркестратор отличает
# «остановили штатно» от «упало».
EXIT_OK = 0
EXIT_FAILED = 1
EXIT_STOPPED = 130


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="procurement-ingest", description=__doc__)
    parser.add_argument("--entity", required=True, choices=sorted(ENTITY_PATHS))
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help=(
            "остановиться после стольких страниц. На публичный сервис без этого "
            "ограничителя лучше не ходить"
        ),
    )
    parser.add_argument(
        "--from-scratch",
        action="store_true",
        help="игнорировать сохранённый курсор и начать с начала",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="страницы качать, записи считать, но никуда не писать",
    )
    parser.add_argument("--out", type=Path, default=None, help="писать записи в JSONL-файл")
    parser.add_argument(
        "--state-backend",
        choices=("file", "postgres"),
        default="file",
        help="где хранить позицию загрузки",
    )
    parser.add_argument(
        "--state",
        type=Path,
        default=Path("data/state.json"),
        help="файл состояния, если выбран backend file",
    )
    parser.add_argument("--timeout", type=float, default=None, help="общий бюджет в секундах")
    parser.add_argument("--pretty-logs", action="store_true", help="логи для человека, не JSON")
    return parser


def build_source(settings: Settings) -> Source:
    if settings.source.kind is SourceKind.FIXTURES:
        return FixtureSource(Path(settings.source.fixtures_dir))
    return RestSource(
        base_url=settings.source.base_url,
        token=settings.source.token.get_secret_value(),
        page_limit=settings.source.page_limit,
        request_timeout_s=settings.http.request_timeout_s,
        connect_timeout_s=settings.http.connect_timeout_s,
        user_agent=settings.http.user_agent,
    )


class JsonlWriter:
    """Обработчик батчей: дописывает записи в файл по одной на строку.

    Формат JSONL выбран не случайно: файл можно дописывать, не перечитывая, и
    он читается построчно без загрузки целиком в память. Для сырого слоя это
    важнее компактности.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._written = 0

    @property
    def written(self) -> int:
        return self._written

    async def __call__(self, batch: Batch) -> None:
        await asyncio.to_thread(self._append, batch)
        self._written += len(batch.records)

    def _append(self, batch: Batch) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("ab") as fh:
            for record in batch.records:
                fh.write(orjson.dumps(record))
                fh.write(b"\n")


class CountingSink:
    def __init__(self) -> None:
        self.records = 0

    async def __call__(self, batch: Batch) -> None:
        self.records += len(batch.records)


def install_stop_handlers(stop: asyncio.Event) -> None:
    """Повесить мягкую остановку на SIGTERM и SIGINT.

    На Linux правильный способ — loop.add_signal_handler. На Windows он не
    реализован (проверено 2026-09-27: ProactorEventLoop бросает
    NotImplementedError), поэтому там откатываемся на signal.signal, а внутрь
    обработчика ставим call_soon_threadsafe: обработчик сигнала выполняется вне
    событийного цикла, и трогать его объекты напрямую оттуда нельзя.
    """
    loop = asyncio.get_running_loop()
    for name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))


async def run(args: argparse.Namespace) -> int:
    settings = get_settings()
    configure_logging(settings.log_level, pretty=args.pretty_logs)

    if settings.source.kind is SourceKind.REST and not settings.source.token.get_secret_value():
        log.error("cli.no_token", hint="задайте PA_SOURCE__TOKEN или PA_SOURCE__KIND=fixtures")
        return EXIT_FAILED

    write_to_file = args.out is not None and not args.dry_run
    handler: JsonlWriter | CountingSink = JsonlWriter(args.out) if write_to_file else CountingSink()

    source = build_source(settings)
    limiter = AdaptiveLimiter(
        start=settings.http.concurrency_start,
        minimum=settings.http.concurrency_min,
        maximum=settings.http.concurrency_max,
    )
    # Подмена хранилища позиции ничего не требует от конвейера: он видит только
    # протокол StateStore из двух методов. Ради этого протокол и делался узким.
    state_store: StateStore
    engine = None
    if args.state_backend == "postgres":
        engine = build_engine(settings.postgres)
        state_store = PostgresStateStore(build_session_factory(engine))
    else:
        state_store = FileStateStore(args.state)

    pipeline = Pipeline(
        source=source,
        state=state_store,
        limiter=limiter,
        handler=handler,
        retry_policy=RetryPolicy(
            max_attempts=settings.http.retry_max_attempts,
            base_delay_s=settings.http.retry_base_delay_s,
            max_delay_s=settings.http.retry_max_delay_s,
        ),
        queue_maxsize=settings.pipeline.queue_maxsize,
        batch_size=settings.pipeline.batch_size,
        workers=settings.pipeline.parser_workers,
    )

    stop = asyncio.Event()
    install_stop_handlers(stop)

    try:
        async with limiter:
            result = await pipeline.run(
                args.entity,
                max_pages=args.max_pages,
                overall_timeout_s=args.timeout or settings.pipeline.overall_timeout_s,
                from_scratch=args.from_scratch,
                stop=stop,
            )
    finally:
        await source.aclose()
        if engine is not None:
            await engine.dispose()

    log.info(
        "cli.result",
        entity=result.entity,
        pages=result.pages,
        records=result.records,
        finished=result.finished,
        cursor=result.cursor,
        reason=result.reason,
    )
    return EXIT_OK if result.finished else EXIT_STOPPED


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return EXIT_STOPPED


if __name__ == "__main__":
    sys.exit(main())
