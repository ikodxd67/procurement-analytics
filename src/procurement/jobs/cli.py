"""Запуск заданий загрузки без оркестратора.

    python -m procurement.jobs.cli backfill --entity contracts --from 2023-01 --to 2025-12
    python -m procurement.jobs.cli incremental --entity contracts --month 2025-06

Airflow вызывает те же функции. Возможность прогнать задание руками — не
удобство, а условие отладки: иначе единственный способ проверить изменение
состоит в том, чтобы поднять планировщик и читать его логи.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import date

from procurement.config import get_settings
from procurement.ingest.synthetic_source import SyntheticSource
from procurement.jobs.backfill import backfill_month
from procurement.jobs.context import SYNTHETIC_RECORDS_PER_MONTH, job_context
from procurement.jobs.incremental import sync_incremental
from procurement.jobs.marts import refresh_monthly_mart
from procurement.logging import configure_logging, get_logger

log = get_logger(__name__)


def parse_month(value: str) -> date:
    try:
        year, month = value.split("-")
        return date(int(year), int(month), 1)
    except (ValueError, TypeError) as err:
        msg = f"месяц задаётся как ГГГГ-ММ, получено {value!r}"
        raise argparse.ArgumentTypeError(msg) from err


def months_between(start: date, end: date) -> list[date]:
    """Список первых чисел каждого месяца включительно.

    Первая версия считала следующий месяц через divmod и на декабре давала тот
    же месяц снова — цикл не заканчивался. Дымовой прогон январь-февраль этого
    не показал, поймалось только на трёхлетнем интервале. Отсюда явный перенос
    года и тест на границу декабря.
    """
    months: list[date] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append(date(year, month, 1))
        month += 1
        if month == 13:
            month = 1
            year += 1
    return months


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="procurement-jobs", description=__doc__)
    parser.add_argument("--pretty-logs", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    back = sub.add_parser("backfill", help="помесячная выкачка с подменой партиций")
    back.add_argument("--entity", default="contracts", choices=("contracts", "lots"))
    back.add_argument("--from", dest="start", type=parse_month, required=True)
    back.add_argument("--to", dest="end", type=parse_month, required=True)
    back.add_argument("--records", type=int, default=SYNTHETIC_RECORDS_PER_MONTH)

    inc = sub.add_parser("incremental", help="догрузка изменений по водяному знаку")
    inc.add_argument("--entity", default="contracts", choices=("contracts", "lots"))
    inc.add_argument("--month", type=parse_month, required=True)
    inc.add_argument("--records", type=int, default=2000)

    marts = sub.add_parser("marts", help="пересчитать витрины")
    marts.add_argument(
        "--month",
        type=parse_month,
        default=None,
        help="пересчитать один месяц вместо всех",
    )

    return parser


async def run_backfill(args: argparse.Namespace) -> int:
    months = months_between(args.start, args.end)
    print(f"Бэкфилл {args.entity}: {len(months)} месяцев, {args.start:%Y-%m} .. {args.end:%Y-%m}")

    def factory(entity: str, month: date):  # type: ignore[no-untyped-def]
        return SyntheticSource(month, records=args.records, page_size=500)

    started = time.perf_counter()
    total_records = 0
    failures = 0

    async with job_context(source_factory=factory) as context:
        for index, month in enumerate(months, start=1):
            try:
                result = await backfill_month(context, entity=args.entity, month=month)
            except Exception as error:
                failures += 1
                print(f"  [{index:>2}/{len(months)}] {month:%Y-%m}  ОШИБКА: {error}")
                continue
            total_records += result.records
            print(
                f"  [{index:>2}/{len(months)}] {month:%Y-%m}  "
                f"{result.records:>7} записей за {result.duration_s:5.1f} с  "
                f"({result.rows_per_second:>6.0f} строк/с)  партиции: {','.join(result.partitions)}"
            )

    elapsed = time.perf_counter() - started
    print()
    print(f"Итого: {total_records} записей за {elapsed:.1f} с")
    print(f"Средняя пропускная способность: {total_records / elapsed:.0f} строк/с")
    if failures:
        print(f"Месяцев с ошибкой: {failures}")
    return 1 if failures else 0


async def run_incremental(args: argparse.Namespace) -> int:
    async with job_context() as context:
        source = SyntheticSource(args.month, records=args.records, page_size=500)
        result = await sync_incremental(context, entity=args.entity, source=source)

    print(f"Получено:     {result.fetched}")
    print(f"Опубликовано: {result.published}")
    print(f"Пропущено как устаревшее: {result.skipped_as_old}")
    print(f"Водяной знак: {result.watermark_before} -> {result.watermark_after}")
    print()
    print(result.report.render())
    return 0


async def run_marts(args: argparse.Namespace) -> int:
    async with job_context() as context:
        result = await refresh_monthly_mart(context, month=args.month)

    print(f"Витрина {result.table}")
    print(f"  строк    : {result.rows}")
    print(f"  партиций : {len(result.months)}")
    print(f"  время    : {result.duration_s:.2f} с")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(get_settings().log_level, pretty=args.pretty_logs)

    if args.command == "backfill":
        return asyncio.run(run_backfill(args))
    if args.command == "marts":
        return asyncio.run(run_marts(args))
    return asyncio.run(run_incremental(args))


if __name__ == "__main__":
    sys.exit(main())
