"""Запись фактов в ClickHouse и идемпотентная заливка по партициям.

Здесь два разных способа класть данные, и выбор между ними неслучаен.

**Прямая вставка (ClickHouseWriter).** Для инкрементальной синхронизации.
Изменившиеся записи приходят вперемешку из любых месяцев, и заранее неизвестно,
какие партиции они затронут. Повторная вставка тех же записей не ломает
результат: ReplacingMergeTree схлопнет их по ключу сортировки. Но схлопнет не
сразу, и до слияния запросы обязаны использовать FINAL или устойчивые к дублям
агрегаты.

**Подмена партиции (PartitionSwapLoader).** Для помесячного бэкфилла. Месяц
целиком собирается во временную таблицу, а затем `ALTER TABLE ... REPLACE
PARTITION` подменяет партицию одним неделимым действием. Повторный запуск за тот
же месяц даёт ровно тот же результат — данные не задваиваются даже физически, и
FINAL для них не нужен.

У второго способа есть жёсткое условие: партиция подменяется **целиком**.
Значит бэкфилл обязан выкачать месяц полностью. Залить половину и подменить —
значит потерять вторую половину.
"""

from __future__ import annotations

from clickhouse_connect.driver import AsyncClient

from procurement.ingest.models import Batch
from procurement.logging import get_logger
from procurement.storage.clickhouse.schema import EntitySpec, safe_identifier, safe_partition

log = get_logger(__name__)


class ClickHouseWriter:
    """Обработчик батчей: складывает записи в таблицу фактов."""

    def __init__(self, client: AsyncClient, spec: EntitySpec, *, table: str | None = None) -> None:
        self._client = client
        self._spec = spec
        self._table = safe_identifier(table or spec.table, what="имя таблицы")
        self._written = 0

    @property
    def written(self) -> int:
        return self._written

    @property
    def table(self) -> str:
        return self._table

    async def __call__(self, batch: Batch) -> None:
        if not batch.records:
            return

        rows = self._spec.to_rows(batch.records)
        await self._client.insert(
            self._table,
            rows,
            column_names=self._spec.column_names,
        )
        self._written += len(rows)
        log.debug(
            "clickhouse.inserted",
            table=self._table,
            rows=len(rows),
            page=batch.page_index,
        )


class PartitionSwapLoader:
    """Идемпотентная заливка: собрать во временную таблицу, подменить партиции.

    Порядок работы:

        loader = PartitionSwapLoader(client, spec)
        await loader.prepare()              # создать и очистить staging
        writer = loader.writer()            # отдать конвейеру как обработчик
        ...                                 # прогон загрузчика
        swapped = await loader.commit()     # подменить партиции в боевой таблице
        await loader.cleanup()
    """

    def __init__(self, client: AsyncClient, spec: EntitySpec, *, suffix: str) -> None:
        self._client = client
        self._spec = spec
        # Суффикс разводит одновременные бэкфиллы разных месяцев: у каждого своя
        # временная таблица, и они не топчут друг друга.
        self._staging = safe_identifier(
            f"{spec.table}_staging_{suffix}", what="имя временной таблицы"
        )

    @property
    def staging_table(self) -> str:
        return self._staging

    async def prepare(self) -> None:
        # CREATE TABLE ... AS повторяет структуру целиком, включая движок, ключ
        # партиционирования и ключ сортировки. Для REPLACE PARTITION это
        # обязательное условие: таблицы должны совпадать.
        await self._client.command(f"DROP TABLE IF EXISTS {self._staging}")
        await self._client.command(f"CREATE TABLE {self._staging} AS {self._spec.table}")
        log.info("loader.staging_ready", staging=self._staging)

    def writer(self) -> ClickHouseWriter:
        return ClickHouseWriter(self._client, self._spec, table=self._staging)

    async def touched_partitions(self) -> list[str]:
        # Имя таблицы здесь — значение в условии, а не идентификатор, поэтому
        # передаётся параметром запроса, а не подстановкой в текст.
        result = await self._client.query(
            "SELECT DISTINCT partition FROM system.parts "
            "WHERE active AND table = {staging:String} AND database = currentDatabase()",
            parameters={"staging": self._staging},
        )
        return [str(row[0]) for row in result.result_rows]

    async def commit(self) -> list[str]:
        """Подменить в боевой таблице все партиции, собранные в staging."""
        # Схлопываем ревизии внутри staging до подмены: в боевую таблицу должен
        # уехать уже чистый месяц, иначе идемпотентность будет только на словах.
        await self._client.command(f"OPTIMIZE TABLE {self._staging} FINAL")

        partitions = await self.touched_partitions()
        for partition in partitions:
            safe_partition(partition)
            await self._client.command(
                f"ALTER TABLE {self._spec.table} REPLACE PARTITION '{partition}' "
                f"FROM {self._staging}"
            )

        log.info("loader.partitions_swapped", table=self._spec.table, partitions=partitions)
        return partitions

    async def cleanup(self) -> None:
        await self._client.command(f"DROP TABLE IF EXISTS {self._staging}")
