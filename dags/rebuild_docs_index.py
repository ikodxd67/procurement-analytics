"""DAG пересборки индекса документов ассистента.

Раз в неделю: правила госзакупок на adilet правятся приказами по нескольку раз
в год, а описание данных меняется вместе с кодом. Чаще — пустая работа.

Логики здесь нет: задача вызывает rebuild_docs_index, ту же функцию, что и
`python -m procurement.jobs.cli docs-index`. Повторный запуск безопасен:
неизменённые пункты не пересчитываются, удалённые из редакции — удаляются,
всё за одну транзакцию на источник.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import timedelta
from typing import Any

import pendulum
from airflow.sdk import dag, task

from procurement.jobs.context import job_context
from procurement.jobs.docs_index import rebuild_docs_index

DEFAULT_ARGS = {
    # adilet иногда отвечает медленно — повторить позже, а не сразу.
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}


@dag(
    dag_id="rebuild_docs_index",
    description="Пересборка индекса документов ассистента (правила и описание данных)",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # Первый запуск качает модель (~1.1 ГБ) и считает ~700 эмбеддингов на
    # процессоре (локально 2 мин загрузка + 89 с счёт), отсюда запас.
    dagrun_timeout=timedelta(hours=1),
    default_args=DEFAULT_ARGS,
    tags=["procurement", "assistant", "rag"],
    doc_md=__doc__,
)
def rebuild_docs_index_dag() -> None:
    @task
    def rebuild(**_: Any) -> list[dict[str, Any]]:
        async def run() -> list[dict[str, Any]]:
            async with job_context(ensure_schema=False) as context:
                results = await rebuild_docs_index(context)
            return [asdict(r) for r in results]

        return asyncio.run(run())

    rebuild()


rebuild_docs_index_dag()
