"""Запуск evals из командной строки.

    python -m procurement.assistant.evals.cli snapshot      # обновить снимок правил с adilet
    python -m procurement.assistant.evals.cli retrieval     # поиск: hit@k, MRR, порог (без LLM)
    python -m procurement.assistant.evals.cli gold          # эталонный SQL на стенде (без LLM)
    python -m procurement.assistant.evals.cli agent --yes   # агент целиком (платно)

Отчёты пишутся в evals/reports/, сырые ответы агента — в evals/runs/ (не в git).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx

from procurement.assistant.evals.dataset import Kind, load
from procurement.assistant.evals.gold import gold_value
from procurement.assistant.evals.retrieval import evaluate_retrieval, index_corpus
from procurement.assistant.evals.runner import gold_refs, render, run_questions
from procurement.assistant.evals.snapshot import SNAPSHOT_DIR, read_snapshot, write_snapshot
from procurement.assistant.rag.adilet import fetch_document
from procurement.assistant.rag.dictionary import SOURCE, chunk_dictionary, load_text
from procurement.assistant.rag.embeddings import build_embedder
from procurement.assistant.stack import open_assistant
from procurement.assistant.toolbox import build_readonly_client
from procurement.config import get_settings
from procurement.logging import configure_logging
from procurement.storage.postgres.engine import build_engine, build_session_factory
from procurement.storage.postgres.models import EMBEDDING_DIM

REPORTS = SNAPSHOT_DIR.parent / "reports"
RUNS = SNAPSHOT_DIR.parent / "runs"
NGR = "V2400035238"


def _stamp() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d-%H%M")


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


async def cmd_snapshot() -> int:
    settings = get_settings()
    async with httpx.AsyncClient(timeout=settings.http.request_timeout_s) as client:
        document = await fetch_document(client, settings.assistant.rag.adilet_api_url, NGR)
    path = write_snapshot(document)
    print(f"снимок {path.name}: редакция {document.version}. Перепроверь якоря: make test-unit")
    return 0


async def cmd_retrieval() -> int:
    settings = get_settings()
    embedder = build_embedder(settings.assistant.rag, dim=EMBEDDING_DIM)
    version, rules = read_snapshot(NGR)
    dictionary = load_text()
    engine = build_engine(settings.postgres)
    try:
        sessions = build_session_factory(engine)
        await index_corpus(
            sessions,
            embedder,
            [(f"adilet:{NGR}", version, rules), (SOURCE, "eval", chunk_dictionary(dictionary))],
        )
        report = await evaluate_retrieval(sessions, embedder, load())
    finally:
        await engine.dispose()
    path = _write(REPORTS / f"retrieval-{_stamp()}.md", report.render())
    print(report.render())
    print(f"отчёт: {path}")
    return 0


async def cmd_gold() -> int:
    settings = get_settings()
    readonly = await build_readonly_client(settings)
    try:
        for q in load().of_kind(Kind.DATA, Kind.MIXED):
            print(f"{q.id}: {await gold_value(readonly, q)}")
    finally:
        await readonly.close()
    return 0


async def cmd_agent(limit: int | None, kinds: list[str] | None) -> int:
    settings = get_settings()
    questions = list(load().question)
    if kinds:
        questions = [q for q in questions if q.kind.value in kinds]
    questions = questions[:limit] if limit else questions

    _, rules = read_snapshot(NGR)
    refs = gold_refs([*rules, *chunk_dictionary(load_text())])
    async with open_assistant(settings) as assistant:
        rows = await run_questions(
            assistant.agent, questions, readonly=assistant.readonly, refs=refs
        )

    stamp = _stamp()
    _write(
        RUNS / f"agent-{stamp}.jsonl",
        "".join(json.dumps(r.as_dict(), ensure_ascii=False) + "\n" for r in rows),
    )
    text = render(rows, settings.assistant.llm.model)
    path = _write(REPORTS / f"agent-{stamp}.md", text)
    print(text)
    print(f"отчёт: {path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="procurement-evals", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("snapshot")
    sub.add_parser("retrieval")
    sub.add_parser("gold")
    agent = sub.add_parser("agent")
    agent.add_argument("--yes", action="store_true", help="подтвердить платный прогон")
    agent.add_argument("--limit", type=int, default=None)
    agent.add_argument("--kind", action="append", choices=[k.value for k in Kind])
    args = parser.parse_args(argv)
    configure_logging(get_settings().log_level)

    if args.command == "snapshot":
        return asyncio.run(cmd_snapshot())
    if args.command == "retrieval":
        return asyncio.run(cmd_retrieval())
    if args.command == "gold":
        return asyncio.run(cmd_gold())
    if not args.yes:
        print("прогон агента платный (вызовы модели); запусти с --yes")
        return 2
    return asyncio.run(cmd_agent(args.limit, args.kind))


if __name__ == "__main__":
    sys.exit(main())
