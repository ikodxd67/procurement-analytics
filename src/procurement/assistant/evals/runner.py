"""Прогон агента по набору вопросов: ответ, оценка, токены, цена, время.

Это единственная часть evals, которой нужна языковая модель, — и она платная.
Поэтому её запускают руками (``make eval-agent``), а в CI идут дешёвые уровни:
поиск и эталонный SQL.
"""

from __future__ import annotations

import re
import time
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass

from clickhouse_connect.driver import AsyncClient

from procurement.assistant.agent import Agent
from procurement.assistant.evals.dataset import Kind, Question
from procurement.assistant.evals.gold import gold_value
from procurement.assistant.evals.grading import grade
from procurement.assistant.pricing import cost_usd
from procurement.assistant.rag.chunks import DocChunk

_POINT = re.compile(r"п\. ([\d-]+)")


def gold_refs(chunks: Iterable[DocChunk]) -> dict[str, tuple[str, str]]:
    """«source#anchor» → (url, номер пункта) для оценщика ссылок."""
    refs: dict[str, tuple[str, str]] = {}
    for c in chunks:
        match = _POINT.search(c.title.rsplit("›", 1)[-1])
        refs[f"{c.source}#{c.anchor}"] = (c.url, match.group(1) if match else "")
    return refs


@dataclass
class RunRow:
    question_id: str
    kind: str
    passed: bool
    checks: dict[str, bool]
    note: str
    stop: str
    steps: int
    tools: list[str]
    input_tokens: int
    output_tokens: int
    seconds: float
    gold: float | None
    answer: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


async def run_questions(
    agent: Agent,
    questions: Sequence[Question],
    *,
    readonly: AsyncClient | None,
    refs: dict[str, tuple[str, str]],
) -> list[RunRow]:
    rows: list[RunRow] = []
    for q in questions:
        gold = await gold_value(readonly, q) if q.gold_sql and readonly is not None else None
        started = time.perf_counter()
        answer = await agent.ask(q.text)
        seconds = time.perf_counter() - started
        g = grade(q, answer, gold_value=gold, gold_refs=refs)
        rows.append(
            RunRow(
                question_id=q.id,
                kind=q.kind.value,
                passed=g.passed,
                checks=g.checks,
                note=g.note,
                stop=answer.stop.value,
                steps=answer.steps,
                tools=[c.name for c in answer.tool_calls],
                input_tokens=answer.input_tokens,
                output_tokens=answer.output_tokens,
                seconds=round(seconds, 2),
                gold=gold,
                answer=answer.text,
            )
        )
    return rows


def render(rows: Sequence[RunRow], model: str) -> str:
    by_kind: dict[str, list[RunRow]] = defaultdict(list)
    for r in rows:
        by_kind[r.kind].append(r)

    total_in = sum(r.input_tokens for r in rows)
    total_out = sum(r.output_tokens for r in rows)
    cost = cost_usd(model, total_in, total_out)
    passed = sum(r.passed for r in rows)
    lines = [
        f"# Агент: {model}",
        "",
        f"Прошло {passed} из {len(rows)}. Токены: вход {total_in}, выход {total_out}. "
        + (
            f"Цена прогона ${cost:.4f}, на вопрос ${cost / len(rows):.5f}."
            if cost and rows
            else "Цена: модель не в таблице цен."
        ),
        "",
        "| Вид | Прошло | Шагов в среднем | Секунд в среднем |",
        "|---|---|---|---|",
    ]
    for kind in Kind:
        group = by_kind.get(kind.value, [])
        if group:
            lines.append(
                f"| {kind.value} | {sum(r.passed for r in group)}/{len(group)} | "
                f"{sum(r.steps for r in group) / len(group):.1f} | "
                f"{sum(r.seconds for r in group) / len(group):.1f} |"
            )
    failed = [r for r in rows if not r.passed]
    lines += ["", f"## Не прошли: {len(failed)}", ""]
    for r in failed:
        lines.append(f"- `{r.question_id}` ({r.stop}, инструменты {r.tools}): {r.note}")
    return "\n".join(lines) + "\n"
