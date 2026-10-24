"""Задать вопрос ассистенту из командной строки.

    python -m procurement.assistant.cli "Топ-5 заказчиков по концентрации за 2025 год"

Нужны: ключ и модель (PA_ASSISTANT__LLM__KIND=anthropic, API_KEY), поднятые
базы (make up), пользователь агента (make assistant-user) и индекс документов
(make docs-index). Печатает ответ, какие инструменты звались и сколько токенов
ушло — по последнему видно, во что обходится вопрос.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from procurement.assistant.agent import AgentAnswer
from procurement.assistant.stack import open_assistant
from procurement.config import get_settings
from procurement.logging import configure_logging


async def ask(question: str) -> AgentAnswer:
    async with open_assistant(get_settings()) as assistant:
        return await assistant.agent.ask(question)


def render(answer: AgentAnswer) -> str:
    lines = [answer.text, "", f"— остановка: {answer.stop.value}, шагов модели: {answer.steps}"]
    if answer.route is not None:
        lines.append(f"— маршрут: {answer.route.kind.value} ({answer.route.reason})")
    for call in answer.tool_calls:
        mark = "ошибка" if call.is_error else "ок"
        lines.append(f"— {call.name} [{mark}, {call.duration_ms} мс] {call.input}")
    lines.append(f"— токены: вход {answer.input_tokens}, выход {answer.output_tokens}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="procurement-assistant", description=__doc__)
    parser.add_argument("question")
    parser.add_argument("--pretty-logs", action="store_true")
    args = parser.parse_args(argv)
    configure_logging(get_settings().log_level, pretty=args.pretty_logs)
    print(render(asyncio.run(ask(args.question))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
