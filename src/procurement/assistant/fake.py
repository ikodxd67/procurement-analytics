"""Поддельная модель для тестов.

Отвечает по сценарию: заранее заданный список ответов (или исключений)
выдаётся по одному на каждый вызов. Все запросы записываются, чтобы тест мог
проверить, что именно ушло в модель: промпт, инструменты, историю диалога.

Зачем это нужно, а не мок: тесты агента пойдут без ключа, без сети и без денег,
и при этом каждый ответ проходит ту же проверку ``LLMResponse``, что и ответ
настоящей модели. Это тот же приём, что ``FixtureSource`` у загрузчика.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from procurement.assistant.models import (
    LLMResponse,
    Message,
    StopReason,
    TextBlock,
    ToolSpec,
    ToolUse,
)


class FakeScriptError(AssertionError):
    """Сценарий не сходится с тем, что делает код: вызовов больше, чем ответов,
    или ответ зовёт инструмент, которого в запросе не было.

    Наследник AssertionError, а не LLMError: это ошибка теста или кода, а не
    модели, и обработчики ошибок модели не должны её проглотить.
    """


@dataclass(frozen=True)
class FakeCall:
    messages: list[Message]
    system: str | None
    tools: list[ToolSpec]
    max_tokens: int | None


class FakeLLM:
    def __init__(self, script: Iterable[LLMResponse | Exception]) -> None:
        self._script: deque[LLMResponse | Exception] = deque(script)
        self.calls: list[FakeCall] = []
        self.closed = False

    @staticmethod
    def text(text: str) -> LLMResponse:
        return LLMResponse(content=[TextBlock(text=text)], stop_reason=StopReason.END_TURN)

    @staticmethod
    def tool_call(name: str, input_: dict[str, object], *, id_: str = "call_1") -> LLMResponse:
        return LLMResponse(
            content=[ToolUse(id=id_, name=name, input=dict(input_))],
            stop_reason=StopReason.TOOL_USE,
        )

    @property
    def remaining(self) -> int:
        return len(self._script)

    async def complete(
        self,
        messages: Sequence[Message],
        *,
        system: str | None = None,
        tools: Sequence[ToolSpec] = (),
        max_tokens: int | None = None,
    ) -> LLMResponse:
        # Копия, а не ссылка: агент дописывает в свой список новые сообщения,
        # и без копии запись прошлого вызова тихо изменилась бы задним числом.
        self.calls.append(
            FakeCall(
                messages=[m.model_copy(deep=True) for m in messages],
                system=system,
                tools=list(tools),
                max_tokens=max_tokens,
            )
        )
        if not self._script:
            msg = f"сценарий кончился, это вызов №{len(self.calls)}"
            raise FakeScriptError(msg)

        step = self._script.popleft()
        if isinstance(step, Exception):
            raise step

        # Настоящая модель не может позвать инструмент, которого ей не дали.
        # Если сценарий так делает — тест проверяет невозможную ситуацию.
        offered = {t.name for t in tools}
        unknown = [u.name for u in step.tool_uses if u.name not in offered]
        if unknown:
            msg = f"сценарий зовёт инструменты, которых нет в запросе: {unknown}"
            raise FakeScriptError(msg)
        return step

    async def aclose(self) -> None:
        self.closed = True
