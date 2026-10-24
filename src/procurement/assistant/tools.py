"""Инструменты агента: описание для модели, проверка аргументов, выполнение.

Модель просит вызвать инструмент (``ToolUse``), ``ToolBox`` выполняет и
возвращает ``ToolResult``. Главное правило: **ошибку инструмента отдаём модели
текстом, а не роняем агента**. Неверный БИН, пустой период, кривой SQL —
модель прочтёт ошибку и исправит вызов. Падаем только на ошибках нашего кода:
их модель исправить не может, и прятать их нельзя.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ValidationError

from procurement.api.errors import ApiError
from procurement.assistant.models import ToolResult, ToolSpec, ToolUse
from procurement.logging import get_logger

log = get_logger(__name__)

# Сколько символов результата отдаём модели. Каждый символ — токены и деньги,
# а таблица на тысячу строк модели не нужна: ей нужен ответ на вопрос. Если
# результат длиннее, модель увидит пометку и уточнит запрос.
MAX_RESULT_CHARS = 8000


class ToolError(Exception):
    """Ошибка, которую модель может исправить сама: текст уйдёт ей как есть."""


@dataclass(frozen=True, slots=True)
class Executed:
    """Итог вызова: что уйдёт модели и что вернул инструмент до перевода в текст.

    ``value`` нужен тем, кто читает журнал агента программно (ручка /ask берёт
    из него источники): разбирать обратно JSON, который к тому же мог быть
    обрезан по длине, — хрупко. У ошибки ``value`` — None.
    """

    result: ToolResult
    value: Any = None


@dataclass(frozen=True)
class Tool[ArgsT: BaseModel]:
    name: str
    description: str
    args: type[ArgsT]
    run: Callable[[ArgsT], Awaitable[Any]]

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name=self.name,
            description=self.description,
            input_schema=self.args.model_json_schema(),
        )


class ToolBox:
    def __init__(self, tools: Iterable[Tool[Any]]) -> None:
        self._tools: dict[str, Tool[Any]] = {}
        for tool in tools:
            if tool.name in self._tools:
                msg = f"инструмент {tool.name!r} объявлен дважды"
                raise ValueError(msg)
            self._tools[tool.name] = tool

    @property
    def specs(self) -> list[ToolSpec]:
        # Порядок постоянный: список инструментов входит в начало запроса, и
        # от его стабильности зависит кэш промпта у провайдера.
        return [self._tools[name].spec for name in sorted(self._tools)]

    async def execute(self, use: ToolUse) -> ToolResult:
        return (await self.run(use)).result

    async def run(self, use: ToolUse) -> Executed:
        tool = self._tools.get(use.name)
        if tool is None:
            return _error(
                use, f"инструмента {use.name!r} нет; есть: {', '.join(sorted(self._tools))}"
            )

        try:
            args = tool.args.model_validate(use.input)
        except ValidationError as exc:
            return _error(use, "аргументы не прошли проверку: " + _describe(exc))

        try:
            value = await tool.run(args)
        except (ToolError, ApiError) as exc:
            # ApiError — те же ошибки, что API отдаёт клиенту: неизвестный БИН,
            # перевёрнутый период, недоступное хранилище. Текст у них уже
            # безопасный, без внутренностей.
            log.info("tool.error", tool=use.name, error=str(exc)[:300])
            return _error(use, str(exc))

        content = to_tool_content(value)
        log.info("tool.ok", tool=use.name, chars=len(content))
        return Executed(ToolResult(tool_use_id=use.id, content=content), value)


def to_tool_content(value: Any) -> str:
    """Результат инструмента -> JSON-текст для модели, с обрезкой по длине."""
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    elif isinstance(value, list):
        value = [v.model_dump(mode="json") if isinstance(v, BaseModel) else v for v in value]
    # default=str: Decimal и даты, которые пришли не через pydantic. Деньги
    # строкой, а не float — иначе модель увидит 1234.5600000001.
    text = json.dumps(value, ensure_ascii=False, default=str)
    if len(text) > MAX_RESULT_CHARS:
        cut = len(text) - MAX_RESULT_CHARS
        text = (
            text[:MAX_RESULT_CHARS]
            + f"\n…[обрезано {cut} символов: сузь запрос — меньше строк или период короче]"
        )
    return text


def _error(use: ToolUse, message: str) -> Executed:
    return Executed(ToolResult(tool_use_id=use.id, content=message, is_error=True))


def _describe(exc: ValidationError) -> str:
    # Коротко и по делу: поле и что не так. Полный текст pydantic с ссылками
    # на документацию модели не нужен и стоит токенов.
    parts = []
    for err in exc.errors(include_url=False):
        where = ".".join(str(p) for p in err["loc"]) or "аргументы"
        parts.append(f"{where}: {err['msg']}")
    return "; ".join(parts)
