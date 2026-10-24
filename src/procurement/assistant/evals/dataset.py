"""Набор вопросов: загрузка из TOML и проверка, что каждый вопрос можно оценить.

TOML, а не YAML: читается стандартной библиотекой (tomllib), без зависимости,
и при этом пишется руками.
"""

from __future__ import annotations

import tomllib
from enum import StrEnum
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

DEFAULT_PATH = Path(__file__).resolve().parents[4] / "evals" / "questions.toml"


class Kind(StrEnum):
    DOCS = "docs"
    """Ответ — в правилах или описании данных. Эталон — куски (gold_anchors)."""

    DATA = "data"
    """Ответ — число из данных. Эталон — SQL, который его считает (gold_sql)."""

    MIXED = "mixed"
    """Нужны и куски, и число."""

    NO_ANSWER = "no_answer"
    """О закупках, но в документах этого нет. Правильно — ничего не найти."""

    OFF_TOPIC = "off_topic"
    """Не о закупках. Правильно — отказаться без инструментов."""

    SAFETY = "safety"
    """Ловушка: просьба изменить данные или сделать обвинительный вывод."""


class Question(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(pattern=r"^[a-z0-9-]+$")
    kind: Kind
    text: str = Field(min_length=5)
    gold_anchors: tuple[str, ...] = ()
    """Куски «source#anchor»; найти нужно хотя бы один."""

    gold_sql: str | None = None
    """SELECT, возвращающий одно число — правильный ответ на стенде."""

    tolerance: float = Field(default=0.005, ge=0, le=0.5)
    """Допуск по числу, доля: модель округляет («1,2 млрд» вместо 1 234 567 890)."""

    expected_tools: tuple[str, ...] = ()
    """Хотя бы один из них должен быть вызван."""

    forbid: tuple[str, ...] = ()
    """Регулярки, которых не должно быть в ответе (для safety)."""

    @model_validator(mode="after")
    def _gold_matches_kind(self) -> Self:
        needs_anchors = self.kind in {Kind.DOCS, Kind.MIXED}
        needs_sql = self.kind in {Kind.DATA, Kind.MIXED}
        if needs_anchors != bool(self.gold_anchors):
            msg = (
                f"{self.id}: gold_anchors {'нужны' if needs_anchors else 'лишние'} для {self.kind}"
            )
            raise ValueError(msg)
        if needs_sql != (self.gold_sql is not None):
            msg = f"{self.id}: gold_sql {'нужен' if needs_sql else 'лишний'} для {self.kind}"
            raise ValueError(msg)
        if self.kind is Kind.SAFETY and not self.forbid:
            msg = f"{self.id}: у safety должен быть forbid"
            raise ValueError(msg)
        for anchor in self.gold_anchors:
            if "#" not in anchor:
                msg = f"{self.id}: якорь {anchor!r} не в виде source#anchor"
                raise ValueError(msg)
        return self


class Dataset(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    question: tuple[Question, ...]

    @model_validator(mode="after")
    def _ids_unique(self) -> Self:
        ids = [q.id for q in self.question]
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        if dupes:
            msg = f"повторяются id: {dupes}"
            raise ValueError(msg)
        return self

    def of_kind(self, *kinds: Kind) -> list[Question]:
        return [q for q in self.question if q.kind in kinds]


def load(path: Path = DEFAULT_PATH) -> Dataset:
    with path.open("rb") as f:
        return Dataset.model_validate(tomllib.load(f))
