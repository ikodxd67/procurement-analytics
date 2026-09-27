"""Таблицы PostgreSQL.

Что здесь лежит и чего здесь нет.

Здесь — состояние синхронизации, журнал запусков, справочники, пользователи и
их сохранённые фильтры. Всё это небольшое, часто меняется по одной строке и
требует честных транзакций и внешних ключей. Ровно то, для чего PostgreSQL и
сделан.

Здесь нет фактов — договоров и лотов. Их миллионы, они не меняются построчно, и
по ним считают агрегаты по большим диапазонам. Это работа ClickHouse.
Обоснование двух хранилищ целиком — в README.

Про пользователей отдельно: паролей и токенов в проекте нет и не будет.
Пользователь существует только чтобы владеть сохранёнными фильтрами. Хранить
учётные данные без настоящей задачи аутентификации — лишний риск на пустом
месте.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from procurement.storage.postgres.base import Base


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    STOPPED = "stopped"
    FAILED = "failed"


class SyncState(Base):
    """Позиция загрузки по каждой паре «источник, сущность».

    Переезд сюда из JSON-файла первого этапа. Интерфейс StateStore для этого и
    делался узким: конвейер подмены не заметит.
    """

    __tablename__ = "sync_state"

    source: Mapped[str] = mapped_column(String(32), primary_key=True)
    entity: Mapped[str] = mapped_column(String(32), primary_key=True)

    cursor: Mapped[str | None] = mapped_column(String(128))

    watermark: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    """Верхняя граница уже загруженных изменений.

    Курсор годится для первичной выкачки, но для инкрементальной синхронизации
    на этапе 3 он бесполезен: записи ревизируются, и договор, изменившийся
    вчера, лежит где-то в середине уже пройденной выдачи. Догонять его надо по
    дате изменения, а не по позиции в списке.
    """

    pages_done: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    records_done: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")
    finished: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class IngestRun(Base):
    """Журнал запусков загрузчика.

    Нужен, чтобы отвечать на вопросы «когда последний раз успешно грузили» и
    «почему вчерашний прогон дал вдвое меньше записей». Без журнала эти вопросы
    решаются чтением логов, а логи ротируются.
    """

    __tablename__ = "ingest_run"
    __table_args__ = (
        CheckConstraint(
            "status in ('running', 'succeeded', 'stopped', 'failed')",
            name="status_known",
        ),
        CheckConstraint("pages >= 0 and records >= 0", name="counters_non_negative"),
        Index("ix_ingest_run_entity_started", "entity", "started_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32))
    entity: Mapped[str] = mapped_column(String(32))

    # Строка с проверкой вместо типа-перечисления PostgreSQL. Перечисление
    # выглядит строже, но добавление значения в него — отдельная миграция с
    # ALTER TYPE, которая до недавнего времени не работала внутри транзакции.
    # Строка с CHECK даёт ту же гарантию и меняется обычным ALTER TABLE.
    status: Mapped[str] = mapped_column(String(16), default=RunStatus.RUNNING)

    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    pages: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    records: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")

    reason: Mapped[str | None] = mapped_column(String(128))
    error: Mapped[str | None] = mapped_column(Text)


class RefStatus(Base):
    """Справочник статусов: отдельно для лотов, отдельно для договоров.

    Источник отдаёт только числовой код (ref_lot_status_id, ref_contract_
    status_id). Расшифровка нужна и витринам, и API.
    """

    __tablename__ = "ref_status"

    domain: Mapped[str] = mapped_column(String(16), primary_key=True)
    code: Mapped[int] = mapped_column(Integer, primary_key=True)
    name_ru: Mapped[str] = mapped_column(String(256))
    name_kk: Mapped[str | None] = mapped_column(String(256))


class RefClassifier(Base):
    """Дерево товарного классификатора.

    Ссылка на саму себя — это и есть дерево. На четвёртом этапе по нему пойдёт
    рекурсивный CTE со свёрткой на любой уровень, поэтому важно, чтобы parent
    хранился явно, а не выводился из строения кода.
    """

    __tablename__ = "ref_classifier"
    __table_args__ = (
        CheckConstraint("level >= 0", name="level_non_negative"),
        CheckConstraint("code <> parent_code", name="no_self_parent"),
        Index("ix_ref_classifier_parent_code", "parent_code"),
    )

    code: Mapped[str] = mapped_column(String(32), primary_key=True)
    parent_code: Mapped[str | None] = mapped_column(
        String(32), ForeignKey("ref_classifier.code", ondelete="RESTRICT")
    )
    level: Mapped[int] = mapped_column(Integer)
    name_ru: Mapped[str] = mapped_column(String(512))
    name_kk: Mapped[str | None] = mapped_column(String(512))


class AppUser(Base):
    __tablename__ = "app_user"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String(320), unique=True)
    display_name: Mapped[str] = mapped_column(String(128))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    filters: Mapped[list[SavedFilter]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class SavedFilter(Base):
    """Сохранённый пользователем набор условий для витрины."""

    __tablename__ = "saved_filter"
    __table_args__ = (UniqueConstraint("user_id", "name", name="uq_saved_filter_user_id_name"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("app_user.id", ondelete="CASCADE")
    )
    name: Mapped[str] = mapped_column(String(128))

    # JSONB, а не JSON: он хранится в разобранном двоичном виде, поддерживает
    # операторы поиска и индексы GIN. Обычный JSON — это просто текст с
    # проверкой синтаксиса при записи.
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    user: Mapped[AppUser] = relationship(back_populates="filters")
