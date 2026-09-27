"""Наполнение справочника классификатора в обеих базах.

Дерево синтетическое: настоящий ЕНС ТРУ отдаётся API только по токену, а он
ещё не выдан. Форма повторяет реальную — четыре уровня, код собирается из
уровней через точку (07.3.2.041), — и этого достаточно, чтобы рекурсивный
обход был честным.

Справочник кладётся в обе базы намеренно. В PostgreSQL он нужен как источник
истины с внешними ключами. В ClickHouse — как копия рядом с фактами: иначе
соединение фактов со справочником означало бы запрос между двумя базами, чего
ClickHouse не умеет, а тянуть миллионы строк в приложение ради соединения
бессмысленно.

Запуск:
    .venv/Scripts/python scripts/seed_reference.py
"""

from __future__ import annotations

import asyncio

import clickhouse_connect
from sqlalchemy import delete, text
from sqlalchemy.dialects.postgresql import insert

from procurement.config import get_settings
from procurement.ingest.synthetic_source import (
    CLASSIFIER_GROUPS,
    CLASSIFIER_ITEMS,
    CLASSIFIER_SECTIONS,
    CLASSIFIER_SUBGROUPS,
)
from procurement.storage.postgres.engine import build_engine, build_session_factory, session_scope
from procurement.storage.postgres.models import RefClassifier, RefStatus

SECTION_NAMES = (
    "Продукция сельского хозяйства",
    "Продукция горнодобывающей промышленности",
    "Продукты пищевые",
    "Текстиль и одежда",
    "Древесина и изделия из неё",
    "Бумага и бумажные изделия",
    "Кокс и нефтепродукты",
    "Вещества химические",
    "Продукты фармацевтические",
    "Изделия резиновые и пластмассовые",
    "Продукция минеральная неметаллическая",
    "Металлы основные",
    "Изделия металлические готовые",
    "Оборудование компьютерное и электронное",
    "Оборудование электрическое",
    "Машины и оборудование",
    "Средства автотранспортные",
    "Мебель",
    "Работы строительные",
    "Услуги профессиональные",
)

CONTRACT_STATUSES = {
    200: "Проект договора",
    210: "Подписан заказчиком",
    220: "Подписан поставщиком",
    230: "Исполнен",
}

LOT_STATUSES = {
    210: "Опубликован",
    220: "Приём заявок завершён",
    230: "Итоги подведены",
    240: "Не состоялся",
}


def build_tree() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for section in range(1, CLASSIFIER_SECTIONS + 1):
        section_code = f"{section:02d}"
        rows.append(
            {
                "code": section_code,
                "parent_code": None,
                "level": 0,
                "name_ru": SECTION_NAMES[section - 1],
                "name_kk": None,
            }
        )
        for group in range(1, CLASSIFIER_GROUPS + 1):
            group_code = f"{section_code}.{group}"
            rows.append(
                {
                    "code": group_code,
                    "parent_code": section_code,
                    "level": 1,
                    "name_ru": f"{SECTION_NAMES[section - 1]}: группа {group}",
                    "name_kk": None,
                }
            )
            for subgroup in range(1, CLASSIFIER_SUBGROUPS + 1):
                subgroup_code = f"{group_code}.{subgroup}"
                rows.append(
                    {
                        "code": subgroup_code,
                        "parent_code": group_code,
                        "level": 2,
                        "name_ru": f"Подгруппа {subgroup_code}",
                        "name_kk": None,
                    }
                )
                for item in range(1, CLASSIFIER_ITEMS + 1):
                    rows.append(
                        {
                            "code": f"{subgroup_code}.{item:03d}",
                            "parent_code": subgroup_code,
                            "level": 3,
                            "name_ru": f"Позиция {subgroup_code}.{item:03d}",
                            "name_kk": None,
                        }
                    )
    return rows


async def seed_postgres(rows: list[dict[str, object]]) -> None:
    settings = get_settings()
    engine = build_engine(settings.postgres)
    sessions = build_session_factory(engine)

    async with session_scope(sessions) as session:
        # Порядок важен: внешний ключ ссылается на эту же таблицу, поэтому
        # сначала снимаем ограничение на время вставки, иначе пришлось бы
        # вставлять строго по уровням.
        await session.execute(delete(RefClassifier))
        await session.execute(text("ALTER TABLE ref_classifier DISABLE TRIGGER ALL"))
        await session.execute(insert(RefClassifier), rows)
        await session.execute(text("ALTER TABLE ref_classifier ENABLE TRIGGER ALL"))

        await session.execute(delete(RefStatus))
        statuses = [
            {"domain": "contract", "code": code, "name_ru": name, "name_kk": None}
            for code, name in CONTRACT_STATUSES.items()
        ] + [
            {"domain": "lot", "code": code, "name_ru": name, "name_kk": None}
            for code, name in LOT_STATUSES.items()
        ]
        await session.execute(insert(RefStatus), statuses)

    await engine.dispose()
    print(f"PostgreSQL: {len(rows)} узлов классификатора, {len(statuses)} статусов")


def seed_clickhouse(rows: list[dict[str, object]]) -> None:
    settings = get_settings()
    client = clickhouse_connect.get_client(
        host=settings.clickhouse.host,
        port=settings.clickhouse.port,
        database=settings.clickhouse.database,
        username=settings.clickhouse.user,
        password=settings.clickhouse.password.get_secret_value(),
    )
    client.command("DROP TABLE IF EXISTS ref_classifier")
    client.command("""
        CREATE TABLE ref_classifier
        (
            code        String,
            parent_code String,
            level       UInt8,
            name_ru     String
        )
        ENGINE = MergeTree ORDER BY code
    """)
    client.insert(
        "ref_classifier",
        [[r["code"], r["parent_code"] or "", r["level"], r["name_ru"]] for r in rows],
        column_names=["code", "parent_code", "level", "name_ru"],
    )
    total = client.query("SELECT count() FROM ref_classifier").result_rows[0][0]
    print(f"ClickHouse: {total} узлов классификатора")


def main() -> None:
    rows = build_tree()
    leaves = sum(1 for r in rows if r["level"] == 3)
    print(f"Дерево: {len(rows)} узлов, из них листьев {leaves}")
    asyncio.run(seed_postgres(rows))
    seed_clickhouse(rows)


if __name__ == "__main__":
    main()
