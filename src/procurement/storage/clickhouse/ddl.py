"""Схема фактов в ClickHouse.

Три решения, каждое проверено замерами или документацией источника.

**Движок ReplacingMergeTree(last_update_date).** Записи ревизируются: у
договора меняется статус, сумма, сроки. Загрузчик получает новую версию и
вставляет её как новую строку — обновлять строку на месте ClickHouse не умеет и
не должен. Движок при слиянии кусков оставляет из строк с одинаковым ключом
сортировки ту, у которой больше значение версии. Версия — дата последнего
изменения, то есть ровно то, что делает запись более новой.

**Ключ сортировки выбран экспериментом**, а не на глаз. Замеры на 20 млн
записей — в docs/bench/order_by.md. Коротко: первая колонка ключа решает почти
всё, и `(customer_bin, supplier_biin, id)` выигрывает на запросах вида «кто
поставляет этому заказчику», которые и составляют основу четвёртого этапа.

Что в ключе быть **не может**: изменяемые поля. Если положить в ключ статус,
то при ревизии ключ станет другим, и движок сочтёт старую и новую строки
разными записями. Схлопывания не произойдёт, а данные тихо задвоятся.

**Колонка партиционирования обязана быть неизменной.** Физическое слияние
кусков в ReplacingMergeTree идёт только внутри партиции. Если партиционировать
по дате изменения, ревизия уедет в соседнюю партицию, и схлопнуть её с
оригиналом станет невозможно в принципе — никакой OPTIMIZE не поможет.
Проверено на ClickHouse 24.8 (см. tests/test_clickhouse_storage.py): ответ при
этом остаётся верным, потому что SELECT ... FINAL склеивает и через партиции,
но таблица растёт без предела и каждый запрос навсегда обязан платить за FINAL.

У договора неизменная дата есть — crdate, дата создания. У лота её нет: в схеме
API у него только lastUpdateDate и indexDate, обе меняются (проверено
2026-09-27 по схеме GraphQL). Поэтому лоты партиционируются по диапазону id —
он стабилен. Помесячное партиционирование лотов станет возможно на третьем
этапе, когда появится дата объявления из связанной сущности TrdBuy.
"""

from __future__ import annotations

from clickhouse_connect.driver import AsyncClient

CONTRACTS_DDL = """
CREATE TABLE IF NOT EXISTS contracts
(
    id                     UInt64,
    contract_number        String,
    trd_buy_number_anno    String,
    supplier_biin          String,
    customer_bin           String,
    contract_sum           Decimal(18, 2),
    contract_sum_wnds      Decimal(18, 2),
    fakt_sum               Decimal(18, 2),
    ref_contract_status_id UInt16,
    crdate                 DateTime,
    sign_date              DateTime,
    last_update_date       DateTime,
    _loaded_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(last_update_date)
PARTITION BY toYYYYMM(crdate)
ORDER BY (customer_bin, supplier_biin, id)
"""

LOTS_DDL = """
CREATE TABLE IF NOT EXISTS lots
(
    id                  UInt64,
    lot_number          String,
    ref_lot_status_id   UInt16,
    customer_bin        String,
    trd_buy_number_anno String,
    name_ru             String,
    -- Код товарной позиции по классификатору ЕНС ТРУ. В API у лота это список
    -- (enstruList), здесь берётся первый элемент — основная позиция. Упрощение
    -- сознательное: аналитике нужна одна позиция на лот, иначе любая сумма по
    -- категории задвоится на лотах с несколькими кодами.
    enstru_code         String,
    count               Decimal(18, 3),
    amount              Decimal(18, 2),
    last_update_date    DateTime,
    _loaded_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(last_update_date)
PARTITION BY intDiv(id, 5000000)
ORDER BY (customer_bin, id)
"""

# Суммы — Decimal, а не Float. Float64 хранит 0.1 приближённо, и сумма
# миллиона договоров разойдётся с бухгалтерией в последних разрядах. Для денег
# это недопустимо, а выигрыша в скорости на наших объёмах нет.

MART_CONTRACTS_MONTHLY_DDL = """
CREATE TABLE IF NOT EXISTS mart_contracts_monthly
(
    month        Date,
    customer_bin String,
    contracts    UInt64,
    total        Decimal(18, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (month, customer_bin)
"""

# Почему витрина, а не материализованное представление. Представление
# срабатывает на INSERT в исходную таблицу, а бэкфилл кладёт данные через
# ALTER TABLE ... REPLACE PARTITION — это не INSERT, и представление его не
# увидит. Пересчёт заданием после загрузки надёжнее и виден в Airflow как
# отдельный шаг.
#
# Почему витрина именно помесячная. Первой мыслью была витрина по паре
# «заказчик + поставщик». Проверка показала 1 781 982 уникальных пары на
# 1 800 000 договоров — сжатие в 1.01 раза, то есть никакого. Помесячная по
# заказчику даёт 6000 x 36 строк вместо 1.8 млн.

ALL_DDL: tuple[str, ...] = (CONTRACTS_DDL, LOTS_DDL, MART_CONTRACTS_MONTHLY_DDL)


async def apply_schema(client: AsyncClient) -> None:
    for statement in ALL_DDL:
        await client.command(statement)
