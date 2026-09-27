-- Свёртка сумм лотов на любой уровень дерева классификатора.
--
-- Задача: у лота стоит код листа (07.3.2.041), а нужен итог по разделу,
-- группе или подгруппе. Уровень задаётся параметром, дерево обходится вверх
-- рекурсивным CTE.
--
-- Почему рекурсия, а не разбор кода строкой. Здесь код собран из уровней через
-- точку, и соблазн отрезать хвост велик. Но структура кода — свойство этого
-- конкретного классификатора, а связь родитель-потомок хранится явно. Запрос,
-- опирающийся на форму строки, сломается на первом же справочнике с другой
-- нумерацией.
--
-- Параметр уровня передаётся позиционно как $1 (0 раздел, 1 группа,
-- 2 подгруппа, 3 сам лист): asyncpg понимает только такую форму.

WITH RECURSIVE up AS (
    -- Стартуем с листьев: каждый лист сам себе предок нулевого шага.
    SELECT
        code AS leaf_code,
        code,
        parent_code,
        level
    FROM bench.ref_classifier
    WHERE level = 3

    UNION ALL

    -- Шаг вверх: от узла к его родителю, таща за собой исходный лист.
    SELECT
        u.leaf_code,
        parent.code,
        parent.parent_code,
        parent.level
    FROM up u
    JOIN bench.ref_classifier parent ON parent.code = u.parent_code
),
mapping AS (
    SELECT leaf_code, code AS rollup_code
    FROM up
    WHERE level = $1
)
SELECT
    m.rollup_code,
    node.name_ru,
    count(*)          AS lots,
    sum(l.amount)     AS total_amount,
    round(avg(l.amount), 2) AS avg_amount
FROM bench.lots l
JOIN mapping m           ON m.leaf_code = l.enstru_code
JOIN bench.ref_classifier node ON node.code = m.rollup_code
GROUP BY m.rollup_code, node.name_ru
ORDER BY total_amount DESC
LIMIT 20;
