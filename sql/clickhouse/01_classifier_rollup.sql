-- Свёртка сумм лотов на любой уровень дерева классификатора.
-- Версия для ClickHouse. Отличия от PostgreSQL:
--   * FINAL при чтении фактов — иначе в сумму попадут устаревшие ревизии;
--   * parent_code хранится пустой строкой, а не NULL: в ClickHouse Nullable
--     стоит места и мешает пропуску гранул, а пустая строка здесь однозначна.
--
-- WITH RECURSIVE поддерживается начиная с 24.х; проверено на 24.8.14.39.

WITH RECURSIVE up AS (
    SELECT
        code AS leaf_code,
        code,
        parent_code,
        level
    FROM ref_classifier
    WHERE level = 3

    UNION ALL

    SELECT
        u.leaf_code,
        parent.code,
        parent.parent_code,
        parent.level
    FROM up AS u
    JOIN ref_classifier AS parent ON parent.code = u.parent_code
),
mapping AS (
    SELECT leaf_code, code AS rollup_code
    FROM up
    WHERE level = {target_level:UInt8}
)
SELECT
    m.rollup_code,
    node.name_ru,
    count()                 AS lots,
    sum(l.amount)           AS total_amount,
    round(avg(l.amount), 2) AS avg_amount
FROM lots AS l FINAL
JOIN mapping AS m            ON m.leaf_code = l.enstru_code
JOIN ref_classifier AS node  ON node.code = m.rollup_code
GROUP BY m.rollup_code, node.name_ru
ORDER BY total_amount DESC
LIMIT 20;
