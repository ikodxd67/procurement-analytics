-- Сравнение месяца с предыдущим через LAG. Версия для ClickHouse.
--
-- Отличия:
--   * toStartOfMonth вместо date_trunc;
--   * lagInFrame вместо lag. Это не синоним: lagInFrame смотрит внутрь рамки
--     окна, поэтому рамка задана явно от начала раздела.
--   * toNullable и NULL третьим аргументом. Без них lagInFrame на первой
--     строке возвращает не NULL, а значение по умолчанию для типа, то есть
--     ноль. PostgreSQL в том же месте даёт NULL, и сравнение результатов
--     разошлось ровно здесь: 0.0 против None. Третий аргумент задаёт, что
--     подставить, когда предыдущей строки нет.
--   * nullif оставлен ради одинакового результата: деление на ноль в
--     ClickHouse даёт inf, а не ошибку.

WITH monthly AS (
    SELECT
        toStartOfMonth(crdate) AS month,
        count()                AS contracts,
        sum(contract_sum)      AS total
    FROM contracts FINAL
    GROUP BY month
)
SELECT
    month,
    contracts,
    total,
    prev_total,
    total - prev_total AS delta,
    round(100.0 * toFloat64(total - prev_total) / nullif(toFloat64(prev_total), 0), 1) AS change_pct
FROM (
    SELECT
        month,
        contracts,
        total,
        lagInFrame(toNullable(total), 1, NULL) OVER (
            ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS prev_total
    FROM monthly
)
ORDER BY month;
