-- Сравнение месяца с предыдущим через LAG.
--
-- LAG берёт значение из предыдущей строки окна. Альтернатива — соединить
-- таблицу саму с собой по «месяц минус один», и она хуже сразу по двум
-- причинам: соединение дороже, и пропущенный месяц молча выпадет из выборки
-- вместо того, чтобы показать разрыв.
--
-- lag вызывается трижды, и это не расточительство: PostgreSQL вычисляет
-- одинаковые оконные выражения один раз.

WITH monthly AS (
    SELECT
        date_trunc('month', crdate)::date AS month,
        count(*)          AS contracts,
        sum(contract_sum) AS total
    FROM bench.contracts
    GROUP BY 1
)
SELECT
    month,
    contracts,
    total,
    lag(total) OVER (ORDER BY month) AS prev_total,
    total - lag(total) OVER (ORDER BY month) AS delta,
    round(
        100.0 * (total - lag(total) OVER (ORDER BY month))
        / nullif(lag(total) OVER (ORDER BY month), 0),
        1
    ) AS change_pct
FROM monthly
ORDER BY month;
