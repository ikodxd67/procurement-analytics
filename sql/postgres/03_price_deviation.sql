-- Отклонение цены лота от медианы по товарной позиции.
--
-- Считается цена за единицу (amount / count), а не сумма лота: лот на тысячу
-- пачек бумаги и лот на одну пачку сравнивать по сумме бессмысленно.
--
-- Медиана, а не среднее. Среднее утаскивается вверх единичным дорогим лотом,
-- и тогда «отклонение от среднего» покажет отклонение от выброса. Медиана к
-- выбросам устойчива.
--
-- Позиции меньше чем с 30 лотами отброшены: медиана по трём наблюдениям — это
-- не медиана, а случайное число.
--
-- Приведение к numeric не для красоты. percentile_cont возвращает double
-- precision, а round с двумя аргументами в PostgreSQL определён только для
-- numeric: round(double precision, integer) не существует вовсе. Без
-- приведения запрос падает с UndefinedFunction, и именно так он и упал при
-- первом прогоне.

WITH unit_prices AS (
    SELECT
        id,
        enstru_code,
        amount / count AS unit_price
    FROM bench.lots
    WHERE count > 0 AND amount > 0
),
position_stats AS (
    SELECT
        enstru_code,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY unit_price) AS median_price,
        count(*) AS lots_in_position
    FROM unit_prices
    GROUP BY enstru_code
    HAVING count(*) >= 30
)
SELECT
    u.id,
    u.enstru_code,
    round(u.unit_price, 2)                  AS unit_price,
    round(s.median_price::numeric, 2)       AS median_price,
    s.lots_in_position,
    round(
        100.0 * (u.unit_price - s.median_price::numeric) / s.median_price::numeric,
        1
    ) AS deviation_pct
FROM unit_prices u
JOIN position_stats s USING (enstru_code)
ORDER BY deviation_pct DESC
LIMIT 20;
