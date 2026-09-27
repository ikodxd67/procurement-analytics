-- Отклонение цены лота от медианы по товарной позиции. Версия для ClickHouse.
--
-- Главное отличие — медиана, и здесь легко ошибиться.
--
-- percentile_cont в PostgreSQL ИНТЕРПОЛИРУЕТ: на выборке [1,2,3,4] он даёт
-- 2.5, числа которого в данных нет. quantileExact в ClickHouse возвращает
-- настоящий элемент выборки и на тех же данных даёт 3.0. Обе функции считают
-- «медиану», но по разным определениям — проверено, и первый прогон сравнения
-- на этом и разошёлся.
--
-- Соответствие даёт quantileExactInclusive: он интерполирует так же, как
-- percentile_cont. Обычный quantile брать нельзя вовсе — он приближённый,
-- через резервуарную выборку, и расхождение было бы не из-за баз, а из-за
-- метода.

WITH unit_prices AS (
    SELECT
        id,
        enstru_code,
        toFloat64(amount) / toFloat64(count) AS unit_price
    FROM lots FINAL
    WHERE count > 0 AND amount > 0
),
position_stats AS (
    SELECT
        enstru_code,
        quantileExactInclusive(0.5)(unit_price) AS median_price,
        count() AS lots_in_position
    FROM unit_prices
    GROUP BY enstru_code
    HAVING count() >= 30
)
SELECT
    u.id,
    u.enstru_code,
    round(u.unit_price, 2)   AS unit_price,
    round(s.median_price, 2) AS median_price,
    s.lots_in_position,
    round(100.0 * (u.unit_price - s.median_price) / s.median_price, 1) AS deviation_pct
FROM unit_prices AS u
JOIN position_stats AS s ON s.enstru_code = u.enstru_code
ORDER BY deviation_pct DESC
LIMIT 20;
