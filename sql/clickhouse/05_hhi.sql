-- Концентрация закупок заказчика: индекс Херфиндаля и доля первой тройки.
-- Версия для ClickHouse.
--
-- Структура та же: два этажа оконных функций, вложить окно в окно нельзя ни
-- здесь, ни там. Отличия только в приведении типов и в том, что CASE WHEN без
-- ELSE в ClickHouse даёт 0, а не NULL, — поэтому вместо max(CASE ...) взят
-- maxIf, он считает только по нужным строкам.--
-- ORDER BY дополнен customer_bin намеренно. Индекс округляется до целого, и
-- сотни заказчиков получают одно и то же значение; при сортировке только по
-- нему LIMIT 20 выбирает из них произвольные двадцать, и две базы выбирают
-- разные. Это не расхождение вычислений, а недетерминированный запрос.
-- Сортировка с LIMIT обязана быть однозначной, иначе результат невоспроизводим
-- даже в одной базе.

WITH per_supplier AS (
    SELECT
        customer_bin,
        supplier_biin,
        sum(contract_sum) AS total
    FROM contracts FINAL
    GROUP BY customer_bin, supplier_biin
),
shares AS (
    SELECT
        customer_bin,
        supplier_biin,
        total,
        100.0 * toFloat64(total) / toFloat64(sum(total) OVER (PARTITION BY customer_bin)) AS share_pct,
        row_number() OVER (PARTITION BY customer_bin ORDER BY total DESC) AS position
    FROM per_supplier
),
cumulative AS (
    SELECT
        customer_bin,
        position,
        share_pct,
        sum(share_pct) OVER (
            PARTITION BY customer_bin ORDER BY position
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_share_pct
    FROM shares
)
SELECT
    customer_bin,
    count()                              AS suppliers,
    round(sum(share_pct * share_pct))    AS hhi,
    round(max(share_pct), 1)             AS top1_share_pct,
    round(maxIf(cum_share_pct, position = 3), 1) AS top3_share_pct
FROM cumulative
GROUP BY customer_bin
HAVING count() >= 5
ORDER BY hhi DESC, customer_bin
LIMIT 20;
