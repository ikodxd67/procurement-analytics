-- Концентрация закупок заказчика: индекс Херфиндаля и доля первой тройки.
--
-- Индекс Херфиндаля — сумма квадратов долей рынка в процентах. Диапазон от
-- 10000/N (все поставщики равны) до 10000 (один поставщик забрал всё).
-- Возведение в квадрат нужно, чтобы крупные доли весили непропорционально
-- больше: это и отличает концентрацию от простого числа участников.
--
-- Оконные функции идут в два этажа, потому что вложить окно в окно нельзя:
-- сначала считаем долю каждого поставщика, потом накопительную сумму долей по
-- убыванию. Второй этаж и даёт «кумулятивную концентрацию».
--
-- Что этот показатель означает и чего не означает. Высокий индекс — факт о
-- распределении сумм, и только. Причины бывают любые: единственный поставщик
-- на рынке, специфика предмета закупки, размер заказчика. Никаких выводов о
-- добросовестности отсюда не следует и в проекте не делается.--
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
    FROM bench.contracts
    GROUP BY customer_bin, supplier_biin
),
shares AS (
    SELECT
        customer_bin,
        supplier_biin,
        total,
        100.0 * total / sum(total) OVER (PARTITION BY customer_bin) AS share_pct,
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
    count(*)                                   AS suppliers,
    round(sum(share_pct * share_pct))          AS hhi,
    round(max(share_pct), 1)                   AS top1_share_pct,
    round(max(CASE WHEN position = 3 THEN cum_share_pct END), 1) AS top3_share_pct
FROM cumulative
GROUP BY customer_bin
HAVING count(*) >= 5
ORDER BY hhi DESC, customer_bin
LIMIT 20;
