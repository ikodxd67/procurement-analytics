-- Доля поставщика в закупках заказчика. Версия для ClickHouse.
--
-- Оконные функции поддерживаются с версии 21.х и синтаксически совпадают со
-- стандартом. Отличия только в FINAL и в приведении типов: деление Decimal на
-- Decimal в ClickHouse даёт Decimal с большой шкалой, поэтому явно переводим в
-- Float64 перед округлением.

SELECT
    customer_bin,
    supplier_biin,
    count()                 AS contracts,
    sum(contract_sum)       AS supplier_total,
    sum(sum(contract_sum)) OVER (PARTITION BY customer_bin) AS customer_total,
    round(
        100.0 * toFloat64(sum(contract_sum))
        / toFloat64(sum(sum(contract_sum)) OVER (PARTITION BY customer_bin)),
        2
    ) AS share_pct
FROM contracts FINAL
GROUP BY customer_bin, supplier_biin
HAVING sum(contract_sum) > 0
ORDER BY share_pct DESC, supplier_total DESC
LIMIT 20;
