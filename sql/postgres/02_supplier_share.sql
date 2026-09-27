-- Доля поставщика в закупках заказчика.
--
-- Оконная функция здесь нужна вот зачем: в одной строке требуется и сумма по
-- паре «заказчик + поставщик», и итог по всему заказчику. Обычной группировкой
-- это не получить — GROUP BY даёт один уровень детализации, а нужны два сразу.
-- Подзапрос с повторным соединением решил бы задачу, но прочитал бы таблицу
-- дважды.
--
-- sum(sum(...)) OVER (...) читается странно, но смысл прямой: внутренний sum
-- считает агрегат группы, внешний — окно поверх уже посчитанных групп.
--
-- Формулировка вывода фактическая: «доля поставщика 94%» — это результат
-- расчёта. Никаких утверждений о причинах такой доли из этих данных не следует.

SELECT
    customer_bin,
    supplier_biin,
    count(*)                AS contracts,
    sum(contract_sum)       AS supplier_total,
    sum(sum(contract_sum)) OVER (PARTITION BY customer_bin) AS customer_total,
    round(
        100.0 * sum(contract_sum)
        / sum(sum(contract_sum)) OVER (PARTITION BY customer_bin),
        2
    ) AS share_pct
FROM bench.contracts
GROUP BY customer_bin, supplier_biin
HAVING sum(contract_sum) > 0
ORDER BY share_pct DESC, supplier_total DESC
LIMIT 20;
