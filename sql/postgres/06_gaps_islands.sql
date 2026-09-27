-- Периоды непрерывной активности поставщика (gaps and islands).
--
-- Задача: у поставщика есть месяцы с договорами и месяцы без. Нужно свернуть
-- подряд идущие активные месяцы в отрезки и найти самые длинные.
--
-- Приём классический и на первый взгляд странный: из порядкового номера
-- месяца вычитается номер строки в окне. Пока месяцы идут подряд, обе величины
-- растут на единицу, и разность постоянна — она и служит меткой отрезка. Как
-- только появляется пропуск, номер месяца прыгает вперёд, а номер строки нет,
-- и разность меняется. Дальше обычная группировка по этой метке.
--
-- Месяц переводится в сплошной счётчик (год * 12 + месяц) намеренно: по датам
-- вычитание номера строки не имеет смысла, а по номеру месяца граница года
-- проходит без разрывов.

WITH active_months AS (
    SELECT DISTINCT
        supplier_biin,
        date_trunc('month', crdate)::date AS month
    FROM bench.contracts
),
numbered AS (
    SELECT
        supplier_biin,
        month,
        (EXTRACT(YEAR FROM month)::int * 12 + EXTRACT(MONTH FROM month)::int)
            - row_number() OVER (PARTITION BY supplier_biin ORDER BY month)::int
          AS island_key
    FROM active_months
)
SELECT
    supplier_biin,
    min(month) AS started,
    max(month) AS ended,
    count(*)   AS months_in_row
FROM numbered
GROUP BY supplier_biin, island_key
ORDER BY months_in_row DESC, supplier_biin
LIMIT 20;
