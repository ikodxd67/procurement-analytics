-- Периоды непрерывной активности поставщика (gaps and islands).
-- Версия для ClickHouse.
--
-- Приём тот же: номер месяца минус номер строки в окне даёт постоянную метку,
-- пока месяцы идут подряд.
--
-- Отличия:
--   * toYear/toMonth вместо EXTRACT;
--   * DISTINCT заменён на GROUP BY. В ClickHouse SELECT DISTINCT работает, но
--     GROUP BY по тем же колонкам идёт через ту же агрегацию и читается
--     яснее, когда дальше всё равно нужны оконные функции.

WITH active_months AS (
    SELECT
        supplier_biin,
        toStartOfMonth(crdate) AS month
    FROM contracts FINAL
    GROUP BY supplier_biin, month
),
numbered AS (
    SELECT
        supplier_biin,
        month,
        toInt32(toYear(month) * 12 + toMonth(month))
            - toInt32(row_number() OVER (PARTITION BY supplier_biin ORDER BY month))
          AS island_key
    FROM active_months
)
SELECT
    supplier_biin,
    min(month) AS started,
    max(month) AS ended,
    count()    AS months_in_row
FROM numbered
GROUP BY supplier_biin, island_key
ORDER BY months_in_row DESC, supplier_biin
LIMIT 20;
