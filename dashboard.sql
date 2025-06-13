-- Пример запросов

-- 1. Сводка по ROI
SELECT utm_source, utm_campaign,
       SUM(total_cost) AS cost,
       SUM(revenue) AS revenue,
       ROUND((SUM(revenue) - SUM(total_cost)) / SUM(total_cost) * 100, 2) AS roi
FROM marketing_data
GROUP BY utm_source, utm_campaign
ORDER BY roi DESC;

-- 2. Расчёт CPL, CPPU
SELECT utm_source, utm_campaign,
       SUM(total_cost) / NULLIF(SUM(leads_count), 0) AS cpl,
       SUM(total_cost) / NULLIF(SUM(purchases_count), 0) AS cppu
FROM marketing_data
GROUP BY utm_source, utm_campaign;
