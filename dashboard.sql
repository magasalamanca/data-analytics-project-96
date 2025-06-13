-- 1. Сводка по ROI
SELECT
    utm_source,
    utm_campaign,
    SUM(total_cost) AS marketing_cost,
    SUM(revenue) AS total_revenue,
    ROUND(
        (SUM(revenue) - SUM(total_cost)) / 
        NULLIF(SUM(total_cost), 0) * 100, 
        2
    ) AS roi_percentage
FROM 
    marketing_data
GROUP BY 
    utm_source, 
    utm_campaign
ORDER BY 
    roi_percentage DESC;

-- 2. Расчёт CPL, CPPU
SELECT
    utm_source,
    utm_campaign,
    SUM(total_cost) / NULLIF(SUM(leads_count), 0) AS cost_per_lead,
    SUM(total_cost) / NULLIF(SUM(purchases_count), 0) AS cost_per_purchase
FROM 
    marketing_data
GROUP BY 
    utm_source, 
    utm_campaign;
