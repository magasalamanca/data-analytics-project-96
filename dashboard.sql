-- Маркетинговый дашборд: полный анализ эффективности
----------------------------------------------
-- 1. Основные метрики эффективности по кампаниям
----------------------------------------------
WITH campaign_stats AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(DISTINCT visitor_id) AS unique_visitors,
        SUM(leads_count) AS leads_generated,
        SUM(purchases_count) AS purchases_completed,
        SUM(total_cost) AS total_spend,
        SUM(revenue) AS total_revenue,
        MIN(visit_date) AS campaign_start_date,
        MAX(visit_date) AS campaign_end_date
    FROM
        marketing_data
    GROUP BY
        utm_source, utm_medium, utm_campaign
)

SELECT
    cs.utm_source,
    cs.utm_medium,
    cs.utm_campaign,
    cs.unique_visitors,
    cs.leads_generated,
    cs.purchases_completed,
    cs.total_spend,
    cs.total_revenue,
    ROUND(cs.total_revenue - cs.total_spend, 2) AS net_profit,
    ROUND(
        (cs.total_revenue - cs.total_spend) / 
        NULLIF(cs.total_spend, 0) * 100, 
        2
    ) AS roi_percentage,
    ROUND(
        cs.leads_generated / 
        NULLIF(cs.unique_visitors, 0) * 100, 
        2
    ) AS visit_to_lead_rate,
    ROUND(
        cs.purchases_completed / 
        NULLIF(cs.leads_generated, 0) * 100, 
        2
    ) AS lead_to_purchase_rate,
    ROUND(
        cs.total_spend / 
        NULLIF(cs.unique_visitors, 0), 
        2
    ) AS cpu,
    ROUND(
        cs.total_spend / 
        NULLIF(cs.leads_generated, 0), 
        2
    ) AS cpl,
    ROUND(
        cs.total_spend / 
        NULLIF(cs.purchases_completed, 0), 
        2
    ) AS cppu,
    cs.campaign_start_date,
    cs.campaign_end_date,
    (cs.campaign_end_date - cs.campaign_start_date) AS campaign_duration_days
FROM
    campaign_stats cs
ORDER BY
    net_profit DESC;

----------------------------------------------
-- 2. Анализ скорости закрытия лидов
----------------------------------------------
WITH lead_conversion AS (
    SELECT
        l.visitor_id,
        l.lead_id,
        l.created_at AS lead_date,
        l.closing_reason,
        l.status_id,
        MIN(s.visit_date) AS first_visit_date,
        MAX(s.visit_date) AS last_visit_date,
        (l.created_at - MIN(s.visit_date)) AS days_to_convert
    FROM
        leads l
    JOIN
        sessions s ON l.visitor_id = s.visitor_id
    GROUP BY
        l.visitor_id, l.lead_id, l.created_at, l.closing_reason, l.status_id
)

SELECT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_convert) AS median_days_to_convert,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_to_convert) AS p90_days_to_convert,
    COUNT(*) FILTER (WHERE days_to_convert <= 1) AS converted_within_1_day,
    COUNT(*) FILTER (WHERE days_to_convert <= 3) AS converted_within_3_days,
    COUNT(*) FILTER (WHERE days_to_convert <= 7) AS converted_within_1_week,
    COUNT(*) FILTER (WHERE days_to_convert <= 30) AS converted_within_1_month,
    ROUND(
        COUNT(*) FILTER (WHERE days_to_convert <= 3) / 
        NULLIF(COUNT(*), 0) * 100, 
        2
    ) AS pct_converted_in_3_days,
    ROUND(
        COUNT(*) FILTER (WHERE closing_reason = 'Успешно реализовано') / 
        NULLIF(COUNT(*), 0) * 100, 
        2
    ) AS success_rate
FROM
    lead_conversion;

----------------------------------------------
-- 3. Динамика эффективности по неделям
----------------------------------------------
SELECT
    DATE_TRUNC('week', visit_date) AS week_start,
    utm_source,
    COUNT(DISTINCT visitor_id) AS weekly_visitors,
    SUM(leads_count) AS weekly_leads,
    SUM(purchases_count) AS weekly_purchases,
    SUM(total_cost) AS weekly_spend,
    SUM(revenue) AS weekly_revenue,
    ROUND(
        (SUM(revenue) - SUM(total_cost)) / 
        NULLIF(SUM(total_cost), 0) * 100, 
        2
    ) AS weekly_roi,
    ROUND(
        SUM(leads_count) / 
        NULLIF(COUNT(DISTINCT visitor_id), 0) * 100, 
        2
    ) AS weekly_conversion_rate
FROM
    marketing_data
GROUP BY
    week_start, utm_source
ORDER BY
    week_start, weekly_roi DESC;

----------------------------------------------
-- 4. Рекомендации по оптимизации бюджета
----------------------------------------------
SELECT
    utm_source,
    utm_campaign,
    SUM(total_cost) AS total_spend,
    SUM(revenue) AS total_revenue,
    ROUND(
        (SUM(revenue) - SUM(total_cost)) / 
        NULLIF(SUM(total_cost), 0) * 100, 
        2
    ) AS roi,
    CASE
        WHEN (SUM(revenue) - SUM(total_cost)) / NULLIF(SUM(total_cost), 0) * 100 > 50 
            THEN 'Увеличить бюджет'
        WHEN (SUM(revenue) - SUM(total_cost)) / NULLIF(SUM(total_cost), 0) * 100 BETWEEN 0 AND 50 
            THEN 'Поддерживать текущий уровень'
        WHEN (SUM(revenue) - SUM(total_cost)) / NULLIF(SUM(total_cost), 0) * 100 < 0 
            THEN 'Сократить или оптимизировать'
        ELSE 'Требуется дополнительный анализ'
    END AS budget_recommendation
FROM
    marketing_data
GROUP BY
    utm_source, utm_campaign
ORDER BY
    roi DESC;
