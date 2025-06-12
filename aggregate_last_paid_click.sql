-- aggregate_last_paid_click.sql

WITH PaidClicks AS (
    SELECT 
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_spent
    FROM vk_ads
    UNION ALL
    SELECT 
        campaign_date,
        utm_source,
        utm_medium,
        utm_campaign,
        daily_spent
    FROM ya_ads
),
LastPaidClick AS (
    SELECT 
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        s.visitor_id,
        pc.daily_spent,
        l.lead_id,
        l.created_at,
        l.amount AS revenue,
        l.closing_reason,
        l.status_id,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id 
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions s
    LEFT JOIN PaidClicks pc 
        ON s.visit_date = pc.campaign_date
        AND s.source = pc.utm_source
        AND s.medium = pc.utm_medium
        AND s.campaign = pc.utm_campaign
    LEFT JOIN leads l 
        ON s.visitor_id = l.visitor_id 
        AND l.created_at >= s.visit_date
)
SELECT 
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign,
    COUNT(DISTINCT visitor_id) AS visitors_count,
    SUM(COALESCE(daily_spent, 0)) AS total_cost,
    COUNT(DISTINCT CASE WHEN lead_id IS NOT NULL THEN lead_id END) AS leads_count,
    COUNT(DISTINCT CASE 
        WHEN (closing_reason = 'Успешно реализовано' OR status_id = 142) 
        THEN lead_id END) AS purchases_count,
    SUM(CASE 
        WHEN (closing_reason = 'Успешно реализовано' OR status_id = 142) 
        THEN revenue ELSE 0 END) AS revenue
FROM LastPaidClick
WHERE rn = 1
GROUP BY 
    visit_date,
    utm_source,
    utm_medium,
    utm_campaign
ORDER BY 
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 15;