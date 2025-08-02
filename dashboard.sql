-- 1. Ежедневное количество уникальных посетителей сайта
SELECT
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    TO_CHAR(visit_date, 'YYYY-MM-DD') AS visit_day
FROM sessions
GROUP BY visit_day
ORDER BY visit_day;


-- 2. Количество посетителей по каналам (ежедневная детализация)
SELECT
    source,
    medium,
    campaign,
    COUNT(DISTINCT visitor_id) AS visitors,
    TO_CHAR(visit_date, 'YYYY-MM-DD') AS visit_day
FROM sessions
GROUP BY visit_day, source, medium, campaign
ORDER BY visit_day, source, medium, campaign;


-- 3. Количество посетителей по каналам (по неделям)
SELECT
    source,
    medium,
    campaign,
    COUNT(DISTINCT visitor_id) AS visitors,
    EXTRACT(WEEK FROM visit_date) AS week_number,
    TO_CHAR(visit_date, 'YYYY') AS year_num
FROM sessions
GROUP BY year_num, week_number, source, medium, campaign
ORDER BY year_num, week_number, source, medium, campaign;


-- 4. Количество посетителей по каналам (по месяцам)
SELECT
    source,
    medium,
    campaign,
    COUNT(DISTINCT visitor_id) AS visitors,
    TO_CHAR(visit_date, 'YYYY-MM') AS visit_month
FROM sessions
GROUP BY visit_month, source, medium, campaign
ORDER BY visit_month, source, medium, campaign;


-- 5. Количество лидов по дням
SELECT
    TO_CHAR(created_at, 'YYYY-MM-DD') AS lead_date,
    COUNT(lead_id) AS leads_count
FROM leads
GROUP BY lead_date
ORDER BY lead_date;


-- 6. Конверсия лидов в оплаченные (процент оплаченных лидов)
WITH lead_stats AS (
    SELECT
        COUNT(*) FILTER (WHERE status_id = 142) AS paid_count,
        COUNT(*) AS total_count
    FROM leads
)

SELECT
    ROUND(
        (paid_count::NUMERIC * 100.0) / NULLIF(total_count, 0),
        2
    ) AS conversion_rate_percent
FROM lead_stats;


-- 7. Расходы по рекламным каналам (ежедневно)
SELECT
    TO_CHAR(campaign_date, 'YYYY-MM-DD') AS ad_date,
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS total_spent
FROM vk_ads
GROUP BY ad_date, utm_source, utm_medium, utm_campaign

UNION ALL

SELECT
    TO_CHAR(campaign_date, 'YYYY-MM-DD') AS ad_date,
    utm_source,
    utm_medium,
    utm_campaign,
    SUM(daily_spent) AS total_spent
FROM ya_ads
GROUP BY ad_date, utm_source, utm_medium, utm_campaign

ORDER BY ad_date, utm_source, utm_medium, utm_campaign;


-- 8. ROI (окупаемость рекламы) по каналам
WITH revenue_by_channel AS (
    SELECT
        s.source,
        s.medium,
        s.campaign,
        TO_CHAR(l.created_at, 'YYYY-MM-DD') AS lead_date,
        COALESCE(SUM(l.amount), 0) AS revenue
    FROM leads AS l
    INNER JOIN sessions AS s ON l.visitor_id = s.visitor_id
    GROUP BY lead_date, s.source, s.medium, s.campaign
),

ad_spend_by_channel AS (
    SELECT
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS spend_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS daily_cost
    FROM vk_ads
    GROUP BY spend_date, utm_source, utm_medium, utm_campaign

    UNION ALL

    SELECT
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS spend_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS daily_cost
    FROM ya_ads
    GROUP BY spend_date, utm_source, utm_medium, utm_campaign
)

SELECT
    r.lead_date,
    r.source,
    r.medium,
    r.campaign,
    ROUND(
        SUM((r.revenue - COALESCE(s.daily_cost, 0)) / NULLIF(s.daily_cost, 0))
        * 100.0,
        2
    ) AS roi_percentage
FROM revenue_by_channel AS r
INNER JOIN ad_spend_by_channel AS s
    ON
        r.lead_date = s.spend_date
        AND r.source = s.utm_source
        AND r.medium = s.utm_medium
        AND r.campaign = s.utm_campaign
GROUP BY r.lead_date, r.source, r.medium, r.campaign
ORDER BY r.lead_date, r.source, r.medium, r.campaign;
