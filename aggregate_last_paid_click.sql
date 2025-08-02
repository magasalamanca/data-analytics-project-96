-- Анализ эффективности рекламных каналов: посетители, лиды, продажи, доход и расходы
WITH first_touch_sessions AS (
    -- Определяем первую сессию посетителя до или в день лида (last non-organic touch)
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
        l.lead_id,
        l.created_at AS lead_created_at,
        l.amount AS deal_value,
        l.closing_reason,
        l.status_id
    FROM sessions s
    LEFT JOIN leads l
        ON s.visitor_id = l.visitor_id
        AND s.visit_date <= l.created_at  -- сессия до или в день лида
    WHERE s.medium != 'organic'  -- исключаем органический трафик
),

last_non_organic_touch AS (
    -- Оставляем только последнюю сессию посетителя перед конверсией
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY visitor_id
            ORDER BY visit_date DESC, lead_created_at NULLS LAST
        ) AS rn
    FROM first_touch_sessions
),

qualified_visitors AS (
    -- Агрегируем данные по каналам и датам
    SELECT
        source,
        medium,
        campaign,
        TO_CHAR(visit_date, 'YYYY-MM-DD') AS session_date,
        COUNT(visitor_id) AS total_sessions,
        COUNT(lead_id) AS leads_generated,
        COUNT(CASE WHEN status_id = 142 THEN 1 END) AS successful_purchases,
        COALESCE(SUM(CASE WHEN status_id = 142 THEN deal_value END), 0) AS revenue
    FROM last_non_organic_touch
    WHERE rn = 1  -- только последний касаний
    GROUP BY session_date, source, medium, campaign
),

ad_spend_daily AS (
    -- Суммарные расходы по рекламе (VK + Яндекс)
    SELECT
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS spend_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS daily_ad_cost
    FROM vk_ads
    GROUP BY spend_date, utm_source, utm_medium, utm_campaign

    UNION ALL

    SELECT
        TO_CHAR(campaign_date, 'YYYY-MM-DD') AS spend_date,
        utm_source,
        utm_medium,
        utm_campaign,
        SUM(daily_spent) AS daily_ad_cost
    FROM ya_ads
    GROUP BY spend_date, utm_source, utm_medium, utm_campaign
)

-- Финальный отчет: объединение конверсий и расходов
SELECT
    qv.session_date::DATE AS report_date,
    qv.source AS utm_source,
    qv.medium AS utm_medium,
    qv.campaign AS utm_campaign,
    qv.total_sessions AS visitors,
    COALESCE(ad.daily_ad_cost, 0) AS ad_spend,
    qv.leads_generated AS leads,
    qv.successful_purchases AS purchases,
    qv.revenue AS revenue
FROM qualified_visitors qv
LEFT JOIN ad_spend_daily ad
    ON qv.session_date = ad.spend_date
    AND qv.source = ad.utm_source
    AND qv.medium = ad.utm_medium
    AND qv.campaign = ad.utm_campaign
ORDER BY
    qv.revenue DESC NULLS LAST,
    qv.session_date ASC,
    qv.total_sessions DESC,
    utm_source,
    utm_medium,
    utm_campaign
LIMIT 15;
