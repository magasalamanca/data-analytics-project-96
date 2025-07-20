WITH ads AS (
    SELECT
        ad_id,
        campaign_id,
        campaign_name,
        utm_source,
        utm_medium,
        utm_campaign,
        campaign_date
    FROM vk_ads
    UNION ALL
    SELECT
        ad_id,
        campaign_id,
        campaign_name,
        utm_source,
        utm_medium,
        utm_campaign,
        campaign_date
    FROM ya_ads
),

sessions_with_ads AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        a.utm_source,
        a.utm_medium,
        a.utm_campaign
    FROM sessions AS s
    LEFT JOIN ads AS a ON DATE(s.visit_date) = DATE(a.campaign_date)
    WHERE
        a.utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

last_paid_click AS (
    SELECT
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY visitor_id
            ORDER BY visit_date DESC
        ) AS rn
    FROM sessions_with_ads
),

final_data AS (
    SELECT
        lpc.visitor_id,
        lpc.visit_date,
        lpc.utm_source,
        lpc.utm_medium,
        lpc.utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM last_paid_click AS lpc
    LEFT JOIN leads AS l ON lpc.visitor_id = l.visitor_id
    WHERE lpc.rn = 1
)

SELECT * FROM final_data
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10;
