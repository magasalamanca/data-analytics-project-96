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
        a.utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions AS s
    LEFT JOIN ads AS a
        ON DATE(s.visit_date) = DATE(a.campaign_date)
    WHERE
        a.utm_medium IN ('cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social')
),

final_data AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.utm_source,
        s.utm_medium,
        s.utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM sessions_with_ads AS s
    LEFT JOIN leads AS l
        ON s.visitor_id = l.visitor_id
    WHERE s.rn = 1
)

SELECT * FROM final_data
ORDER BY
    amount DESC NULLS LAST,
    visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign ASC
LIMIT 10;
