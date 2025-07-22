-- Запрос 1: Основные данные для дашборда (на основе aggregate_last_paid_click.sql)
-- Создаёт агрегированные данные, использованные в дашборде
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
        THEN revenue ELSE 0 END) AS revenue,
    CASE WHEN COUNT(DISTINCT visitor_id) > 0 THEN SUM(COALESCE(daily_spent, 0)) / COUNT(DISTINCT visitor_id) ELSE 0 END AS cpu,
    CASE WHEN COUNT(DISTINCT CASE WHEN lead_id IS NOT NULL THEN lead_id END) > 0 THEN SUM(COALESCE(daily_spent, 0)) / COUNT(DISTINCT CASE WHEN lead_id IS NOT NULL THEN lead_id END) ELSE 0 END AS cpl,
    CASE WHEN COUNT(DISTINCT CASE WHEN (closing_reason = 'Успешно реализовано' OR status_id = 142) THEN lead_id END) > 0 THEN SUM(COALESCE(daily_spent, 0)) / COUNT(DISTINCT CASE WHEN (closing_reason = 'Успешно реализовано' OR status_id = 142) THEN lead_id END) ELSE 0 END AS cppu,
    CASE WHEN SUM(COALESCE(daily_spent, 0)) > 0 THEN (SUM(CASE WHEN (closing_reason = 'Успешно реализовано' OR status_id = 142) THEN revenue ELSE 0 END) - SUM(COALESCE(daily_spent, 0))) / SUM(COALESCE(daily_spent, 0)) * 100 ELSE 0 END AS roi
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
    utm_campaign ASC;

-- Запрос 2: Анализ скорости закрытия лидов
-- Адаптирован для таблиц sessions и leads, используется для анализа времени конверсии (слайд 7)
WITH lead_conversion AS (
    SELECT
        l.visitor_id,
        l.lead_id,
        l.created_at AS lead_date,
        l.closing_reason,
        l.status_id,
        MIN(s.visit_date) AS first_visit_date,
        MAX(s.visit_date) AS last_visit_date,
        DATEDIFF('day', MIN(s.visit_date), l.created_at) AS days_to_convert
    FROM leads AS l
    INNER JOIN sessions AS s
        ON
            l.visitor_id = s.visitor_id
            AND l.created_at >= s.visit_date
    GROUP BY
        l.visitor_id,
        l.lead_id,
        l.created_at,
        l.closing_reason,
        l.status_id
)

SELECT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_convert)
        AS median_days_to_convert,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY days_to_convert)
        AS p90_days_to_convert,
    COUNT(*) FILTER (WHERE days_to_convert <= 1) AS converted_within_1_day,
    COUNT(*) FILTER (WHERE days_to_convert <= 3) AS converted_within_3_days,
    COUNT(*) FILTER (WHERE days_to_convert <= 7) AS converted_within_1_week,
    COUNT(*) FILTER (WHERE days_to_convert <= 30) AS converted_within_1_month,
    ROUND(
        COUNT(*) FILTER (WHERE days_to_convert <= 3)
        / NULLIF(COUNT(*), 0)
        * 100,
        2
    ) AS pct_converted_in_3_days,
    ROUND(
        COUNT(*) FILTER (
            WHERE closing_reason = 'Успешно реализовано' OR status_id = 142
        )
        / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS success_rate
FROM lead_conversion;


-- Запрос 3: Динамика эффективности по неделям
-- Адаптирован для использования LastPaidClick CTE из запроса 1
WITH PAIDCLICKS AS (
    SELECT
        CAMPAIGN_DATE,
        UTM_SOURCE,
        UTM_MEDIUM,
        UTM_CAMPAIGN,
        DAILY_SPENT
    FROM VK_ADS
    UNION ALL
    SELECT
        CAMPAIGN_DATE,
        UTM_SOURCE,
        UTM_MEDIUM,
        UTM_CAMPAIGN,
        DAILY_SPENT
    FROM YA_ADS
),

LASTPAIDCLICK AS (
    SELECT
        S.VISIT_DATE,
        S.SOURCE AS UTM_SOURCE,
        S.MEDIUM AS UTM_MEDIUM,
        S.CAMPAIGN AS UTM_CAMPAIGN,
        S.VISITOR_ID,
        PC.DAILY_SPENT,
        L.LEAD_ID,
        L.CREATED_AT,
        L.AMOUNT AS REVENUE,
        L.CLOSING_REASON,
        L.STATUS_ID,
        ROW_NUMBER() OVER (
            PARTITION BY L.LEAD_ID
            ORDER BY S.VISIT_DATE DESC
        ) AS RN
    FROM SESSIONS AS S
    LEFT JOIN PAIDCLICKS AS PC
        ON
            S.VISIT_DATE = PC.CAMPAIGN_DATE
            AND S.SOURCE = PC.UTM_SOURCE
            AND S.MEDIUM = PC.UTM_MEDIUM
            AND S.CAMPAIGN = PC.UTM_CAMPAIGN
    LEFT JOIN LEADS AS L
        ON
            S.VISITOR_ID = L.VISITOR_ID
            AND S.VISIT_DATE <= L.CREATED_AT
)

SELECT
    UTM_SOURCE,
    DATE_TRUNC('week', VISIT_DATE) AS WEEK_START,
    COUNT(DISTINCT VISITOR_ID) AS WEEKLY_VISITORS,
    COUNT(DISTINCT LEAD_ID)
        AS WEEKLY_LEADS,
    COUNT(DISTINCT CASE
        WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
            THEN LEAD_ID
    END) AS WEEKLY_PURCHASES,
    SUM(COALESCE(DAILY_SPENT, 0)) AS WEEKLY_SPEND,
    SUM(CASE
        WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
            THEN REVENUE
        ELSE 0
    END) AS WEEKLY_REVENUE,
    ROUND(
        (
            SUM(
                CASE
                    WHEN
                        (
                            CLOSING_REASON = 'Успешно реализовано'
                            OR STATUS_ID = 142
                        )
                        THEN REVENUE
                    ELSE 0
                END
            )
            - SUM(COALESCE(DAILY_SPENT, 0))
        )
        / NULLIF(SUM(COALESCE(DAILY_SPENT, 0)), 0) * 100,
        2
    ) AS WEEKLY_ROI,
    ROUND(
        COUNT(DISTINCT LEAD_ID)
        / NULLIF(COUNT(DISTINCT VISITOR_ID), 0) * 100,
        2
    ) AS WEEKLY_CONVERSION_RATE
FROM LASTPAIDCLICK
WHERE RN = 1
GROUP BY
    WEEK_START,
    UTM_SOURCE
ORDER BY
    WEEK_START ASC,
    WEEKLY_ROI DESC;

-- Запрос 4: Рекомендации по оптимизации бюджета
-- Адаптирован для использования LastPaidClick CTE из запроса 1
WITH PAIDCLICKS AS (
    SELECT
        CAMPAIGN_DATE,
        UTM_SOURCE,
        UTM_MEDIUM,
        UTM_CAMPAIGN,
        DAILY_SPENT
    FROM VK_ADS
    UNION ALL
    SELECT
        CAMPAIGN_DATE,
        UTM_SOURCE,
        UTM_MEDIUM,
        UTM_CAMPAIGN,
        DAILY_SPENT
    FROM YA_ADS
),

LASTPAIDCLICK AS (
    SELECT
        S.VISIT_DATE,
        S.SOURCE AS UTM_SOURCE,
        S.MEDIUM AS UTM_MEDIUM,
        S.CAMPAIGN AS UTM_CAMPAIGN,
        S.VISITOR_ID,
        PC.DAILY_SPENT,
        L.LEAD_ID,
        L.CREATED_AT,
        L.AMOUNT AS REVENUE,
        L.CLOSING_REASON,
        L.STATUS_ID,
        ROW_NUMBER() OVER (
            PARTITION BY L.LEAD_ID
            ORDER BY S.VISIT_DATE DESC
        ) AS RN
    FROM SESSIONS AS S
    LEFT JOIN PAIDCLICKS AS PC
        ON
            S.VISIT_DATE = PC.CAMPAIGN_DATE
            AND S.SOURCE = PC.UTM_SOURCE
            AND S.MEDIUM = PC.UTM_MEDIUM
            AND S.CAMPAIGN = PC.UTM_CAMPAIGN
    LEFT JOIN LEADS AS L
        ON
            S.VISITOR_ID = L.VISITOR_ID
            AND S.VISIT_DATE <= L.CREATED_AT
)

SELECT
    UTM_SOURCE,
    UTM_CAMPAIGN,
    SUM(COALESCE(DAILY_SPENT, 0)) AS TOTAL_SPEND,
    SUM(CASE
        WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
            THEN REVENUE
        ELSE 0
    END) AS TOTAL_REVENUE,
    ROUND(
        (
            SUM(
                CASE
                    WHEN
                        (
                            CLOSING_REASON = 'Успешно реализовано'
                            OR STATUS_ID = 142
                        )
                        THEN REVENUE
                    ELSE 0
                END
            )
            - SUM(COALESCE(DAILY_SPENT, 0))
        )
        / NULLIF(SUM(COALESCE(DAILY_SPENT, 0)), 0) * 100,
        2
    ) AS ROI,
    CASE
        WHEN
            (
                SUM(
                    CASE
                        WHEN
                            (
                                CLOSING_REASON = 'Успешно реализовано'
                                OR STATUS_ID = 142
                            )
                            THEN REVENUE
                        ELSE 0
                    END
                )
                - SUM(COALESCE(DAILY_SPENT, 0))
            )
            / NULLIF(SUM(COALESCE(DAILY_SPENT, 0)), 0) * 100 > 50
            THEN 'Увеличить бюджет'
        WHEN
            (
                SUM(
                    CASE
                        WHEN
                            (
                                CLOSING_REASON = 'Успешно реализовано'
                                OR STATUS_ID = 142
                            )
                            THEN REVENUE
                        ELSE 0
                    END
                )
                - SUM(COALESCE(DAILY_SPENT, 0))
            )
            / NULLIF(SUM(COALESCE(DAILY_SPENT, 0)), 0) * 100 BETWEEN 0 AND 50
            THEN 'Поддерживать текущий уровень'
        WHEN
            (
                SUM(
                    CASE
                        WHEN
                            (
                                CLOSING_REASON = 'Успешно реализовано'
                                OR STATUS_ID = 142
                            )
                            THEN REVENUE
                        ELSE 0
                    END
                )
                - SUM(COALESCE(DAILY_SPENT, 0))
            )
            / NULLIF(SUM(COALESCE(DAILY_SPENT, 0)), 0) * 100 < 0
            THEN 'Сократить или оптимизировать'
        ELSE 'Требуется дополнительный анализ'
    END AS BUDGET_RECOMMENDATION
FROM LASTPAIDCLICK
WHERE RN = 1
GROUP BY
    UTM_SOURCE,
    UTM_CAMPAIGN
ORDER BY
    ROI DESC;
