-- Запрос 1: Основные данные для дашборда
WITH PAID_CLICKS AS (
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
LAST_PAID_CLICK AS (
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
    LEFT JOIN PAID_CLICKS AS PC
        ON S.VISIT_DATE = PC.CAMPAIGN_DATE
        AND S.SOURCE = PC.UTM_SOURCE
        AND S.MEDIUM = PC.UTM_MEDIUM
        AND S.CAMPAIGN = PC.UTM_CAMPAIGN
    LEFT JOIN LEADS AS L
        ON S.VISITOR_ID = L.VISITOR_ID
        AND L.CREATED_AT >= S.VISIT_DATE
)

SELECT
    VISIT_DATE,
    UTM_SOURCE,
    UTM_MEDIUM,
    UTM_CAMPAIGN,
    COUNT(DISTINCT VISITOR_ID) AS VISITORS_COUNT,
    SUM(COALESCE(DAILY_SPENT, 0)) AS TOTAL_COST,
    COUNT(DISTINCT LEAD_ID) AS LEADS_COUNT,
    COUNT(DISTINCT CASE
        WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
        THEN LEAD_ID
    END) AS PURCHASES_COUNT,
    SUM(CASE
        WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
        THEN REVENUE
        ELSE 0
    END) AS REVENUE,
    CASE
        WHEN COUNT(DISTINCT VISITOR_ID) > 0
        THEN SUM(COALESCE(DAILY_SPENT, 0)) / COUNT(DISTINCT VISITOR_ID)
        ELSE 0
    END AS CPU,
    CASE
        WHEN COUNT(DISTINCT LEAD_ID) > 0
        THEN SUM(COALESCE(DAILY_SPENT, 0)) / COUNT(DISTINCT LEAD_ID)
        ELSE 0
    END AS CPL,
    CASE
        WHEN COUNT(DISTINCT CASE
                WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
                THEN LEAD_ID
            END) > 0
        THEN SUM(COALESCE(DAILY_SPENT, 0)) / COUNT(DISTINCT CASE
                WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
                THEN LEAD_ID
            END)
        ELSE 0
    END AS CPPU,
    CASE
        WHEN SUM(COALESCE(DAILY_SPENT, 0)) > 0
        THEN (SUM(CASE
                WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
                THEN REVENUE
                ELSE 0
            END) - SUM(COALESCE(DAILY_SPENT, 0))) / SUM(COALESCE(DAILY_SPENT, 0)) * 100
        ELSE 0
    END AS ROI
FROM LAST_PAID_CLICK
WHERE RN = 1
GROUP BY
    VISIT_DATE,
    UTM_SOURCE,
    UTM_MEDIUM,
    UTM_CAMPAIGN
ORDER BY
    REVENUE DESC NULLS LAST,
    VISIT_DATE ASC,
    VISITORS_COUNT DESC,
    UTM_SOURCE ASC,
    UTM_MEDIUM ASC,
    UTM_CAMPAIGN ASC;

-- Запрос 2: Анализ скорости закрытия лидов
WITH LEAD_CONVERSION AS (
    SELECT
        L.VISITOR_ID,
        L.LEAD_ID,
        L.CREATED_AT AS LEAD_DATE,
        L.CLOSING_REASON,
        L.STATUS_ID,
        MIN(S.VISIT_DATE) AS FIRST_VISIT_DATE,
        MAX(S.VISIT_DATE) AS LAST_VISIT_DATE,
        DATEDIFF('day', MIN(S.VISIT_DATE), L.CREATED_AT) AS DAYS_TO_CONVERT
    FROM LEADS AS L
    INNER JOIN SESSIONS AS S
        ON L.VISITOR_ID = S.VISITOR_ID
        AND L.CREATED_AT >= S.VISIT_DATE
    GROUP BY
        L.VISITOR_ID,
        L.LEAD_ID,
        L.CREATED_AT,
        L.CLOSING_REASON,
        L.STATUS_ID
)

SELECT
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY DAYS_TO_CONVERT
    ) AS MEDIAN_DAYS_TO_CONVERT,
    PERCENTILE_CONT(0.9) WITHIN GROUP (
        ORDER BY DAYS_TO_CONVERT
    ) AS P90_DAYS_TO_CONVERT,
    COUNT(*) FILTER (WHERE DAYS_TO_CONVERT <= 1) AS CONVERTED_WITHIN_1_DAY,
    COUNT(*) FILTER (WHERE DAYS_TO_CONVERT <= 3) AS CONVERTED_WITHIN_3_DAYS,
    COUNT(*) FILTER (WHERE DAYS_TO_CONVERT <= 7) AS CONVERTED_WITHIN_1_WEEK,
    COUNT(*) FILTER (WHERE DAYS_TO_CONVERT <= 30) AS CONVERTED_WITHIN_1_MONTH,
    ROUND(
        COUNT(*) FILTER (WHERE DAYS_TO_CONVERT <= 3) / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS PCT_CONVERTED_IN_3_DAYS,
    ROUND(
        COUNT(*) FILTER (
            WHERE CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142
        ) / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS SUCCESS_RATE
FROM LEAD_CONVERSION;

-- Запрос 3: Динамика эффективности по неделям
WITH PAID_CLICKS AS (
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
LAST_PAID_CLICK AS (
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
    LEFT JOIN PAID_CLICKS AS PC
        ON S.VISIT_DATE = PC.CAMPAIGN_DATE
        AND S.SOURCE = PC.UTM_SOURCE
        AND S.MEDIUM = PC.UTM_MEDIUM
        AND S.CAMPAIGN = PC.UTM_CAMPAIGN
    LEFT JOIN LEADS AS L
        ON S.VISITOR_ID = L.VISITOR_ID
        AND S.VISIT_DATE <= L.CREATED_AT
)

SELECT
    UTM_SOURCE,
    DATE_TRUNC('week', VISIT_DATE) AS WEEK_START,
    COUNT(DISTINCT VISITOR_ID) AS WEEKLY_VISITORS,
    COUNT(DISTINCT LEAD_ID) AS WEEKLY_LEADS,
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
                    WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
                    THEN REVENUE
                    ELSE 0
                END
            ) - SUM(COALESCE(DAILY_SPENT, 0))
        ) / NULLIF(SUM(COALESCE(DAILY_SPENT, 0)), 0) * 100,
        2
    ) AS WEEKLY_ROI,
    ROUND(
        COUNT(DISTINCT LEAD_ID) / NULLIF(COUNT(DISTINCT VISITOR_ID), 0) * 100,
        2
    ) AS WEEKLY_CONVERSION_RATE
FROM LAST_PAID_CLICK
WHERE RN = 1
GROUP BY
    WEEK_START,
    UTM_SOURCE
ORDER BY
    WEEK_START ASC,
    WEEKLY_ROI DESC;

-- Запрос 4: Рекомендации по оптимизации бюджета
WITH PAID_CLICKS AS (
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
LAST_PAID_CLICK AS (
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
    LEFT JOIN PAID_CLICKS AS PC
        ON S.VISIT_DATE = PC.CAMPAIGN_DATE
        AND S.SOURCE = PC.UTM_SOURCE
        AND S.MEDIUM = PC.UTM_MEDIUM
        AND S.CAMPAIGN = PC.UTM_CAMPAIGN
    LEFT JOIN LEADS AS L
        ON S.VISITOR_ID = L.VISITOR_ID
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
                    WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
                    THEN REVENUE
                    ELSE 0
                END
            ) - SUM(COALESCE(DAILY_SPENT, 0))
        ) / NULLIF(SUM(COALESCE(DAILY_SPENT, 0)), 0) * 100,
        2
    ) AS ROI,
    CASE
        WHEN (
            SUM(
                CASE
                    WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
                    THEN REVENUE
                    ELSE 0
                END
            ) - SUM(COALESCE(DAILY_SPENT, 0))
        ) / NULLIF(SUM(COALESCE(DAILY_SPENT, 0)), 0) * 100 > 50
            THEN 'Увеличить бюджет'
        WHEN (
            SUM(
                CASE
                    WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
                    THEN REVENUE
                    ELSE 0
                END
            ) - SUM(COALESCE(DAILY_SPENT, 0))
        ) / NULLIF(SUM(COALESCE(DAILY_SPENT, 0)), 0) * 100 BETWEEN 0 AND 50
            THEN 'Поддерживать текущий уровень'
        WHEN (
            SUM(
                CASE
                    WHEN (CLOSING_REASON = 'Успешно реализовано' OR STATUS_ID = 142)
                    THEN REVENUE
                    ELSE 0
                END
            ) - SUM(COALESCE(DAILY_SPENT, 0))
        ) / NULLIF(SUM(COALESCE(DAILY_SPENT, 0)), 0) * 100 < 0
            THEN 'Сократить или оптимизировать'
        ELSE 'Требуется дополнительный анализ'
    END AS BUDGET_RECOMMENDATION
FROM LAST_PAID_CLICK
WHERE RN = 1
GROUP BY
    UTM_SOURCE,
    UTM_CAMPAIGN
ORDER BY
    ROI DESC;
