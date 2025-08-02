-- Выборка последнего рекламного касания для каждого посетителя (до конверсии)
-- с присоединением информации о лидах и платежах (кроме органического трафика)

WITH latest_paid_touch AS (
    SELECT
        visitor_id,
        visit_date,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign
    FROM (
        SELECT
            visitor_id,
            visit_date,
            source,
            medium,
            campaign,
            -- Ранжируем сессии по дате (самая свежая — первая)
            ROW_NUMBER() OVER (
                PARTITION BY visitor_id
                ORDER BY visit_date DESC
            ) AS session_order
        FROM sessions
        WHERE medium != 'organic'  -- исключаем органический трафик
    ) AS ranked_sessions
    WHERE session_order = 1  -- оставляем только последнюю сессию
)

-- Присоединяем данные о лидах: только те, что созданы после сессии
SELECT
    lpt.visitor_id,
    lpt.visit_date,
    lpt.utm_source,
    lpt.utm_medium,
    lpt.utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
FROM latest_paid_touch AS lpt
LEFT JOIN leads AS l
    ON
        lpt.visitor_id = l.visitor_id
        AND lpt.visit_date <= l.created_at  -- лид после или в день сессии
ORDER BY
    l.amount DESC NULLS LAST,           -- сначала крупные сделки
    lpt.visit_date ASC,                 -- затем по возрастанию даты визита
    lpt.utm_source ASC NULLS LAST,
    lpt.utm_medium ASC NULLS LAST,
    lpt.utm_campaign ASC NULLS LAST
LIMIT 10;
