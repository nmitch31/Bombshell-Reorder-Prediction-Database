--Code below rebuilds reorder stats table with capped gaps at 365 days. Excludes long gaps from cadence calculation but does not delete them from data
--Keeps all customers, prevents 2-4 year gaps from poisoning averages, preserves max_gap_days for churn / reactivation logic

DROP TABLE IF EXISTS analytics.customer_reorder_stats_v2;

CREATE TABLE analytics.customer_reorder_stats_v2 AS
SELECT
    billing_name,

    COUNT(*) - 1 AS reorder_count,

    AVG(
        EXTRACT(EPOCH FROM time_since_last_order) / 86400
    ) FILTER (
        WHERE EXTRACT(EPOCH FROM time_since_last_order) / 86400 <= 365
    ) AS avg_days_between_orders,

    MAX(order_date) AS last_order_date,

    MIN(order_date) AS first_order_date,

    MAX(
        EXTRACT(EPOCH FROM time_since_last_order) / 86400
    ) AS max_gap_days

FROM analytics.customer_order_facts
WHERE time_since_last_order IS NOT NULL
GROUP BY billing_name;

--Create prediction table with eligibility rules
--Code below keeps everyone, predicts only when eligible, flags extreme cases instead of deleting them:

DROP TABLE IF EXISTS analytics.customer_reorder_predictions_v2;

CREATE TABLE analytics.customer_reorder_predictions_v2 AS
SELECT
    billing_name,
    reorder_count,
    avg_days_between_orders,
    first_order_date,
    last_order_date,
    max_gap_days,

    
    CASE
        WHEN reorder_count < 2 THEN false
        WHEN last_order_date < CURRENT_DATE - INTERVAL '24 months' THEN false
        WHEN avg_days_between_orders IS NULL THEN false
        ELSE true
    END AS is_prediction_eligible,

    
    CASE
        WHEN reorder_count < 2 THEN NULL
        WHEN last_order_date < CURRENT_DATE - INTERVAL '24 months' THEN NULL
        WHEN avg_days_between_orders IS NULL THEN NULL
        ELSE last_order_date
             + (avg_days_between_orders * INTERVAL '1 day')
    END AS predicted_next_order_date

FROM analytics.customer_reorder_stats_v2;

-- Prediction eligibility
-- Predicted next order date (only if eligible)


--add overdue calculation: 
ALTER TABLE analytics.customer_reorder_predictions_v2
ADD COLUMN days_overdue numeric;

UPDATE analytics.customer_reorder_predictions_v2
SET days_overdue =
    CASE
        WHEN predicted_next_order_date IS NULL THEN NULL
        ELSE
            EXTRACT(
                EPOCH FROM (CURRENT_DATE - predicted_next_order_date)
            ) / 86400
    END;

-- Explicitly label churned, reactivated, and active customers:
ALTER TABLE analytics.customer_reorder_predictions_v2
ADD COLUMN customer_status text;

UPDATE analytics.customer_reorder_predictions_v2
SET customer_status =
    CASE
        WHEN last_order_date < CURRENT_DATE - INTERVAL '36 months'
            THEN 'churned'

        WHEN max_gap_days > 730
             AND last_order_date >= CURRENT_DATE - INTERVAL '24 months'
            THEN 'reactivated'

        WHEN last_order_date BETWEEN
             CURRENT_DATE - INTERVAL '24 months'
             AND CURRENT_DATE - INTERVAL '12 months'
            THEN 'at_risk'

        WHEN last_order_date >= CURRENT_DATE - INTERVAL '12 months'
            THEN 'active'

        ELSE 'unknown'
    END;

-- flag extreme negatives/meaningless predictions:
ALTER TABLE analytics.customer_reorder_predictions_v2
ADD COLUMN prediction_quality text;

UPDATE analytics.customer_reorder_predictions_v2
SET prediction_quality =
    CASE
        WHEN is_prediction_eligible = false
            THEN 'not_predicted'

        WHEN days_overdue < -365
            THEN 'far_future_prediction'

        WHEN days_overdue BETWEEN -365 AND 0
            THEN 'future'

        WHEN days_overdue BETWEEN 0 AND 90
            THEN 'slightly_overdue'

        WHEN days_overdue > 90
            THEN 'severely_overdue'

        ELSE 'unknown'
    END;
