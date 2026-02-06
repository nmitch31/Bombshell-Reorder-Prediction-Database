CREATE TABLE analytics.orders AS
SELECT
    "Billing Name"        AS billing_name,
    "Order ID"            AS order_id,
    MIN("Created at")::timestamptz     AS order_date,
    SUM("Subtotal")::numeric AS order_total
FROM analytics.shopify_orders_raw
GROUP BY
    "Billing Name",
    "Order ID";

CREATE TABLE analytics.customer_order_facts AS
SELECT
    billing_name,
    order_id,
    order_date,
    order_total,

    order_date
      - LAG(order_date) OVER (
            PARTITION BY billing_name
            ORDER BY order_date
        ) AS time_since_last_order

FROM analytics.orders
ORDER BY
    billing_name,
    order_date;


CREATE TABLE analytics.customer_reorder_stats AS
SELECT
    billing_name,
    COUNT(*) - 1 AS reorder_count,

    AVG(
        EXTRACT(EPOCH FROM time_since_last_order) / 86400
    ) AS avg_days_between_orders,

    MAX(order_date) AS last_order_date

FROM analytics.customer_order_facts
WHERE time_since_last_order IS NOT NULL
GROUP BY billing_name;

CREATE TABLE analytics.customer_reorder_predictions AS
SELECT
    billing_name,
    reorder_count,
    avg_days_between_orders,
    last_order_date,

    last_order_date
      + (avg_days_between_orders * INTERVAL '1 day')
        AS predicted_next_order_date

FROM analytics.customer_reorder_stats;


ALTER TABLE analytics.customer_reorder_predictions
ADD COLUMN days_overdue INT,
ADD COLUMN customer_status TEXT;


UPDATE analytics.customer_reorder_predictions
SET
    days_overdue = CURRENT_DATE - predicted_next_order_date::date,
    customer_status = CASE
        WHEN CURRENT_DATE <= predicted_next_order_date::date THEN 'Active'
        WHEN (CURRENT_DATE - predicted_next_order_date::date) BETWEEN 1 AND 90 THEN 'Slightly Overdue'
        WHEN (CURRENT_DATE - predicted_next_order_date::date) BETWEEN 91 AND 365 THEN 'At Risk'
        WHEN (CURRENT_DATE - predicted_next_order_date::date) BETWEEN 366 AND 730 THEN 'Lapsed'
        ELSE 'Dormant'
    END;
