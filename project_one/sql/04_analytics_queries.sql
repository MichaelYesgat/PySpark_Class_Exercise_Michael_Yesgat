-- Project 1 - Analytical SQL queries for reporting
-- Run this file after the data has been loaded and validated.

USE DATABASE RETAIL_PIPELINE_DB;


-- 1. Overall retail performance.
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_units,
    ROUND(SUM(calculated_total_amount), 2) AS total_revenue,
    ROUND(AVG(calculated_total_amount), 2) AS average_order_value
FROM CURATED.FACT_ORDERS;


-- 2. Revenue and estimated profit by product category.
SELECT
    p.category,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.quantity) AS total_units,
    ROUND(SUM(o.calculated_total_amount), 2) AS total_revenue,
    ROUND(
        SUM(
            o.calculated_total_amount
            - (o.quantity * p.cost)
        ),
        2
    ) AS estimated_profit
FROM CURATED.FACT_ORDERS AS o
JOIN CURATED.DIM_PRODUCTS AS p
    ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;


-- 3. Top 10 customers by revenue.
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    c.state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.quantity) AS total_units,
    ROUND(SUM(o.calculated_total_amount), 2) AS total_revenue
FROM CURATED.FACT_ORDERS AS o
JOIN CURATED.DIM_CUSTOMERS AS c
    ON o.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    c.state
ORDER BY total_revenue DESC
LIMIT 10;


-- 4. Top 10 products by quantity sold.
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.quantity) AS total_units,
    ROUND(SUM(o.calculated_total_amount), 2) AS total_revenue,
    ROUND(
        SUM(
            o.calculated_total_amount
            - (o.quantity * p.cost)
        ),
        2
    ) AS estimated_profit
FROM CURATED.FACT_ORDERS AS o
JOIN CURATED.DIM_PRODUCTS AS p
    ON o.product_id = p.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.category,
    p.brand
ORDER BY total_units DESC, total_revenue DESC
LIMIT 10;


-- 5. Revenue by order status and payment method.
SELECT
    order_status,
    payment_method,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_units,
    ROUND(SUM(calculated_total_amount), 2) AS total_revenue
FROM CURATED.FACT_ORDERS
GROUP BY
    order_status,
    payment_method
ORDER BY total_revenue DESC;


-- 6. Monthly sales trend.
SELECT
    DATE_TRUNC('month', order_date) AS sales_month,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_units,
    ROUND(SUM(calculated_total_amount), 2) AS total_revenue,
    ROUND(AVG(calculated_total_amount), 2) AS average_order_value
FROM CURATED.FACT_ORDERS
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY sales_month;


-- 7. Average shipping time by order status.
SELECT
    order_status,
    COUNT(*) AS shipped_order_count,
    ROUND(
        AVG(DATEDIFF('day', order_date, ship_date)),
        2
    ) AS average_shipping_days
FROM CURATED.FACT_ORDERS
WHERE ship_date IS NOT NULL
GROUP BY order_status
ORDER BY average_shipping_days;
