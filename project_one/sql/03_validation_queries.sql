-- Project 1 - Snowflake validation queries
-- Run this file after 02_snowflake_load.sql.

USE DATABASE RETAIL_PIPELINE_DB;


-- 1. Source-to-target counts calculated by PySpark.
SELECT
    dataset,
    source_record_count,
    target_record_count,
    removed_record_count
FROM DQ.DQ_SUMMARY
ORDER BY dataset;


-- 2. Compare the PySpark target counts with the rows loaded into Snowflake.
WITH snowflake_counts AS (
    SELECT 'customers' AS dataset, COUNT(*) AS snowflake_record_count
    FROM CURATED.DIM_CUSTOMERS

    UNION ALL

    SELECT 'products', COUNT(*)
    FROM CURATED.DIM_PRODUCTS

    UNION ALL

    SELECT 'orders', COUNT(*)
    FROM CURATED.FACT_ORDERS
)
SELECT
    dq.dataset,
    dq.target_record_count AS pyspark_target_count,
    sf.snowflake_record_count,
    CASE
        WHEN dq.target_record_count = sf.snowflake_record_count
            THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_result
FROM DQ.DQ_SUMMARY AS dq
JOIN snowflake_counts AS sf
    ON dq.dataset = sf.dataset
ORDER BY dq.dataset;


-- 3. Invalid customer foreign keys: should return 0 rows.
SELECT o.*
FROM CURATED.FACT_ORDERS AS o
LEFT JOIN CURATED.DIM_CUSTOMERS AS c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;


-- 4. Invalid product foreign keys: should return 0 rows.
SELECT o.*
FROM CURATED.FACT_ORDERS AS o
LEFT JOIN CURATED.DIM_PRODUCTS AS p
    ON o.product_id = p.product_id
WHERE p.product_id IS NULL;


-- 5. Invalid order business values: should return 0 rows.
SELECT *
FROM CURATED.FACT_ORDERS
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR product_id IS NULL
   OR order_date IS NULL
   OR quantity < 1
   OR quantity > 100
   OR unit_price <= 0
   OR discount_pct < 0
   OR discount_pct > 100
   OR calculated_total_amount <= 0
   OR ship_date < order_date;


-- 6. Duplicate order IDs: should return 0 rows.
SELECT
    order_id,
    COUNT(*) AS duplicate_count
FROM CURATED.FACT_ORDERS
GROUP BY order_id
HAVING COUNT(*) > 1;


-- 7. Recalculate each order total and compare it with the PySpark result.
-- This should return PASS for every order.
SELECT
    order_id,
    calculated_total_amount AS pyspark_total,
    ROUND(
        quantity * unit_price * (1 - discount_pct / 100),
        2
    ) AS snowflake_recalculated_total,
    CASE
        WHEN calculated_total_amount = ROUND(
            quantity * unit_price * (1 - discount_pct / 100),
            2
        )
            THEN 'PASS'
        ELSE 'FAIL'
    END AS validation_result
FROM CURATED.FACT_ORDERS
ORDER BY order_id;
