-- Databricks notebook source
CREATE OR REPLACE TABLE pulsetrack.gold.gold_category_performance AS

SELECT

    p.category,

    COUNT(DISTINCT o.order_id) AS total_orders,

    SUM(o.quantity) AS total_quantity_sold,

    SUM(o.price) AS total_revenue,

    COUNT(DISTINCT o.user_id) AS unique_customers

FROM pulsetrack.silver.silver_orders o

LEFT JOIN pulsetrack.silver.silver_products p
    ON o.product_id = p.product_id

WHERE
    o.status = 'completed'
    AND o.price > 0
    AND category IS NOT NULL

GROUP BY p.category;

-- COMMAND ----------

SELECT *
FROM pulsetrack.gold.gold_category_performance
ORDER BY total_revenue DESC;