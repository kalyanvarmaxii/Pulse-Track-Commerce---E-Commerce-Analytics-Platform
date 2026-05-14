-- Databricks notebook source
CREATE OR REPLACE TABLE pulsetrack.gold.gold_product_performance AS

SELECT

    p.product_id,

    p.product_name,

    p.category,

    COUNT(DISTINCT o.order_id) AS total_orders,

    SUM(o.quantity) AS total_quantity_sold,

    SUM(o.price) AS total_revenue,

    AVG(o.price) AS average_selling_price

FROM pulsetrack.silver.silver_orders o

LEFT JOIN pulsetrack.silver.silver_products p
    ON o.product_id = p.product_id

WHERE
    o.status = 'completed'
    AND o.price > 0
    AND o.product_id IS NOT NULL
    AND p.product_id IS NOT NULL

GROUP BY

    p.product_id,
    p.product_name,
    p.category;

-- COMMAND ----------

select * from pulsetrack.gold.gold_product_performance
order by total_revenue desc


-- COMMAND ----------

