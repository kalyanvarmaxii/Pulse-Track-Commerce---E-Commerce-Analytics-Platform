-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW first_purchase AS

SELECT

    user_id,

    MIN(DATE(order_date)) AS first_purchase_date

FROM pulsetrack.silver.silver_orders

WHERE
    status = 'completed'
    AND user_id IS NOT NULL

GROUP BY user_id;

-- COMMAND ----------

CREATE OR REPLACE TABLE pulsetrack.gold.gold_retention_analysis AS

SELECT

    YEAR(fp.first_purchase_date) AS cohort_year,

    MONTH(fp.first_purchase_date) AS cohort_month,

    YEAR(o.order_date) AS activity_year,

    MONTH(o.order_date) AS activity_month,

    COUNT(DISTINCT o.user_id) AS retained_users

FROM pulsetrack.silver.silver_orders o

JOIN first_purchase fp
    ON o.user_id = fp.user_id

WHERE
    o.status = 'completed'

GROUP BY

    YEAR(fp.first_purchase_date),
    MONTH(fp.first_purchase_date),
    YEAR(o.order_date),
    MONTH(o.order_date);

-- COMMAND ----------

SELECT *
FROM pulsetrack.gold.gold_retention_analysis
ORDER BY cohort_year, cohort_month;