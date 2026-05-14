-- Databricks notebook source
CREATE OR REPLACE TABLE pulsetrack.gold.gold_mau AS

SELECT

    YEAR(event_time) AS year,

    MONTH(event_time) AS month,

    COUNT(DISTINCT user_id) AS monthly_active_users

FROM pulsetrack.silver.silver_events

WHERE
    user_id IS NOT NULL
    AND event_time IS NOT NULL

GROUP BY
    YEAR(event_time),
    MONTH(event_time);

-- COMMAND ----------

SELECT *
FROM pulsetrack.gold.gold_mau
ORDER BY year, month;