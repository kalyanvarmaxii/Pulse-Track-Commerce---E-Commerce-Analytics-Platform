-- Databricks notebook source
CREATE OR REPLACE TABLE pulsetrack.gold.gold_dau AS

SELECT

    DATE(event_time) AS activity_date,

    COUNT(DISTINCT user_id) AS daily_active_users

FROM pulsetrack.silver.silver_events

WHERE
    user_id IS NOT NULL
    AND event_time IS NOT NULL

GROUP BY DATE(event_time);

-- COMMAND ----------

select * from pulsetrack.gold.gold_dau

-- COMMAND ----------

