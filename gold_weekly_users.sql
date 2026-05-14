-- Databricks notebook source
CREATE OR REPLACE TABLE pulsetrack.gold.gold_wau AS

SELECT

    YEAR(event_time) AS year,

    WEEKOFYEAR(event_time) AS week_number,

    COUNT(DISTINCT user_id) AS weekly_active_users

FROM pulsetrack.silver.silver_events

WHERE
    user_id IS NOT NULL
    AND event_time IS NOT NULL

GROUP BY
    YEAR(event_time),
    WEEKOFYEAR(event_time);

-- COMMAND ----------

SELECT *
FROM pulsetrack.gold.gold_wau
ORDER BY year, week_number;