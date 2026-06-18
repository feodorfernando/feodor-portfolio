-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Gold aggregates + a Genie-ready semantic layer
-- MAGIC Build BI-ready gold tables, then add the metadata (comments, certified
-- MAGIC metrics) that makes **Databricks Genie / AI-BI** answer natural-language
-- MAGIC questions accurately. Good comments + a clean star schema are the single
-- MAGIC biggest lever on text-to-SQL quality.
-- MAGIC
-- MAGIC Author: Feodor Fernando

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS lakehouse.gold.revenue_daily AS
SELECT
    date(event_time)              AS date,
    region,
    count(*)                      AS txn_count,
    round(sum(amount), 2)         AS revenue,
    round(avg(amount), 2)         AS avg_order_value,
    count(distinct user_id)       AS active_users
FROM lakehouse.silver.events
GROUP BY date(event_time), region;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Semantic metadata for Genie
-- MAGIC Genie grounds NL→SQL in table/column comments and certified metrics.
-- MAGIC Describing columns in business language is what stops it guessing.

-- COMMAND ----------
COMMENT ON TABLE lakehouse.gold.revenue_daily IS
  'Daily revenue and engagement by region. One row per (date, region). Source of truth for revenue reporting.';

ALTER TABLE lakehouse.gold.revenue_daily ALTER COLUMN revenue
  COMMENT 'Total gross revenue in USD for the date/region. Sum of order amounts.';
ALTER TABLE lakehouse.gold.revenue_daily ALTER COLUMN active_users
  COMMENT 'Distinct users who transacted that day in that region.';
ALTER TABLE lakehouse.gold.revenue_daily ALTER COLUMN avg_order_value
  COMMENT 'Average order value (AOV) = revenue / txn_count.';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Certified metric view (the layer Genie should prefer)
-- MAGIC A curated view encodes the *correct* business definitions so Genie does
-- MAGIC not reinvent them. Example NL questions this enables:
-- MAGIC  - "What was revenue last week by region?"
-- MAGIC  - "Which region had the highest AOV in May?"
-- MAGIC  - "Show the 7-day revenue trend."

-- COMMAND ----------
CREATE OR REPLACE VIEW lakehouse.gold.metrics_revenue
  COMMENT 'Certified revenue metrics for AI-BI / Genie. Prefer this view for revenue questions.'
AS
SELECT
    date,
    region,
    revenue,
    txn_count,
    avg_order_value,
    active_users,
    sum(revenue) OVER (PARTITION BY region ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_7d_rolling
FROM lakehouse.gold.revenue_daily;
