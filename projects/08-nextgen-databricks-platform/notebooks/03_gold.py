# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold (BI-ready aggregates + semantic layer) — env-aware
# MAGIC Builds the gold tables CI/CD promotes all the way through, and annotates them so
# MAGIC the BI Copilot / Genie can answer questions accurately (see project 07).
# MAGIC
# MAGIC Author: Feodor Fernando

# COMMAND ----------
dbutils.widgets.text("env", "")
from config.environments import get_config, assert_safe_to_write
from src.spark_resilience import tune_session, adaptive_run
from pyspark.sql import functions as F

cfg = get_config(spark, dbutils)
assert_safe_to_write(cfg)
tune_session(spark, cfg.max_autoscale_workers)

SILVER = cfg.table(cfg.silver_schema, "events")
GOLD   = cfg.table(cfg.gold_schema, "revenue_daily")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.gold_schema}")
print(f"{cfg.env}: {SILVER} -> {GOLD}")

# COMMAND ----------
@adaptive_run(spark, max_attempts=3)
def build_gold():
    (spark.table(SILVER)
        .groupBy(F.to_date("event_time").alias("date"), "region")
        .agg({"amount": "sum", "event_id": "count", "user_id": "approx_count_distinct"})
        .withColumnRenamed("sum(amount)", "revenue")
        .withColumnRenamed("count(event_id)", "txn_count")
        .withColumnRenamed("approx_count_distinct(user_id)", "active_users")
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD))

build_gold()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Semantic metadata — makes BI Copilot / Genie accurate (see project 07 & paper 02)
# COMMAND ----------
spark.sql(f"COMMENT ON TABLE {GOLD} IS 'Daily revenue & engagement by region. One row per (date, region). Certified source of truth.'")
spark.sql(f"ALTER TABLE {GOLD} ALTER COLUMN revenue COMMENT 'Total gross revenue (USD) = sum of order amounts for the date/region.'")
spark.sql(f"ALTER TABLE {GOLD} ALTER COLUMN active_users COMMENT 'Distinct users who transacted that day in that region.'")
print("gold built + annotated")
