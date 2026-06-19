# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver (clean + dedup + MERGE) with adaptive OOM/skew handling
# MAGIC The transform most likely to OOM (big shuffle + join + MERGE). It is wrapped in
# MAGIC `adaptive_run`: if a stage fails with OOM or skew, the job **re-tunes the
# MAGIC configuration at runtime and retries** instead of dying.
# MAGIC
# MAGIC Author: Feodor Fernando

# COMMAND ----------
dbutils.widgets.text("env", "")

from config.environments import get_config, assert_safe_to_write
from src.spark_resilience import tune_session, adaptive_run, salt_skewed_key, write_dead_letter
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

cfg = get_config(spark, dbutils)
assert_safe_to_write(cfg)                 # refuses interactive prod writes
tune_session(spark, cfg.max_autoscale_workers)

BRONZE = cfg.table(cfg.bronze_schema, "events")
SILVER = cfg.table(cfg.silver_schema, "events")
DLQ    = cfg.table(cfg.silver_schema, "events_dlq")
print(f"{cfg.env}: {BRONZE} -> {SILVER}")

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.silver_schema}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER} (
        event_id STRING, user_id STRING, region STRING,
        amount DOUBLE, event_time TIMESTAMP, updated_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------
def build_clean():
    raw = spark.table(BRONZE)
    # split good vs bad: bad rows go to a dead-letter table, not silently dropped
    bad = raw.where(F.col("event_id").isNull() | F.col("amount").isNull() | (F.col("amount") < 0))
    if bad.head(1):
        write_dead_letter(bad, DLQ, "null_key_or_negative_amount")
    good = (raw.where(F.col("event_id").isNotNull() & (F.col("amount") >= 0))
               .withColumn("amount", F.col("amount").cast("double"))
               .withColumn("event_time", F.to_timestamp("event_time")))
    # latest per key
    return (good.withColumn("_rn", F.row_number().over(
                Window.partitionBy("event_id").orderBy(F.col("event_time").desc())))
               .where(F.col("_rn") == 1).drop("_rn"))

# COMMAND ----------
@adaptive_run(spark, max_attempts=3)      # <- OOM/skew -> re-tune conf + retry
def merge_silver():
    latest = build_clean()
    # If a single region/user is wildly skewed, salt it so one task can't OOM.
    # latest = salt_skewed_key(latest, "region")   # enable if AQE skew handling insufficient
    (DeltaTable.forName(spark, SILVER).alias("t")
        .merge(latest.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdate(condition="s.event_time > t.event_time", set={
            "user_id": "s.user_id", "region": "s.region", "amount": "s.amount",
            "event_time": "s.event_time", "updated_at": F.current_timestamp()})
        .whenNotMatchedInsert(values={
            "event_id": "s.event_id", "user_id": "s.user_id", "region": "s.region",
            "amount": "s.amount", "event_time": "s.event_time",
            "updated_at": F.current_timestamp()})
        .execute())

merge_silver()

# COMMAND ----------
# MAGIC %sql
# MAGIC OPTIMIZE IDENTIFIER(:SILVER) ZORDER BY (region, event_time);
