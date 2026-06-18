# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver transform (clean + dedup + SCD-friendly upsert)
# MAGIC Reads bronze, applies quality rules and type casting, then MERGEs into a
# MAGIC silver Delta table. Idempotent: rerunning a window is a no-op.
# MAGIC
# MAGIC Author: Feodor Fernando

# COMMAND ----------
dbutils.widgets.text("window_start", "1970-01-01")
window_start = dbutils.widgets.get("window_start")

BRONZE_TBL = "lakehouse.bronze.events"
SILVER_TBL = "lakehouse.silver.events"

# COMMAND ----------
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

src = (
    spark.table(BRONZE_TBL)
    .where(F.col("_ingested_at") >= F.lit(window_start))
    .where(F.col("event_id").isNotNull() & F.col("amount").isNotNull())
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("event_time", F.to_timestamp("event_time"))
    .where(F.col("amount") >= 0)                       # drop impossible negatives
)

# keep the latest record per event_id within the batch
latest = (
    src.withColumn(
        "_rn", F.row_number().over(
            Window.partitionBy("event_id").orderBy(F.col("event_time").desc())
        )
    )
    .where(F.col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------
# create silver table on first run
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TBL} (
        event_id STRING, user_id STRING, region STRING,
        amount DOUBLE, event_time TIMESTAMP, updated_at TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------
silver = DeltaTable.forName(spark, SILVER_TBL)
(
    silver.alias("t")
    .merge(latest.alias("s"), "t.event_id = s.event_id")
    .whenMatchedUpdate(
        condition="s.event_time > t.event_time",
        set={
            "user_id": "s.user_id", "region": "s.region", "amount": "s.amount",
            "event_time": "s.event_time", "updated_at": F.current_timestamp(),
        },
    )
    .whenNotMatchedInsert(values={
        "event_id": "s.event_id", "user_id": "s.user_id", "region": "s.region",
        "amount": "s.amount", "event_time": "s.event_time",
        "updated_at": F.current_timestamp(),
    })
    .execute()
)

# COMMAND ----------
# MAGIC %sql
# MAGIC OPTIMIZE lakehouse.silver.events ZORDER BY (region, event_time);  -- read perf
