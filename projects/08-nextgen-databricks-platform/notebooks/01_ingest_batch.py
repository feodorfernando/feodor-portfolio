# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Batch ingestion → bronze (environment-aware)
# MAGIC For periodic full/incremental loads from operational stores or vendor drops.
# MAGIC Uses `COPY INTO` for idempotent, exactly-once file ingestion (re-running skips
# MAGIC already-loaded files). Same env-aware routing as the streaming path.
# MAGIC
# MAGIC Author: Feodor Fernando

# COMMAND ----------
dbutils.widgets.text("env", "")
dbutils.widgets.text("source_path", "")

from config.environments import get_config, assert_safe_to_write
from src.secrets import configure_storage_access
from src.spark_resilience import tune_session

cfg = get_config(spark, dbutils)
tune_session(spark, cfg.max_autoscale_workers)
configure_storage_access(spark, dbutils, cfg)
print(f"environment = {cfg.env}  ->  catalog = {cfg.catalog}")

# COMMAND ----------
TABLE  = cfg.table(cfg.bronze_schema, "orders")
source = dbutils.widgets.get("source_path") or cfg.abfss("landing", "orders")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.bronze_schema}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        order_id STRING, customer_id STRING, amount DOUBLE,
        status STRING, updated_at TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------
# COPY INTO is idempotent: files already ingested are skipped on rerun (exactly-once batch).
spark.sql(f"""
    COPY INTO {TABLE}
    FROM '{source}'
    FILEFORMAT = PARQUET
    COPY_OPTIONS ('mergeSchema' = 'true')
""")
print(f"batch-loaded into {TABLE}")

# COMMAND ----------
# MAGIC %sql
# MAGIC -- freshness/volume sanity check (cheap data-quality gate)
# MAGIC SELECT count(*) AS rows, max(updated_at) AS latest FROM IDENTIFIER(:TABLE)
