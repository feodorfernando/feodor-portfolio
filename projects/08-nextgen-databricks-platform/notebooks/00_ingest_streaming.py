# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Streaming ingestion → bronze (environment-aware)
# MAGIC Auto Loader streams new files from ADLS gen2 into the bronze Delta table for
# MAGIC **whatever environment this runs in** — dev workspace writes `dev_catalog`,
# MAGIC prod writes `prod_catalog`. Same code, no edits between environments.
# MAGIC
# MAGIC Author: Feodor Fernando

# COMMAND ----------
# env param is passed by the job (CI/CD); falls back to workspace/url detection.
dbutils.widgets.text("env", "")

from config.environments import get_config
from src.secrets import configure_storage_access
from src.spark_resilience import tune_session

cfg = get_config(spark, dbutils)              # <- resolves dev / test / prod
tune_session(spark, cfg.max_autoscale_workers)
configure_storage_access(spark, dbutils, cfg) # secrets from Key Vault-backed scope
print(f"environment = {cfg.env}  ->  catalog = {cfg.catalog}")

# COMMAND ----------
RAW   = cfg.abfss("landing", "events")                       # source files
TABLE = cfg.table(cfg.bronze_schema, "events")               # e.g. prod_catalog.bronze.events
CHK   = cfg.abfss("bronze", "_checkpoints/events")
SCHEMA= cfg.abfss("bronze", "_schema/events")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.bronze_schema}")

# COMMAND ----------
from pyspark.sql import functions as F

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.maxBytesPerTrigger", "1g")   # bound batch size -> bound memory (anti-OOM)
    .load(RAW)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------
(
    stream.writeStream.format("delta")
    .option("checkpointLocation", CHK)               # exactly-once via checkpoint
    .option("mergeSchema", "true")
    .trigger(availableNow=True)                       # batch-style: drain new files, then stop
    .toTable(TABLE)
)
print(f"streamed into {TABLE}")
