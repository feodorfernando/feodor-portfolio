# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingest with Auto Loader
# MAGIC Incrementally load raw files from ADLS gen2 into a Delta bronze table.
# MAGIC Auto Loader tracks new files via checkpoints, so reruns only pick up deltas.
# MAGIC
# MAGIC Author: Feodor Fernando

# COMMAND ----------
RAW_PATH    = "abfss://bronze@feodorlake.dfs.core.windows.net/raw/events"
BRONZE_TBL  = "lakehouse.bronze.events"
CHECKPOINT  = "abfss://bronze@feodorlake.dfs.core.windows.net/_checkpoints/events"
SCHEMA_LOC  = "abfss://bronze@feodorlake.dfs.core.windows.net/_schema/events"

# COMMAND ----------
from pyspark.sql import functions as F

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)      # schema inference + evolution
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(RAW_PATH)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------
(
    bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)        # batch-style: process all new files, then stop
    .toTable(BRONZE_TBL)
)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- quick health check
# MAGIC SELECT count(*) AS rows, max(_ingested_at) AS latest FROM lakehouse.bronze.events;
