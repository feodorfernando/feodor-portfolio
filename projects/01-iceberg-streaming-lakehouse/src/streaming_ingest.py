"""
streaming_ingest.py
-------------------
Spark Structured Streaming -> Apache Iceberg (merge-on-read upserts).

Reads a Kafka topic of JSON events, lands raw rows in a bronze Iceberg table
(append-only), then MERGEs deduplicated/corrected rows into a silver table using
merge-on-read so per-micro-batch commits stay cheap.

Run locally (see docker-compose.yml for Kafka + a local catalog):

    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        src/streaming_ingest.py

Author: Feodor Fernando
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# --- event contract -------------------------------------------------------- #
# Incoming Kafka value is JSON shaped like:
#   {"event_id": "...", "user_id": "...", "amount": 12.5, "event_time": "2026-06-18T10:00:00Z"}
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("amount", DoubleType(), nullable=True),
        StructField("event_time", TimestampType(), nullable=True),
    ]
)

CATALOG = "lakehouse"
BRONZE = f"{CATALOG}.raw.events_bronze"
SILVER = f"{CATALOG}.curated.events_silver"


def build_spark(warehouse: str) -> SparkSession:
    """Spark session wired for a local Hadoop (filesystem) Iceberg catalog."""
    return (
        SparkSession.builder.appName("iceberg-streaming-ingest")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        # Fewer shuffle partitions keeps micro-batch files from fragmenting.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def ensure_tables(spark: SparkSession) -> None:
    """Create bronze (append) and silver (merge-on-read upsert) tables."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.raw")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.curated")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE} (
            event_id   STRING,
            user_id    STRING,
            amount     DOUBLE,
            event_time TIMESTAMP,
            ingest_ts  TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '134217728'  -- 128 MB
        )
        """
    )

    # Silver is upsert/dedup target: merge-on-read so streaming MERGEs are cheap.
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER} (
            event_id   STRING,
            user_id    STRING,
            amount     DOUBLE,
            event_time TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'write.update.mode'            = 'merge-on-read',
            'write.delete.mode'            = 'merge-on-read',
            'write.merge.mode'             = 'merge-on-read',
            'write.target-file-size-bytes' = '134217728',
            'format-version'               = '2'
        )
        """
    )


def upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: append to bronze, then MERGE de-duped rows into silver.

    Idempotent on replay: re-processing the same Kafka offsets re-runs the same
    MERGE, which is a no-op for rows already present at the same event_time.
    """
    spark = batch_df.sparkSession

    # Deduplicate within the micro-batch: keep the latest event per event_id.
    deduped = (
        batch_df.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("event_id").orderBy(F.col("event_time").desc())
            ),
        )
        .where(F.col("rn") == 1)
        .drop("rn")
    )

    deduped.withColumn("ingest_ts", F.current_timestamp()).writeTo(BRONZE).append()

    deduped.createOrReplaceTempView("incoming")
    spark.sql(
        f"""
        MERGE INTO {SILVER} t
        USING incoming s
        ON t.event_id = s.event_id
        WHEN MATCHED AND s.event_time > t.event_time THEN UPDATE SET
            t.user_id    = s.user_id,
            t.amount     = s.amount,
            t.event_time = s.event_time,
            t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (event_id, user_id, amount, event_time, updated_at)
            VALUES (s.event_id, s.user_id, s.amount, s.event_time, current_timestamp())
        """
    )
    print(f"[batch {batch_id}] merged {deduped.count()} rows into {SILVER}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--warehouse", default="/tmp/iceberg-warehouse")
    parser.add_argument("--checkpoint", default="/tmp/iceberg-checkpoints/events")
    parser.add_argument("--trigger", default="1 minute",
                        help="micro-batch interval; larger = fewer/bigger files")
    args = parser.parse_args()

    spark = build_spark(args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .where(F.col("event_id").isNotNull())
    )

    query = (
        parsed.writeStream.foreachBatch(upsert_batch)
        .option("checkpointLocation", args.checkpoint)  # one checkpoint per stream
        .trigger(processingTime=args.trigger)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
