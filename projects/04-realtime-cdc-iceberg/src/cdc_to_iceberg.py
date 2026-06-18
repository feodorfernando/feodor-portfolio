"""
cdc_to_iceberg.py
-----------------
Apply Debezium CDC events from Kafka to an Iceberg table, mirroring a source
RDBMS table with inserts, updates, and deletes — using merge-on-read so deletes
and updates are cheap.

Each micro-batch:
  1. parse the Debezium envelope (before / after / op / ts_ms / lsn)
  2. collapse to the latest change per primary key within the batch
  3. MERGE: upsert non-deletes, delete tombstones — guarded by LSN so replay
     and out-of-order events are idempotent.

Run:
    spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      src/cdc_to_iceberg.py --topic dbserver.public.orders --pk id

Author: Feodor Fernando
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, MapType,
)

CATALOG = "lakehouse"

# Minimal Debezium envelope. `before`/`after` kept as JSON maps so this works for
# any source table without a hard-coded row schema.
DEBEZIUM_SCHEMA = StructType(
    [
        StructField("op", StringType()),                       # c / u / d / r
        StructField("ts_ms", LongType()),
        StructField("before", MapType(StringType(), StringType())),
        StructField("after", MapType(StringType(), StringType())),
        StructField(
            "source",
            StructType([StructField("lsn", LongType()), StructField("table", StringType())]),
        ),
    ]
)


def build_spark(warehouse: str) -> SparkSession:
    return (
        SparkSession.builder.appName("cdc-to-iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession, table: str, pk: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {pk}       STRING,
            payload    STRING,      -- full row as JSON; project columns downstream
            _op        STRING,
            _lsn       BIGINT,
            _ts_ms     BIGINT
        )
        USING iceberg
        TBLPROPERTIES (
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.merge.mode'  = 'merge-on-read',
            'format-version'    = '2'
        )
        """
    )


def make_apply_batch(table: str, pk: str):
    def apply_batch(batch_df: DataFrame, batch_id: int) -> None:
        spark = batch_df.sparkSession

        events = (
            batch_df.select(
                F.from_json(F.col("value").cast("string"), DEBEZIUM_SCHEMA).alias("e")
            )
            .select("e.*")
            .where(F.col("op").isNotNull())
        )

        # Row image: deletes use `before`, everything else uses `after`.
        imaged = events.withColumn(
            "image", F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after"))
        ).select(
            F.col("image").getItem(pk).alias(pk),
            F.to_json(F.col("image")).alias("payload"),
            F.col("op").alias("_op"),
            F.col("source.lsn").alias("_lsn"),
            F.col("ts_ms").alias("_ts_ms"),
        ).where(F.col(pk).isNotNull())

        # Collapse to the latest change per PK within this batch (by LSN).
        latest = (
            imaged.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy(pk).orderBy(F.col("_lsn").desc_nulls_last())
                ),
            )
            .where(F.col("rn") == 1)
            .drop("rn")
        )
        latest.createOrReplaceTempView("cdc_batch")

        # LSN guard => idempotent under replay / out-of-order delivery.
        spark.sql(
            f"""
            MERGE INTO {table} t
            USING cdc_batch s
            ON t.{pk} = s.{pk}
            WHEN MATCHED AND s._op = 'd' AND s._lsn >= t._lsn THEN DELETE
            WHEN MATCHED AND s._op <> 'd' AND s._lsn > t._lsn THEN UPDATE SET
                t.payload = s.payload, t._op = s._op, t._lsn = s._lsn, t._ts_ms = s._ts_ms
            WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT
                ({pk}, payload, _op, _lsn, _ts_ms)
                VALUES (s.{pk}, s.payload, s._op, s._lsn, s._ts_ms)
            """
        )
        print(f"[batch {batch_id}] applied {latest.count()} CDC changes to {table}")

    return apply_batch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", required=True, help="Debezium topic, e.g. dbserver.public.orders")
    parser.add_argument("--pk", default="id", help="primary key column name")
    parser.add_argument("--table", default="lakehouse.cdc.orders")
    parser.add_argument("--warehouse", default="/tmp/iceberg-warehouse")
    parser.add_argument("--checkpoint", default="/tmp/iceberg-checkpoints/cdc-orders")
    parser.add_argument("--trigger", default="30 seconds")
    args = parser.parse_args()

    spark = build_spark(args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark, args.table, args.pk)

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")  # include Debezium initial snapshot
        .load()
    )

    query = (
        stream.writeStream.foreachBatch(make_apply_batch(args.table, args.pk))
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
