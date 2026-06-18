"""
iceberg_maintenance.py
----------------------
Out-of-band table maintenance for streaming Iceberg tables. Run on a schedule
(e.g. hourly via Airflow/cron) so it never blocks the ingest path.

Covers the four jobs that keep a streaming lakehouse healthy:
  1. rewrite_data_files          -> compact small files (the #1 streaming issue)
  2. rewrite_position_delete_files -> bound merge-on-read read amplification
  3. expire_snapshots            -> drop old snapshots/metadata (keep time-travel window)
  4. remove_orphan_files         -> clean files left by failed commits

Author: Feodor Fernando
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

CATALOG = "lakehouse"


def build_spark(warehouse: str) -> SparkSession:
    return (
        SparkSession.builder.appName("iceberg-maintenance")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .getOrCreate()
    )


def compact(spark: SparkSession, table: str, target_mb: int = 128) -> None:
    """Bin-pack small files up to the target size and rewrite position deletes."""
    target_bytes = target_mb * 1024 * 1024
    print(f"-> compacting {table} (target {target_mb} MB)")
    spark.sql(
        f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table => '{table}',
            options => map(
                'target-file-size-bytes', '{target_bytes}',
                'min-input-files', '5'
            )
        )
        """
    ).show(truncate=False)

    spark.sql(
        f"CALL {CATALOG}.system.rewrite_position_delete_files(table => '{table}')"
    ).show(truncate=False)


def expire(spark: SparkSession, table: str, retain_days: int = 7) -> None:
    """Drop snapshots older than the time-travel window."""
    print(f"-> expiring snapshots on {table} older than {retain_days}d")
    spark.sql(
        f"""
        CALL {CATALOG}.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{_days_ago(retain_days)}',
            retain_last => 5
        )
        """
    ).show(truncate=False)


def remove_orphans(spark: SparkSession, table: str) -> None:
    """Remove files no snapshot references (e.g. from failed micro-batch commits)."""
    print(f"-> removing orphan files on {table}")
    spark.sql(
        f"CALL {CATALOG}.system.remove_orphan_files(table => '{table}')"
    ).show(truncate=False)


def rewrite_manifests(spark: SparkSession, table: str) -> None:
    print(f"-> rewriting manifests on {table}")
    spark.sql(
        f"CALL {CATALOG}.system.rewrite_manifests(table => '{table}')"
    ).show(truncate=False)


def _days_ago(days: int) -> str:
    """Return a 'yyyy-MM-dd HH:mm:ss' literal `days` before now (computed in SQL-safe form)."""
    from datetime import datetime, timedelta, timezone

    ts = datetime.now(timezone.utc) - timedelta(days=days)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", default="/tmp/iceberg-warehouse")
    parser.add_argument("--table", default="lakehouse.curated.events_silver")
    parser.add_argument("--target-mb", type=int, default=128)
    parser.add_argument("--retain-days", type=int, default=7)
    args = parser.parse_args()

    spark = build_spark(args.warehouse)
    spark.sparkContext.setLogLevel("WARN")

    compact(spark, args.table, args.target_mb)
    rewrite_manifests(spark, args.table)
    expire(spark, args.table, args.retain_days)
    remove_orphans(spark, args.table)
    print("maintenance complete.")


if __name__ == "__main__":
    main()
