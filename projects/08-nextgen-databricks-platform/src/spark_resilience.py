"""
spark_resilience.py
-------------------
Make Spark jobs survive the real world: OOM (the "container killed / GC overhead /
OutOfMemoryError"), data skew, transient cloud errors, and bad records.

Two layers:
  1) PREVENT  -> tune the session so OOM/skew rarely happen (AQE, skew join,
                 adaptive coalescing, sane shuffle + file sizes, no giant broadcasts).
  2) RECOVER  -> if a stage still fails with OOM/skew, an adaptive retry decorator
                 detects the failure class, *changes the configuration dynamically*
                 (more shuffle partitions, smaller input splits, disable broadcast),
                 and retries — instead of just dying.

This is the "dynamic way" the platform handles errors at runtime.

Author: Feodor Fernando
"""
from __future__ import annotations

import time
import functools


# Error signatures we know how to react to, grouped by remediation.
_OOM_SIGNS = ("OutOfMemoryError", "GC overhead limit", "Container killed",
              "java.lang.OutOfMemory", "ExecutorLostFailure", "exceeds memory limit")
_SKEW_SIGNS = ("skew", "single partition", "PartitionTooLargeException")
_TRANSIENT_SIGNS = ("Operation timed out", "Connection reset", "ServiceUnavailable",
                    "Too Many Requests", "429", "RequestTimeout", "SocketTimeout")


def tune_session(spark, max_workers: int = 8) -> None:
    """Layer 1: prevention. Apply the AQE-first defaults that avoid most OOM/skew."""
    conf = {
        # Adaptive Query Execution: the single biggest lever against OOM + skew.
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",   # avoid tiny/huge partitions
        "spark.sql.adaptive.skewJoin.enabled": "true",             # auto-split skewed partitions
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        # Right-size shuffle relative to cluster; AQE then coalesces down.
        "spark.sql.shuffle.partitions": str(max(200, max_workers * 32)),
        # Cap input split size so a single task can't load a giant file into memory.
        "spark.sql.files.maxPartitionBytes": "128m",
        # Don't broadcast large tables (a classic driver/executor OOM cause).
        "spark.sql.autoBroadcastJoinThreshold": "50m",
        # Spill sooner rather than OOM.
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
    }
    for k, v in conf.items():
        try:
            spark.conf.set(k, v)
        except Exception:
            pass  # some confs are static on certain runtimes; ignore


def _classify(err: Exception) -> str:
    msg = str(err)
    if any(s in msg for s in _OOM_SIGNS):
        return "oom"
    if any(s in msg for s in _SKEW_SIGNS):
        return "skew"
    if any(s in msg for s in _TRANSIENT_SIGNS):
        return "transient"
    return "unknown"


def _remediate(spark, failure: str, attempt: int) -> None:
    """Layer 2: change the configuration in response to the failure class."""
    if failure == "oom":
        # More, smaller shuffle partitions => less memory per task.
        cur = int(spark.conf.get("spark.sql.shuffle.partitions", "200"))
        spark.conf.set("spark.sql.shuffle.partitions", str(cur * 2))
        # Smaller input splits => smaller tasks.
        spark.conf.set("spark.sql.files.maxPartitionBytes", "64m")
        # Stop broadcasting entirely — broadcast OOM is common and silent.
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    elif failure == "skew":
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
        cur = int(spark.conf.get("spark.sql.shuffle.partitions", "200"))
        spark.conf.set("spark.sql.shuffle.partitions", str(cur * 2))
    # transient: nothing to change, just back off and retry


def adaptive_run(spark, max_attempts: int = 3, base_backoff: float = 5.0):
    """Decorator: run a Spark action; on OOM/skew/transient failure, adjust conf and retry.

    Usage:
        @adaptive_run(spark)
        def build_silver():
            ...spark work that triggers an action (write/count/collect)...
        build_silver()
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as err:          # py4j wraps JVM errors as Python exceptions
                    failure = _classify(err)
                    last = err
                    if failure == "unknown" or attempt == max_attempts:
                        raise
                    print(f"[adaptive_run] attempt {attempt} failed ({failure}); "
                          f"remediating and retrying. cause: {str(err)[:160]}")
                    _remediate(spark, failure, attempt)
                    time.sleep(base_backoff * attempt)   # linear backoff
            raise last
        return wrapper
    return decorator


def salt_skewed_key(df, key: str, salt_buckets: int = 16):
    """Manual skew fix for hot keys: explode a salt so one mega-key spreads across tasks.

    Use when AQE skew handling isn't enough (e.g. a single key is 90% of rows).
    Pair with a salted join/group, then strip the salt.
    """
    from pyspark.sql import functions as F
    return df.withColumn("_salt", (F.rand() * salt_buckets).cast("int")) \
             .withColumn(f"{key}_salted", F.concat_ws("_", F.col(key), F.col("_salt")))


def write_dead_letter(df, target_table: str, reason: str) -> None:
    """Quarantine bad records instead of failing the whole job (or dropping silently)."""
    from pyspark.sql import functions as F
    (df.withColumn("_dlq_reason", F.lit(reason))
       .withColumn("_dlq_ts", F.current_timestamp())
       .write.mode("append").saveAsTable(target_table))
