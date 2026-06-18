# 🧊 Iceberg Streaming Lakehouse

Real-time ingestion from **Kafka → Apache Iceberg** with merge-on-read upserts,
late-data correction, and the maintenance jobs that keep a streaming table
healthy. Built with **Spark Structured Streaming**.

This project is both a working pipeline *and* a research note on Iceberg
internals — see [`docs/iceberg-streaming-research.md`](docs/iceberg-streaming-research.md).

## What it demonstrates

- **Bronze → Silver** layering: append-only raw landing, then idempotent
  `MERGE INTO` deduplication/upsert into a curated table.
- **Merge-on-read** for cheap per-micro-batch streaming commits, plus the
  compaction jobs that bound read amplification.
- **Late / out-of-order data correction** via `MERGE ... WHEN MATCHED AND
  s.event_time > t.event_time`.
- **Exactly-once-ish** semantics through Spark checkpoints + idempotent merges.
- **Table maintenance**: compaction, position-delete rewrite, snapshot
  expiration, orphan-file cleanup.

## Architecture

```
 Kafka topic ──► Spark Structured Streaming (foreachBatch)
                      │  dedup within batch (latest per event_id)
                      ├──► bronze  lakehouse.raw.events_bronze     (append)
                      └──► silver  lakehouse.curated.events_silver (MERGE, MOR)
                                          │
                              iceberg_maintenance.py (out-of-band):
                              compact · expire_snapshots · remove_orphans
```

## Files

| File | Purpose |
|---|---|
| [`src/streaming_ingest.py`](src/streaming_ingest.py) | The streaming job: Kafka → bronze append + silver MERGE |
| [`src/iceberg_maintenance.py`](src/iceberg_maintenance.py) | Compaction / expire / orphan-cleanup, run on a schedule |
| [`src/produce_events.py`](src/produce_events.py) | Synthetic event generator (emits late/duplicate events) |
| [`docker-compose.yml`](docker-compose.yml) | Local single-node Kafka (KRaft) |
| [`docs/iceberg-streaming-research.md`](docs/iceberg-streaming-research.md) | Deep dive on Iceberg metadata, COW vs MOR, small files |

## Run it locally

```bash
pip install -r requirements.txt

# 1. start Kafka and create the topic
docker compose up -d
docker compose exec kafka kafka-topics.sh --create --topic events \
    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 2. start the streaming job (downloads Iceberg + Kafka Spark packages)
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  src/streaming_ingest.py --trigger "30 seconds"

# 3. (new terminal) start producing events
python src/produce_events.py --rate 50

# 4. (periodically) run maintenance
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  src/iceberg_maintenance.py --table lakehouse.curated.events_silver
```

Inspect snapshots / time-travel from a Spark SQL shell:

```sql
SELECT * FROM lakehouse.curated.events_silver.snapshots;
SELECT count(*) FROM lakehouse.curated.events_silver VERSION AS OF <snapshot_id>;
```

## Tech

`Apache Iceberg` · `Spark Structured Streaming` · `Kafka` · `Parquet` · `Python`
