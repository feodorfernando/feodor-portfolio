# 🔄 Real-Time CDC → Iceberg

Mirror an operational database (Postgres/MySQL) into an **Apache Iceberg** table
in near-real-time using **Debezium CDC**, applying inserts, updates, and
**deletes** as row-level operations via merge-on-read.

Pairs a [design note](docs/cdc-design.md) on the hard parts of CDC (ordering,
deletes, exactly-once, schema evolution, snapshot→stream handoff) with a working
Spark Structured Streaming consumer.

## Pipeline

```
 Postgres WAL ─Debezium─► Kafka (per-table, keyed by PK) ─Spark─► Iceberg (MERGE upsert+delete)
```

## What the code handles

- Parses the **Debezium envelope** (`before` / `after` / `op` / `lsn` / `ts_ms`).
- Collapses to the **latest change per primary key** within each micro-batch.
- `MERGE INTO` that **upserts** non-deletes and applies **tombstone deletes**,
  guarded by **LSN** so replay and out-of-order events are idempotent.
- Reads from `earliest` so the Debezium **initial snapshot** seeds the table,
  then continues with streamed changes — identical handling for both.
- **Merge-on-read** tables → cheap deletes/updates (no full-file rewrites).

## Files

| File | Purpose |
|---|---|
| [`docs/cdc-design.md`](docs/cdc-design.md) | Design note: CDC architecture, ordering, deletes, schema evolution |
| [`src/cdc_to_iceberg.py`](src/cdc_to_iceberg.py) | Spark streaming consumer applying Debezium CDC to Iceberg |

## Run it

```bash
pip install -r requirements.txt
# assumes a Debezium connector is publishing to e.g. dbserver.public.orders
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  src/cdc_to_iceberg.py --topic dbserver.public.orders --pk id --table lakehouse.cdc.orders
```

> Compaction matters even more for CDC tables (every update = delete + insert
> under MOR). Use the maintenance job from the *Iceberg Streaming Lakehouse*
> project on a tight schedule.

## Tech

`Debezium CDC` · `Apache Iceberg` · `Spark Structured Streaming` · `Kafka` · `Python`
