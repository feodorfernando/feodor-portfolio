# Real-Time CDC into the Lakehouse: A Design Note

> Feodor Fernando — design note, 2026-06.

## Problem

Operational databases (Postgres, MySQL) hold the source-of-truth, mutable state.
The lakehouse needs a **near-real-time, queryable replica** of that state for
analytics, ML features, and BI — without nightly full-table dumps and without
hammering the production DB. The answer is **Change Data Capture (CDC)**: stream
the database's own change log into the lakehouse and replay inserts, updates, and
deletes as row-level operations on an Iceberg table.

## Pipeline

```
 Postgres WAL  ──Debezium──►  Kafka topic        ──Spark/Flink──►  Iceberg table
 (logical          (CDC          (one per source       (MERGE: upsert        (mirror of
  decoding)       connector)      table, ordered)        + delete)             source)
```

1. **Debezium** reads the database's write-ahead log (Postgres logical decoding /
   MySQL binlog) and emits a structured change event per row mutation.
2. Events land on a **Kafka** topic per source table, ordered per primary key.
3. A streaming job consumes the topic and applies each change to an **Iceberg**
   table via `MERGE INTO`, including row deletes.

## The Debezium change envelope

Every event carries `before`, `after`, and an `op` code:

| `op` | meaning | action on Iceberg |
|------|---------|-------------------|
| `c`  | create (insert) | INSERT (or MERGE upsert) |
| `u`  | update | MERGE upsert on PK |
| `d`  | delete | MERGE delete on PK |
| `r`  | read (initial snapshot) | INSERT/upsert |

```json
{
  "op": "u",
  "ts_ms": 1718700000000,
  "before": {"id": 42, "status": "pending", "amount": 10.0},
  "after":  {"id": 42, "status": "paid",    "amount": 10.0},
  "source": {"lsn": 123456789, "table": "orders"}
}
```

## The hard parts (and how to handle them)

### 1. Ordering & out-of-order delivery
Per-key ordering matters: applying an old update after a newer one corrupts
state. Debezium guarantees per-partition order, and we key the Kafka topic by
primary key so all changes for one row land on one partition in order. In the
MERGE we *also* guard with the source LSN / `ts_ms` — only apply a change if it
is newer than what's already in the table. This makes replay idempotent.

### 2. Deletes
Deletes are the reason Hive-style tables fail at CDC. Iceberg's **merge-on-read
row-level deletes** make `WHEN MATCHED THEN DELETE` cheap — a small delete file,
no full-file rewrite. Tombstone events (`op=d`, null `after`) map directly.

### 3. Exactly-once vs at-least-once
Kafka + Spark checkpoints give at-least-once delivery; the MERGE makes it
*effectively* exactly-once because re-applying the same change (same PK, same or
older LSN) is a no-op. Idempotency at the sink beats trying to force
exactly-once across the whole chain.

### 4. Schema evolution
Source DDL (a new column) flows through Debezium as a schema change. Iceberg's
column-ID-based evolution lets us `ALTER TABLE ... ADD COLUMN` without rewriting
history. A schema-registry-aware consumer reconciles the new field before MERGE.

### 5. Snapshot + streaming handoff
Debezium first emits an initial **snapshot** (`op=r`) of the whole table, then
switches to streaming WAL changes. The sink treats both identically (upsert), so
the table is correct from the first event and stays correct as changes stream in.

## Maintenance (same as any streaming Iceberg table)

CDC tables accumulate delete files fast (every update = a delete + insert under
MOR). Compaction is non-negotiable:

- `rewrite_data_files` + `rewrite_position_delete_files` on a tight schedule
- `expire_snapshots` to bound metadata growth
- monitor the **delete-file ratio** — when it climbs, reads slow down

See the companion project *Iceberg Streaming Lakehouse* for the maintenance job.

## Why Iceberg specifically for CDC

- Row-level MOR deletes → cheap update/delete application.
- Snapshot isolation → analysts query a consistent table while it's being
  mutated by the stream.
- Time travel → "what did this table look like before the bad batch?" for
  debugging CDC bugs.
- Multi-engine → the same CDC mirror is readable by Spark, Trino, and DuckDB
  without copies.
