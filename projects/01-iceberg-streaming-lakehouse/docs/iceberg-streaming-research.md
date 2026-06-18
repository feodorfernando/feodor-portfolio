# Streaming into Apache Iceberg: Architecture, Internals & Trade-offs

> Research note — Feodor Fernando. Last revised 2026-06.

## 1. Why Iceberg for streaming

Streaming workloads punish naive table formats. A Kafka topic delivering tens of
thousands of events per second produces a flood of small files, constant schema
drift, and a stream of late / out-of-order records that need correcting *after*
they have already landed. Hive-style tables (directory = partition, no
transaction log) cannot keep up: there is no atomic commit, no snapshot
isolation, and readers see half-written partitions.

Apache Iceberg solves this at the **metadata layer**. It is not a storage engine
and not a service — it is an open *table format*: a specification for how to lay
out data files plus a tree of metadata that gives you ACID commits, snapshot
isolation, hidden partitioning, and schema evolution on top of plain object
storage (S3 / ADLS / GCS).

For a streaming lakehouse the properties that matter most are:

| Requirement | Iceberg mechanism |
|---|---|
| Atomic micro-batch commits | Optimistic concurrency on the metadata pointer |
| Exactly-once-ish ingestion | Idempotent commits keyed on snapshot + checkpoint |
| Correcting late data | Row-level upserts via merge-on-read / copy-on-write |
| Small-file control | Background `rewrite_data_files` compaction |
| Reproducible reads | Immutable snapshots + time travel |
| Schema drift | Column-ID–based schema evolution (add/drop/rename, safe) |

## 2. The metadata tree (what actually happens on commit)

Iceberg's superpower is that a "commit" is a single atomic swap of one pointer.
The layout below is the whole story:

```
catalog ──► table metadata pointer (e.g. metadata/v3.metadata.json)
                       │
   ┌───────────────────┼────────────────────┐
   │                   │                    │
 schema(s)        partition spec(s)     snapshot list
                                             │
                                       current snapshot
                                             │
                                     manifest list (avro)
                                       /            \
                                manifest         manifest      ← manifest files
                                  /  \              /  \
                            data    data       data    delete   ← data + delete files
                            file    file       file     file       (parquet / orc)
```

- **Data files** — immutable Parquet/ORC files holding rows.
- **Delete files** — for merge-on-read: position deletes (file + row offset) or
  equality deletes (e.g. `id = 42`). This is how streaming upserts/deletes avoid
  rewriting whole files on every micro-batch.
- **Manifest files** — list data/delete files plus per-file stats (row counts,
  null counts, lower/upper bounds per column). These stats drive *file pruning*.
- **Manifest list** — the set of manifests that make up one snapshot.
- **Snapshot** — an immutable view of the table at a point in time.
- **Table metadata** — the root JSON: schema history, partition specs, snapshot
  log, properties.

A commit = write new data/delete files → write new manifests → write new manifest
list → write new `metadata.json` → **atomically swap the catalog pointer**. If
two writers race, the loser detects the pointer moved and retries against the new
snapshot (optimistic concurrency). This is exactly what makes per-micro-batch
streaming commits safe.

## 3. Copy-on-write vs merge-on-read (the central streaming trade-off)

This is the decision that defines your latency/cost profile.

**Copy-on-write (COW)** — an update rewrites the entire data file(s) containing
the affected rows. Reads are fast (no merge), writes are expensive. Good for
batch / slowly-changing tables, *bad* for high-frequency streaming upserts.

**Merge-on-read (MOR)** — an update writes a small *delete file* plus the new
rows; the old data file is untouched. Writes are cheap and fast (ideal for
streaming), but reads must merge data + delete files on the fly, so read latency
degrades as deletes accumulate — until compaction folds them back in.

```
                 write cost        read cost        best for
  COW            high              low              append-mostly, batch
  MOR            low               grows w/ deletes streaming upserts/CDC
```

Practical rule: **stream in with MOR, then compact aggressively.** Set
`write.update.mode = merge-on-read` and `write.delete.mode = merge-on-read`, and
schedule `rewrite_data_files` + `rewrite_position_delete_files` to keep read
amplification bounded.

## 4. The small-file problem (the #1 streaming failure mode)

A streaming job committing every 30s on a partitioned table can create millions
of tiny files in a day. Symptoms: query planning slows to a crawl (every file =
a manifest entry to scan), object-storage LIST/GET costs explode, and executors
spend all their time opening files instead of reading rows.

Mitigations, in order of impact:

1. **Compaction.** Run `rewrite_data_files` to bin-pack small files up to
   `write.target-file-size-bytes` (default 512 MB; 128–256 MB is sane for
   streaming). This is the single most important maintenance job.
2. **Right-size the trigger.** A longer micro-batch interval (e.g. 1–5 min)
   produces fewer, larger files than a 5s trigger. Trade latency for file size.
3. **Partition sanely.** Don't over-partition. `days(event_time)` is usually
   enough; partitioning by a high-cardinality key recreates the small-file
   problem inside every partition.
4. **Expire snapshots + remove orphans.** `expire_snapshots` drops old metadata
   and unreferenced data files; `remove_orphan_files` cleans files no snapshot
   references (e.g. from failed commits).

## 5. Exactly-once with Spark Structured Streaming

Iceberg's Spark sink commits each micro-batch as one Iceberg snapshot. Combined
with Spark's checkpointed offsets, you get effectively-once semantics: if a batch
fails after writing files but before committing, the next run re-processes the
same offsets and the half-written files become orphans (cleaned by
`remove_orphan_files`). The catalog pointer only ever advances on a clean commit.

Keys to getting this right:
- A **stable checkpoint location** per stream (never share across streams).
- Idempotent `MERGE INTO` for upserts so re-processing a batch is a no-op.
- `fanout-enabled=true` when writing many partitions per batch to avoid
  per-partition sort/spill.

## 6. Reference architecture

```
  Sources                Ingest / Transform           Lakehouse (Iceberg)        Serving
 ┌─────────┐   Kafka   ┌──────────────────────┐     ┌────────────────────┐    ┌──────────┐
 │ app logs│──topic───►│ Spark Structured     │     │ bronze (raw append)│    │ Trino /  │
 │ CDC     │           │ Streaming            │────►│ silver (MERGE/MOR) │───►│ Spark /  │
 │ IoT     │           │  + foreachBatch      │     │ gold   (agg/COW)   │    │ DuckDB   │
 └─────────┘           └──────────────────────┘     └─────────┬──────────┘    └──────────┘
                                                               │
                                                     maintenance jobs:
                                          rewrite_data_files · expire_snapshots
                                          rewrite_manifests · remove_orphan_files
```

Bronze appends raw events (cheap, append-only). Silver applies MERGE-on-read
upserts to deduplicate and correct late data. Gold holds compacted,
copy-on-write aggregates optimized for BI reads. Maintenance runs out-of-band so
it never blocks the streaming path.

## 7. Engine comparison (2026 snapshot)

| Format | ACID | Row-level deletes | Hidden partitioning | Multi-engine reads | Streaming upserts |
|---|---|---|---|---|---|
| **Iceberg** | ✅ | ✅ (pos + eq) | ✅ | Spark, Trino, Flink, Snowflake, DuckDB, BigQuery | ✅ MOR |
| Delta Lake | ✅ | ✅ | ❌ (explicit) | Spark-first, broadening | ✅ |
| Hudi | ✅ | ✅ | ❌ | Spark/Flink-first | ✅ (CoW/MoR) |

Iceberg's differentiators for a vendor-neutral lakehouse: **hidden partitioning**
(queries don't reference partition columns, and you can evolve the partition spec
without rewriting old data) and the broadest cross-engine read support. The REST
catalog spec has also made the catalog layer portable across vendors.

## 8. Operational checklist

- [ ] `write.*.mode = merge-on-read` for streaming silver tables
- [ ] `rewrite_data_files` every N hours (target 128–256 MB)
- [ ] `rewrite_position_delete_files` to bound read amplification
- [ ] `expire_snapshots` retaining e.g. 7 days for time-travel
- [ ] `remove_orphan_files` (older than `commit.timeout`) to clean failed commits
- [ ] `rewrite_manifests` when manifest count grows large
- [ ] Monitor: files-per-partition, delete-file ratio, snapshot count, avg file size
- [ ] One checkpoint dir per stream; idempotent MERGE for replay safety

## References / further reading

- Apache Iceberg spec — table format v2/v3 (metadata tree, delete files)
- "Iceberg: A Modern Table Format for Huge Analytic Datasets" (Netflix)
- Spark Structured Streaming + Iceberg sink documentation
- Trino / Flink Iceberg connector docs
