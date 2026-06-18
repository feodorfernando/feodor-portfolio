# 🧱 Databricks Lakehouse + Genie Analytics

A **Delta Lake medallion** pipeline (bronze → silver → gold) on Databricks, topped
with a **Genie-ready semantic layer** so business users can ask questions in plain
English and get correct SQL.

## Architecture

```
 ADLS gen2 raw ──Auto Loader──► bronze (Delta) ──MERGE──► silver (clean, dedup)
                                                              │
                                                    gold (aggregates, star)
                                                              │
                                          comments + certified metric views
                                                              │
                                      Databricks Genie / AI-BI  ◄── "revenue last week?"
```

## What it demonstrates

- **Auto Loader** incremental file ingestion with schema evolution + checkpoints.
- **MERGE-based silver** transform: dedup, quality filters, idempotent upserts.
- **`OPTIMIZE` + `ZORDER`** for read performance on the silver table.
- **Genie / AI-BI enablement** — the part most teams skip: table & column
  **comments in business language** and a **certified metric view**. This is the
  single biggest lever on natural-language-to-SQL accuracy (see the companion
  research paper *Improving Databricks Genie*).

## How I built it

1. Land raw JSON in ADLS gen2; **Auto Loader** streams new files into bronze
   with `trigger(availableNow=True)` so it runs batch-style but only on deltas.
2. Silver applies type casting + quality rules and **MERGEs** the latest record
   per key — rerunning a window is a no-op (idempotent).
3. Gold builds the daily revenue star table, then I **annotate every column** and
   publish a **certified metric view** that encodes the right business
   definitions, so Genie answers from curated logic instead of guessing.

## Files

| File | Purpose |
|---|---|
| [`notebooks/01_bronze_autoloader.py`](notebooks/01_bronze_autoloader.py) | Auto Loader incremental ingest → Delta bronze |
| [`notebooks/02_silver_transform.py`](notebooks/02_silver_transform.py) | Clean/dedup + MERGE upsert + OPTIMIZE/ZORDER |
| [`notebooks/03_gold_genie_semantic.sql`](notebooks/03_gold_genie_semantic.sql) | Gold aggregates + Genie semantic metadata & certified metrics |

## Tech

`Databricks` · `Delta Lake` · `Auto Loader` · `Databricks Genie / AI-BI` ·
`Spark SQL` · `Unity Catalog` · `Python`
