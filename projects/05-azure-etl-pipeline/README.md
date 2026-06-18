# ☁️ Azure End-to-End Data Pipeline

A production-shaped Azure data platform spanning **operational sources →
ingestion/ETL → analytical storage & processing → BI**, with both a **real-time
(hot) path** and a **batch (cold) path**.

## Architecture

```
 Operational            Ingestion / ETL          Analytical Storage          Serving
 ┌──────────────┐      ┌──────────────────┐     ┌──────────────────────┐    ┌──────────┐
 │ Event Hub    │─hot─►│ Stream Analytics │────►│ Power BI (live tiles) │    │          │
 │ (telemetry)  │      │  (windowed agg)  │  └─►│ ADLS gen2  (raw/cold) │    │ Power BI │
 ├──────────────┤      ├──────────────────┤     ├──────────────────────┤───►│dashboards│
 │ Azure SQL    │      │ Azure Data       │     │ Databricks (medallion)│    │          │
 │ Cosmos DB    │─cold►│ Factory (copy)   │────►│ Synapse Serverless    │    └──────────┘
 │ Dataverse    │      │                  │     │ ADLS gen2 (bronze/gold)│
 └──────────────┘      └──────────────────┘     └──────────────────────┘
```

- **Hot path:** Event Hub → Stream Analytics tumbling-window aggregates → Power BI
  live dashboard, plus an anomaly path to a Service Bus alert queue.
- **Cold path:** Azure SQL + Cosmos DB → Azure Data Factory copy → ADLS gen2
  bronze → Databricks medallion transform → Synapse serverless external tables →
  Power BI.

## How I built it (and the trade-offs)

1. **Ingestion split by latency need.** Streaming telemetry goes through Stream
   Analytics (sub-minute); slowly-changing operational tables go through ADF
   incremental copy (watermark on `modified_at`). One tool per job beats forcing
   everything through one engine.
2. **Incremental, not full, loads.** The ADF copy uses a windowed watermark so we
   only move changed rows — cheaper and faster than nightly full dumps.
3. **Medallion in Databricks.** ADF triggers a Databricks notebook for the
   bronze→silver→gold transform (see the companion Databricks project).
4. **Serverless serving.** Synapse serverless SQL exposes gold Parquet as
   external tables — no dedicated SQL pool to pay for when idle; Power BI queries
   the views directly.

## Files

| File | Purpose |
|---|---|
| [`stream_analytics/realtime_query.sql`](stream_analytics/realtime_query.sql) | Stream Analytics job: hot aggregates + cold raw landing + anomaly alerts |
| [`pipeline/adf_copy_pipeline.json`](pipeline/adf_copy_pipeline.json) | ADF pipeline: incremental copy from Azure SQL + Cosmos → bronze, then trigger Databricks |
| [`synapse/create_external_tables.sql`](synapse/create_external_tables.sql) | Synapse serverless external tables + views for Power BI |

## Tech

`Azure Event Hub` · `Stream Analytics` · `Azure Data Factory` · `ADLS gen2` ·
`Synapse` · `Databricks` · `Cosmos DB` · `Power BI`
