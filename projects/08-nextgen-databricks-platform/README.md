# 🚀 Next-Gen Databricks Platform (env-aware · DevOps · resilient)

An industry-standard Databricks medallion platform built the way mature teams ship it:
**the same code runs in dev / test / prod** and routes itself to the right catalog, secrets
come from **Azure Key Vault**, ingestion handles **streaming and batch**, deployment is
**CI/CD via Databricks Asset Bundles**, and Spark jobs **survive OOM / skew / transient
errors by re-tuning configuration at runtime**.

## The big ideas

| Concern | How it's handled |
|---|---|
| **Dynamic / env-aware code** | [`config/environments.py`](config/environments.py) resolves where it runs (job param → env var → workspace URL) and routes every read/write: dev workspace → `dev_catalog`, prod → `prod_catalog`. No hard-coded table names. |
| **Secrets** | [`src/secrets.py`](src/secrets.py): Databricks secret scopes **backed by Azure Key Vault**; never a credential in code. |
| **Streaming ingest** | [`notebooks/00_ingest_streaming.py`](notebooks/00_ingest_streaming.py): Auto Loader → bronze, schema evolution, bounded batch size (anti-OOM), exactly-once via checkpoints. |
| **Batch ingest** | [`notebooks/01_ingest_batch.py`](notebooks/01_ingest_batch.py): idempotent `COPY INTO` (re-runs skip loaded files). |
| **Silver → Gold** | MERGE upsert with dead-lettering of bad rows; gold aggregates + semantic comments for BI Copilot/Genie. |
| **OOM / skew / errors** | [`src/spark_resilience.py`](src/spark_resilience.py): AQE-first prevention + an `adaptive_run` decorator that detects OOM/skew/transient failures and **changes Spark config dynamically, then retries**. |
| **Clusters** | [`config/databricks.yml`](config/databricks.yml): autoscaling job clusters, Photon, spot-with-fallback, AQE on. |
| **DevOps / CI-CD** | [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml): PR → dev, main → test, tag → prod, via `databricks bundle deploy`. Prod writes gated behind `ALLOW_PROD_WRITES`. |

## How dynamic environment routing works

```
            ┌─ job param `env` (CI/CD passes it) ─┐
 resolve ───┼─ ENVIRONMENT cluster var ───────────┼──► EnvConfig
            ├─ workspace URL → env map ───────────┤      catalog, storage acct,
            └─ default: dev (never accidental prod)┘      Key Vault scope, autoscale

 run in DEV workspace  → writes dev_catalog.silver.events
 run in PROD workspace → writes prod_catalog.silver.events     (same notebook)
```

## How runtime errors are handled dynamically

```
 build_silver()  ──run──►  ✅ success
        │ ❌ fails
        ▼
   classify(error)  ─ oom   → ↑ shuffle partitions, ↓ maxPartitionBytes, broadcast off
                    ├ skew  → enable/strengthen AQE skew join, ↑ partitions
                    ├ transient → backoff
                    └ unknown → raise (don't mask real bugs)
        │
        └─► re-run with new config (up to N attempts)
```

The job doesn't just die on an out-of-memory error — it diagnoses the failure class,
adjusts the configuration, and retries. Bad *records* (not infra failures) are quarantined
to a dead-letter table instead of being dropped.

## Deploy

```bash
# dev (default)
databricks bundle deploy -t dev   --var="env=dev"
# prod (from CI, on a tagged release)
ALLOW_PROD_WRITES=true databricks bundle deploy -t prod --var="env=prod"
```

## Tech

`Databricks` · `Delta Lake` · `Unity Catalog` · `Auto Loader` · `Structured Streaming` ·
`Azure Key Vault` · `Databricks Asset Bundles` · `GitHub Actions` · `Photon` · `AQE` · `Python`
