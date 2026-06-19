# Feodor Fernando — Data &amp; AI Engineering Portfolio

> Data Engineer · Cloud Wrangler · Chennai, India
> [LinkedIn](https://www.linkedin.com/in/feodorfernando/) ·
> 🌐 Live site: **https://feodorfernando.github.io/feodor-portfolio/**

An animated, theme-aware, code-first portfolio at the intersection of **data engineering**,
**streaming lakehouses**, **Azure & Databricks platforms**, and **generative AI**. Every project
and research paper has its own page you can open and read in the browser — overview, full
architecture diagram, tech stack, and details. Light/dark theme follows your system, with a
manual toggle on every page.

## 🚀 Projects (8)

| # | Project | What it is | Page · Code |
|---|---------|-----------|------|
| 01 | Iceberg Streaming Lakehouse | Kafka → Iceberg streaming (MOR upserts, late-data, maintenance) + research note | [page](site/projects/iceberg-streaming.html) · [code](projects/01-iceberg-streaming-lakehouse/) |
| 02 | GenAI Catalog Assistant (RAG) | Grounded RAG over a lakehouse catalog with citations (Claude) | [page](site/projects/genai-rag.html) · [code](projects/02-genai-rag-lakehouse/) |
| 03 | LLM Data Quality Agent | Claude proposes typed rules (tool-use); pandas enforces them | [page](site/projects/llm-data-quality.html) · [code](projects/03-llm-data-quality/) |
| 04 | Real-Time CDC → Iceberg | Debezium CDC mirrored into Iceberg (idempotent MERGE) + design note | [page](site/projects/cdc-iceberg.html) · [code](projects/04-realtime-cdc-iceberg/) |
| 05 | Azure End-to-End Pipeline | Event Hub/SQL/Cosmos → Stream Analytics/ADF → ADLS/Databricks/Synapse → Power BI | [page](site/projects/azure-etl-pipeline.html) · [code](projects/05-azure-etl-pipeline/) |
| 06 | Databricks Lakehouse + Genie | Delta medallion + Genie-ready semantic layer for NL analytics | [page](site/projects/databricks-lakehouse-genie.html) · [code](projects/06-databricks-lakehouse-genie/) |
| 07 | **GenAI BI Copilot** | RAG/LLM copilot for the BI team: NL → safe SQL + result + insight over gold | [page](site/projects/genai-bi-copilot.html) · [code](projects/07-genai-bi-copilot/) |
| 08 | **Next-Gen Databricks Platform** | Env-aware (dev/prod catalog) medallion, Key Vault, streaming+batch, CI/CD, dynamic OOM/skew handling | [page](site/projects/nextgen-databricks-platform.html) · [code](projects/08-nextgen-databricks-platform/) |

## 📄 Research papers (6)

| # | Paper | Topic |
|---|-------|-------|
| 01 | [Integrating Generative AI into Production Data Pipelines](site/research/ai-in-data-pipelines.html) | Pipelines × AI |
| 02 | [Improving Databricks Genie: Raising NL-to-SQL Accuracy](site/research/databricks-genie-improvement.html) | Databricks / AI-BI |
| 03 | [Database Performance Improvement at Scale](site/research/database-performance.html) | Indexing, partitioning, tuning |
| 04 | [Real-Time Lakehouse Architectures](site/research/realtime-lakehouse.html) | Streaming, CDC, table formats |
| 05 | [AI-Augmented Data Quality &amp; Observability](site/research/data-quality-ai.html) | Quality & observability |
| 06 | [Next-Gen Databricks: Env-Aware Medallion, DevOps &amp; Resilient Spark](site/research/nextgen-databricks-platform.html) | Platform engineering |

## Highlights of the two newest projects

**07 · GenAI BI Copilot** — a self-service analytics copilot. The BI team asks in plain English;
Claude generates SQL grounded on the certified gold schema, a safety guard enforces read-only /
gold-only / LIMIT, it executes on a Databricks SQL warehouse, and returns the result + a narrative
insight + the exact SQL.

**08 · Next-Gen Databricks Platform** — the same code runs across dev/test/prod and routes itself
(dev workspace → `dev_catalog`, prod → `prod_catalog`); secrets come from **Azure Key Vault**;
**streaming (Auto Loader) and batch (COPY INTO)** ingestion feed a Delta medallion through to gold;
clusters and jobs are code (Asset Bundles) promoted by **CI/CD**; and Spark survives **OOM / skew /
transient errors** by classifying the failure and **re-tuning configuration at runtime, then
retrying** (with dead-lettering of bad records and a prod-write gate).

## Repository layout

```
feodor-portfolio/
├── index.html · style.css · main.js    # animated, theme-aware landing
├── site/
│   ├── projects/*.html                 # 8 readable project pages (overview · design · stack · details)
│   ├── research/*.html                 # 6 readable research papers (with architectures)
│   └── assets/                         # shared CSS/JS + theme handling
├── projects/
│   ├── 01-iceberg-streaming-lakehouse/
│   ├── 02-genai-rag-lakehouse/
│   ├── 03-llm-data-quality/
│   ├── 04-realtime-cdc-iceberg/
│   ├── 05-azure-etl-pipeline/
│   ├── 06-databricks-lakehouse-genie/
│   ├── 07-genai-bi-copilot/            # RAG BI copilot (Claude + SQL guard)
│   └── 08-nextgen-databricks-platform/ # env-aware medallion + DevOps + resilient Spark
└── README.md
```

## Theme & accessibility
Follows `prefers-color-scheme`; manual ☀️/🌙 toggle persists in `localStorage`; respects
`prefers-reduced-motion`.

## License
MIT — see [LICENSE](LICENSE).
