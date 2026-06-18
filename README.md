# Feodor Fernando — Data &amp; AI Engineering Portfolio

> Data Engineer · Cloud Wrangler · Chennai, India
> [LinkedIn](https://www.linkedin.com/in/feodorfernando/) ·
> 🌐 Live site: **https://feodorfernando.github.io/feodor-portfolio/**

An animated, code-first portfolio at the intersection of **data engineering**, **streaming
lakehouses**, **Azure data platforms**, and **generative AI**. Every project and research
paper has its own page you can open and read in the browser — overview, design diagram,
tech stack, and details.

## 🚀 Projects (6)

| # | Project | What it is | Page · Code |
|---|---------|-----------|------|
| 01 | Iceberg Streaming Lakehouse | Kafka → Iceberg streaming (MOR upserts, late-data, maintenance) + research note | [page](site/projects/iceberg-streaming.html) · [code](projects/01-iceberg-streaming-lakehouse/) |
| 02 | GenAI Catalog Assistant (RAG) | Grounded RAG over a lakehouse catalog with citations (Claude) | [page](site/projects/genai-rag.html) · [code](projects/02-genai-rag-lakehouse/) |
| 03 | LLM Data Quality Agent | Claude proposes typed rules (tool-use); pandas enforces them | [page](site/projects/llm-data-quality.html) · [code](projects/03-llm-data-quality/) |
| 04 | Real-Time CDC → Iceberg | Debezium CDC mirrored into Iceberg (idempotent MERGE) + design note | [page](site/projects/cdc-iceberg.html) · [code](projects/04-realtime-cdc-iceberg/) |
| 05 | **Azure End-to-End Pipeline** | Event Hub/SQL/Cosmos → Stream Analytics/ADF → ADLS/Databricks/Synapse → Power BI | [page](site/projects/azure-etl-pipeline.html) · [code](projects/05-azure-etl-pipeline/) |
| 06 | **Databricks Lakehouse + Genie** | Delta medallion + Genie-ready semantic layer for NL analytics | [page](site/projects/databricks-lakehouse-genie.html) · [code](projects/06-databricks-lakehouse-genie/) |

## 📄 Research papers (5)

| # | Paper | Topic |
|---|-------|-------|
| 01 | [Integrating Generative AI into Production Data Pipelines](site/research/ai-in-data-pipelines.html) | Pipelines × AI |
| 02 | [Improving Databricks Genie: Raising NL-to-SQL Accuracy](site/research/databricks-genie-improvement.html) | Databricks / AI-BI |
| 03 | [Database Performance Improvement at Scale](site/research/database-performance.html) | Indexing, partitioning, tuning |
| 04 | [Real-Time Lakehouse Architectures](site/research/realtime-lakehouse.html) | Streaming, CDC, table formats |
| 05 | [AI-Augmented Data Quality &amp; Observability](site/research/data-quality-ai.html) | Quality & observability |

## Repository layout

```
feodor-portfolio/
├── index.html                 # animated portfolio landing (Pages entrypoint)
├── style.css · main.js        # site styling + animations
├── site/
│   ├── projects/*.html        # 6 readable project pages (overview · design · stack · details)
│   ├── research/*.html        # 5 readable research papers
│   └── assets/                # shared CSS/JS for sub-pages
├── projects/
│   ├── 01-iceberg-streaming-lakehouse/
│   ├── 02-genai-rag-lakehouse/
│   ├── 03-llm-data-quality/
│   ├── 04-realtime-cdc-iceberg/
│   ├── 05-azure-etl-pipeline/          # ADF + Stream Analytics + Synapse
│   └── 06-databricks-lakehouse-genie/  # Delta medallion notebooks + Genie semantic layer
└── README.md
```

## Run the code

Each project folder has its own README with run instructions and (where relevant) a
`requirements.txt`. Projects 05/06 are Azure/Databricks artifacts (ADF JSON, Stream Analytics
SQL, Synapse SQL, Databricks notebooks).

## Publishing

Pages-ready (`index.html` at root + `.nojekyll`). Already deployed at
`https://feodorfernando.github.io/feodor-portfolio/`. To update: `git push` — Pages rebuilds
automatically.

## License

MIT — see [LICENSE](LICENSE).
