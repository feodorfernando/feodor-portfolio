# Feodor Fernando — Data &amp; AI Engineering Portfolio

> Data Engineer · Cloud Wrangler · Chennai, India
> [LinkedIn](https://www.linkedin.com/in/feodorfernando/)

A code-first portfolio at the intersection of **data engineering**, **streaming
lakehouses**, and **generative AI**. Each project ships runnable code plus a
written research / design note.

🌐 **Portfolio site:** open [`index.html`](index.html) locally, or publish via
GitHub Pages (see below).

## Projects

| # | Project | What it is | Tech |
|---|---------|-----------|------|
| 01 | [Iceberg Streaming Lakehouse](projects/01-iceberg-streaming-lakehouse/) | Kafka → Iceberg streaming ingestion (merge-on-read upserts, late-data correction, maintenance) + research note on Iceberg internals | Iceberg, Spark Streaming, Kafka |
| 02 | [GenAI Catalog Assistant (RAG)](projects/02-genai-rag-lakehouse/) | Grounded RAG over a lakehouse catalog — schemas, lineage, PII — with table citations | Claude API, RAG, embeddings |
| 03 | [LLM Data Quality Agent](projects/03-llm-data-quality/) | Claude proposes typed validation rules (tool-use); pandas enforces them deterministically. CI-gate ready | Claude tool-use, pandas |
| 04 | [Real-Time CDC → Iceberg](projects/04-realtime-cdc-iceberg/) | Debezium CDC mirrored into Iceberg with LSN-guarded idempotent MERGEs + design note | Debezium, Iceberg, Spark Streaming |

### Featured writing
- [Streaming into Apache Iceberg: Architecture, Internals & Trade-offs](projects/01-iceberg-streaming-lakehouse/docs/iceberg-streaming-research.md)
- [Real-Time CDC into the Lakehouse: A Design Note](projects/04-realtime-cdc-iceberg/docs/cdc-design.md)

## Repository layout

```
feodor-portfolio/
├── index.html                 # portfolio landing page (GitHub Pages entrypoint)
├── style.css
├── projects/
│   ├── 01-iceberg-streaming-lakehouse/   # streaming ingest + maintenance + research
│   ├── 02-genai-rag-lakehouse/           # RAG catalog assistant (Claude)
│   ├── 03-llm-data-quality/              # LLM-proposed, deterministically-enforced DQ
│   └── 04-realtime-cdc-iceberg/          # Debezium CDC → Iceberg + design note
└── README.md
```

Each project folder has its own README with run instructions and a
`requirements.txt`.

## Publishing to GitHub Pages

This repo is Pages-ready (`index.html` at the root, plus `.nojekyll`):

```bash
# create the GitHub repo and push (requires the gh CLI, authenticated)
gh repo create feodor-portfolio --public --source . --remote origin --push

# enable Pages from the main branch root
gh api -X POST repos/:owner/feodor-portfolio/pages \
  -f "source[branch]=main" -f "source[path]=/" 2>/dev/null || \
echo "Then: repo Settings → Pages → Branch: main, Folder: / (root)"
```

Your site will be served at `https://<username>.github.io/feodor-portfolio/`.

## License

MIT — see [LICENSE](LICENSE).
