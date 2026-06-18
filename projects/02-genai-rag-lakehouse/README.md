# 🤖 GenAI Catalog Assistant (RAG over a Lakehouse)

A retrieval-augmented assistant that answers natural-language questions about a
**data lakehouse catalog** — schemas, lineage, ownership, PII — grounded in the
catalog metadata and citing the tables it used.

> "Which tables contain PII and who owns them?"
> "What feeds `curated.revenue_daily`?"
> "Where would I find churn features and how fresh are they?"

## Why this matters for data engineering

Catalog/lineage knowledge is usually trapped in wikis, dbt docs, and people's
heads. This is a clean, **grounded** RAG pattern that turns structured catalog
metadata into a conversational interface — with guardrails so the model won't
hallucinate tables or columns.

## How it works

```
 catalog.json ─► ingest_catalog.py ─► local embeddings index (numpy + json)
                                                │
 user question ─► embed ─► cosine top-k ─► stuff metadata into prompt
                                                │
                                          Claude (claude-sonnet-4-6)
                                                │
                                  grounded answer + [table] citations
```

- **Embeddings are local** (`all-MiniLM-L6-v2`) — no external embedding vendor.
- **Generation uses the Claude API** with a strict system prompt: answer only
  from `<context>`, cite table names, say "not in catalog" rather than invent.
- Swap `ANSWER_MODEL` to `claude-opus-4-8` for hardest reasoning or
  `claude-haiku-4-5-20251001` to cut cost/latency.

## Files

| File | Purpose |
|---|---|
| [`src/ingest_catalog.py`](src/ingest_catalog.py) | Render catalog → embed → build local index |
| [`src/rag_assistant.py`](src/rag_assistant.py) | Retrieve top-k + grounded Claude answer with citations |
| [`data/catalog.json`](data/catalog.json) | Sample lakehouse catalog (bronze→silver→gold + feature table) |

## Run it

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...

python src/ingest_catalog.py --catalog data/catalog.json --out index/
python src/rag_assistant.py "which tables contain PII and who owns them?"
```

## Tech

`Claude API (Anthropic)` · `RAG` · `sentence-transformers` · `NumPy` · `Python`
