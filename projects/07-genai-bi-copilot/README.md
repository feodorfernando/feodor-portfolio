# 💬 GenAI BI Copilot (RAG for the BI team)

A self-service analytics copilot that lets the **BI team ask business questions in plain
English** and get back **safe SQL + the result + a narrative insight** — grounded on the
certified **gold-layer** semantic context, with hard guardrails so it can never touch
anything but read-only gold.

> "What was total revenue by region last week?"
> "Which segment has the highest average order value this month?"
> "Show the 7-day active-user trend for EMEA."

## Why it matters

BI analysts queue behind the data team for ad-hoc questions. This copilot removes the
queue for the 80% of questions that are answerable from certified gold tables — while
keeping answers trustworthy (grounded on certified metrics) and safe (read-only,
gold-only, LIMIT-enforced). It pairs naturally with the
[Next-Gen Databricks Platform](../08-nextgen-databricks-platform/) (which builds &
annotates the gold layer) and the [Databricks Genie](../06-databricks-lakehouse-genie/)
project.

## How it works

```
 question + certified schema ──► Claude (tool-use: emit_sql)
                                     │
                              sql_guard: read-only? gold-only? LIMIT? ──► reject if unsafe
                                     │
                              execute on Databricks SQL warehouse
                                     │
                              Claude narrates result ──► answer + table + the SQL it ran
```

- **Grounded generation** — Claude only sees the certified gold schema + metric
  definitions, so it doesn't invent tables or redefine "revenue".
- **Safety guard** ([`src/sql_guard.py`](src/sql_guard.py)) — single statement, SELECT/WITH
  only, every table inside the allow-listed gold schema, LIMIT injected. A copilot must be
  *unable* to mutate data, not merely instructed not to.
- **Transparent** — returns the exact SQL it ran, so analysts can verify and reuse it.

## Files

| File | Purpose |
|---|---|
| [`src/bi_copilot.py`](src/bi_copilot.py) | NL → SQL (tool-use) → guard → execute → narrate |
| [`src/sql_guard.py`](src/sql_guard.py) | read-only / gold-only / LIMIT enforcement |
| [`src/semantic_index.py`](src/semantic_index.py) | builds the certified-schema grounding context |
| [`data/metrics_manifest.json`](data/metrics_manifest.json) | sample certified gold metrics |

## Run it (dry-run, no warehouse needed)

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...
python src/bi_copilot.py "total revenue by region last week"
# prints the SAFE SQL it would run (add a SQL warehouse to execute live)
```

## Tech

`Claude API (tool-use)` · `RAG` · `NL-to-SQL` · `Databricks SQL` · `Unity Catalog` · `Python`
