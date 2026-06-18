# 🔎 LLM-Powered Data Quality Agent

An agent that **profiles a dataset**, asks **Claude to propose data quality
rules** from that profile, then **executes the rules deterministically** in
pandas and reports violations.

## The key design decision

LLMs are great at *judgement* ("this column is called `email`, it should match an
email pattern; `age = 199` is implausible") but you must never let a model
silently *be* the validation — a hallucinated "pass" lets bad data through.

So this splits responsibilities:

```
  profile + samples ──► Claude ──► structured rules (tool-use JSON)
                                        │
                                        ▼
                          deterministic pandas execution ──► PASS / FAIL report
```

The LLM only ever *proposes* typed rules (`not_null`, `unique`, `min`, `max`,
`in_set`, `regex`, `max_null_fraction`). Enforcement is plain, auditable Python.
Claude returns rules via **tool-use**, so there's no brittle text parsing.

## On the sample data

[`data/customers.csv`](data/customers.csv) is seeded with realistic problems —
a missing email, an invalid email string, a negative age, an impossible age
(199), an invalid country code (`XX`), a duplicate `customer_id`, and a null
`lifetime_value`. A good rule set should catch all of them.

## Files

| File | Purpose |
|---|---|
| [`src/data_quality_agent.py`](src/data_quality_agent.py) | Profile → Claude proposes rules (tool-use) → deterministic execution |
| [`data/customers.csv`](data/customers.csv) | Sample dataset with intentional quality issues |

## Run it

```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...

python src/data_quality_agent.py --csv data/customers.csv
```

Exits non-zero if any check fails — drop it straight into a CI / Airflow gate.

## Tech

`Claude API (tool-use)` · `pandas` · `data quality` · `Python`
