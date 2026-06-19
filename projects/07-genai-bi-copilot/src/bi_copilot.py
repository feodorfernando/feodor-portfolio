"""
bi_copilot.py
-------------
A self-service analytics copilot for the BI team. A business user asks a question in
plain English; the copilot grounds Claude in the certified gold-layer semantic context,
generates SQL, passes it through a safety guard (read-only, gold-only, LIMIT-enforced),
executes it against a Databricks SQL warehouse, and returns BOTH the result and a
plain-English narrative insight.

Flow:  question + certified schema  ──►  Claude (tool-use: emit_sql)
                                          │
                                   sql_guard (read-only, allow-listed)
                                          │
                                   execute on warehouse
                                          │
                                   Claude narrates the result  ──►  answer + table + SQL

Env:  ANTHROPIC_API_KEY  (required)
      DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_WAREHOUSE_ID (for live execution)

Author: Feodor Fernando
"""
from __future__ import annotations

import os
import sys

from anthropic import Anthropic

from semantic_index import load_manifest, render_context
from sql_guard import guard_sql

SQL_MODEL = "claude-sonnet-4-6"        # strong NL→SQL; swap to opus for the hardest asks
NARRATE_MODEL = "claude-haiku-4-5-20251001"  # cheap/fast for the prose summary

EMIT_SQL_TOOL = {
    "name": "emit_sql",
    "description": "Return a single read-only SQL query answering the question.",
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {"type": "string", "description": "one SELECT/WITH query, no trailing semicolon"},
            "assumptions": {"type": "string", "description": "any assumptions made"},
        },
        "required": ["sql"],
    },
}


class BICopilot:
    def __init__(self, manifest_path: str, warehouse=None):
        self.manifest = load_manifest(manifest_path)
        self.context = render_context(self.manifest)
        self.schema = self.manifest["schema"]          # e.g. prod_catalog.gold
        self.client = Anthropic()
        self.warehouse = warehouse                     # injectable; None -> dry-run

    def _generate_sql(self, question: str) -> str:
        resp = self.client.messages.create(
            model=SQL_MODEL,
            max_tokens=1024,
            tools=[EMIT_SQL_TOOL],
            tool_choice={"type": "tool", "name": "emit_sql"},
            system=(
                "You are a BI analytics copilot. Generate ONE read-only SQL query over the "
                "certified gold schema below. Use ONLY tables/columns shown. Prefer certified "
                "metric definitions. Never write/modify data.\n\n"
                f"<schema>\n{self.context}\n</schema>"
            ),
            messages=[{"role": "user", "content": question}],
        )
        for block in resp.content:
            if block.type == "tool_use" and block.name == "emit_sql":
                return block.input["sql"]
        raise RuntimeError("model did not return SQL")

    def _narrate(self, question: str, rows: list[dict]) -> str:
        resp = self.client.messages.create(
            model=NARRATE_MODEL,
            max_tokens=400,
            system="Summarize the query result for a business user in 2-3 sentences. "
                   "Be precise, cite the numbers, no fluff.",
            messages=[{"role": "user",
                       "content": f"Question: {question}\nResult rows: {rows[:50]}"}],
        )
        return resp.content[0].text

    def ask(self, question: str) -> dict:
        raw_sql = self._generate_sql(question)
        guard = guard_sql(raw_sql, allowed_schema=self.schema, max_rows=1000)
        if not guard.ok:
            return {"ok": False, "reason": guard.reason, "sql": raw_sql}

        if self.warehouse is None:
            # dry-run: return the safe SQL without executing (no warehouse configured)
            return {"ok": True, "sql": guard.sql, "rows": None,
                    "insight": "(dry-run — configure a SQL warehouse to execute)"}

        rows = self.warehouse.run(guard.sql)           # -> list[dict]
        insight = self._narrate(question, rows)
        return {"ok": True, "sql": guard.sql, "rows": rows, "insight": insight}


def main():
    if not os.getenv("ANTHROPIC_API_KEY"):
        sys.exit("ANTHROPIC_API_KEY not set.")
    question = " ".join(sys.argv[1:]) or "What was total revenue by region last week?"
    copilot = BICopilot("data/metrics_manifest.json")  # dry-run (no warehouse)
    out = copilot.ask(question)
    if not out["ok"]:
        print(f"❌ blocked by guard: {out['reason']}\n   sql: {out['sql']}")
        return
    print("✅ safe SQL:\n" + out["sql"])
    print("\ninsight: " + str(out["insight"]))


if __name__ == "__main__":
    main()
