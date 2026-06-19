"""
semantic_index.py
-----------------
Build the retrieval context the BI Copilot grounds on: the certified gold-layer
metrics and their business definitions. In production this is sourced from Unity
Catalog (`information_schema.columns` + `COMMENT`s + certified metric views); here a
JSON manifest stands in so the example is self-contained.

Author: Feodor Fernando
"""
from __future__ import annotations

import json


def load_manifest(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def render_context(manifest: dict) -> str:
    """Flatten certified tables/metrics into a compact, model-friendly schema brief."""
    lines = [f"Catalog/schema: {manifest['schema']}", ""]
    for t in manifest["tables"]:
        lines.append(f"TABLE {t['name']}  — {t['description']}  (grain: {t['grain']})")
        for c in t["columns"]:
            note = f" — {c['comment']}" if c.get("comment") else ""
            lines.append(f"    {c['name']} {c['type']}{note}")
        lines.append("")
    if manifest.get("metrics"):
        lines.append("CERTIFIED METRICS (prefer these definitions):")
        for m in manifest["metrics"]:
            lines.append(f"    {m['name']}: {m['definition']}")
    if manifest.get("defaults"):
        lines.append("")
        lines.append("DEFAULTS: " + "; ".join(manifest["defaults"]))
    return "\n".join(lines)
