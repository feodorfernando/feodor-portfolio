"""
ingest_catalog.py
-----------------
Build a local semantic index over a data lakehouse catalog so an LLM can answer
questions like "which tables contain PII?" or "what feeds the revenue_daily
table?".

Each catalog entry (table schema + description + lineage + owner) is rendered to
a text "document", embedded with a local sentence-transformers model, and stored
as a numpy matrix + JSON sidecar. No external embedding API required.

Usage:
    python src/ingest_catalog.py --catalog data/catalog.json --out index/

Author: Feodor Fernando
"""
from __future__ import annotations

import argparse
import json
import os

import numpy as np
from sentence_transformers import SentenceTransformer  # pip install sentence-transformers

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def render_document(entry: dict) -> str:
    """Flatten a catalog entry into a retrieval-friendly text blob."""
    cols = ", ".join(
        f"{c['name']} ({c['type']}{', PII' if c.get('pii') else ''})"
        for c in entry.get("columns", [])
    )
    lineage = " <- ".join(entry.get("upstream", [])) or "none"
    return (
        f"Table: {entry['table']}\n"
        f"Layer: {entry.get('layer', 'unknown')}\n"
        f"Owner: {entry.get('owner', 'unknown')}\n"
        f"Description: {entry.get('description', '')}\n"
        f"Columns: {cols}\n"
        f"Upstream lineage: {lineage}\n"
        f"Refresh: {entry.get('refresh', 'unknown')}\n"
        f"Format: {entry.get('format', 'iceberg')}"
    )


def build_index(catalog_path: str, out_dir: str) -> None:
    with open(catalog_path) as f:
        catalog = json.load(f)

    docs = [render_document(e) for e in catalog]
    print(f"embedding {len(docs)} catalog entries with {EMBED_MODEL} ...")
    model = SentenceTransformer(EMBED_MODEL)
    embeddings = model.encode(docs, normalize_embeddings=True, show_progress_bar=True)

    os.makedirs(out_dir, exist_ok=True)
    np.save(os.path.join(out_dir, "embeddings.npy"), embeddings.astype("float32"))
    with open(os.path.join(out_dir, "documents.json"), "w") as f:
        json.dump(
            [{"table": e["table"], "text": d} for e, d in zip(catalog, docs)],
            f,
            indent=2,
        )
    print(f"index written to {out_dir}/ ({embeddings.shape[0]} vectors, "
          f"dim={embeddings.shape[1]})")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="data/catalog.json")
    parser.add_argument("--out", default="index")
    args = parser.parse_args()
    build_index(args.catalog, args.out)


if __name__ == "__main__":
    main()
