"""
rag_assistant.py
----------------
A retrieval-augmented "data catalog assistant". Retrieves the most relevant
table metadata from the local index and asks Claude to answer the user's
question grounded ONLY in that context — with citations to the table names it
used.

Pattern: embed query -> cosine top-k over the catalog index -> stuff retrieved
metadata into the prompt -> Claude generates a grounded answer.

Env:
    ANTHROPIC_API_KEY   required

Usage:
    python src/rag_assistant.py "which tables contain PII and who owns them?"

Author: Feodor Fernando
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import numpy as np
from anthropic import Anthropic  # pip install anthropic
from sentence_transformers import SentenceTransformer

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
# Latest capable Claude model for grounded synthesis. Swap to claude-opus-4-8 for
# the hardest reasoning, or claude-haiku-4-5-20251001 to cut cost/latency.
ANSWER_MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = (
    "You are a data catalog assistant for a lakehouse. Answer questions using "
    "ONLY the table metadata provided in <context>. Cite the table names you "
    "relied on in square brackets, e.g. [curated.revenue_daily]. If the context "
    "does not contain the answer, say so plainly — do not invent tables, "
    "columns, or lineage."
)


class CatalogRAG:
    def __init__(self, index_dir: str) -> None:
        self.embeddings = np.load(os.path.join(index_dir, "embeddings.npy"))
        with open(os.path.join(index_dir, "documents.json")) as f:
            self.documents = json.load(f)
        self.embed_model = SentenceTransformer(EMBED_MODEL)
        self.client = Anthropic()  # reads ANTHROPIC_API_KEY

    def retrieve(self, query: str, k: int = 4) -> list[dict]:
        q = self.embed_model.encode([query], normalize_embeddings=True)[0]
        # embeddings are normalized -> dot product == cosine similarity
        scores = self.embeddings @ q
        top = np.argsort(scores)[::-1][:k]
        return [
            {**self.documents[i], "score": float(scores[i])} for i in top
        ]

    def answer(self, query: str, k: int = 4) -> str:
        hits = self.retrieve(query, k)
        context = "\n\n---\n\n".join(h["text"] for h in hits)
        used = ", ".join(h["table"] for h in hits)
        print(f"[retrieved: {used}]\n", file=sys.stderr)

        resp = self.client.messages.create(
            model=ANSWER_MODEL,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"<context>\n{context}\n</context>\n\n"
                        f"Question: {query}"
                    ),
                }
            ],
        )
        return resp.content[0].text


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("question", nargs="+", help="natural-language question")
    parser.add_argument("--index", default="index")
    parser.add_argument("-k", type=int, default=4, help="retrieved docs")
    args = parser.parse_args()

    if not os.getenv("ANTHROPIC_API_KEY"):
        sys.exit("ANTHROPIC_API_KEY not set. export it and retry.")

    rag = CatalogRAG(args.index)
    print(rag.answer(" ".join(args.question), args.k))


if __name__ == "__main__":
    main()
