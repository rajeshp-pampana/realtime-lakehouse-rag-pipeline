"""Build / refresh the local vector index.

Milestone 3 implements this: embed historical briefings, schema notes, and prior
anomaly write-ups and upsert them into a local vector store (Chroma or FAISS).
Embeddings are produced locally (Ollama) to keep the privacy-first story intact.

Placeholder only until Milestone 3.
"""

from __future__ import annotations

COLLECTION = "market_context"


def build_index(*args, **kwargs):
    raise NotImplementedError("Milestone 3: index builder not implemented yet")
