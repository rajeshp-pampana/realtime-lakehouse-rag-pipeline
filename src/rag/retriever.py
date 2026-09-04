"""Retrieval step run before every LLM call.

Milestone 3 implements this: embed a query with the same local Ollama
embedding model used to build the index, and return the top-k most similar
passages from the Chroma collection (historical briefings + schema/context
notes) that ``src/llm/briefing_generator.py`` grounds its output in.
"""

from __future__ import annotations

import time

from src import config

TOP_K = config.RAG_TOP_K


def _observe_retrieval(seconds: float) -> None:
    """Record retrieval latency, if the metrics stack is importable.

    Wrapped because retrieval must work in a plain CLI run with no observability
    dependencies present - instrumentation is an addition to this module, not a
    requirement of it.
    """
    try:
        from src.observability.metrics import RETRIEVAL_SECONDS

        RETRIEVAL_SECONDS.observe(seconds)
    except Exception:  # noqa: BLE001 - never let metrics break retrieval
        pass


def retrieve(
    query: str,
    k: int = TOP_K,
    persist_dir: str | None = None,
    collection_name: str | None = None,
) -> dict:
    """Return the top-k passages for ``query``, plus timing.

    Result shape: ``{"passages": [{"text", "metadata", "distance"}, ...],
    "latency_seconds": float}``. Returns an empty passage list (not an error)
    if the index hasn't been built yet, so callers can degrade gracefully.
    """
    from src.rag import _chromadb_compat  # noqa: F401, I001 - must precede `import chromadb`

    import chromadb  # noqa: I001
    import ollama

    persist_dir = persist_dir or config.VECTORSTORE_DIR
    collection_name = collection_name or config.RAG_COLLECTION
    started = time.perf_counter()

    try:
        client = chromadb.PersistentClient(
            path=persist_dir, settings=chromadb.Settings(anonymized_telemetry=False)
        )
        collection = client.get_collection(name=collection_name)
    except Exception:
        latency = time.perf_counter() - started
        _observe_retrieval(latency)
        return {"passages": [], "latency_seconds": round(latency, 3)}

    query_embedding = ollama.embed(model=config.OLLAMA_EMBED_MODEL, input=[query]).embeddings[0]
    result = collection.query(query_embeddings=[query_embedding], n_results=k)

    passages = []
    ids = result.get("ids", [[]])[0]
    documents = result.get("documents", [[]])[0]
    metadatas = result.get("metadatas", [[]])[0]
    distances = result.get("distances", [[]])[0]
    for doc_id, text, meta, dist in zip(ids, documents, metadatas, distances, strict=False):
        passages.append({"id": doc_id, "text": text, "metadata": meta, "distance": dist})

    latency = time.perf_counter() - started
    _observe_retrieval(latency)
    return {"passages": passages, "latency_seconds": round(latency, 3)}


if __name__ == "__main__":
    import sys

    q = " ".join(sys.argv[1:]) or "MSFT market context"
    out = retrieve(q)
    print(f"[retriever] query={q!r} latency={out['latency_seconds']}s")
    for p in out["passages"]:
        print(f"  - {p['id']} (dist={p['distance']:.4f}): {p['text'][:100]}...")
