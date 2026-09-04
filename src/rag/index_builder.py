"""Build / refresh the local vector index.

Milestone 3 implements this: embed historical briefings (``data/briefings/``)
and schema/context/methodology notes (``docs/context/``) with a local Ollama
embedding model, and upsert them into a local, persistent Chroma collection
(``data/vectorstore/``). ``src/rag/retriever.py`` queries this collection
before every LLM call.
"""

from __future__ import annotations

import glob
import os
import re
import time

from src import config

COLLECTION = config.RAG_COLLECTION

_FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n(.*)$", re.DOTALL)


def _parse_doc(path: str) -> dict:
    """Split a markdown file into (metadata, body). Frontmatter is a plain
    ``key: value`` block between ``---`` lines - no external yaml dependency
    for a handful of short, hand-written docs.
    """
    with open(path, encoding="utf-8") as f:
        raw = f.read()

    metadata: dict[str, str] = {}
    body = raw
    match = _FRONTMATTER_RE.match(raw)
    if match:
        front, body = match.groups()
        for line in front.splitlines():
            if ":" in line:
                key, _, value = line.partition(":")
                metadata[key.strip()] = value.strip()

    metadata["source"] = os.path.basename(path)
    return {"id": os.path.basename(path), "text": body.strip(), "metadata": metadata}


def _load_docs(directory: str) -> list[dict]:
    if not os.path.isdir(directory):
        return []
    return [_parse_doc(p) for p in sorted(glob.glob(os.path.join(directory, "*.md")))]


def _embed(texts: list[str]) -> list[list[float]]:
    import ollama

    if not texts:
        return []
    response = ollama.embed(model=config.OLLAMA_EMBED_MODEL, input=texts)
    return response.embeddings


def build_index(
    context_dir: str | None = None,
    briefings_dir: str | None = None,
    persist_dir: str | None = None,
    collection_name: str | None = None,
) -> dict:
    """Embed every doc under ``context_dir`` and ``briefings_dir`` and upsert
    them into the Chroma collection. Returns a metrics dict.
    """
    from src.rag import _chromadb_compat  # noqa: F401, I001 - must precede `import chromadb`

    import chromadb  # noqa: I001

    context_dir = context_dir or config.CONTEXT_DOCS_DIR
    briefings_dir = briefings_dir or config.BRIEFINGS_DIR
    persist_dir = persist_dir or config.VECTORSTORE_DIR
    collection_name = collection_name or COLLECTION
    started = time.perf_counter()

    docs = _load_docs(context_dir) + _load_docs(briefings_dir)
    if not docs:
        return {
            "docs_indexed": 0,
            "duration_seconds": round(time.perf_counter() - started, 2),
            "collection": collection_name,
        }

    embeddings = _embed([d["text"] for d in docs])

    os.makedirs(persist_dir, exist_ok=True)
    # anonymized_telemetry=False disables most, but chromadb 0.5.4 still logs a
    # harmless "Failed to send telemetry event" warning on client init - a
    # known upstream chromadb/posthog version-compatibility wart, not
    # something wrong with this code or the local index it builds.
    client = chromadb.PersistentClient(
        path=persist_dir, settings=chromadb.Settings(anonymized_telemetry=False)
    )
    collection = client.get_or_create_collection(name=collection_name)
    collection.upsert(
        ids=[d["id"] for d in docs],
        embeddings=embeddings,
        documents=[d["text"] for d in docs],
        metadatas=[d["metadata"] for d in docs],
    )

    metrics = {
        "docs_indexed": len(docs),
        "embedding_dim": len(embeddings[0]) if embeddings else 0,
        "duration_seconds": round(time.perf_counter() - started, 2),
        "collection": collection_name,
        "collection_count": collection.count(),
    }
    print(f"[index_builder] {metrics}")
    return metrics


if __name__ == "__main__":
    build_index()
