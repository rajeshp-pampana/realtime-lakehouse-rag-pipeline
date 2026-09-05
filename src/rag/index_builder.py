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

# A section with at least this many bullets is treated as a list and split per
# item. Below it, the section is short enough that one vector represents it.
_MIN_BULLETS_TO_SPLIT = 4

# Below this a chunk is a bare heading or title with no content of its own.
_MIN_CHUNK_CHARS = 80

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


_HEADING_RE = re.compile(r"^##+\s+(.*)$", re.MULTILINE)
_BULLET_RE = re.compile(r"^- (?:\*\*)?", re.MULTILINE)


def _split_sections(body: str) -> list[tuple[str, str]]:
    """Split a markdown body into ``(heading, text)`` sections at ``##`` headings.

    Text before the first heading is returned with an empty heading.
    """
    matches = list(_HEADING_RE.finditer(body))
    if not matches:
        return [("", body.strip())]

    sections: list[tuple[str, str]] = []
    intro = body[: matches[0].start()].strip()
    if intro:
        sections.append(("", intro))
    for i, match in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        sections.append((match.group(1).strip(), body[match.end() : end].strip()))
    return sections


def _chunk_section(heading: str, text: str) -> list[str]:
    """Split a list-shaped section into one chunk per bullet.

    Why this exists: the evaluation showed most retrieval misses came from
    `tickers.md`, a list of 17 companies. Embedded whole it produces one vector
    averaged across every company, which matches no single-company query
    strongly. A section that is mostly a long list gets one chunk per item;
    prose sections stay intact, because splitting prose mid-argument loses the
    context that makes it retrievable.
    """
    bullets = _BULLET_RE.findall(text)
    if len(bullets) < _MIN_BULLETS_TO_SPLIT:
        return [text]

    parts = re.split(r"^- ", text, flags=re.MULTILINE)
    chunks = []
    for item in parts[1:]:
        item = item.strip()
        if not item:
            continue
        # Prefix with the section heading only - deliberately NOT the section's
        # lead-in paragraph. That lead-in is identical on every bullet, so
        # including it adds a few hundred characters of shared text to every
        # chunk and dilutes the part that actually distinguishes them.
        chunks.append(f"{heading}\n\n- {item}" if heading else f"- {item}")
    return chunks or [text]


def _chunk_doc(doc: dict) -> list[dict]:
    """Split one parsed document into retrievable chunks.

    Every chunk keeps the original file name in ``metadata['source']``, so
    citations and the evaluation labels stay at document granularity even though
    retrieval now happens at chunk granularity.
    """
    if not config.RAG_CHUNKING:
        return [doc]

    title = doc["metadata"].get("title", "").strip()
    chunks: list[dict] = []
    for heading, text in _split_sections(doc["text"]):
        if not text:
            continue
        for piece in _chunk_section(heading, text):
            header = " - ".join(x for x in (title, heading) if x)
            body = f"{header}\n\n{piece}" if header and header not in piece else piece
            body = body.strip()
            # A chunk that is only a title or heading carries no information -
            # it embeds to a vector that can match a query without answering
            # it, which is worse than not being indexed at all.
            if len(body) < _MIN_CHUNK_CHARS:
                continue
            chunks.append(
                {
                    "id": f"{doc['id']}#{len(chunks)}",
                    "text": body,
                    "metadata": {**doc["metadata"], "chunk": len(chunks)},
                }
            )
    return chunks or [doc]


def _load_docs(directory: str) -> list[dict]:
    if not os.path.isdir(directory):
        return []
    parsed = [_parse_doc(p) for p in sorted(glob.glob(os.path.join(directory, "*.md")))]
    return [chunk for doc in parsed for chunk in _chunk_doc(doc)]


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
    current_ids = [d["id"] for d in docs]
    collection.upsert(
        ids=current_ids,
        embeddings=embeddings,
        documents=[d["text"] for d in docs],
        metadatas=[d["metadata"] for d in docs],
    )

    # Remove entries whose ids no longer exist. upsert alone only adds and
    # overwrites, so anything that used to be indexed under a different id
    # survives forever and keeps competing in every query.
    #
    # This is not hypothetical: turning chunking on changed ids from
    # "tickers.md" to "tickers.md#0..16", and the stale whole-file vector - the
    # single averaged embedding that chunking exists to eliminate - stayed in
    # the collection alongside its own replacements. Deleting a source document
    # would leave an orphan the same way.
    existing_ids = set(collection.get(include=[])["ids"])
    orphans = sorted(existing_ids - set(current_ids))
    if orphans:
        collection.delete(ids=orphans)
        print(f"[index_builder] removed {len(orphans)} stale entries: {orphans[:5]}"
              + (" ..." if len(orphans) > 5 else ""))

    metrics = {
        "docs_indexed": len(docs),
        "stale_removed": len(orphans),
        "embedding_dim": len(embeddings[0]) if embeddings else 0,
        "duration_seconds": round(time.perf_counter() - started, 2),
        "collection": collection_name,
        "collection_count": collection.count(),
    }
    print(f"[index_builder] {metrics}")
    return metrics


if __name__ == "__main__":
    build_index()
