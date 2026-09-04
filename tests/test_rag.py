"""RAG index/retriever tests.

`test_index_and_retrieve_end_to_end` is a real integration test: builds a
temp Chroma index from synthetic docs using the real Ollama embedding model,
then asserts retrieval actually ranks the topically-relevant doc first. Needs
a reachable Ollama server, which CI doesn't have - skipped there (documented
reason), run for real locally where Ollama is available.
"""

from __future__ import annotations

import importlib

import pytest


def test_rag_modules_import():
    builder = importlib.import_module("src.rag.index_builder")
    retriever = importlib.import_module("src.rag.retriever")
    assert hasattr(builder, "build_index")
    assert hasattr(retriever, "retrieve")


def _ollama_reachable() -> bool:
    try:
        import httpx

        from src import config

        httpx.get(config.OLLAMA_HOST, timeout=1.0)
        return True
    except Exception:
        return False


pytestmark_ollama = pytest.mark.skipif(
    not _ollama_reachable(),
    reason="No reachable Ollama server (needed for real embeddings) - this test needs "
    "OLLAMA_HOST running with the embedding model pulled; skipped in CI, run locally",
)


@pytestmark_ollama
def test_index_and_retrieve_end_to_end(tmp_path):
    context_dir = tmp_path / "context"
    context_dir.mkdir()
    (context_dir / "kafka.md").write_text(
        "Apache Kafka streaming ingestion, KRaft mode, consumer lag, micro-batches.",
        encoding="utf-8",
    )
    (context_dir / "baking.md").write_text(
        "Sourdough bread baking: hydration ratio, proofing time, oven spring.",
        encoding="utf-8",
    )

    from src.rag.index_builder import build_index
    from src.rag.retriever import retrieve

    persist_dir = str(tmp_path / "vectorstore")
    metrics = build_index(
        context_dir=str(context_dir),
        briefings_dir=str(tmp_path / "no_briefings"),
        persist_dir=persist_dir,
        collection_name="test_collection",
    )
    assert metrics["docs_indexed"] == 2
    assert metrics["embedding_dim"] > 0

    result = retrieve(
        "Kafka consumer lag and streaming throughput",
        k=2,
        persist_dir=persist_dir,
        collection_name="test_collection",
    )
    assert result["passages"], "expected at least one retrieved passage"
    assert result["passages"][0]["id"] == "kafka.md"
    assert result["latency_seconds"] >= 0
