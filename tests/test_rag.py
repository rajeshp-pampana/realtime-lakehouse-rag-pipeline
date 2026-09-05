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


def _embedding_model_available() -> bool:
    """True only if Ollama is up AND the embedding model is actually pulled.

    Reachability alone is not enough, and the difference is not academic: a
    freshly started Ollama whose model store is still downloading answers
    /api/tags happily, so this test would run and then fail with
    `model "nomic-embed-text" not found` - a confusing failure where a skip is
    the correct outcome. Caught while pulling models into the containerised
    Ollama added in the containerised-briefing work.
    """
    try:
        import httpx

        from src import config

        response = httpx.get(f"{config.OLLAMA_HOST}/api/tags", timeout=2.0)
        names = {m["name"].split(":")[0] for m in response.json().get("models", [])}
        return config.OLLAMA_EMBED_MODEL.split(":")[0] in names
    except Exception:
        return False


pytestmark_ollama = pytest.mark.skipif(
    not _embedding_model_available(),
    reason="Needs a reachable Ollama with the embedding model pulled "
    "(`ollama pull nomic-embed-text`); skipped in CI, run locally",
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
