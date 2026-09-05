"""Tests for the retrieval-quality evaluation.

The eval set exists so that "retrieval works" is a measured claim rather than an
impression. These tests split into two kinds:

- **Portable** checks on the eval set itself - that it is well formed, that
  every label names a document that actually exists, and that the questions do
  not simply quote their target document. That last one matters: a query copied
  verbatim out of the file it is meant to retrieve would score near-perfectly
  while telling us nothing.
- **A live check** that runs the real evaluation, skipped when Ollama is not
  reachable (it needs embeddings), the same pattern tests/test_rag.py uses.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

EVAL_SET = REPO_ROOT / "docs" / "eval" / "retrieval_eval.yaml"
CONTEXT_DIR = REPO_ROOT / "docs" / "context"


def _load() -> dict:
    import yaml

    return yaml.safe_load(EVAL_SET.read_text(encoding="utf-8"))


def _embedding_model_available() -> bool:
    """True only if Ollama is up AND the embedding model is actually pulled.

    Checking only that the server responds is not enough: a freshly started
    Ollama with an empty model store answers /api/tags happily and then fails
    the embed call with a 404. That produced a confusing test failure rather
    than a skip while the model was still downloading.
    """
    import httpx

    from src import config

    try:
        response = httpx.get(f"{config.OLLAMA_HOST}/api/tags", timeout=2.0)
        names = {m["name"].split(":")[0] for m in response.json().get("models", [])}
    except Exception:
        return False
    return config.OLLAMA_EMBED_MODEL.split(":")[0] in names


pytestmark_ollama = pytest.mark.skipif(
    not _embedding_model_available(),
    reason=(
        "needs a reachable Ollama with the embedding model pulled "
        "(`ollama pull nomic-embed-text`)"
    ),
)


def test_eval_set_exists_and_parses():
    spec = _load()
    assert spec["questions"], "the eval set has no questions"
    assert spec["corpus"]["context_dir"], "the eval set must name its corpus"


def test_every_label_names_a_document_that_exists():
    """A label pointing at a missing file scores 0 forever and looks like a
    retrieval failure rather than a broken eval set."""
    spec = _load()
    available = {p.name for p in CONTEXT_DIR.glob("*.md")}
    missing = sorted(
        {q["expected"] for q in spec["questions"]} - available
    )
    assert not missing, f"eval set labels documents that do not exist: {missing}"


def test_question_ids_are_unique():
    ids = [q["id"] for q in _load()["questions"]]
    assert len(ids) == len(set(ids)), "duplicate question ids"


def test_every_corpus_document_is_exercised():
    """A document nothing asks about contributes nothing but noise to the score."""
    spec = _load()
    labelled = {q["expected"] for q in spec["questions"]}
    available = {p.name for p in CONTEXT_DIR.glob("*.md")}
    unexercised = sorted(available - labelled)
    assert not unexercised, (
        f"no eval question targets {unexercised} - either add questions or "
        f"the score does not represent the whole corpus"
    )


def test_questions_do_not_quote_their_target_document():
    """Guards against an eval that flatters itself.

    A question lifted word-for-word out of the document it is supposed to
    retrieve would score highly on string overlap alone. This checks no long
    phrase from a question appears verbatim in its own label's text.
    """
    spec = _load()
    for item in spec["questions"]:
        target = (CONTEXT_DIR / item["expected"]).read_text(encoding="utf-8").lower()
        words = re.findall(r"[a-z0-9]+", item["question"].lower())
        # Any 5-word run appearing verbatim in the target is too close.
        for i in range(len(words) - 4):
            phrase = " ".join(words[i : i + 5])
            assert phrase not in target, (
                f"question {item['id']!r} quotes its target document "
                f"({item['expected']}) verbatim: {phrase!r}"
            )


def test_eval_set_is_balanced_across_documents():
    """A score dominated by one document is not a corpus-wide measurement."""
    from collections import Counter

    counts = Counter(q["expected"] for q in _load()["questions"])
    assert min(counts.values()) >= 3, (
        f"every document needs at least 3 questions for the per-document "
        f"figure to mean anything: {dict(counts)}"
    )


@pytestmark_ollama
def test_retrieval_eval_runs_and_reports_metrics():
    """The real thing: build an index, score the set, sanity-check the shape.

    Deliberately does NOT assert a quality threshold. The measured value belongs
    in docs/METRICS.md, and a test that pins it would either be tautological or
    would start failing for reasons unrelated to a code change (a different
    embedding model, a reworded document). What is asserted is that the harness
    produces a coherent report.
    """
    from eval_retrieval import evaluate

    report = evaluate(top_k=3)

    assert report["questions"] == len(_load()["questions"])
    assert report["corpus_documents"] >= 3
    assert 0.0 <= report["precision_at_1"] <= 1.0
    assert 0.0 <= report["mrr"] <= 1.0
    # Every question must have produced a ranking, hit or miss.
    assert all("ranked" in r for r in report["results"])


# --- Chunking (the retrieval-quality improvement) ---------------------------
#
# Portable: these exercise the splitting logic directly, no embeddings needed.


def test_chunking_splits_a_list_document_per_item():
    """The whole point: a 17-company list must not be one averaged vector.

    Measured before/after in docs/METRICS.md - this is what took precision@1
    from 0.528 to 0.917.
    """
    from src.rag.index_builder import _load_docs

    chunks = _load_docs(str(CONTEXT_DIR))
    tickers = [c for c in chunks if c["metadata"]["source"] == "tickers.md"]
    assert len(tickers) >= 10, (
        f"tickers.md produced {len(tickers)} chunks; a list of 17 companies "
        f"embedded as one vector is what caused the original misses"
    )


def test_chunking_keeps_prose_sections_whole():
    """Splitting prose mid-argument loses the context that makes it findable."""
    from src.rag.index_builder import _load_docs

    chunks = _load_docs(str(CONTEXT_DIR))
    methodology = [c for c in chunks if c["metadata"]["source"] == "methodology.md"]
    # 3 prose sections - if this ballooned, prose is being split per bullet too.
    assert 1 <= len(methodology) <= 6, (
        f"methodology.md produced {len(methodology)} chunks; prose sections "
        f"should stay intact"
    )


def test_every_chunk_carries_its_source_document():
    """Citations and eval labels are document-level even though retrieval is not."""
    from src.rag.index_builder import _load_docs

    for chunk in _load_docs(str(CONTEXT_DIR)):
        assert chunk["metadata"].get("source", "").endswith(".md"), chunk["id"]


def test_no_chunk_is_just_a_heading():
    """A title-only chunk embeds to a vector that matches without answering."""
    from src.rag.index_builder import _load_docs

    tiny = [c for c in _load_docs(str(CONTEXT_DIR)) if len(c["text"]) < 80]
    assert not tiny, f"content-free chunks would add noise: {[c['id'] for c in tiny]}"


def test_chunking_can_be_switched_off():
    """The before/after in METRICS.md must stay reproducible."""
    import importlib

    from src import config

    original = config.RAG_CHUNKING
    try:
        config.RAG_CHUNKING = False
        import src.rag.index_builder as ib

        importlib.reload(ib)
        ib.config.RAG_CHUNKING = False
        whole = ib._load_docs(str(CONTEXT_DIR))
        assert len(whole) == len(list(CONTEXT_DIR.glob("*.md"))), (
            "with chunking off there should be exactly one entry per file"
        )
    finally:
        config.RAG_CHUNKING = original
        import src.rag.index_builder as ib

        importlib.reload(ib)


def test_top_k_exceeds_the_saturation_point():
    """k must not drop below where retrieval accuracy saturates.

    hit@2 is already 1.000 on the eval corpus once chunked, so k=6 is chosen for
    context volume rather than accuracy - chunks are ~5x smaller than the
    documents they replaced. Guard against someone lowering it past the point
    where recall would start to suffer.
    """
    from src import config

    assert config.RAG_TOP_K >= 3, (
        f"RAG_TOP_K={config.RAG_TOP_K} leaves no margin above the measured "
        f"saturation point"
    )


@pytestmark_ollama
def test_reindexing_removes_entries_whose_ids_disappeared(tmp_path):
    """upsert alone leaves orphans that keep competing in every query.

    Found for real: turning chunking on changed ids from "tickers.md" to
    "tickers.md#0..16", and the stale whole-file vector - the single averaged
    embedding that chunking exists to remove - stayed in the live collection
    alongside its own replacements. Deleting a source document orphans an entry
    the same way.
    """
    from src.rag.index_builder import build_index

    context = tmp_path / "context"
    context.mkdir()
    briefings = tmp_path / "briefings"
    briefings.mkdir()
    persist = tmp_path / "idx"

    (context / "alpha.md").write_text(
        "Alpha notes. " + ("Kafka streaming consumer lag and micro-batches. " * 6),
        encoding="utf-8",
    )
    (context / "beta.md").write_text(
        "Beta notes. " + ("Sourdough proving times and oven temperature. " * 6),
        encoding="utf-8",
    )
    first = build_index(
        context_dir=str(context), briefings_dir=str(briefings),
        persist_dir=str(persist), collection_name="orphan_test",
    )
    assert first["collection_count"] == first["docs_indexed"]

    # Remove a source document and rebuild.
    (context / "beta.md").unlink()
    second = build_index(
        context_dir=str(context), briefings_dir=str(briefings),
        persist_dir=str(persist), collection_name="orphan_test",
    )

    assert second["stale_removed"] >= 1, "the deleted document left an orphan"
    assert second["collection_count"] == second["docs_indexed"], (
        f"collection holds {second['collection_count']} entries but only "
        f"{second['docs_indexed']} documents were indexed - orphans remain"
    )
