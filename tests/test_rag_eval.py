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
