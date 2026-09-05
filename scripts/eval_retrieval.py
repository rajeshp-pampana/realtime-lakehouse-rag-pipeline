"""Measure retrieval quality, not just retrieval latency.

docs/METRICS.md records how fast retrieval is (~4.5s) but nothing about whether
the right document comes back. This scores the retriever against the labelled
set in docs/eval/retrieval_eval.yaml and prints metrics suitable for pasting
into METRICS.md.

    python scripts/eval_retrieval.py            # score the committed corpus
    python scripts/eval_retrieval.py --json     # machine-readable

Reading the output honestly:

- ``index_builder`` embeds one vector per *document* - there is no chunking -
  so the unit of retrieval is a whole file and the label is a filename.
- The committed corpus is 3 documents. hit@k for k at or near the corpus size
  is trivially 1.0 and measures nothing; **precision@1 and MRR are the
  informative numbers here**, and the corpus size is printed alongside them so
  the figures cannot be quoted without that context.
- The index is built fresh in a temp directory from the committed context docs
  only. Generated briefings are in the live index but vary run to run, which
  would make the score unrepeatable.

Needs a reachable Ollama for embeddings; exits 2 with a clear message if absent.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

EVAL_SET = REPO_ROOT / "docs" / "eval" / "retrieval_eval.yaml"


def _ollama_reachable() -> bool:
    import httpx

    from src import config

    try:
        httpx.get(config.OLLAMA_HOST, timeout=3.0)
        return True
    except Exception:
        return False


def load_eval_set(path: Path = EVAL_SET) -> dict:
    import yaml

    return yaml.safe_load(path.read_text(encoding="utf-8"))


def evaluate(top_k: int = 5) -> dict:
    """Build a fresh index from the committed corpus and score every question."""
    from src.rag.index_builder import build_index
    from src.rag.retriever import retrieve

    spec = load_eval_set()
    context_dir = REPO_ROOT / spec["corpus"]["context_dir"]
    questions = spec["questions"]

    corpus_files = sorted(p.name for p in context_dir.glob("*.md"))
    workdir = Path(tempfile.mkdtemp(prefix="rlrp_eval_"))
    try:
        # Empty briefings dir: the committed context docs alone, so the score is
        # reproducible from a clean checkout.
        empty_briefings = workdir / "no_briefings"
        empty_briefings.mkdir()
        persist = workdir / "index"

        build = build_index(
            context_dir=str(context_dir),
            briefings_dir=str(empty_briefings),
            persist_dir=str(persist),
            collection_name="eval_corpus",
        )

        results = []
        latencies = []
        for item in questions:
            started = time.perf_counter()
            out = retrieve(
                item["question"],
                k=min(top_k, len(corpus_files)),
                persist_dir=str(persist),
                collection_name="eval_corpus",
            )
            latencies.append(time.perf_counter() - started)

            ranked = [
                p["metadata"].get("source", p["id"]) for p in out["passages"]
            ]
            expected = item["expected"]
            rank = ranked.index(expected) + 1 if expected in ranked else None
            results.append(
                {
                    "id": item["id"],
                    "question": item["question"],
                    "expected": expected,
                    "ranked": ranked,
                    "rank": rank,
                    "hit_at_1": rank == 1,
                    "hit_at_3": rank is not None and rank <= 3,
                    "reciprocal_rank": (1.0 / rank) if rank else 0.0,
                }
            )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    total = len(results)
    hits1 = sum(r["hit_at_1"] for r in results)
    hits3 = sum(r["hit_at_3"] for r in results)
    mrr = sum(r["reciprocal_rank"] for r in results) / total if total else 0.0

    return {
        "corpus_documents": len(corpus_files),
        "corpus_files": corpus_files,
        "questions": total,
        "precision_at_1": round(hits1 / total, 4) if total else 0.0,
        "hit_at_1": hits1,
        "hit_at_3": hits3,
        "hit_rate_at_3": round(hits3 / total, 4) if total else 0.0,
        "mrr": round(mrr, 4),
        "mean_query_latency_seconds": round(sum(latencies) / len(latencies), 3),
        "index_build_seconds": build.get("duration_seconds"),
        "embedding_dim": build.get("embedding_dim"),
        "results": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit JSON only")
    parser.add_argument("--top-k", type=int, default=5)
    args = parser.parse_args()

    if not _ollama_reachable():
        print(
            "Ollama is not reachable - retrieval needs it for embeddings.\n"
            "Start it (`ollama serve`, or `docker compose --profile llm up -d`) "
            "and re-run.",
            file=sys.stderr,
        )
        return 2

    if args.json:
        # build_index and chromadb both write progress to stdout; with --json
        # that corrupts the output for anything trying to parse it. Send their
        # chatter to stderr so stdout carries JSON and nothing else.
        import contextlib

        with contextlib.redirect_stdout(sys.stderr):
            report = evaluate(top_k=args.top_k)
        print(json.dumps(report, indent=2))
        return 0

    report = evaluate(top_k=args.top_k)

    print("=== Retrieval quality ===")
    print(f"  corpus            : {report['corpus_documents']} documents "
          f"({', '.join(report['corpus_files'])})")
    print(f"  questions         : {report['questions']}")
    print(f"  precision@1       : {report['precision_at_1']:.3f}  "
          f"({report['hit_at_1']}/{report['questions']})")
    print(f"  hit rate@3        : {report['hit_rate_at_3']:.3f}  "
          f"({report['hit_at_3']}/{report['questions']})")
    print(f"  MRR               : {report['mrr']:.3f}")
    print(f"  mean query latency: {report['mean_query_latency_seconds']}s")
    print(f"  index build       : {report['index_build_seconds']}s, "
          f"dim {report['embedding_dim']}")
    print()
    print("  NOTE: with a 3-document corpus, hit rate at k>=3 is trivially high.")
    print("        precision@1 and MRR are the informative figures.")
    print()

    misses = [r for r in report["results"] if not r["hit_at_1"]]
    if misses:
        print(f"=== {len(misses)} question(s) where the top result was not the label ===")
        for r in misses:
            top = r["ranked"][0] if r["ranked"] else "(nothing returned)"
            print(f"  [{r['id']}] {r['question']}")
            print(f"      expected {r['expected']}, top hit {top}, rank {r['rank']}")
    else:
        print("=== every question ranked its labelled document first ===")

    return 0


if __name__ == "__main__":
    sys.exit(main())
