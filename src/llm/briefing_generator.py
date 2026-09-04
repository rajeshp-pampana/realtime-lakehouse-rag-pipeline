"""Local LLM briefing generation (Llama 3 via Ollama).

Baseline behaviour (ported from the original ``market_agent.py``): take a short
window of recent bars for one ticker and ask a local model for a two-sentence
institutional-style market update.

Milestone 3 makes this retrieval-grounded: ``src/rag/retriever.py`` runs first
and its output is injected into the prompt, labeled by source, with an explicit
instruction to cite what it draws on - so a briefing can be checked against
prior context, not just the day's raw numbers. Every generated briefing is
also saved into ``data/briefings/`` (with frontmatter), which grows the corpus
``src/rag/index_builder.py`` indexes.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime

import pandas as pd

from src import config

try:  # ollama is optional at import time so tests can run without it
    import ollama
except ImportError:  # pragma: no cover
    ollama = None

MODEL = config.OLLAMA_MODEL
SYSTEM_PROMPT = "You are a Wall Street Equity Research Analyst."


def _prompt(ticker: str, recent: pd.DataFrame, passages: list[dict]) -> str:
    trend_text = recent.to_string(index=False)

    if passages:
        context_block = "\n\n".join(
            f"[{p['metadata'].get('source', p['id'])}]\n{p['text']}" for p in passages
        )
        context_section = f"""
Prior context retrieved for this briefing (cite the bracketed source if you
use it - e.g. "per [schema_notes.md]..." - and don't cite one you didn't
actually draw on):
{context_block}
"""
    else:
        context_section = "\nNo prior context was retrieved for this query.\n"

    return f"""
You are a Senior Equity Research Analyst at a top-tier Wall Street investment bank.
Review the following trailing market data for {ticker}:
{trend_text}
{context_section}
Provide a concise, 2-3 sentence market update suitable for an institutional morning
briefing. Focus on price action, momentum, and technical sentiment. Use professional
financial terminology (e.g. 'consolidating', 'bullish/bearish divergence', 'testing
support/resistance', 'price discovery') where appropriate. Ground your view in the
retrieved context above where it's actually relevant.
""".strip()


def _save_briefing(ticker: str, text: str, sources: list[str]) -> str:
    os.makedirs(config.BRIEFINGS_DIR, exist_ok=True)
    now = datetime.now(UTC)
    date_str = now.strftime("%Y-%m-%d")
    path = os.path.join(config.BRIEFINGS_DIR, f"{ticker}_{now.strftime('%Y%m%dT%H%M%S')}.md")
    frontmatter = (
        "---\n"
        "type: briefing\n"
        f"ticker: {ticker}\n"
        f"date: {date_str}\n"
        f"sources: {', '.join(sources) if sources else 'none'}\n"
        "---\n"
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(frontmatter + text.strip() + "\n")
    return path


def _observe_generation(seconds: float, outcome: str) -> None:
    """Record briefing latency and outcome; never break generation over metrics."""
    try:
        from src.observability.metrics import BRIEFING_SECONDS, BRIEFINGS_TOTAL

        BRIEFING_SECONDS.observe(seconds)
        BRIEFINGS_TOTAL.labels(outcome).inc()
    except Exception:  # noqa: BLE001
        pass


def generate_briefing(ticker: str, recent: pd.DataFrame, save: bool = True) -> dict:
    """Generate a retrieval-grounded briefing for ``ticker`` over ``recent`` bars.

    Returns ``{"text", "retrieved_sources", "retrieval_latency_seconds",
    "generation_latency_seconds", "saved_to"}``.
    """
    if ollama is None:  # pragma: no cover
        raise RuntimeError("ollama is not installed; cannot run local inference")

    from src.rag.retriever import retrieve

    retrieval = retrieve(f"{ticker} market context technical analysis")
    passages = retrieval["passages"]
    sources = [p["metadata"].get("source", p["id"]) for p in passages]

    gen_started = time.perf_counter()
    response = ollama.chat(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _prompt(ticker, recent, passages)},
        ],
    )
    text = response["message"]["content"] if isinstance(response, dict) else response.message.content
    generation_latency = round(time.perf_counter() - gen_started, 2)

    _observe_generation(generation_latency, "success")

    saved_to = _save_briefing(ticker, text, sources) if save else None

    result = {
        "text": text,
        "retrieved_sources": sources,
        "retrieval_latency_seconds": retrieval["latency_seconds"],
        "generation_latency_seconds": generation_latency,
        "saved_to": saved_to,
    }
    print(f"[briefing_generator] ticker={ticker} sources={sources} "
          f"retrieval={retrieval['latency_seconds']}s generation={generation_latency}s")
    return result


if __name__ == "__main__":
    import sys

    from src.ingestion.fetch_market_data import fetch_ticker_history

    tkr = sys.argv[1] if len(sys.argv) > 1 else "MSFT"
    out = generate_briefing(tkr, fetch_ticker_history(tkr).tail(5))
    print(out["text"])
    print(f"\n(sources: {out['retrieved_sources']}, saved to {out['saved_to']})")
