"""Local LLM briefing generation (Llama 3 via Ollama).

Baseline behaviour (ported from the original ``market_agent.py``): take a short
window of recent bars for one ticker and ask a local model for a two-sentence
institutional-style market update.

Milestone 3 makes this retrieval-grounded: a retrieval step (``src/rag/retriever.py``)
runs first and its output is injected into the prompt so the briefing cites prior
context, not just the day's raw numbers.
"""

from __future__ import annotations

import pandas as pd

try:  # ollama is optional at import time so tests can run without it
    import ollama
except ImportError:  # pragma: no cover
    ollama = None

MODEL = "llama3"
SYSTEM_PROMPT = "You are a Wall Street Equity Research Analyst."


def _prompt(ticker: str, recent: pd.DataFrame) -> str:
    trend_text = recent.to_string(index=False)
    return f"""
You are a Senior Equity Research Analyst at a top-tier Wall Street investment bank.
Review the following trailing market data for {ticker}:
{trend_text}

Provide a concise, 2-sentence market update suitable for an institutional morning
briefing. Focus on price action, momentum, and technical sentiment. Use professional
financial terminology (e.g. 'consolidating', 'bullish/bearish divergence', 'testing
support/resistance', 'price discovery') where appropriate.
""".strip()


def generate_briefing(ticker: str, recent: pd.DataFrame) -> str:
    """Return a short generated briefing for ``ticker`` over ``recent`` bars."""
    if ollama is None:  # pragma: no cover
        raise RuntimeError("ollama is not installed; cannot run local inference")

    response = ollama.chat(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _prompt(ticker, recent)},
        ],
    )
    if isinstance(response, dict):
        return response["message"]["content"]
    return response.message.content


if __name__ == "__main__":
    import sys

    from src.ingestion.fetch_market_data import fetch_ticker_history

    tkr = sys.argv[1] if len(sys.argv) > 1 else "MSFT"
    print(generate_briefing(tkr, fetch_ticker_history(tkr).tail(5)))
