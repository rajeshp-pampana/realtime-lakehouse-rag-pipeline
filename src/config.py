"""Shared configuration, loaded from environment / .env.

Milestone 1 introduces this: every module that needs a path or setting reads it
from here instead of hardcoding it (see README "Config & secrets").
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = os.environ.get("DATA_DIR", "data")
DELTA_OHLCV_RAW = os.environ.get("DELTA_OHLCV_RAW", "data/delta/ohlcv_raw")
DELTA_OHLCV_CURATED = os.environ.get("DELTA_OHLCV_CURATED", "data/delta/ohlcv_curated")
DELTA_TICKS_RAW = os.environ.get("DELTA_TICKS_RAW", "data/delta/ticks_raw")
VECTORSTORE_DIR = os.environ.get("VECTORSTORE_DIR", "data/vectorstore")
STREAM_CHECKPOINT_DIR = os.environ.get("STREAM_CHECKPOINT_DIR", "data/checkpoints/ticks_stream")

INGEST_PERIOD = os.environ.get("INGEST_PERIOD", "1mo")

# The portfolio the pipeline covers. Lives here rather than in
# ingestion/fetch_market_data.py because it is configuration, not ingestion
# logic - and because the API needs the list but must not pull yfinance (and
# its transitive deps) into its image just to read a constant. Milestone 5
# made that coupling expensive enough to be worth removing.
TICKERS = [
    "MSFT", "CRWD", "AVGO", "GLE.PA", "NVDA", "AMZN", "AXON", "PANW",
    "INTC", "NOW", "IREN", "GOOG", "MU", "SOFI", "PLTR", "RDW", "DRAM",
]

# --- Kafka (Milestone 2) ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TICKS_TOPIC = os.environ.get("KAFKA_TICKS_TOPIC", "market.ticks")
TICK_INTERVAL_SECONDS = float(os.environ.get("TICK_INTERVAL_SECONDS", "1"))

# --- LLM / RAG (Milestone 3) ---
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3")
OLLAMA_EMBED_MODEL = os.environ.get("OLLAMA_EMBED_MODEL", "nomic-embed-text")
# Retuned from 4 to 6 alongside chunking, measured rather than guessed:
# retrieval accuracy saturates far below it (hit@2 is already 1.000 on the eval
# corpus), so a larger k costs no precision, and chunks are ~5x smaller than the
# whole documents they replaced - at k=4 the model would receive a quarter of
# the context it used to. 6 restores some of that while leaving headroom as
# generated briefings accumulate in the live index. See docs/METRICS.md.
RAG_TOP_K = int(os.environ.get("RAG_TOP_K", "6"))
RAG_COLLECTION = os.environ.get("RAG_COLLECTION", "market_context")
# Split documents into chunks before embedding rather than one vector per file.
# Measured: whole-document embedding of a 17-item list scored precision@1 0.528;
# see docs/METRICS.md for the before/after. Switchable so that comparison stays
# reproducible.
RAG_CHUNKING = os.environ.get("RAG_CHUNKING", "true").lower() in ("1", "true", "yes")
CONTEXT_DOCS_DIR = os.environ.get("CONTEXT_DOCS_DIR", "docs/context")
BRIEFINGS_DIR = os.environ.get("BRIEFINGS_DIR", "data/briefings")

# --- API (Milestone 4) ---
API_HOST = os.environ.get("API_HOST", "0.0.0.0")
API_PORT = int(os.environ.get("API_PORT", "8000"))
# What the Streamlit thin client dials. Separate from API_HOST/API_PORT (which
# are the *bind* address) so the UI can point at a container, a k8s service, or
# a remote host in Milestone 5 without the API's own binding changing.
API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")
# Briefing generation runs a local LLM and can take minutes on modest hardware
# (see docs/METRICS.md) - the UI's HTTP client needs a timeout that reflects
# that, not the usual few seconds.
API_TIMEOUT_SECONDS = float(os.environ.get("API_TIMEOUT_SECONDS", "30"))
API_BRIEFING_TIMEOUT_SECONDS = float(os.environ.get("API_BRIEFING_TIMEOUT_SECONDS", "900"))

# --- Observability (Milestone 6) ---
# Short-lived batch tasks and the non-HTTP streaming consumer cannot be
# scraped, so they push to a Pushgateway instead. Empty disables pushing
# entirely, which is the default for a plain local run - nothing should require
# a metrics stack to be up just to ingest data.
PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "")
METRICS_ENABLED = os.environ.get("METRICS_ENABLED", "true").lower() in ("1", "true", "yes")
