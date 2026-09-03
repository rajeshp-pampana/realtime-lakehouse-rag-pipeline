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

# --- Kafka (Milestone 2) ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TICKS_TOPIC = os.environ.get("KAFKA_TICKS_TOPIC", "market.ticks")
TICK_INTERVAL_SECONDS = float(os.environ.get("TICK_INTERVAL_SECONDS", "1"))

# --- LLM / RAG (Milestone 3) ---
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3")
RAG_TOP_K = int(os.environ.get("RAG_TOP_K", "4"))

# --- API (Milestone 4) ---
API_HOST = os.environ.get("API_HOST", "0.0.0.0")
API_PORT = int(os.environ.get("API_PORT", "8000"))
