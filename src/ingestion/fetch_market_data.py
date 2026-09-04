"""End-of-day market data ingestion.

Baseline behaviour (ported from the original AI Market Terminal repo): pull ~1 month
of daily OHLCV bars per ticker from Yahoo Finance and land them locally.

Milestone 1 adds ``ingest_to_delta``: it fetches the same data and appends it into
a versioned Delta Lake table (``data/delta/ohlcv_raw`` by default) instead of
per-ticker CSV files, and is the callable the Airflow DAG invokes as its ingest
task. ``fetch_and_save_data`` (CSV) is kept as-is for the ported baseline Streamlit
console, which still reads local CSVs until Milestone 4 moves it onto the API.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime

import pandas as pd
import yfinance as yf

from src import config

# Master portfolio list (unchanged from baseline). Defined in src/config.py and
# re-exported here so existing callers keep working; see the note there on why
# it moved (the API needs the list without importing yfinance).
TICKERS = config.TICKERS

RAW_DIR = os.path.join("data", "raw")

# Schema written to the raw Delta table. Enforced explicitly on every ingest so a
# type drift in a Yahoo Finance response (e.g. Volume coming back as float once)
# fails loudly instead of silently corrupting the table.
_DELTA_DTYPES = {
    "Date": "string",
    "Ticker": "string",
    "Open": "float64",
    "High": "float64",
    "Low": "float64",
    "Close": "float64",
    "Volume": "int64",
    "ingested_at_utc": "string",
}


def fetch_ticker_history(ticker: str, period: str = "1mo") -> pd.DataFrame:
    """Return a tidy OHLCV frame for one ticker, or an empty frame on no data."""
    stock = yf.Ticker(ticker)
    df = stock.history(period=period)
    if df.empty:
        return df

    df = df.reset_index()
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    required_cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
    df = df[[c for c in required_cols if c in df.columns]]
    df.insert(1, "Ticker", ticker)
    return df


def fetch_and_save_data(tickers: list[str] | None = None) -> str:
    """Baseline sink: one clean CSV per ticker under ``data/raw/``.

    Retained so the pipeline is runnable before Milestone 1 wires in Delta.
    """
    tickers = tickers or TICKERS
    os.makedirs(RAW_DIR, exist_ok=True)
    today_str = datetime.now().strftime("%Y%m%d")

    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        df = fetch_ticker_history(ticker)
        if df.empty:
            print(f"Warning: no data returned for {ticker}")
            continue
        file_path = os.path.join(RAW_DIR, f"{ticker}_raw_{today_str}.csv")
        df.drop(columns=["Ticker"]).to_csv(file_path, index=False)
        print(f"Saved clean dataset to {file_path}")

    return RAW_DIR


def ingest_to_delta(
    tickers: list[str] | None = None, table_path: str | None = None
) -> dict:
    """Fetch OHLCV for each ticker and append the batch into the raw Delta table.

    This is the Airflow ``ingest`` task's entry point. Returns a metrics dict
    (rows ingested, tickers ok/failed, duration, resulting table version) — the
    numbers docs/METRICS.md and the README's metrics table are filled in from.
    """
    from deltalake import DeltaTable, write_deltalake

    tickers = tickers or TICKERS
    table_path = table_path or config.DELTA_OHLCV_RAW
    started = time.perf_counter()
    now_iso = datetime.now(UTC).isoformat()

    frames: list[pd.DataFrame] = []
    failed: list[str] = []
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        df = fetch_ticker_history(ticker, period=config.INGEST_PERIOD)
        if df.empty:
            print(f"Warning: no data returned for {ticker}")
            failed.append(ticker)
            continue
        frames.append(df)

    if not frames:
        raise RuntimeError("Ingestion produced no rows for any ticker; aborting Delta write")

    batch = pd.concat(frames, ignore_index=True)
    batch["ingested_at_utc"] = now_iso
    for col, dtype in _DELTA_DTYPES.items():
        batch[col] = batch[col].astype(dtype)
    batch = batch[list(_DELTA_DTYPES)]

    os.makedirs(os.path.dirname(table_path) or ".", exist_ok=True)
    write_deltalake(table_path, batch, mode="append")
    table_version = DeltaTable(table_path).version()

    metrics = {
        "rows_ingested": int(len(batch)),
        "tickers_ok": len(frames),
        "tickers_failed": failed,
        "duration_seconds": round(time.perf_counter() - started, 2),
        "table_path": table_path,
        "table_version": table_version,
    }
    print(f"[ingest] {metrics}")
    return metrics


if __name__ == "__main__":
    ingest_to_delta()
