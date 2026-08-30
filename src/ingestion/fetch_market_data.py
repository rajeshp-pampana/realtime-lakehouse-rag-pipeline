"""End-of-day market data ingestion.

Baseline behaviour (ported from the original AI Market Terminal repo): pull ~1 month
of daily OHLCV bars per ticker from Yahoo Finance and land them locally.

Milestone 1 adapts this module to write into a versioned Delta Lake table
(``data/delta/ohlcv_raw``) instead of per-ticker CSV files, and exposes a callable
entry point that the Airflow DAG can invoke as a task.
"""

from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import yfinance as yf

# Master portfolio list (unchanged from baseline).
TICKERS = [
    "MSFT", "CRWD", "AVGO", "GLE.PA", "NVDA", "AMZN", "AXON", "PANW",
    "INTC", "NOW", "IREN", "GOOG", "MU", "SOFI", "PLTR", "RDW", "DRAM",
]

RAW_DIR = os.path.join("data", "raw")


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


if __name__ == "__main__":
    fetch_and_save_data()
