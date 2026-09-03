"""Batch transform tests.

`test_run_transform_computes_indicators` is a real integration test (writes a
fixture Delta table, runs the actual PySpark transform, reads the curated Delta
table back). It's skipped on Windows: PySpark local-mode worker startup is
currently broken on the dev machine (the JVM-spawned Python worker is killed
within ~2s with no output, most likely AV/EDR — see README "Milestone 1 ...
Without Docker"). It runs for real in CI (Linux) and in the Airflow container,
which is what actually proves this milestone's transform works end to end.
"""

import importlib
import platform

import pandas as pd
import pytest

pytestmark = pytest.mark.skipif(
    platform.system() == "Windows",
    reason="PySpark local-mode worker startup is broken on this dev machine; verified in CI/Docker instead",
)


def test_transform_module_imports():
    mod = importlib.import_module("src.processing.transform_spark")
    assert hasattr(mod, "run_transform")


def test_run_transform_computes_indicators(tmp_path):
    from deltalake import DeltaTable, write_deltalake

    from src.processing.transform_spark import run_transform

    dates = pd.date_range("2026-01-01", periods=25, freq="B").strftime("%Y-%m-%d")
    raw = pd.concat(
        [
            pd.DataFrame(
                {
                    "Date": dates,
                    "Ticker": ticker,
                    "Open": 100.0,
                    "High": 101.0,
                    "Low": 99.0,
                    "Close": [100.0 + i * 0.5 for i in range(len(dates))],
                    "Volume": 1_000_000,
                    "ingested_at_utc": "2026-01-01T00:00:00+00:00",
                }
            )
            for ticker in ("AAA", "BBB")
        ],
        ignore_index=True,
    )

    raw_path = str(tmp_path / "ohlcv_raw")
    curated_path = str(tmp_path / "ohlcv_curated")
    write_deltalake(raw_path, raw, mode="append")

    metrics = run_transform(raw_table_path=raw_path, curated_table_path=curated_path)

    assert metrics["rows_in"] == len(raw)
    assert metrics["rows_out"] == len(raw)

    curated = DeltaTable(curated_path).to_pandas()
    assert {"SMA_20", "SMA_50", "daily_return", "volatility_20d"} <= set(curated.columns)

    # First row per ticker has no prior close, so no return/volatility yet.
    first_rows = curated.sort_values(["Ticker", "Date"]).groupby("Ticker").head(1)
    assert first_rows["daily_return"].isna().all()

    # SMA_20 on a monotonically increasing series should itself be monotonically
    # increasing per ticker.
    for _, group in curated.sort_values(["Ticker", "Date"]).groupby("Ticker"):
        assert group["SMA_20"].is_monotonic_increasing
