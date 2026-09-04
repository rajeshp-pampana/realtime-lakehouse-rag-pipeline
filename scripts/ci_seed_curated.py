"""Seed a small curated Delta table so CI's API has something real to serve.

CI has no Yahoo Finance access and no prior pipeline state, but the compose
end-to-end test needs the API to return actual rows rather than a 503. This
writes a genuine Delta table (delta-rs, same writer the pipeline uses) with the
same schema the PySpark transform produces - including the NaN first-bar
`daily_return`, which is what proves the API serialises NaN as null rather than
emitting invalid JSON.
"""

from __future__ import annotations

import pandas as pd
from deltalake import write_deltalake

CURATED_PATH = "data/delta/ohlcv_curated"


def main() -> None:
    frame = pd.DataFrame(
        {
            "Date": ["2026-09-01", "2026-09-02", "2026-09-03"] * 2,
            "Ticker": ["MSFT"] * 3 + ["NVDA"] * 3,
            "Open": [500.0, 501.0, 502.0, 100.0, 101.0, 102.0],
            "High": [510.0, 511.0, 512.0, 110.0, 111.0, 112.0],
            "Low": [495.0, 496.0, 497.0, 95.0, 96.0, 97.0],
            "Close": [505.0, 506.0, 507.0, 105.0, 106.0, 107.0],
            "Volume": [1000, 1100, 1200, 2000, 2100, 2200],
            "ingested_at_utc": ["2026-09-03T00:00:00+00:00"] * 6,
            "SMA_20": [505.0, 505.5, 506.0, 105.0, 105.5, 106.0],
            "SMA_50": [505.0, 505.5, 506.0, 105.0, 105.5, 106.0],
            # First bar of each ticker has no previous close.
            "daily_return": [float("nan"), 0.00198, 0.00198, float("nan"), 0.00952, 0.00943],
            "volatility_20d": [float("nan"), 0.001, 0.001, float("nan"), 0.002, 0.002],
        }
    )
    write_deltalake(CURATED_PATH, frame, mode="overwrite")
    print(f"seeded {len(frame)} curated rows into {CURATED_PATH}")


if __name__ == "__main__":
    main()
