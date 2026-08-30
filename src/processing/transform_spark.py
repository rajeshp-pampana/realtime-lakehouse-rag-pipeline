"""PySpark batch transforms (replaces the baseline pandas rolling-mean logic).

Milestone 1 implements this: read the raw OHLCV Delta table, compute technical
indicators (20/50-day SMA, returns, volatility) in PySpark local mode, and write a
curated Delta table with schema enforcement.

Placeholder only until Milestone 1.
"""

from __future__ import annotations


def run_transform(*args, **kwargs):
    raise NotImplementedError("Milestone 1: PySpark transform not implemented yet")
