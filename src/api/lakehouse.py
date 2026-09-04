"""Read-side data access for the API.

Reads the Delta tables the batch DAG (``ohlcv_curated``) and the streaming
consumer (``ticks_raw``) produce. Uses ``deltalake`` (delta-rs) directly rather
than Spark: serving a few hundred rows over HTTP doesn't need a JVM, and it
keeps the API startable anywhere (including Windows, where PySpark local-mode
task execution is broken - see README). Spark stays where it earns its keep,
in the batch transform.

Everything here returns plain Python structures, so ``main.py`` stays thin and
these functions are unit-testable without an HTTP client.
"""

from __future__ import annotations

import math
from typing import Any

from src import config


class TableUnavailableError(RuntimeError):
    """Raised when a Delta table hasn't been created yet (pipeline not yet run)."""


def _load(table_path: str) -> tuple[Any, int]:
    """Return ``(DataFrame, delta_version)`` for a table, or raise if absent."""
    from deltalake import DeltaTable

    try:
        table = DeltaTable(table_path)
    except BaseException as exc:
        # BaseException, not Exception, and deliberately so: delta-rs is a Rust
        # extension, and for an unreadable or uncreatable table location it
        # raises pyo3_runtime.PanicException, which inherits from BaseException.
        # `except Exception` silently misses it - CI caught this when the
        # container (uid 10001) could not write the bind-mounted data dir and
        # the API returned a 500 traceback instead of the intended 503.
        if isinstance(exc, KeyboardInterrupt | SystemExit):
            raise
        raise TableUnavailableError(
            f"Delta table '{table_path}' is not available yet - run the pipeline first "
            f"({exc.__class__.__name__}: {exc})"
        ) from exc
    return table.to_pandas(), table.version()


def _clean(value: Any) -> Any:
    """Make a pandas/NumPy scalar JSON-safe.

    NaN matters here: the first bar of every ticker has a null ``daily_return``
    (no previous close), and NaN is not valid JSON - it has to become ``null``
    rather than reaching the serializer.
    """
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):  # NumPy scalar -> Python scalar
        value = value.item()
        if isinstance(value, float) and math.isnan(value):
            return None
    return value


def _records(frame: Any) -> list[dict]:
    return [{k: _clean(v) for k, v in row.items()} for row in frame.to_dict(orient="records")]


def get_prices(
    ticker: str, limit: int = 100, table_path: str | None = None
) -> tuple[list[dict], int, str]:
    """Return the most recent ``limit`` curated bars for ``ticker``, oldest first."""
    table_path = table_path or config.DELTA_OHLCV_CURATED
    frame, version = _load(table_path)

    subset = frame[frame["Ticker"].str.upper() == ticker.upper()]
    subset = subset.sort_values("Date").tail(limit)
    for column in ("Date", "ingested_at_utc"):
        if column in subset.columns:
            subset[column] = subset[column].astype(str)
    subset = subset.drop(columns=["ingested_at_utc"], errors="ignore")
    return _records(subset), version, table_path


def get_ticks(
    ticker: str, limit: int = 100, table_path: str | None = None
) -> tuple[list[dict], int, str]:
    """Return the most recent ``limit`` streaming ticks for ``ticker``."""
    table_path = table_path or config.DELTA_TICKS_RAW
    frame, version = _load(table_path)

    subset = frame[frame["ticker"].str.upper() == ticker.upper()]
    subset = subset.sort_values("event_time").tail(limit)
    for column in ("event_time", "kafka_timestamp"):
        if column in subset.columns:
            subset[column] = subset[column].astype(str)
    return _records(subset), version, table_path


def lakehouse_stats() -> list[dict]:
    """Per-table row count and Delta version - an operational surface for the UI.

    Reports unavailable tables as ``available: false`` with a reason rather than
    failing the whole request, so the endpoint still works on a partially-run
    pipeline (e.g. ticks_raw missing because streaming was never started).
    """
    tables = [
        ("ohlcv_raw", config.DELTA_OHLCV_RAW),
        ("ohlcv_curated", config.DELTA_OHLCV_CURATED),
        ("ticks_raw", config.DELTA_TICKS_RAW),
    ]
    stats = []
    for name, path in tables:
        try:
            frame, version = _load(path)
        except TableUnavailableError as exc:
            stats.append(
                {"name": name, "path": path, "available": False, "detail": str(exc)}
            )
            continue
        stats.append(
            {
                "name": name,
                "path": path,
                "available": True,
                "rows": int(len(frame)),
                "version": int(version),
            }
        )
    return stats
