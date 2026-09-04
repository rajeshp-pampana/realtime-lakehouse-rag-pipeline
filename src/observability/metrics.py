"""Prometheus metric definitions and helpers.

Three very different things need to report metrics here, and they cannot all be
scraped the same way:

- **The API** is long-lived and already serves HTTP, so it exposes ``/metrics``
  and Prometheus scrapes it directly.
- **The batch tasks** (ingest, transform) are short-lived. An Airflow task that
  runs for 60 seconds and exits will never be caught by a 15-second scrape, so
  they *push* to a Pushgateway instead.
- **The streaming consumer** is long-lived but serves no HTTP at all - it is a
  Spark driver. It also pushes.

Pushing is deliberately best-effort: `push_job_metrics` never raises. A metrics
backend being down must not fail an ingestion run or kill a streaming query -
that would make observability a source of outages rather than a view of them.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
)

from src import config

# --- API metrics (scraped from /metrics) ------------------------------------
#
# These live on the default registry because the API process owns them for its
# whole lifetime.

API_REQUESTS = Counter(
    "rlrp_api_requests_total",
    "API requests by endpoint and outcome.",
    ["method", "endpoint", "status"],
)

API_REQUEST_SECONDS = Histogram(
    "rlrp_api_request_duration_seconds",
    "API request latency by endpoint.",
    ["method", "endpoint"],
    # Buckets span the measured range: /health ~2.5ms, prices ~24ms,
    # lakehouse/stats ~87ms p50 / 164ms p95 (docs/METRICS.md). Defaults would
    # lump all the read endpoints into one bucket and show nothing useful.
    buckets=(0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

RETRIEVAL_SECONDS = Histogram(
    "rlrp_retrieval_duration_seconds",
    "RAG retrieval latency (query embedding + vector search).",
    buckets=(0.5, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 30.0),
)

BRIEFING_SECONDS = Histogram(
    "rlrp_briefing_generation_duration_seconds",
    "Local LLM briefing generation latency.",
    # Measured 66-93s warm, 480-538s cold on this hardware - so the buckets run
    # far past what a hosted model would need.
    buckets=(30, 60, 90, 120, 300, 600, 900),
)

BRIEFINGS_TOTAL = Counter(
    "rlrp_briefings_generated_total",
    "Briefings generated, by outcome.",
    ["outcome"],
)


# --- Job metrics (pushed to the Pushgateway) --------------------------------

def _gateway() -> str:
    return os.environ.get("PUSHGATEWAY_URL", config.PUSHGATEWAY_URL)


@contextmanager
def timed(histogram: Histogram) -> Iterator[None]:
    """Observe elapsed seconds into ``histogram``, including on exception."""
    started = time.perf_counter()
    try:
        yield
    finally:
        histogram.observe(time.perf_counter() - started)


def push_job_metrics(job: str, metrics: dict[str, float], gateway: str | None = None) -> bool:
    """Push a short-lived job's metrics to the Pushgateway.

    ``metrics`` maps metric name -> value; each becomes a Gauge on a private
    registry so pushes from different jobs never collide.

    Returns True if the push succeeded. Never raises: a metrics backend being
    unreachable must not fail the pipeline job that produced the numbers.
    """
    gateway = gateway or _gateway()
    if not gateway:
        return False

    registry = CollectorRegistry()
    for name, value in metrics.items():
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            # Non-numeric entries (table paths, ticker lists) are metadata, not
            # metrics - skip rather than fail the whole push.
            continue
        Gauge(name, f"{name} reported by job {job}", registry=registry).set(numeric)

    try:
        push_to_gateway(gateway, job=job, registry=registry)
        return True
    except Exception as exc:  # noqa: BLE001 - observability must never break the job
        print(f"[metrics] push to {gateway} failed for job={job}: {exc}")
        return False
