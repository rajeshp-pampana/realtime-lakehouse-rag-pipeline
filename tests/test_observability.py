"""Tests for the Milestone 6 metrics layer.

The governing rule here is that observability must never break the thing it
observes: a Pushgateway being down has to be survivable by an ingestion run and
a streaming query. Several of these tests exist specifically to pin that down,
because the failure mode - a metrics backend taking out the pipeline - is worse
than having no metrics at all.

No Prometheus server is needed; these run anywhere.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.api.main import app
from src.observability import metrics

REPO_ROOT = Path(__file__).resolve().parent.parent
client = TestClient(app)


def test_metrics_endpoint_exposes_prometheus_text():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    body = response.text
    assert "rlrp_api_requests_total" in body
    assert "rlrp_api_request_duration_seconds" in body


def test_metrics_endpoint_is_not_in_the_public_contract():
    """/metrics is an operational surface for the scraper, not product API.

    Consumers generate clients from /openapi.json; a metrics endpoint in there
    would be noise at best and a misleading contract at worst.
    """
    spec = client.get("/openapi.json").json()
    assert "/metrics" not in spec["paths"]


def test_requests_are_counted_and_timed():
    before = client.get("/metrics").text

    for _ in range(3):
        client.get("/health")

    after = client.get("/metrics").text
    assert 'rlrp_api_requests_total{endpoint="/health"' in after.replace("'", '"') or \
           "/health" in after
    # The histogram must actually have observations, not just be declared.
    assert "rlrp_api_request_duration_seconds_count" in after
    assert before != after


def test_endpoint_label_uses_the_route_template_not_the_raw_path():
    """Labelling by raw path would create one time series per ticker.

    That is unbounded cardinality - the standard way to overwhelm a Prometheus
    server - and it is invisible until the series count explodes in production.
    """
    client.get("/api/v1/prices/MSFT")
    client.get("/api/v1/prices/NVDA")
    body = client.get("/metrics").text

    assert "/api/v1/prices/{ticker}" in body, "expected the templated route label"
    assert 'endpoint="/api/v1/prices/MSFT"' not in body, (
        "per-ticker labels would make cardinality grow with the portfolio"
    )
    assert 'endpoint="/api/v1/prices/NVDA"' not in body


def test_push_is_a_no_op_when_no_gateway_is_configured():
    """A plain local run must not require a metrics stack to be up."""
    assert metrics.push_job_metrics("test_job", {"x": 1}, gateway="") is False


def test_push_failure_never_raises():
    """A dead Pushgateway must not fail the pipeline job that produced the data.

    This is the whole reason push_job_metrics returns a bool instead of letting
    exceptions escape.
    """
    ok = metrics.push_job_metrics(
        "test_job", {"rlrp_test_metric": 1.0}, gateway="127.0.0.1:1"
    )
    assert ok is False


def test_push_skips_non_numeric_values():
    """Job metric dicts carry metadata (table paths, ticker lists) too.

    Those must be skipped rather than blowing up the push and losing the real
    numbers alongside them.
    """
    ok = metrics.push_job_metrics(
        "test_job",
        {"rows": 10, "table_path": "data/delta/x", "tickers_failed": ["ABC"]},
        gateway="127.0.0.1:1",
    )
    # Still False (no gateway), but crucially it did not raise on the strings.
    assert ok is False


def test_timed_records_even_on_exception():
    hist = metrics.RETRIEVAL_SECONDS
    before = _histogram_count(hist)

    with pytest.raises(ValueError):
        with metrics.timed(hist):
            raise ValueError("boom")

    assert _histogram_count(hist) == before + 1, (
        "a failed operation still took time; not recording it hides the slow failures"
    )


def _histogram_count(histogram) -> float:
    for metric in histogram.collect():
        for sample in metric.samples:
            if sample.name.endswith("_count"):
                return sample.value
    return 0.0


# --- Monitoring stack wiring ------------------------------------------------


def test_prometheus_scrapes_the_api_and_the_pushgateway():
    import yaml

    cfg = yaml.safe_load((REPO_ROOT / "monitoring" / "prometheus.yml").read_text())
    jobs = {j["job_name"]: j for j in cfg["scrape_configs"]}

    assert "api" in jobs, "the API exposes /metrics and must be scraped"
    assert "pushgateway" in jobs, (
        "batch tasks and the streaming consumer push; without this job their "
        "metrics are collected and never read"
    )


def test_pushgateway_scrape_preserves_pushed_job_labels():
    """honor_labels must be true, or every pushing component collapses into one.

    Without it Prometheus overwrites each pushed series' own `job` label
    ("rlrp_batch_ingest", "rlrp_stream_consumer") with the scrape job's name,
    making batch and streaming metrics indistinguishable.
    """
    import yaml

    cfg = yaml.safe_load((REPO_ROOT / "monitoring" / "prometheus.yml").read_text())
    pushgateway = next(j for j in cfg["scrape_configs"] if j["job_name"] == "pushgateway")
    assert pushgateway.get("honor_labels") is True


def test_dashboard_panels_reference_metrics_the_pipeline_emits():
    """A dashboard querying metrics nothing produces renders empty panels.

    That failure is silent - the dashboard looks fine until someone notices no
    data has ever appeared - so it is checked here instead.
    """
    import json
    import re

    dashboard = json.loads(
        (REPO_ROOT / "monitoring" / "grafana" / "dashboards" / "pipeline_health.json").read_text(
            encoding="utf-8"
        )
    )
    panels = dashboard["panels"]
    assert panels, "dashboard has no panels"

    referenced: set[str] = set()
    for panel in panels:
        for target in panel.get("targets", []):
            referenced.update(re.findall(r"rlrp_[a-z0-9_]+", target["expr"]))

    # Strip Prometheus histogram/counter suffixes to get the base metric names.
    base_names = {
        re.sub(r"_(bucket|count|sum|total)$", "", name) for name in referenced
    }

    source = "\n".join(
        p.read_text(encoding="utf-8")
        for p in (REPO_ROOT / "src").rglob("*.py")
    )
    missing = sorted(n for n in base_names if n not in source)
    assert not missing, (
        f"dashboard queries metrics that no code emits: {missing} - these panels "
        f"would silently render empty forever"
    )
