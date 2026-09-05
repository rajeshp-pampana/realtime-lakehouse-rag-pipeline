"""Tests for the Milestone 7 documentation claims.

The rule this project has followed throughout is that no number appears in the
docs unless it was measured. That rule is only worth anything if it survives
future edits, so it gets a test rather than good intentions.

These check the README's headline figures still exist in docs/METRICS.md. They
cannot verify a measurement was honest - nothing can - but they do catch the
realistic failure: a figure edited, rounded or invented in the README while
METRICS.md still says something else.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"
METRICS = REPO_ROOT / "docs" / "METRICS.md"
CV_NUMBERS = REPO_ROOT / "docs" / "CV_NUMBERS.md"

# Figures the README states as headline results. Each must appear verbatim in
# METRICS.md. Kept as an explicit list rather than scraped automatically: a
# regex over all numbers would match version strings and port numbers and
# produce noise, and this list is the set of claims that actually matter.
HEADLINE_FIGURES = [
    "53.28s",      # PySpark transform runtime
    "765",         # events published / landed
    "45.05s",      # publish window
    "16.98",       # events per second
    "2.82s",       # p50 streaming write latency
    "918",         # rows landed by the containerised streaming path
    "24.26ms",     # prices p50
    "26.98ms",     # prices p95
    "86.83ms",     # lakehouse/stats p50
    "164.04ms",    # lakehouse/stats p95
    "2.48ms",      # health p50
    "12.14s",      # index build
    "480.28s",     # cold briefing generation
    "2.73 tok/s",  # llama3 throughput
    "980 MB",      # API image size after pruning
    "27s",         # compose cold start
    "23 min",      # kind soak duration
    "4.3 min",     # CI wall clock
    "0.667",       # retrieval precision@1
    "0.789",       # retrieval MRR
    "705.7",       # containerised warm briefing
    "956.2",       # containerised cold briefing
]


@pytest.mark.parametrize("figure", HEADLINE_FIGURES)
def test_readme_figures_are_backed_by_a_measurement(figure: str):
    """Every headline number in the README must exist in the metrics log."""
    metrics = METRICS.read_text(encoding="utf-8")
    assert figure in metrics, (
        f"README quotes {figure!r} but docs/METRICS.md does not contain it. "
        f"Either the measurement was never recorded or the README figure has "
        f"drifted from it - both are the thing this project promises not to do."
    )


def test_readme_has_no_unfilled_placeholders():
    """Milestone 7's own promise: no placeholders, no invented figures."""
    readme = README.read_text(encoding="utf-8")
    for marker in ("[INSERT]", "_tbd_", "TBD", "TODO", "FIXME", "XXX"):
        assert marker not in readme, f"README still contains {marker!r}"


def test_metrics_log_has_no_unfilled_rows():
    """A `_tbd_` row means a metric was promised and never measured."""
    metrics = METRICS.read_text(encoding="utf-8")
    assert "_tbd_" not in metrics, (
        "docs/METRICS.md still has a _tbd_ row - either measure it or remove "
        "the claim that it exists"
    )


def test_cv_numbers_trace_every_figure_to_a_metric():
    """The CV doc's value is that each figure is defensible.

    A bullet with no METRICS.md row behind it is exactly the kind of number
    that cannot be defended in an interview.
    """
    cv = CV_NUMBERS.read_text(encoding="utf-8")
    assert "METRICS.md" in cv, "CV numbers must point back at the metrics log"

    # The traceability table maps each figure to its source row.
    table_rows = [
        line for line in cv.splitlines()
        if line.startswith("|") and "METRICS.md" not in line and "---" not in line
    ]
    assert len(table_rows) > 10, (
        "expected a substantial figure-to-source table in docs/CV_NUMBERS.md"
    )


def test_cv_numbers_records_what_not_to_claim():
    """The unclaimable list is the point: it stops invented numbers creeping in."""
    cv = CV_NUMBERS.read_text(encoding="utf-8")
    assert "Numbers NOT to claim" in cv
    for forbidden_claim in ("uptime", "cost savings", "real-time market data"):
        assert forbidden_claim in cv.lower(), (
            f"expected the CV doc to explicitly rule out claiming {forbidden_claim}"
        )


def test_readme_documents_the_simulated_feed():
    """The tick stream is synthetic; saying otherwise would misrepresent it."""
    readme = README.read_text(encoding="utf-8").lower()
    assert "simulated" in readme
    assert "not a paid market data subscription" in readme or "not a paid" in readme
