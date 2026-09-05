"""Tests for the Milestone 4 FastAPI service.

These build real Delta tables in a tmp directory and point the API's config at
them, so the endpoints are exercised against genuine Delta reads (delta-rs, no
Spark and no network) rather than mocks. That keeps them fully portable - they
run identically on Windows and in CI.

The briefing endpoint's happy path needs a local Ollama, so only its error
handling is asserted here; the generation path itself is covered by
``tests/test_rag.py`` and by the real measured runs in docs/METRICS.md.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src import config
from src.api.main import app

client = TestClient(app)


@pytest.fixture
def lakehouse_tables(tmp_path, monkeypatch):
    """Write small but real curated/ticks Delta tables and point config at them."""
    from deltalake import write_deltalake

    curated_path = tmp_path / "ohlcv_curated"
    ticks_path = tmp_path / "ticks_raw"

    curated = pd.DataFrame(
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
            # First bar per ticker has no previous close - NaN must serialise as null.
            "daily_return": [float("nan"), 0.00198, 0.00198, float("nan"), 0.00952, 0.00943],
            "volatility_20d": [float("nan"), 0.001, 0.001, float("nan"), 0.002, 0.002],
        }
    )
    ticks = pd.DataFrame(
        {
            "ticker": ["MSFT", "MSFT", "NVDA"],
            "price": [505.1, 505.2, 105.1],
            "size": [100, 200, 300],
            "event_time": [
                "2026-09-03T18:52:08.189445+00:00",
                "2026-09-03T18:52:09.189445+00:00",
                "2026-09-03T18:52:10.189445+00:00",
            ],
            "kafka_timestamp": ["2026-09-04 00:22:08.189"] * 3,
        }
    )

    write_deltalake(str(curated_path), curated, mode="overwrite")
    write_deltalake(str(ticks_path), ticks, mode="overwrite")

    monkeypatch.setattr(config, "DELTA_OHLCV_CURATED", str(curated_path))
    monkeypatch.setattr(config, "DELTA_TICKS_RAW", str(ticks_path))
    monkeypatch.setattr(config, "DELTA_OHLCV_RAW", str(tmp_path / "missing_raw"))
    return {"curated": curated_path, "ticks": ticks_path}


def test_health_needs_no_lakehouse():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_tickers_endpoint_lists_portfolio():
    response = client.get("/api/v1/tickers")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == len(body["tickers"])
    assert "MSFT" in body["tickers"]


def test_openapi_documents_every_endpoint():
    """The milestone's whole point is a documented contract - assert it exists."""
    spec = client.get("/openapi.json").json()
    for path in (
        "/health",
        "/api/v1/tickers",
        "/api/v1/prices/{ticker}",
        "/api/v1/ticks/{ticker}",
        "/api/v1/briefings/{ticker}",
        "/api/v1/lakehouse/stats",
    ):
        assert path in spec["paths"], f"{path} missing from OpenAPI spec"
    assert "Bar" in spec["components"]["schemas"]


def test_prices_returns_curated_bars_with_indicators(lakehouse_tables):
    response = client.get("/api/v1/prices/MSFT")
    assert response.status_code == 200
    body = response.json()

    assert body["ticker"] == "MSFT"
    assert body["rows"] == 3
    assert body["table_version"] == 0
    bars = body["bars"]
    assert [b["Date"] for b in bars] == ["2026-09-01", "2026-09-02", "2026-09-03"]
    assert bars[0]["SMA_20"] == 505.0
    # NaN -> null, not a crash and not a NaN literal (which isn't valid JSON).
    assert bars[0]["daily_return"] is None
    assert bars[1]["daily_return"] == pytest.approx(0.00198)


def test_prices_is_case_insensitive_and_limit_applies(lakehouse_tables):
    response = client.get("/api/v1/prices/msft", params={"limit": 2})
    assert response.status_code == 200
    body = response.json()
    assert body["rows"] == 2
    # limit keeps the most recent bars, not the first ones.
    assert [b["Date"] for b in body["bars"]] == ["2026-09-02", "2026-09-03"]


def test_prices_unknown_ticker_is_404(lakehouse_tables):
    assert client.get("/api/v1/prices/NOPE").status_code == 404


def test_prices_rejects_invalid_limit(lakehouse_tables):
    assert client.get("/api/v1/prices/MSFT", params={"limit": 0}).status_code == 422
    assert client.get("/api/v1/prices/MSFT", params={"limit": 99999}).status_code == 422


def test_ticks_endpoint_returns_streaming_rows(lakehouse_tables):
    response = client.get("/api/v1/ticks/MSFT")
    assert response.status_code == 200
    body = response.json()
    assert body["rows"] == 2
    assert body["ticks"][0]["ticker"] == "MSFT"
    assert body["ticks"][0]["price"] == pytest.approx(505.1)


def test_missing_table_is_503_not_500(tmp_path, monkeypatch):
    """A pipeline that hasn't run yet is a dependency problem, not a server bug."""
    monkeypatch.setattr(config, "DELTA_OHLCV_CURATED", str(tmp_path / "never_created"))
    response = client.get("/api/v1/prices/MSFT")
    assert response.status_code == 503
    assert "not available yet" in response.json()["detail"]


def test_lakehouse_stats_reports_available_and_missing(lakehouse_tables):
    response = client.get("/api/v1/lakehouse/stats")
    assert response.status_code == 200
    tables = {t["name"]: t for t in response.json()["tables"]}

    assert tables["ohlcv_curated"]["available"] is True
    assert tables["ohlcv_curated"]["rows"] == 6
    assert tables["ticks_raw"]["available"] is True
    # A missing table degrades this one entry rather than failing the request.
    assert tables["ohlcv_raw"]["available"] is False
    assert tables["ohlcv_raw"]["detail"]


def test_briefing_unknown_ticker_is_404(lakehouse_tables):
    """Fails before ever reaching the LLM, so this is portable/offline-safe."""
    assert client.post("/api/v1/briefings/NOPE").status_code == 404


def test_briefing_surfaces_generation_failure_as_502(lakehouse_tables, monkeypatch):
    """A dead local model is an upstream failure, not an opaque 500."""
    import src.llm.briefing_generator as bg

    def boom(*_args, **_kwargs):
        raise RuntimeError("ollama is not reachable")

    monkeypatch.setattr(bg, "generate_briefing", boom)
    response = client.post("/api/v1/briefings/MSFT")
    assert response.status_code == 502
    assert "ollama is not reachable" in response.json()["detail"]


# --- The "thin client" claim itself -------------------------------------------------
#
# The milestone's substance is that the UI stopped reading the filesystem and
# stopped running inference in-process. That's a structural property, so it gets
# a structural test that runs everywhere rather than relying on manual review.

UI_APP = Path(__file__).resolve().parent.parent / "ui" / "streamlit_app.py"


def _ui_tree():
    """Parse the UI as an AST.

    Deliberately AST-based rather than a substring scan of the source: the
    module docstring *describes* what the UI no longer does ("before M4 it read
    data/raw/*.csv"), and a naive text search flags that prose as a violation.
    Only real code should be able to fail these.
    """
    import ast

    return ast.parse(UI_APP.read_text(encoding="utf-8"))


def test_ui_imports_no_data_or_inference_modules():
    import ast

    banned_prefixes = ("src.llm", "src.rag", "deltalake", "ollama")
    imported: list[str] = []
    for node in ast.walk(_ui_tree()):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)

    offenders = [
        name for name in imported if any(name.startswith(p) for p in banned_prefixes)
    ]
    assert not offenders, (
        f"UI must reach data and inference through the API, not import them: {offenders}"
    )


def test_ui_makes_no_direct_data_reads():
    import ast

    banned_calls = {"read_csv", "read_parquet", "DeltaTable"}
    called: list[str] = []
    for node in ast.walk(_ui_tree()):
        if isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "attr", None) or getattr(func, "id", None)
            if name in banned_calls:
                called.append(name)

    assert not called, f"UI must not read data files directly: {called}"


def test_ui_talks_to_the_api():
    import ast

    tree = _ui_tree()
    api_imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "ui.api_client"
        for alias in node.names
    }
    for call in ("get_tickers", "get_prices", "create_briefing"):
        assert call in api_imports, f"UI should use api_client.{call}"


class _SimulatedPanic(BaseException):
    """Stands in for ``pyo3_runtime.PanicException``.

    The real class only exists once the pyo3 runtime has actually panicked, so
    it cannot be imported to write a portable test against. What matters is its
    one relevant property, reproduced here exactly: it derives from
    BaseException rather than Exception, so `except Exception` does not catch
    it.
    """


def test_delta_panic_is_503_not_an_uncaught_crash(tmp_path, monkeypatch):
    """The BaseException fallback actually triggers - the real regression test.

    CI hit this for real: the container (uid 10001) could not write the
    bind-mounted data dir, delta-rs panicked, and because
    pyo3_runtime.PanicException inherits from BaseException the
    `except Exception` guard missed it. The API returned a 500 traceback
    instead of the 503 it is designed to return.

    Note what this does NOT rely on: a missing directory or a file-where-a-
    directory-should-be both raise TableNotFoundError, which IS an Exception
    subclass - tests using those pass with or without the fix and prove
    nothing. Only a BaseException-derived failure exercises the guard.
    """
    import deltalake

    def panic(*_args, **_kwargs):
        raise _SimulatedPanic(
            'The specified table_uri is not valid: InvalidTableLocation('
            '"Could not create local directory: /app/data/delta/ohlcv_curated")'
        )

    monkeypatch.setattr(deltalake, "DeltaTable", panic)
    monkeypatch.setattr(config, "DELTA_OHLCV_CURATED", str(tmp_path / "whatever"))

    response = client.get("/api/v1/prices/MSFT")

    assert response.status_code == 503, (
        f"a delta-rs panic must degrade to 503, got {response.status_code}. "
        f"If this is a 500 or the exception escaped, the BaseException guard "
        f"in src/api/lakehouse.py has regressed to `except Exception`."
    )
    assert "not available yet" in response.json()["detail"]


def test_keyboard_interrupt_is_never_swallowed(tmp_path, monkeypatch):
    """Catching BaseException must not turn Ctrl-C into a 503.

    The broad guard is deliberate but narrow: KeyboardInterrupt and SystemExit
    have to keep propagating, or the process becomes unkillable mid-request.
    """
    import deltalake

    from src.api import lakehouse

    def interrupt(*_args, **_kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(deltalake, "DeltaTable", interrupt)

    with pytest.raises(KeyboardInterrupt):
        lakehouse.get_prices("MSFT", table_path=str(tmp_path / "x"))


def test_unreadable_table_path_is_503_not_a_500_traceback(tmp_path, monkeypatch):
    """A file where a table directory should be also degrades to 503.

    This one goes through TableNotFoundError (an ordinary Exception), so it
    covers the common case rather than the panic path above.
    """
    not_a_directory = tmp_path / "definitely_not_a_table"
    not_a_directory.write_text("this is a file, not a Delta table", encoding="utf-8")

    monkeypatch.setattr(config, "DELTA_OHLCV_CURATED", str(not_a_directory))
    response = client.get("/api/v1/prices/MSFT")

    assert response.status_code == 503, (
        f"expected 503 for an unusable table path, got {response.status_code}"
    )
    assert "not available yet" in response.json()["detail"]


def test_lakehouse_stats_survives_an_unusable_table_path(tmp_path, monkeypatch):
    """One broken table must degrade its own entry, not fail the request."""
    broken = tmp_path / "broken_table"
    broken.write_text("not a table", encoding="utf-8")

    monkeypatch.setattr(config, "DELTA_OHLCV_RAW", str(broken))
    monkeypatch.setattr(config, "DELTA_OHLCV_CURATED", str(tmp_path / "missing"))
    monkeypatch.setattr(config, "DELTA_TICKS_RAW", str(tmp_path / "missing2"))

    response = client.get("/api/v1/lakehouse/stats")
    assert response.status_code == 200
    tables = {t["name"]: t for t in response.json()["tables"]}
    assert tables["ohlcv_raw"]["available"] is False
    assert tables["ohlcv_raw"]["detail"]


def test_briefing_text_is_escaped_before_streamlit_renders_it():
    """Currency amounts must not be rendered as LaTeX.

    Streamlit renders markdown, and markdown treats `$...$` as inline maths. A
    briefing mentioning two prices ("around the $510 level ... the $500 mark")
    had everything between them swallowed into a formula - italicised, spaces
    stripped. It reads as though the model produced mangled text, when the text
    was correct and the renderer mangled it.

    Checked here rather than by eye because the symptom points at the wrong
    component entirely.
    """
    import ast

    source = UI_APP.read_text(encoding="utf-8")
    tree = ast.parse(source)

    # The helper must exist...
    helpers = [
        node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
    ]
    assert "_escape_markdown_math" in helpers, (
        "the UI must escape $ before writing LLM text, or prices render as LaTeX"
    )

    # ...and the briefing text must actually go through it.
    assert "_escape_markdown_math(result[\"text\"])" in source, (
        "briefing text must be passed through _escape_markdown_math before st.write"
    )
    assert 'st.write(result["text"])' not in source, (
        "raw briefing text is written unescaped somewhere - $ will render as maths"
    )


def test_escape_helper_neutralises_dollar_pairs():
    """The escape itself, exercised directly on the text that broke."""
    import ast

    source = UI_APP.read_text(encoding="utf-8")
    tree = ast.parse(source)
    func = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_escape_markdown_math"
    )
    namespace: dict = {}
    exec(compile(ast.Module(body=[func], type_ignores=[]), "<ui>", "exec"), namespace)
    escape = namespace["_escape_markdown_math"]

    broke_before = "hovering around the $510 level. The 20-day SMA ... the $500 mark."
    escaped = escape(broke_before)

    assert "\\$510" in escaped and "\\$500" in escaped
    # No unescaped $ survives, so no pair can open a maths span.
    assert not re.search(r"(?<!\\)\$", escaped), "an unescaped $ would still start LaTeX"
