"""FastAPI service - the real product surface.

Milestone 4 implements this: the pipeline's outputs (curated OHLCV +
indicators, streaming ticks, lakehouse health, and retrieval-grounded
briefings) are exposed as a documented, versioned HTTP API with OpenAPI/Swagger
docs and request validation. The Streamlit console becomes a thin client of
this service (``ui/streamlit_app.py``) instead of reading local files and
running inference in-process, so the UI and the API start, stop, scale, and
fail independently - and the same endpoints could back Power BI or Tableau.

Run it with::

    uvicorn src.api.main:app --reload

then browse ``http://localhost:8000/docs``.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.responses import JSONResponse

from src import config
from src.api import lakehouse
from src.api.schemas import (
    BriefingResponse,
    ErrorResponse,
    HealthResponse,
    LakehouseStatsResponse,
    PricesResponse,
    TickerListResponse,
    TicksResponse,
)
from src.ingestion.fetch_market_data import TICKERS

API_VERSION = "1.0.0"
V1 = "/api/v1"

app = FastAPI(
    title="Realtime Lakehouse RAG Pipeline API",
    version=API_VERSION,
    summary="Curated market data, streaming ticks, and retrieval-grounded briefings.",
    description=(
        "Serves the outputs of a hybrid batch + streaming Lakehouse:\n\n"
        "- **Batch** - Airflow-orchestrated ingestion into Delta Lake, indicators "
        "computed in PySpark (`ohlcv_curated`).\n"
        "- **Streaming** - Kafka ticks landed by Spark Structured Streaming "
        "(`ticks_raw`).\n"
        "- **RAG** - briefings grounded in retrieved prior context via a local "
        "Chroma index and Ollama, never on the day's raw numbers alone.\n"
    ),
    openapi_tags=[
        {"name": "meta", "description": "Health and service metadata."},
        {"name": "market-data", "description": "Curated batch OHLCV and streaming ticks."},
        {"name": "briefings", "description": "Retrieval-grounded LLM briefings."},
    ],
)


@app.exception_handler(lakehouse.TableUnavailableError)
def _table_unavailable_handler(_request, exc: lakehouse.TableUnavailableError) -> JSONResponse:
    """A not-yet-created table is a 503 (dependency not ready), not a 500."""
    return JSONResponse(status_code=503, content={"detail": str(exc)})


@app.get("/health", tags=["meta"], response_model=HealthResponse)
def health() -> HealthResponse:
    """Liveness probe - no lakehouse or model dependency, so it stays fast."""
    return HealthResponse(status="ok", version=API_VERSION)


@app.get(f"{V1}/tickers", tags=["market-data"], response_model=TickerListResponse)
def list_tickers() -> TickerListResponse:
    """The portfolio the pipeline ingests."""
    return TickerListResponse(tickers=list(TICKERS), count=len(TICKERS))


@app.get(
    f"{V1}/lakehouse/stats",
    tags=["meta"],
    response_model=LakehouseStatsResponse,
)
def lakehouse_health() -> LakehouseStatsResponse:
    """Row count and Delta version per table.

    Tables that don't exist yet are reported as unavailable rather than failing
    the request, so this works on a partially-run pipeline.
    """
    return LakehouseStatsResponse(tables=lakehouse.lakehouse_stats())


@app.get(
    f"{V1}/prices/{{ticker}}",
    tags=["market-data"],
    response_model=PricesResponse,
    responses={404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
def get_prices(
    ticker: str = Path(description="Ticker symbol, e.g. `MSFT`."),
    limit: int = Query(default=100, ge=1, le=1000, description="Most recent N bars."),
) -> PricesResponse:
    """Curated daily bars with SMA/return/volatility indicators, oldest first."""
    bars, version, source = lakehouse.get_prices(ticker, limit=limit)
    if not bars:
        raise HTTPException(status_code=404, detail=f"No curated bars found for '{ticker}'")
    return PricesResponse(
        ticker=ticker.upper(), rows=len(bars), source=source, table_version=version, bars=bars
    )


@app.get(
    f"{V1}/ticks/{{ticker}}",
    tags=["market-data"],
    response_model=TicksResponse,
    responses={404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
def get_ticks(
    ticker: str = Path(description="Ticker symbol, e.g. `MSFT`."),
    limit: int = Query(default=100, ge=1, le=1000, description="Most recent N ticks."),
) -> TicksResponse:
    """Most recent streaming ticks landed by the Kafka -> Delta consumer."""
    ticks, version, source = lakehouse.get_ticks(ticker, limit=limit)
    if not ticks:
        raise HTTPException(status_code=404, detail=f"No ticks found for '{ticker}'")
    return TicksResponse(
        ticker=ticker.upper(), rows=len(ticks), source=source, table_version=version, ticks=ticks
    )


@app.post(
    f"{V1}/briefings/{{ticker}}",
    tags=["briefings"],
    response_model=BriefingResponse,
    responses={404: {"model": ErrorResponse}, 502: {"model": ErrorResponse}},
)
def create_briefing(
    ticker: str = Path(description="Ticker symbol, e.g. `MSFT`."),
    bars: int = Query(default=5, ge=1, le=60, description="Trailing bars to summarise."),
) -> BriefingResponse:
    """Generate a briefing grounded in retrieved prior context.

    POST because it is not read-only: generation runs a local model and appends
    the result to ``data/briefings/``, which is itself re-indexed as future
    retrieval context.
    """
    import pandas as pd

    from src.llm.briefing_generator import generate_briefing

    rows, _version, _source = lakehouse.get_prices(ticker, limit=bars)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No curated bars found for '{ticker}'")

    frame = pd.DataFrame(rows)
    try:
        result = generate_briefing(ticker.upper(), frame)
    except Exception as exc:  # local model unreachable / failed
        raise HTTPException(
            status_code=502, detail=f"Briefing generation failed: {exc}"
        ) from exc

    return BriefingResponse(ticker=ticker.upper(), **result)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)
