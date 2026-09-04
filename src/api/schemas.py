"""Pydantic response models for the FastAPI service.

These are the API's contract. Keeping them explicit (rather than returning bare
dicts) is what makes ``/docs`` and ``/openapi.json`` useful: every endpoint
publishes a typed, documented schema that a downstream consumer - the Streamlit
console, Power BI, or anything else - can generate a client from.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(description="``ok`` when the service is up.")
    version: str = Field(description="API version.")


class TickerListResponse(BaseModel):
    tickers: list[str] = Field(description="Portfolio tickers the pipeline ingests.")
    count: int


class Bar(BaseModel):
    """One daily OHLCV bar plus the indicators computed by the Spark transform."""

    Date: str
    Ticker: str
    Open: float | None = None
    High: float | None = None
    Low: float | None = None
    Close: float | None = None
    Volume: int | None = None
    SMA_20: float | None = Field(default=None, description="20-day simple moving average.")
    SMA_50: float | None = Field(default=None, description="50-day simple moving average.")
    daily_return: float | None = Field(
        default=None, description="Close-over-close return; null on a ticker's first bar."
    )
    volatility_20d: float | None = Field(
        default=None, description="20-day rolling stddev of daily_return."
    )


class PricesResponse(BaseModel):
    ticker: str
    rows: int
    source: str = Field(description="Delta table the bars were read from.")
    table_version: int = Field(description="Delta version served, for reproducibility.")
    bars: list[Bar]


class Tick(BaseModel):
    """One simulated market tick as landed by the streaming consumer."""

    ticker: str
    price: float | None = None
    size: int | None = None
    event_time: str | None = None
    kafka_timestamp: str | None = None


class TicksResponse(BaseModel):
    ticker: str
    rows: int
    source: str
    table_version: int
    ticks: list[Tick]


class TableStats(BaseModel):
    name: str
    path: str
    available: bool = Field(description="False when the table hasn't been created yet.")
    rows: int | None = None
    version: int | None = None
    detail: str | None = Field(default=None, description="Why it's unavailable, if it is.")


class LakehouseStatsResponse(BaseModel):
    tables: list[TableStats]


class BriefingResponse(BaseModel):
    """A retrieval-grounded briefing plus the provenance and timings behind it."""

    ticker: str
    text: str
    retrieved_sources: list[str] = Field(
        description="Indexed documents the briefing was grounded in."
    )
    retrieval_latency_seconds: float
    generation_latency_seconds: float
    saved_to: str | None = None


class ErrorResponse(BaseModel):
    detail: str
