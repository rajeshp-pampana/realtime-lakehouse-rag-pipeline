"""Thin HTTP client the Streamlit console uses to reach the FastAPI service.

Milestone 4 introduces this: the UI no longer reads Delta tables or CSVs off
disk, and no longer imports the RAG/LLM code to run inference in-process. It
talks to the API over HTTP only, so the two halves can be started, stopped,
containerised (Milestone 5) and scaled independently.

Errors are surfaced as ``ApiError`` with a readable message so the UI can show
something useful instead of a stack trace - including the common case of the
API simply not being up yet.
"""

from __future__ import annotations

from typing import Any

import httpx

from src import config


class ApiError(RuntimeError):
    """Any failure talking to the API, already formatted for display."""


def _request(method: str, path: str, timeout: float, **kwargs: Any) -> Any:
    url = f"{config.API_BASE_URL.rstrip('/')}{path}"
    try:
        response = httpx.request(method, url, timeout=timeout, **kwargs)
    except httpx.ConnectError as exc:
        raise ApiError(
            f"Cannot reach the API at {config.API_BASE_URL}. Start it with "
            f"`uvicorn src.api.main:app --reload`."
        ) from exc
    except httpx.TimeoutException as exc:
        raise ApiError(f"The API did not respond within {timeout:.0f}s ({path}).") from exc

    if response.status_code >= 400:
        try:
            detail = response.json().get("detail", response.text)
        except Exception:  # noqa: BLE001 - non-JSON error body
            detail = response.text
        raise ApiError(f"API returned {response.status_code}: {detail}")

    return response.json()


def get_tickers() -> list[str]:
    return _request("GET", "/api/v1/tickers", config.API_TIMEOUT_SECONDS)["tickers"]


def get_prices(ticker: str, limit: int = 100) -> dict:
    return _request(
        "GET", f"/api/v1/prices/{ticker}", config.API_TIMEOUT_SECONDS, params={"limit": limit}
    )


def get_lakehouse_stats() -> list[dict]:
    return _request("GET", "/api/v1/lakehouse/stats", config.API_TIMEOUT_SECONDS)["tables"]


def create_briefing(ticker: str, bars: int = 5) -> dict:
    return _request(
        "POST",
        f"/api/v1/briefings/{ticker}",
        config.API_BRIEFING_TIMEOUT_SECONDS,
        params={"bars": bars},
    )
