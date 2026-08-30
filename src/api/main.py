"""FastAPI service - the real product surface.

Milestone 4 implements this: expose the pipeline's outputs (curated OHLCV +
indicators, latest metrics, retrieval-grounded briefings) as a documented, versioned
API with OpenAPI/Swagger docs enabled and request validation. Designed so the same
endpoints could back Power BI / Tableau, not just the Streamlit console.

Placeholder only until Milestone 4 - a minimal app so the module imports and a
health check exists.
"""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(
    title="Realtime Lakehouse RAG Pipeline API",
    version="0.0.0",
    summary="Placeholder - endpoints land in Milestone 4.",
)


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}
