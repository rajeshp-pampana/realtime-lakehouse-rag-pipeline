"""Airflow DAG: end-of-day batch pipeline.

    ingest  ->  transform

Milestone 1 implements this: an Airflow DAG (running in the container built from
``infra/Dockerfile.airflow`` / ``infra/docker-compose.yml``) that calls the
ingestion and PySpark-transform steps in order and lands a versioned Delta table.

The ``index-refresh`` task named in the plan's target repo structure is added in
Milestone 3, once ``src/rag/index_builder.py`` exists — adding it now would make
this DAG fail on every run.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

DAG_ID = "market_pipeline"
SCHEDULE = "0 22 * * 1-5"  # 22:00 UTC on weekdays, after US market close

default_args = {
    "owner": "rajesh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id=DAG_ID,
    description="End-of-day ingest -> PySpark transform -> versioned Delta table.",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["market-pipeline", "milestone-1"],
)
def market_pipeline():
    @task
    def ingest() -> dict:
        from src.ingestion.fetch_market_data import ingest_to_delta

        return ingest_to_delta()

    @task
    def transform(ingest_metrics: dict) -> dict:
        from src.processing.transform_spark import run_transform

        metrics = run_transform()
        metrics["upstream_rows_ingested"] = ingest_metrics["rows_ingested"]
        return metrics

    transform(ingest())


market_pipeline()
