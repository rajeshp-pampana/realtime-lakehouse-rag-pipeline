"""Airflow DAG: end-of-day batch pipeline.

    ingest  ->  transform  ->  refresh_index

Milestone 1 implements this: an Airflow DAG (running in the official Airflow
container from ``infra/docker-compose.yml``) that calls the ingestion, PySpark
transform, and RAG index-refresh steps in order and lands a versioned Delta table.

Placeholder only until Milestone 1.
"""

from __future__ import annotations

# Real DAG definition lands in Milestone 1.
DAG_ID = "market_pipeline"
SCHEDULE = "0 22 * * 1-5"  # 22:00 on weekdays, after US close
