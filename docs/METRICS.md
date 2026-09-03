# Measured metrics log

Real numbers, recorded as they are measured (never reconstructed from memory or
estimated). These feed the README metrics table and replace every `[INSERT]`
placeholder in the CV Projects section.

| Metric | Value | Measured on | How measured | Milestone |
|---|---|---|---|---|
| Rows ingested per batch run (17 tickers, 1mo daily bars) | 392-406 (varies with trading days available) | 2026-08-30, 2026-09-03 | `ingest_to_delta()`, local no-Docker run and real Airflow run (see below) | M1 |
| Ingestion run duration (17 tickers, sequential Yahoo Finance calls) | 9.09s (cold), 2.75s (warm), 7.96s (in Airflow) | 2026-08-30, 2026-09-03 | `duration_seconds` from `ingest_to_delta()` | M1 |
| Delta table versions produced per ingest run | +1 (append) | 2026-08-30 | `DeltaTable(...).version()` before/after two consecutive runs (0 -> 1) | M1 |
| Total rows in raw Delta table after N runs | 784 after 2 runs | 2026-08-30 | `DeltaTable(...).to_pandas()` row count | M1 |
| **PySpark transform runtime (real Airflow run, WSL2 Ubuntu, local[\*])** | **53.28s, 406 rows in -> 406 rows out** | 2026-09-03 | `duration_seconds` from `run_transform()`, executed by `airflow dags test market_pipeline` | M1 |
| **End-to-end DAG runtime (ingest -> transform), real Airflow** | **~75s** (22:47:46 -> 22:49:01 wall clock) | 2026-09-03 | `airflow dags test market_pipeline 2026-09-03`, DagRun start/end timestamps | M1 |
| Simulated tick throughput published (events/sec) | _tbd_ | | | M2 |
| Simulated tick throughput consumed (events/sec) | _tbd_ | | | M2 |
| Kafka consumer lag under normal load | _tbd_ | | | M2 / M6 |
| Streaming -> Delta write latency | _tbd_ | | | M2 |
| Retrieval latency (query -> retrieved context) | _tbd_ | | | M3 |
| Briefing generation latency | _tbd_ | | | M3 |
| API p50 / p95 latency per endpoint | _tbd_ | | | M4 / M6 |
| CI pipeline duration (lint + test + build) | _tbd_ | | | M6 |

## Machine baseline (for context on all timings)

- Windows 10 Home, 8 GB RAM.
- Windows-native: Python 3.12, Java 21. PySpark local-mode task *execution* (not
  just import) is currently broken here - the JVM-spawned Python worker is
  killed within ~2s with no output (see README "Milestone 1 ... Without
  Docker"). Ingestion (pure Python, no JVM) is verified natively on Windows.
- **WSL2 (Ubuntu 26.04)**: the real Airflow verification environment for
  Milestone 1 - Python 3.12.14 (via `uv`, since Ubuntu 26.04's own default is
  3.14), OpenJDK 17, Apache Airflow 2.10.5. `airflow dags test market_pipeline
  <date>` runs the DAG through the real Airflow engine (DAG parsing, task
  execution, XCom, DagRun state) without needing the scheduler/webserver
  daemons or Docker. Two real dependency conflicts fixed along the way,
  both environment-only (no pipeline code changes): PySpark 3.5.3 still does
  `from distutils.version import LooseVersion`, removed from the stdlib in
  Python 3.12 (fixed: `setuptools>=70`, which provides the compatibility
  shim); Airflow 2.10.5's constraints file pins `typing_extensions==4.12.2`,
  but `pydantic_core` needs `Sentinel`, added in 4.13.0 (fixed: bumped back
  up post-install).
- Components are run in subsets locally; the full-stack end-to-end proof runs in CI.
