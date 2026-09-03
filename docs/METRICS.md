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
| **Simulated tick throughput published** | **765 events in 45.05s = 16.98 events/sec** (17 tickers x 1 round/sec) | 2026-09-04 | `tick_producer.run_producer(duration_seconds=45, interval_seconds=1)`, real numbers returned by the function | M2 |
| **Simulated tick throughput consumed, end to end** | **765/765 events landed, 0 lost, across 17 micro-batches** (version 0 -> 16, 17 Delta history entries) | 2026-09-04 | `stream_consumer.run_consumer()`, real Kafka (Docker Engine in WSL2) -> real Spark Structured Streaming -> real Delta table. Full incremental trace (every 3s while both were running): 0, 0, 0, 0, 0, **136**, 272, 323, 357, 374, 408, 442, 476, 510, 544, 578, 612, 646, 680, 714, **748**, 765 (producer finished publishing at 745, consumer fully caught up 3s later) | M2 |
| **Kafka consumer lag** (topic end-offset minus rows landed) | **max 136** (during the cold-start catch-up batch), **final 0** (fully caught up once producer stopped) | 2026-09-04 | `stream_consumer.py`'s `_consumer_lag()`: queries the topic's own end offsets via `KafkaConsumer.end_offsets()` (metadata only) after every micro-batch, since Spark's Kafka source tracks offsets in its own checkpoint rather than a committed Kafka consumer group (so `kafka-consumer-groups.sh --describe` has nothing to show) | M2 |
| **Streaming-to-Delta write latency** (event's own `event_time` -> the Delta commit that landed it) | **avg 4.84s, p50 2.82s, max 15.49s** across all 765 rows / 17 batches | 2026-09-04 | Computed per-row in `stream_consumer.py`'s batch writer, aggregated in `run_consumer()`'s returned metrics. The p50 (2.82s) is close to the 2s trigger interval - representative steady-state latency; the max (15.49s, in batch 0) reflects this environment's query cold-start, not steady-state behavior (see below) | M2 |
| Streaming query cold-start latency (query start -> first micro-batch committed) | Varies run to run: as fast as ~1s (topic pre-existing, warm Ivy cache) up to ~30-70s (topic being created for the first time, or a cold Ivy/JAR cache). Not backlog-size-dependent - reproduced even against an empty topic. Operationally fixed by pre-creating the Kafka topic before starting the consumer (avoids a hard `UnknownTopicOrPartitionException` failure mode entirely - see below) | 2026-09-03/04, 5 separate runs | Wall-clock between starting `stream_consumer.py` and its first `[stream_consumer] batch` log line | M2 |
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
- **Docker Engine in WSL2** (Milestone 2): `docker.io` + `docker-compose-v2` installed directly via apt inside the Ubuntu distro - no Docker Desktop app. `docker compose -f infra/docker-compose.yml up -d kafka` runs `apache/kafka:4.1.0` (KRaft mode, no ZooKeeper). `tick_producer.py`/`stream_consumer.py` run as plain WSL2 Python processes against it (containerized in Milestone 5).
- Components are run in subsets locally; the full-stack end-to-end proof runs in CI.
- **WSL2 environment fixes made along the way** (both while getting real M2 numbers): (1) WSL2's VM auto-terminates on idle by default even with background containers/processes still running, which was silently killing Kafka mid-verification - fixed with `vmIdleTimeout=-1` in `%UserProfile%\.wslconfig`. (2) The Ubuntu distro itself was relocated from C: to `D:\WSL\Ubuntu` (`wsl --export` / `--unregister` / `--import`) to free space on a nearly-full C: drive; confirmed Docker, the repo, and Kafka all still work unchanged afterward.
- **Real bug found and fixed in `stream_consumer.py` while measuring these numbers**: starting the consumer before the topic exists fails hard with `UnknownTopicOrPartitionException` (Spark's Kafka source doesn't retry past this) - fixed operationally by pre-creating the topic before starting either process; documented in the README run instructions.
