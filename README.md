# realtime-lakehouse-rag-pipeline

A hybrid **batch + streaming Lakehouse** for multi-asset equity data, with a
**retrieval-grounded LLM** briefing layer, served through a documented **FastAPI**
service and an internal **Streamlit** analyst console.

Migrated and upgraded from the original *AI Market Terminal* (a single-machine
Yahoo Finance -> pandas -> Streamlit + local Llama 3 script) into a
production-shaped pipeline using the same stack used day-to-day in banking data
engineering: Apache Airflow, PySpark, Apache Kafka, Delta Lake, Docker,
Kubernetes, and GitHub Actions.

> **Status: in active build.** This repo is being built milestone by milestone;
> each milestone lands in a working, committed, CI-passing state. See the
> checklist below for what is live.

---

## Target architecture

```
                       ┌─────────────────────────────┐
   Yahoo Finance  ───▶  │  Airflow DAG (end-of-day)    │
   (daily OHLCV)        │  ingest → transform → index │
                       └──────────────┬──────────────┘
                                      │  PySpark (local mode)
                                      ▼
 Simulated intraday   ┌──────────┐   ┌──────────────────┐   ┌───────────────┐
 tick stream  ──────▶ │  Kafka   │──▶│ Spark Structured │──▶│  Delta Lake   │
 (synthetic events)   │ (KRaft)  │   │   Streaming      │   │  (batch +     │
                       └──────────┘   └──────────────────┘   │   streaming)  │
                                                             └───────┬───────┘
                                                                     │
                            ┌────────────────────────────────────────┤
                            ▼                                        ▼
                    ┌───────────────┐                        ┌───────────────┐
                    │ RAG retriever │  (local vector store)  │  FastAPI      │
                    │  + Llama 3    │ ◀───────────────────── │  (OpenAPI)    │
                    │  (Ollama)     │                        └───────┬───────┘
                    └───────────────┘                                │
                                                              ┌──────▼──────┐
                                                              │  Streamlit  │
                                                              │  console    │
                                                              └─────────────┘

           Prometheus + Grafana  ◀── batch duration, retrieval latency,
                                      Kafka consumer lag, events/sec
```

The intraday tick stream is a **simulated** synthetic event feed (a random walk
around each ticker's last close), not a paid real-time market data subscription.
This is deliberate and called out everywhere it matters.

## Repo structure

```
realtime-lakehouse-rag-pipeline/
├── dags/
│   └── market_pipeline_dag.py        # Airflow DAG: batch ingest -> transform (-> index in M3)
├── src/
│   ├── config.py                        # env-driven settings (paths, hosts, ports)
│   ├── ingestion/fetch_market_data.py   # EOD OHLCV pull; ingest_to_delta() writes the raw Delta table
│   ├── streaming/
│   │   ├── tick_producer.py             # simulated intraday tick producer -> Kafka
│   │   └── stream_consumer.py           # Spark Structured Streaming: Kafka -> ticks_raw Delta table
│   ├── processing/transform_spark.py    # PySpark batch transforms -> curated Delta table
│   ├── rag/
│   │   ├── index_builder.py             # embeds + indexes historical context docs
│   │   └── retriever.py                 # retrieval step before every LLM call
│   ├── llm/briefing_generator.py        # Llama 3 via Ollama, retrieval-grounded
│   └── api/main.py                      # FastAPI service, OpenAPI docs enabled
├── ui/streamlit_app.py              # internal analyst console, calls the API
├── monitoring/                      # prometheus.yml + grafana dashboard
├── tests/                           # pytest: processing, streaming, rag
├── infra/                           # docker-compose, Dockerfiles, k8s manifests/Helm
├── .github/workflows/ci.yml         # lint + test (+ image build, e2e in M6)
├── data/raw/                        # baseline CSVs (ported Streamlit console still reads these until M4)
├── data/delta/                      # Delta Lake tables: ohlcv_raw/ticks_raw (bronze), ohlcv_curated (silver) — gitignored
├── docs/METRICS.md                  # measured metrics log (no estimates)
└── requirements.txt
```

## Build milestones

- [x] **M1 — Orchestration + Lakehouse foundation.** Airflow DAG; Delta Lake tables; pandas transform ported to PySpark local mode.
- [x] **M2 — Streaming ingestion.** Kafka (KRaft); simulated tick producer; Spark Structured Streaming consumer writing the same Delta tables.
- [ ] **M3 — RAG layer.** Local vector store; index past briefings/context; retrieval step before every LLM call.
- [ ] **M4 — Service split.** FastAPI service with OpenAPI docs; Streamlit becomes a thin client.
- [ ] **M5 — Containerization + deployment.** Dockerize API/UI/streaming; Kafka in docker-compose; k8s manifests / Helm on a local `kind` cluster.
- [ ] **M6 — CI/CD + tests + observability.** pytest for batch/stream/retrieval; GitHub Actions lint/test/build; Prometheus + Grafana pipeline-health dashboard.
- [ ] **M7 — Documentation + metrics capture.** README rewrite with architecture diagram and real measured numbers; every CV `[INSERT]` backed by a measurement.

## Running locally

This repo is developed on an 8 GB Windows laptop with Docker Desktop (WSL2). The
full stack does not fit in memory at once, so components are brought up in
**subsets** (e.g. Kafka + streaming consumer + API together; Ollama separately).
The literal "everything up at once" end-to-end proof runs in **CI**, where there
is no local LLM load. Per-milestone run instructions are added as each lands.

```bash
python -m venv .venv && .venv\Scripts\activate   # Windows
pip install -r requirements.txt
cp .env.example .env
pytest -q
```

### Milestone 1 — orchestration + Lakehouse foundation

**Without Docker, ingestion only** (no JVM involved — pure Python/pandas/delta-rs;
verified working on this machine, see [docs/METRICS.md](docs/METRICS.md)):

```bash
python -m src.ingestion.fetch_market_data   # -> data/delta/ohlcv_raw (append)
```

**Without Docker, the PySpark transform** — `python -m src.processing.transform_spark`
— is implemented but **not currently runnable on this Windows machine**: the JVM
driver spawns a Python worker subprocess that is silently killed within ~2 seconds
with zero output (confirmed independent of this shell's own sandboxing, and a
plain Python-spawned-Python child process survives fine — so it's specific to
java.exe spawning python.exe here, most likely AV/EDR real-time protection
intercepting that particular parent/child signature). Not worth chasing further
locally: the transform runs correctly inside the Linux Airflow container below,
where this Windows-only issue doesn't exist.

**With Airflow, for real** (the actual "Done when" criterion — verified via
**WSL2**, not Docker: same Windows-worker issue as above ruled out Docker
Desktop as the fast path here too, and `airflow dags test` runs the DAG through
the real Airflow engine without needing the scheduler/webserver daemons):

```bash
# One-time setup, inside WSL2 (Ubuntu):
curl -LsSf https://astral.sh/uv/install.sh | sh          # standalone Python installer
uv python install 3.12                                    # Ubuntu's own default python3 may be too new for pyspark/airflow
uv venv --python 3.12 .venv && . .venv/bin/activate
uv pip install -r requirements.txt
uv pip install "apache-airflow==2.10.5" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
uv pip install "typing_extensions>=4.13"   # airflow's constraints pin 4.12.2, too old for pydantic_core's Sentinel

# Run the DAG for real:
export AIRFLOW_HOME=~/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export PYTHONPATH=$(pwd)
airflow db migrate
airflow dags test market_pipeline $(date +%F)
```

**Verified**, 2026-09-03: both tasks (`ingest`, `transform`) SUCCESS, DagRun
state `success`. Real numbers in [docs/METRICS.md](docs/METRICS.md).

The `infra/Dockerfile.airflow` / `infra/docker-compose.yml` written for this
milestone aren't wasted, just not the path used here: Docker Desktop wasn't
installed on this machine, and WSL2 (needed either way — Docker Desktop's own
backend on Windows *is* WSL2) got us to a real, verified Airflow run faster and
lighter than also installing and configuring Docker Desktop on top. They fold
into **Milestone 5**'s containerization work instead.

### Milestone 2 — streaming ingestion

The intraday tick stream is entirely **simulated**: `tick_producer.py` generates
a synthetic random walk per ticker (seeded from that ticker's latest known
Close in the batch raw Delta table), not a paid real-time market data feed.

**Design note** on "writes micro-batches into the same Delta tables the batch
job uses": ticks (event-level) and the batch DAG's daily bars (one row per
ticker per day) are different natural grains, so the streaming consumer writes
its own bronze table, `data/delta/ticks_raw`, rather than unioning mismatched
schemas into `ohlcv_raw` — real trading/risk platforms keep tick and bar tables
separate for the same reason, sometimes compacting ticks into bars downstream.
Both tables live in the same Lakehouse root (`data/delta/`), which is what
"alongside the batch history" refers to.

```bash
# Kafka (KRaft mode, single broker) via Docker Engine in WSL2 - no Docker Desktop:
docker compose -f infra/docker-compose.yml up -d kafka

# Pre-create the topic before starting either process. Spark's Kafka source
# fails hard (UnknownTopicOrPartitionException, no retry) if the consumer
# subscribes before the topic exists - found this the hard way.
docker exec rlrp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic market.ticks --partitions 1 --replication-factor 1 --if-not-exists

# Two plain WSL2 processes, no Docker for these yet (containerized in M5):
python -m src.streaming.stream_consumer --timeout 110 &   # start first - see note below
sleep 20                                                  # let the query warm up
python -m src.streaming.tick_producer --duration 45 --interval 1
```

Start the consumer well before the producer: Spark Structured Streaming's query
startup (Kafka source init, checkpoint bootstrap) is inconsistent on this
machine - anywhere from ~1s to ~30-70s to commit its first micro-batch, not
correlated with backlog size or a cold JAR cache. Once running, watch
`data/delta/ticks_raw` grow while the producer is still active:

```bash
python -c "from deltalake import DeltaTable; print(len(DeltaTable('data/delta/ticks_raw').to_pandas()))"
```

**Verified**, 2026-09-04: 765 events published (17 tickers, 1/sec, 45s) → 765
landed in `ticks_raw`, 0 lost, across 17 incremental Delta commits, while the
producer was still running. Real throughput, consumer lag (max 136, final 0),
and streaming-to-Delta write latency (p50 2.82s, close to the 2s trigger
interval) in [docs/METRICS.md](docs/METRICS.md).

## Measured metrics

All performance numbers live in [docs/METRICS.md](docs/METRICS.md), recorded as
they are measured. The README metrics table is populated in M7 — no placeholders,
no invented figures.

## Config & secrets

All configuration is `.env`-based (`.env.example` is the template); nothing is
hardcoded. In a real deployment: API keys and Kafka credentials would come from a
secret manager (not `.env`), market/PII data handling would follow the firm's
data-governance policy, and the vector store would be access-controlled. Kept
explicit here as a governance-awareness note even though this is a solo project.
