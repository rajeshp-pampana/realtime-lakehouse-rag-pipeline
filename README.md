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
│   └── market_pipeline_dag.py        # Airflow DAG: batch ingest -> transform -> index
├── src/
│   ├── ingestion/fetch_market_data.py   # EOD OHLCV pull, adapted to write Delta
│   ├── streaming/
│   │   ├── tick_producer.py             # simulated intraday tick producer -> Kafka
│   │   └── stream_consumer.py           # Spark Structured Streaming: Kafka -> Delta
│   ├── processing/transform_spark.py    # PySpark batch transforms
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
├── data/raw/                        # Delta-managed location
├── docs/METRICS.md                  # measured metrics log (no estimates)
└── requirements.txt
```

## Build milestones

- [ ] **M1 — Orchestration + Lakehouse foundation.** Airflow DAG; Delta Lake tables; pandas transform ported to PySpark local mode.
- [ ] **M2 — Streaming ingestion.** Kafka (KRaft); simulated tick producer; Spark Structured Streaming consumer writing the same Delta tables.
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
