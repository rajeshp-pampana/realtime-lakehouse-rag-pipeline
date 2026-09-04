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
│   │   ├── index_builder.py             # embeds + indexes docs/context + data/briefings into Chroma
│   │   ├── retriever.py                 # retrieval step before every LLM call
│   │   └── _chromadb_compat.py          # Windows onnxruntime/chromadb import-time fix
│   ├── llm/briefing_generator.py        # Llama 3 via Ollama, retrieval-grounded, saves each briefing
│   └── api/
│       ├── main.py                      # FastAPI service, OpenAPI docs enabled
│       ├── schemas.py                   # Pydantic response models = the published contract
│       └── lakehouse.py                 # Delta read layer (delta-rs, no Spark/JVM needed)
├── ui/
│   ├── streamlit_app.py             # internal analyst console — thin client, HTTP only
│   └── api_client.py                # the console's HTTP client for the API
├── monitoring/                      # prometheus.yml + grafana dashboard
├── tests/                           # pytest: processing, streaming, rag, api, infra
├── infra/
│   ├── docker-compose.yml           # kafka + api + ui by default; streaming/orchestration behind profiles
│   ├── Dockerfile.{api,ui,streaming,airflow}
│   ├── requirements-{api,ui,streaming}.txt   # per-image deps; versions pinned by requirements.txt
│   └── k8s/
│       ├── kind-cluster.yaml     # local cluster: pinned node image, NodePort->host, lakehouse mount
│       ├── deployment.yaml      # Namespace, ConfigMap, API + UI Deployments
│       ├── service.yaml         # ClusterIP (in-cluster) + NodePort (host access)
│       └── helm/rlrp/           # the same resources as a parameterised chart
├── .github/workflows/ci.yml         # lint + test (+ image build, e2e in M6)
├── data/raw/                        # baseline CSVs from the ported ingestion path (no longer read by the UI)
├── data/delta/                      # Delta Lake tables: ohlcv_raw/ticks_raw (bronze), ohlcv_curated (silver) — gitignored
├── data/briefings/                  # every briefing ever generated (frontmatter + text) — gitignored, grows locally
├── data/vectorstore/                # Chroma persistent index — gitignored
├── docs/context/                    # schema/ticker/methodology notes — real, committed, seeds the RAG index
├── docs/METRICS.md                  # measured metrics log (no estimates)
└── requirements.txt
```

## Build milestones

- [x] **M1 — Orchestration + Lakehouse foundation.** Airflow DAG; Delta Lake tables; pandas transform ported to PySpark local mode.
- [x] **M2 — Streaming ingestion.** Kafka (KRaft); simulated tick producer; Spark Structured Streaming consumer writing the same Delta tables.
- [x] **M3 — RAG layer.** Local vector store; index past briefings/context; retrieval step before every LLM call.
- [x] **M4 — Service split.** FastAPI service with OpenAPI docs; Streamlit becomes a thin client.
- [x] **M5 — Containerization + deployment.** Dockerize API/UI/streaming; Kafka in docker-compose; k8s manifests / Helm deployed to a local `kind` cluster.
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

### Milestone 3 — RAG layer

Local vector store: **Chroma** (persistent, `data/vectorstore/`). Embeddings:
**Ollama's `nomic-embed-text`** (768-dim, ~274MB), not `sentence-transformers`
— keeps the whole stack local/no-new-runtime and avoids pulling in torch
(2GB+) on an 8 GB machine, consistent with the project's privacy-first story.

The index covers two sources: `docs/context/` (schema notes, ticker
reference, methodology/governance notes — real project documentation,
committed to the repo) and `data/briefings/` (every briefing the pipeline has
ever generated, saved automatically — gitignored, grows locally as you use
it). `src/llm/briefing_generator.py` now always retrieves before generating:
the retrieved passages are injected into the prompt, labeled by source, with
an explicit instruction to cite what it actually draws on.

```bash
python -m src.rag.index_builder                    # build/refresh the index
python -m src.rag.retriever "MSFT technical momentum"   # query it directly
python -m src.llm.briefing_generator MSFT           # generate a grounded briefing
```

A Windows-specific wart, fixed: chromadb's `Collection` class evaluates its
default (ONNX-based) embedding function as a class-level default argument at
`import chromadb` time, which needs `onnxruntime` importable — but
`onnxruntime`'s native extension reliably fails to load in the same process
as `pandas`/`pyarrow` (a real DLL conflict, reproduced in isolation: either
import alone works, the combination doesn't). Since this project always
supplies its own embeddings explicitly and never touches chromadb's default,
`src/rag/_chromadb_compat.py` pre-registers a dummy `onnxruntime` module
before `chromadb` is ever imported, sidestepping the conflict entirely
without needing the real package to actually work.

**Verified**: see [docs/METRICS.md](docs/METRICS.md) for the real generated
briefing, its cited sources, and measured retrieval/generation latency.

### Milestone 4 — Service split

The pipeline's outputs are now a documented HTTP API, and the Streamlit console
is a thin client of it rather than a program that reads the filesystem.

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness — no lakehouse or model dependency |
| `GET /api/v1/tickers` | The ingested portfolio |
| `GET /api/v1/prices/{ticker}` | Curated daily bars + SMA/return/volatility from `ohlcv_curated` |
| `GET /api/v1/ticks/{ticker}` | Streaming ticks landed by the Kafka consumer from `ticks_raw` |
| `GET /api/v1/lakehouse/stats` | Row count + Delta version per table |
| `POST /api/v1/briefings/{ticker}` | Retrieval-grounded briefing (POST: it runs a model and appends to the corpus) |

```bash
uvicorn src.api.main:app --reload      # API + Swagger UI at /docs
streamlit run ui/streamlit_app.py      # thin client, talks HTTP only
```

Design decisions worth calling out:

- **The read layer uses delta-rs, not Spark** (`src/api/lakehouse.py`). Serving
  a few hundred rows over HTTP doesn't need a JVM, and it keeps the API
  startable anywhere — including Windows, where PySpark local-mode task
  execution is broken here. Spark stays in the batch transform, where the
  distributed compute actually earns its cost.
- **Responses are Pydantic models** (`src/api/schemas.py`), so `/openapi.json`
  publishes a real contract a consumer can generate a client from — the point
  of the milestone, not decoration. A test asserts every endpoint and the `Bar`
  schema are present in the spec.
- **A missing Delta table returns 503, not 500.** A pipeline that hasn't run
  yet is an unready dependency, not a server bug, and `/lakehouse/stats`
  degrades per-table rather than failing the whole request.
- **The UI now plots the curated table.** Before M4 it read `data/raw/*.csv`
  and recomputed its own rolling means, so it never actually displayed the
  lakehouse's output; the SMA lines are now the ones PySpark computed.

#### Where the UI gets its data

The console has exactly two data sources, and the rule separating them is
deliberate:

| Data | Source | Why |
|---|---|---|
| Prices, indicators, ticks, lakehouse status, briefings | **The API** | This is pipeline-backed data — it is ingested, versioned in Delta, and transformed. It must be served through the API so there is one read path, one contract, and one place where schema and freshness are defined. |
| Analyst targets, earnings dates, income statement, news | **Yahoo Finance, directly** | This data never enters the Lakehouse. It is presentation-only, fetched live, and nothing downstream depends on it. |

**The boundary: anything that enters the Lakehouse is served by the API;
anything that never does may be fetched directly.** So the rule is not "the UI
may call whatever is convenient" — a direct call is only legitimate for data
the pipeline does not own. The moment fundamentals or news become
pipeline-backed (ingested, stored, used to ground a briefing), they move behind
the API like everything else. `tests/test_api.py` enforces the half that
matters: the UI may not import the data or inference modules, and may not read
data files.

**Verified**: real p50/p95 per endpoint in [docs/METRICS.md](docs/METRICS.md),
measured over HTTP against a running server — plus a measured note on the
full-scan read path the numbers exposed.

### Milestone 5 — Containerization

Every component now has an image, and the streaming producer/consumer move
from plain WSL2 processes into containers. The Kubernetes/Helm half of this
milestone is deliberately not started yet — see "Status" below.

```bash
cd infra
docker compose up                                       # kafka + api + ui
docker compose --profile streaming up                   # + producer/consumer
docker compose --profile orchestration up               # + airflow
```

Services without a `profiles` key start by default; Spark and Airflow are the
memory-hungry ones and stay opt-in, because this is an 8 GB machine and
bringing everything up at once is not the common case.

**Dependencies are split per service** (`infra/requirements-*.txt`) rather than
every image installing the top-level `requirements.txt`. That file remains the
source of truth for *versions* — CI and the dev venv install it, so it is what
the test suite actually runs against — while the per-service files only choose
*which* of those packages an image needs. `tests/test_infra.py` enforces the
relationship: every per-service package must exist in `requirements.txt` with
an identical specifier, so the split can't silently drift into shipping a
version CI never tested.

The split is also a check on the architecture, not just a size optimisation:

- The **UI image has no `deltalake`, `pyspark`, `chromadb` or `ollama`**. Since
  M4 the console is a thin HTTP client, so if it ever regressed to reading data
  or running inference directly, its container would fail outright.
- The **API image has no `pyspark`, `streamlit` or `yfinance`**. It reads Delta
  through delta-rs and needs no JVM.

Two couplings this milestone exposed and fixed:

- `src/api/main.py` imported `TICKERS` from `ingestion/fetch_market_data.py`,
  pulling `yfinance` into the API image for a list of 17 strings. `TICKERS`
  moved to `src/config.py`, where it belongs — it is configuration, not
  ingestion logic.
- `tick_producer` and `stream_consumer` default to **bounded** runs (30s/60s),
  because Milestone 2 only ever needed verification runs. Under
  `restart: unless-stopped` a bounded run is a restart loop, not a service, so
  both now accept `0` to mean run-until-stopped and compose passes it
  explicitly.

Two container-only constraints worth knowing about, both found by running the
stack rather than by reading the config:

- **Spark checkpoints live on a named volume, not under `../data`.** Spark
  chmods its checkpoint directory, which fails on a Windows-backed bind mount
  as a non-root user. The named volume is also seeded with the image's
  ownership of `/checkpoints`, which is why the Dockerfile creates and
  `chown`s it — a volume that arrives root-owned stops Spark dead.
- **The streaming image bakes the Kafka connector JARs.** `spark.jars.packages`
  otherwise resolves them from Maven Central when the *query* starts, making
  every cold container start depend on public internet access.

Ollama is deliberately **not** containerized: it is a 4.7 GB model server that
would dwarf this stack and is already installed natively. The API reaches it
via the host gateway, which also requires `OLLAMA_HOST=0.0.0.0` on the host so
it listens beyond loopback. Without that, every read endpoint works and only
`POST /briefings` returns 502.

**Verified** (see [docs/METRICS.md](docs/METRICS.md)): the default stack reaches
all-healthy in 27s; the UI container fetches curated bars from the API over the
compose network; and with the streaming profile up, **918 new tick rows landed
in Delta in 76s** through the containerized producer → Kafka → Spark consumer
path, read back through the API. Peak footprint is ~1.27 GB across five
containers.

#### Kubernetes (kind + Helm)

The API and console also deploy to a real local Kubernetes cluster. Two paths
are maintained — plain manifests for readability, a Helm chart for packaging —
and a test asserts they agree on images and ports so they cannot drift.

```bash
# tooling (installs to ~/.local/bin, no sudo needed)
# kubectl v1.37.0 / kind v0.33.0 / helm v4.2.4

kind create cluster --config infra/k8s/kind-cluster.yaml
kind load docker-image rlrp-api:local rlrp-ui:local --name rlrp   # no registry

# either path:
kubectl apply -f infra/k8s/deployment.yaml -f infra/k8s/service.yaml
helm install rlrp infra/k8s/helm/rlrp -n rlrp --create-namespace --wait
```

Then `http://localhost:8000/docs` and `http://localhost:8501`.

Decisions worth calling out:

- **The node image is pinned by digest.** kind's default node image changes
  with every kind release, so an unpinned cluster silently changes Kubernetes
  version when the tool is upgraded — the same drift class that broke the
  streaming build when `python:3.12-slim` moved to Debian 13.
- **`imagePullPolicy: IfNotPresent` everywhere.** These images exist only
  locally (loaded with `kind load`); any policy that reaches out lands in
  `ImagePullBackOff` against a registry that has never heard of `rlrp-api`.
- **NodePort, not Ingress.** A single-node local cluster would need an ingress
  controller installed, running and debugged to gain nothing here. The
  NodePorts are paired with `extraPortMappings` so they surface on the host at
  the same ports compose used.
- **The chart takes its namespace from `.Release.Namespace`**, not a values
  key — templating it separately lets the chart disagree with the `-n` flag it
  is installed with.
- **The UI Deployment mounts no volumes.** It is a thin HTTP client and its
  image ships no `deltalake`/`pyspark`, so the k8s layer reflects the same
  boundary the image enforces.

One caveat about `kubectl apply --dry-run=server` on these manifests: it
reports `namespaces "rlrp" not found`, because a server dry-run does not
actually create the Namespace that the same file defines. That is a property
of dry-run, not a defect in the manifests — the real apply succeeds.

**Keeping the cluster alive on WSL2.** This distro tears its services down
when no process holds it open, which stops Docker and takes the kind node with
it — the node used to die within 1–3 minutes. Hold the distro open before
creating the cluster:

```bash
wsl -d Ubuntu -- bash -lc 'sleep 86400' &   # or any long-lived process
```

The failure this produces is misleading: the *next* Docker operation fails with
a cgroup/systemd scope error, which reads like a Docker cgroup-driver problem
rather than "the distro was shut down underneath you". See
[docs/METRICS.md](docs/METRICS.md) for the evidence that distinguished them.

#### Status

Docker/Compose and Kubernetes are both complete and verified against a real
`kind` cluster, including a 20-minute soak test showing the cluster stays
healthy rather than merely deploying fast. Nothing here is marked done on the
strength of looking plausible — see [docs/METRICS.md](docs/METRICS.md) for what
was measured.

## Known limitations

Deliberate trade-offs, chosen with the cost understood and measured. They are
listed because a design's boundaries are part of the design — not as a backlog.

**The API read path scans whole tables.** `src/api/lakehouse.py` calls
`DeltaTable(...).to_pandas()` and then filters by ticker and slices in pandas,
so every request materialises the entire table regardless of how few rows it
returns. The measurements show this plainly: `GET /prices/MSFT?limit=5` costs
23.25ms p50 and `?limit=100` costs 24.26ms — essentially identical, because
latency tracks table size and Delta history depth rather than response size.
The same code path takes 55.93ms against `ticks_raw` (765 rows, Delta version
16) versus 24.26ms against `ohlcv_curated` (406 rows, version 0).

This is accepted at this project's data volumes: a few hundred rows per table
makes the simple implementation comfortably fast, and it keeps the read layer
JVM-free and easy to reason about. It would not hold at production volumes,
where the fix is to push the predicate into the Parquet scan
(`DeltaTable.to_pyarrow_dataset()` with a filter) or partition the tables by
`Ticker` so a request touches only relevant files. Stated so the numbers above
are not mistaken for evidence of an efficient read path.

**Briefing generation is slow on this hardware, and that is a hardware fact,
not a pipeline one.** ~66-93s warm and ~480s cold, because `llama3` is a 4.7GB
model on an 8GB machine where it cannot stay resident. The pipeline around it
is not the bottleneck — retrieval is ~4.5s and the entire Delta read is
~24ms. See [docs/METRICS.md](docs/METRICS.md) for the evidence behind that
attribution.

**Spark runs in local mode, and PySpark task execution does not work natively
on Windows here.** The batch transform is verified in WSL2 and in CI instead;
the API deliberately avoids Spark for reads so the service itself stays
portable. This is a dev-environment constraint, not a design limit of the
pipeline.

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
