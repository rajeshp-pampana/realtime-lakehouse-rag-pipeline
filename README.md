# realtime-lakehouse-rag-pipeline

A hybrid **batch + streaming Lakehouse** for multi-asset equity data, with a
**retrieval-grounded LLM** briefing layer, served through a documented
**FastAPI** service and an internal **Streamlit** analyst console.

Migrated and upgraded from the original *AI Market Terminal* — a single-machine
Yahoo Finance → pandas → Streamlit + local Llama 3 script — into a
production-shaped pipeline using the stack used day-to-day in banking data
engineering: Apache Airflow, PySpark, Apache Kafka, Delta Lake, Docker,
Kubernetes, Prometheus/Grafana, and GitHub Actions.

> Every number in this README was measured on a real run and is traceable to a
> row in [docs/METRICS.md](docs/METRICS.md). Nothing here is estimated.

---

## What it does

- **Batch**: an Airflow DAG pulls end-of-day OHLCV for 17 tickers into a
  versioned Delta Lake table, then computes technical indicators in PySpark and
  writes a curated table.
- **Streaming**: a simulated intraday tick feed flows through Kafka into a Spark
  Structured Streaming consumer that lands micro-batches in a separate Delta
  bronze table.
- **RAG**: every LLM briefing runs a retrieval step first, grounding the output
  in indexed context documents *and* previously generated briefings — so the
  corpus feeds back into itself.
- **Serving**: a versioned FastAPI service with OpenAPI docs exposes the
  lakehouse; the Streamlit console is a thin HTTP client of it.
- **Operations**: everything is containerised, deploys to Kubernetes via a Helm
  chart, and reports to Prometheus/Grafana.

The intraday tick stream is a **simulated** synthetic feed (a random walk around
each ticker's last close), not a paid market data subscription. That is called
out everywhere it matters rather than left ambiguous.

---

## Architecture

```
   Yahoo Finance            ┌──────────────────────────────┐
   (daily OHLCV)  ────────▶ │  Airflow DAG (end-of-day)    │
                            │  ingest ──▶ transform        │
                            └───────────────┬──────────────┘
                                            │ PySpark (local mode)
                                            ▼
  Simulated tick   ┌─────────┐   ┌──────────────────┐   ┌──────────────────────┐
  stream    ─────▶ │  Kafka  │──▶│ Spark Structured │──▶│      Delta Lake      │
  (synthetic)      │ (KRaft) │   │    Streaming     │   │  ohlcv_raw    bronze │
                   └─────────┘   └──────────────────┘   │  ticks_raw    bronze │
                                                        │  ohlcv_curated silver│
                                                        └──────────┬───────────┘
                                                                   │ delta-rs (no JVM)
                        ┌──────────────────────────────────────────┤
                        ▼                                          ▼
              ┌───────────────────┐                      ┌───────────────────┐
              │  RAG retrieval    │   retrieved context  │  FastAPI service  │
              │  Chroma + Ollama  │ ───────────────────▶ │  OpenAPI /docs    │
              │  nomic-embed-text │                      │  /metrics         │
              └─────────┬─────────┘                      └─────────┬─────────┘
                        │ grounded prompt                          │ HTTP only
                        ▼                                          ▼
              ┌───────────────────┐                      ┌───────────────────┐
              │  Llama 3 (Ollama) │                      │ Streamlit console │
              │  briefing ────────┼─▶ saved, re-indexed  │  (thin client)    │
              └───────────────────┘                      └───────────────────┘

  Prometheus ◀── /metrics (API)   ◀── Pushgateway ◀── batch tasks + Spark consumer
       │                                               (short-lived / no HTTP)
       ▼
   Grafana — pipeline health: lag, throughput, latency, batch duration, failures
```

Two collection paths for metrics, because the components genuinely differ: the
API is long-lived and serves HTTP so it is **scraped**; batch tasks exit in
seconds and the Spark driver serves no HTTP at all, so they **push**.

---

## Measured results

Selected headline figures. Full log, including how each was measured, in
[docs/METRICS.md](docs/METRICS.md).

### Pipeline throughput

| Metric | Measured |
|---|---|
| Batch ingest → curated, end-to-end via Airflow | **~75s** (17 tickers, 406 rows) |
| PySpark transform | **53.28s**, 406 rows in → 406 out |
| Tick throughput published | **765 events in 45.05s** (16.98 events/sec) |
| Tick delivery, end-to-end | **765/765 landed, 0 lost** across 17 micro-batches |
| Kafka consumer lag | **max 136** during cold-start catch-up, **0** once caught up |
| Streaming → Delta write latency | **p50 2.82s**, avg 4.84s (2s trigger interval) |
| Containerised streaming | **918 new rows in 76s**, producer → Kafka → Spark → Delta |

### Service latency

| Endpoint | p50 | p95 |
|---|---|---|
| `GET /health` | 2.48 ms | 3.11 ms |
| `GET /api/v1/tickers` | 2.25 ms | 2.58 ms |
| `GET /api/v1/prices/{ticker}` | 24.26 ms | 26.98 ms |
| `GET /api/v1/ticks/{ticker}` | 55.93 ms | 66.99 ms |
| `GET /api/v1/lakehouse/stats` | 86.83 ms | 164.04 ms |

50 real HTTP requests per endpoint against a running uvicorn. The same endpoints
measured **3–6× slower** when containerised over a cross-OS bind mount — see
[What broke](#what-broke-and-what-it-changed).

### RAG

| Metric | Measured |
|---|---|
| Retrieval latency (embed + vector search) | **4.49–5.47s** typical |
| Index build (5 docs) | **12.14s**, 768-dim embeddings |
| Briefing generation, warm | **65.6s / 92.6s** |
| Briefing generation, cold (model load) | **480.3s** |
| **Retrieval precision@1** | **0.917** (33/36 labelled questions) |
| **Retrieval MRR** | **0.958** |
| Local `llama3` throughput | **2.73 tok/s** generation |

Generation is memory-bound, not pipeline-bound: `llama3` is a 4.7 GB model, while
retrieval around it costs ~4.5s and the entire Delta read costs ~24 ms.

Retrieval quality is measured against a committed 36-question labelled set
(`docs/eval/retrieval_eval.yaml`), reproducibly. Chunking the corpus took
precision@1 from **0.528 to 0.917** and MRR from 0.731 to 0.958 — before it,
`tickers.md` was 17 companies in a single averaged vector. 36 questions is a
small sample, so read these as indicative rather than precise; the full
before/after is in [docs/METRICS.md](docs/METRICS.md).

### Deployment and operations

| Metric | Measured |
|---|---|
| Container images | API **980 MB**, UI 1.09 GB, streaming 1.97 GB |
| Compose stack cold start | **27s** to all-healthy |
| Runtime memory, default stack | **~486 MB** (API 89, UI 46, Kafka 352) |
| Kubernetes deploy (Helm) | install + upgrade to 2/2 replicas, **0 restarts** |
| Cluster stability soak | **23 min continuous**, 21 samples, every one healthy |
| Monitoring stack overhead | **~90 MB** (Grafana 55, Prometheus 25, Pushgateway 9) |
| Full CI pipeline | **4.3 min** wall clock, 9.8 runner-minutes, 6 parallel jobs |
| Test suite | **116 tests** (113 passed, 3 skipped locally) |

---

## Quickstart

```bash
python -m venv .venv && .venv\Scripts\activate    # Windows
pip install -r requirements.txt
cp .env.example .env
pytest -q
```

Run the product surface:

```bash
cd infra
docker compose up                                 # kafka + api + ui
```

- API + Swagger docs → <http://localhost:8000/docs>
- Streamlit console → <http://localhost:8501>

For running it in front of someone — start/stop commands, the warm-up step that
keeps a live briefing under 90s instead of 7 minutes, and a troubleshooting
table — see [docs/DEMO.md](docs/DEMO.md).

Add the optional stacks as needed:

```bash
docker compose --profile streaming up             # + tick producer and Spark consumer
docker compose --profile monitoring up            # + prometheus, grafana, pushgateway
docker compose --profile orchestration up         # + airflow
docker compose --profile llm up                   # + ollama, so briefings work
```

Services without a `profiles` key start by default. Spark and Airflow are the
memory-hungry ones and stay opt-in.

---

## How it works

### Batch — Airflow to Delta Lake

`dags/market_pipeline_dag.py` runs `ingest` then `transform`. Ingestion pulls
OHLCV per ticker and **appends** to `data/delta/ohlcv_raw`, producing a new Delta
version each run. The transform reads that table, computes 20/50-day SMAs, daily
return and 20-day volatility per ticker in PySpark, and **overwrites**
`data/delta/ohlcv_curated`.

Delta I/O goes through `deltalake` (delta-rs) rather than Spark's own Delta
connector — see [Design decisions](#design-decisions).

### Streaming — Kafka to Spark to Delta

`tick_producer.py` generates a synthetic random walk per ticker, seeded from that
ticker's latest known close in the batch table, and publishes to Kafka (KRaft
mode, no ZooKeeper). `stream_consumer.py` runs a Spark Structured Streaming query
that parses the events and appends micro-batches to `data/delta/ticks_raw` on a
2-second trigger, recording write latency and consumer lag after every batch.

Ticks and daily bars are **different natural grains**, so the consumer writes its
own bronze table rather than unioning mismatched schemas into `ohlcv_raw`. Real
trading and risk platforms keep tick and bar tables separate for the same reason.

### RAG — retrieval before every LLM call

`src/rag/index_builder.py` embeds and indexes two sources into a persistent
Chroma collection: `docs/context/` (schema notes, ticker reference,
methodology — real committed documentation) and `data/briefings/` (every
briefing the pipeline has ever generated). `src/rag/retriever.py` runs before
every generation, and the retrieved passages are injected into the prompt
labelled by source, with an instruction to cite what was used.

Because each briefing is saved and re-indexed, the corpus grows into itself:
later retrievals surface earlier briefings alongside the static context docs.

### Serving — FastAPI and a thin Streamlit client

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness — no lakehouse or model dependency |
| `GET /api/v1/tickers` | The ingested portfolio |
| `GET /api/v1/prices/{ticker}` | Curated bars + SMA/return/volatility |
| `GET /api/v1/ticks/{ticker}` | Streaming ticks from `ticks_raw` |
| `GET /api/v1/lakehouse/stats` | Row count + Delta version per table |
| `POST /api/v1/briefings/{ticker}` | Retrieval-grounded briefing |
| `GET /metrics` | Prometheus exposition (not in the OpenAPI contract) |

Responses are Pydantic models, so `/openapi.json` publishes a real contract a
consumer can generate a client from. Briefings are `POST` because generation runs
a model and appends to the corpus that later retrieval reads.

#### Where the UI gets its data

The console has exactly two data sources, and the rule separating them is
deliberate:

| Data | Source | Why |
|---|---|---|
| Prices, indicators, ticks, lakehouse status, briefings | **The API** | Pipeline-backed — ingested, versioned in Delta, transformed. One read path, one contract, one place where schema and freshness are defined. |
| Analyst targets, earnings, income statement, news | **Yahoo Finance, directly** | Never enters the Lakehouse. Presentation-only, fetched live, nothing downstream depends on it. |

**The boundary: anything that enters the Lakehouse is served by the API; anything
that never does may be fetched directly.** A direct call is only legitimate for
data the pipeline does not own. If fundamentals ever become pipeline-backed, they
move behind the API like everything else. `tests/test_api.py` enforces the half
that matters — the UI may not import the data or inference modules, and may not
read data files.

### Deployment — Compose and Kubernetes

Four images (`api`, `ui`, `streaming`, `airflow`), each installing its own
dependency set rather than the full `requirements.txt`. Kubernetes manifests and
a Helm chart deploy the API and console to a local `kind` cluster:

```bash
kind create cluster --config infra/k8s/kind-cluster.yaml
kind load docker-image rlrp-api:local rlrp-ui:local --name rlrp   # no registry

kubectl apply -f infra/k8s/deployment.yaml -f infra/k8s/service.yaml     # plain manifests
helm install rlrp infra/k8s/helm/rlrp -n rlrp --create-namespace --wait  # or Helm
```

Both paths are maintained and a test asserts they agree on images and ports.

> **WSL2 note.** This distro tears down its services when no process holds it
> open, which stops Docker and kills the kind node. Hold it open first with a
> long-lived background process (for example `sleep 86400`) inside the distro.
> The failure is misleading — the *next* Docker operation fails with a
> cgroup/systemd scope error that reads like a Docker cgroup-driver problem.

### Observability — Prometheus and Grafana

```bash
docker compose --profile monitoring up
```

Grafana <http://localhost:3000> (anonymous admin, local dev only) and Prometheus
<http://localhost:9090>. The datasource and a 14-panel **Pipeline Health**
dashboard are auto-provisioned, so the stack is useful the moment it starts.

Every metric push is best-effort and swallows its own failures. A metrics backend
being down must not fail an ingestion run or kill a streaming query —
observability that can take out the pipeline is worse than none.

---

## Deployment matrix

Every feature works on every path; what differs is speed and setup cost.

Legend: **✅ CI** verified by CI on every push · **✅ local** verified by hand on
this machine, not by CI · **⚙️ built, unverified** implemented but never run ·
**—** not deployed on that path.

| Feature | Native | Docker Compose | Kubernetes (kind) |
|---|---|---|---|
| Prices, ticks, lakehouse stats | ✅ local | ✅ CI | ✅ CI |
| Streamlit console | ✅ local | ✅ local | ✅ CI (UI→API over cluster DNS) |
| OpenAPI docs, `/metrics` | ✅ local | ✅ CI | ✅ CI |
| **Retrieval-grounded briefings** | ✅ local | ✅ local `--profile llm` | ⚙️ built, unverified `--set ollama.enabled=true` |
| Kafka → Spark → Delta | manual | ✅ CI `--profile streaming` | — |
| Prometheus + Grafana | — | ✅ local `--profile monitoring` | — |
| Airflow | ✅ local (WSL2) | ⚙️ built, unverified `--profile orchestration` | — |

**On the one ⚙️ that matters most:** the Helm chart can deploy Ollama, and CI
proves the chart still renders and deploys correctly — but CI runs it with
`ollama.enabled=false`. **A briefing has never been generated in-cluster.** The
k8s smoke test proves the chart is valid, not that in-cluster RAG works. Nothing
here should be read as claiming otherwise.

**No briefing is verified by CI on any path.** CI runners have no GPU, and a
~12-minute CPU generation would dominate a 4-minute pipeline. The compose
briefing below was generated by hand on this machine.

Briefings work on the two verified paths, but not equally fast:

| | Native | Containerised |
|---|---|---|
| Briefing, warm | **65–93 s** | 705.7 s |
| Briefing, cold | 480 s | 956.2 s |
| Generation throughput | 2.73 tok/s | 1.54–1.72 tok/s |
| Retrieval | 4.5 s | **1.2 s (faster)** |

Containerised generation is ~1.6× slower per token: llama3 is 6.2 GB resident
against WSL2's 8 GB, so the VM swaps, and generation is memory-bandwidth-bound.
Retrieval is *faster* there, because the embedding model shares an always-on
Ollama with a 30-minute keep-alive rather than being evicted by llama3 between
calls. Stopping the other containers was tried and did not help — the constraint
is the model against total VM memory, not the neighbours.

---

## Design decisions

**Delta I/O through delta-rs, not Spark's Delta connector.** On Windows the JVM
`delta-spark` connector needs a version-matched JAR plus `winutils.exe` and
`hadoop.dll` on `HADOOP_HOME`, a well-known source of breakage. Using delta-rs
for the table boundary and PySpark purely for distributed computation keeps
genuine Spark compute *and* genuine Delta versioning, schema enforcement and time
travel, without the fragile Windows Hadoop setup.

**The API avoids Spark entirely.** Serving a few hundred rows over HTTP does not
need a JVM, and avoiding it keeps the service startable anywhere — including
Windows, where PySpark local-mode task execution is broken on this machine.

**A missing Delta table is 503, not 500.** A pipeline that has not run yet is an
unready dependency, not a server fault. `/lakehouse/stats` degrades per-table
rather than failing the whole request.

**`API_BASE_URL` is separate from `API_HOST`/`API_PORT`.** One is what the client
dials, the other is what the service binds. Keeping them separate is what let the
UI move from `localhost` to `http://api:8000` (Compose) to
`http://rlrp-api:8000` (Kubernetes) with configuration changes and no code
changes.

**Dependencies are split per image.** Not primarily for size — the UI image
(1.09 GB) is barely smaller than the API's (980 MB), since both are dominated by
pandas/numpy/pyarrow. The value is architectural: the streaming image is the only
one carrying a JVM, and the UI image ships no `deltalake`/`pyspark`/`chromadb`,
so a regression to reading data directly fails at import inside the container
instead of silently working in dev.

**Metric labels use the route template, not the raw path.** Labelling
`/api/v1/prices/MSFT` per-ticker would add a time series per symbol — unbounded
cardinality, the standard way to overwhelm a Prometheus server.

**Base images and the kind node image are pinned.** `python:3.12-slim` floated to
Debian 13 mid-project and dropped OpenJDK 17, breaking the streaming build; kind's
default node image changes with every kind release. Both are now pinned, one by
digest.

---

## What broke, and what it changed

The failures were more instructive than the successes, and several only exist
outside a developer's machine.

**Four bugs that only exist inside a container.** The producer imported `TICKERS`
from the ingestion module, dragging `yfinance` into an image that does not ship
it (crash loop). Spark cannot `chmod` a checkpoint directory on a Windows bind
mount. Named volumes arrive root-owned while the container runs as uid 10001. The
producer logged nothing at all in run-until-stopped mode, making "is it
publishing?" unanswerable. **Three of the four were green under "the container is
`Up`"** — a crash-looping consumer reported `Up` the whole time.

**A Rust panic that `except Exception` cannot catch.** delta-rs raises
`pyo3_runtime.PanicException` for an unusable table location, and that inherits
from `BaseException`. The API returned a 500 traceback instead of its designed
503, and the producer crashed instead of falling back. This was a production bug,
not a CI artefact — it never fired locally only because the directory always
existed and was writable. The regression test is deliberate about this: a missing
directory raises `TableNotFoundError`, an ordinary `Exception`, so tests using
that pass with or without the fix. Only a `BaseException`-derived failure
exercises the guard, and the test was confirmed to fail against the pre-fix code.

**A dependency that existed everywhere except the image.** `prometheus-client`
was added to `requirements.txt` but not to the API image's requirements. The dev
venv and CI passed while the container had no metrics client — presenting as a
Prometheus target stuck `down` with every panel empty, which reads like a scrape
problem rather than a missing package.

**A Windows workaround that turned into a 190 MB saving.** `chromadb` evaluates
its default ONNX embedding function at import time, needing `onnxruntime`
importable — but that native extension conflicts with pandas/pyspark in-process
on Windows. Stubbing `sys.modules["onnxruntime"]` fixed the crash, and because
the real package is then never needed, it could be dropped from the image
entirely along with chromadb's unused `kubernetes` dependency.

**The same endpoints are 3–6× slower containerised.** `lakehouse/stats` goes from
86.83 ms to 492 ms p50. `/health`, which touches no storage, is *faster* in the
container — which rules out the framework and runtime and points at the `/mnt/c`
9p bind mount. It compounds the full-scan read path exactly as that model
predicts.

**A verification bug that nearly produced a false pass.** The streaming check
originally read its baseline row count before starting the stack; the API was
down, `curl` failed, and it fell back to `0` — which would have reported 765
pre-existing rows as newly landed, "proving" containerised streaming worked
without a single new row. A test whose baseline silently defaults to zero cannot
fail. It now aborts instead.

---

## Known limitations

Deliberate trade-offs, chosen with the cost understood and measured. They are
listed because a design's boundaries are part of the design — not as a backlog.

**The API read path scans whole tables.** `src/api/lakehouse.py` calls
`DeltaTable(...).to_pandas()` and then filters in pandas, so every request
materialises the entire table regardless of how few rows it returns. `?limit=5`
costs 23.25 ms and `?limit=100` costs 24.26 ms — essentially identical, because
latency tracks table size and Delta history depth rather than response size.
Accepted at these volumes; it would not hold at production ones, where the fix is
to push the predicate into the Parquet scan (`DeltaTable.to_pyarrow_dataset()`
with a filter) or partition by `Ticker`. Stated so the numbers are not mistaken
for evidence of an efficient read path.

**Briefing generation is slow, and that is a hardware fact.** ~66–93s warm,
~480s cold, because `llama3` is a 4.7 GB model that cannot stay resident here.
The pipeline around it is not the bottleneck.

**Spark runs in local mode, and PySpark task execution does not work natively on
Windows here.** The batch transform is verified in WSL2 and in CI instead; the
API deliberately avoids Spark so the service itself stays portable. A
dev-environment constraint, not a design limit.

**The tick stream is simulated.** A random walk seeded from real closes, not a
market data subscription.

---

## Running individual components

<details>
<summary><b>Batch pipeline (Airflow)</b></summary>

Ingestion alone needs no JVM:

```bash
python -m src.ingestion.fetch_market_data      # -> data/delta/ohlcv_raw (append)
```

The PySpark transform is verified through the real Airflow engine in WSL2:

```bash
# One-time setup, inside WSL2 (Ubuntu):
curl -LsSf https://astral.sh/uv/install.sh | sh
uv python install 3.12
uv venv --python 3.12 .venv && . .venv/bin/activate
uv pip install -r requirements.txt
uv pip install "apache-airflow==2.10.5" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
uv pip install "typing_extensions>=4.13"   # airflow pins 4.12.2, too old for pydantic_core

export AIRFLOW_HOME=~/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export PYTHONPATH=$(pwd)
airflow db migrate
airflow dags test market_pipeline $(date +%F)
```

`airflow dags test` runs the DAG through the real engine — DAG parsing, task
execution, XCom, DagRun state — without needing the scheduler/webserver daemons.
</details>

<details>
<summary><b>Streaming (Kafka + Spark), without containers</b></summary>

```bash
docker compose -f infra/docker-compose.yml up -d kafka

# Pre-create the topic. Spark's Kafka source fails hard
# (UnknownTopicOrPartitionException, no retry) if the consumer subscribes
# before the topic exists - found the hard way.
docker exec rlrp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic market.ticks --partitions 1 --replication-factor 1 --if-not-exists

python -m src.streaming.stream_consumer --timeout 110 &   # start first
sleep 20                                                  # let the query warm up
python -m src.streaming.tick_producer --duration 45 --interval 1
```

Start the consumer well before the producer: query startup is inconsistent here,
anywhere from ~1s to ~30-70s to commit its first micro-batch, uncorrelated with
backlog size. Pass `0` to either `--duration`/`--timeout` to run until stopped —
which is what the containers use.

Watch rows land while the producer is still running:

```bash
python -c "from deltalake import DeltaTable; print(len(DeltaTable('data/delta/ticks_raw').to_pandas()))"
```
</details>

<details>
<summary><b>RAG and briefings</b></summary>

```bash
python -m src.rag.index_builder                        # build/refresh the index
python -m src.rag.retriever "MSFT technical momentum"  # query it directly
python -m src.llm.briefing_generator MSFT              # generate a grounded briefing
```

Vector store: Chroma (persistent, `data/vectorstore/`). Embeddings: Ollama's
`nomic-embed-text` (768-dim), chosen over `sentence-transformers` to keep the
stack local and avoid pulling ~2 GB of torch.

Ollama also runs as a service in the stack, so briefings work in containers and
in Kubernetes, not only natively:

```bash
docker compose --profile llm up -d                          # compose
helm install rlrp infra/k8s/helm/rlrp --set ollama.enabled=true   # kubernetes
```

Both are opt-in: the first start pulls ~4.7 GB and llama3 needs 6.2 GB
resident. See [Deployment matrix](#deployment-matrix) for what that costs.
</details>

<details>
<summary><b>API and console, without containers</b></summary>

```bash
uvicorn src.api.main:app --reload     # API + Swagger UI at /docs
streamlit run ui/streamlit_app.py     # thin client, talks HTTP only
```
</details>

---

## Repo structure

```
realtime-lakehouse-rag-pipeline/
├── dags/market_pipeline_dag.py          # Airflow DAG: ingest -> transform
├── src/
│   ├── config.py                        # env-driven settings; the portfolio list
│   ├── ingestion/fetch_market_data.py   # EOD OHLCV pull -> raw Delta table
│   ├── processing/transform_spark.py    # PySpark indicators -> curated Delta table
│   ├── streaming/
│   │   ├── tick_producer.py             # simulated tick producer -> Kafka
│   │   └── stream_consumer.py           # Spark Structured Streaming -> ticks_raw
│   ├── rag/
│   │   ├── index_builder.py             # embeds docs/context + data/briefings
│   │   ├── retriever.py                 # retrieval step before every LLM call
│   │   └── _chromadb_compat.py          # Windows onnxruntime/chromadb import fix
│   ├── llm/briefing_generator.py        # Llama 3, retrieval-grounded, saves output
│   ├── api/
│   │   ├── main.py                      # FastAPI service, OpenAPI + /metrics
│   │   ├── schemas.py                   # Pydantic models = the published contract
│   │   └── lakehouse.py                 # Delta read layer (delta-rs, no JVM)
│   └── observability/metrics.py         # metrics + best-effort Pushgateway client
├── ui/
│   ├── streamlit_app.py                 # analyst console - thin client, HTTP only
│   └── api_client.py                    # its HTTP client
├── infra/
│   ├── docker-compose.yml               # kafka+api+ui default; others behind profiles
│   ├── Dockerfile.{api,ui,streaming,airflow}
│   ├── requirements-{api,ui,streaming}.txt   # per-image deps
│   └── k8s/                             # kind config, manifests, Helm chart
├── monitoring/
│   ├── prometheus.yml                   # scrape config: api + pushgateway
│   └── grafana/                         # provisioned datasource + dashboard
├── scripts/                             # CI step logic, runnable locally
├── tests/                               # api, infra, observability, processing, rag, streaming
├── docs/
│   ├── METRICS.md                       # every measured number, with method
│   ├── CV_NUMBERS.md                    # CV-ready figures, each traced to a metric
│   ├── DEMO.md                          # start/stop runbook for showing it live
│   └── context/                         # committed docs that seed the RAG index
├── data/                                # Delta tables, vector store, briefings - gitignored
└── .github/workflows/ci.yml             # lint/test, image builds, compose e2e, kind
```

---

## Continuous integration

Six parallel jobs, **4.3 min** wall clock. The heavy ones are gated behind the
cheap one so a lint failure fails fast, and `cancel-in-progress` stops superseded
pushes burning minutes.

| Job | What it proves |
|---|---|
| `lint-and-test` | ruff + the full 116-test suite |
| `build-images` (x3) | Each image builds and contains what it should — **and nothing it shouldn't** |
| `compose-e2e` | Kafka -> producer -> Spark consumer -> Delta, asserting rows that did not exist before, plus `/metrics` exposition |
| `k8s-smoke` | Helm lint/render, deploy to a real kind cluster, UI->API over cluster DNS, upgrade path |

Step logic lives in `scripts/` rather than inline YAML, so every step is readable
and runnable locally.

---

## Config & secrets

All configuration is `.env`-based (`.env.example` is the template); nothing is
hardcoded. In a real deployment: API keys and Kafka credentials would come from a
secret manager rather than `.env`, market/PII data handling would follow the
firm's data-governance policy, and the vector store would be access-controlled.
Kept explicit as a governance-awareness note even though this is a solo project.

---

## Build milestones

- [x] **M1 — Orchestration + Lakehouse foundation.** Airflow DAG; Delta Lake tables; pandas transform ported to PySpark.
- [x] **M2 — Streaming ingestion.** Kafka (KRaft); simulated tick producer; Spark Structured Streaming consumer.
- [x] **M3 — RAG layer.** Local vector store; index past briefings/context; retrieval before every LLM call.
- [x] **M4 — Service split.** FastAPI with OpenAPI docs; Streamlit becomes a thin client.
- [x] **M5 — Containerization + deployment.** Dockerised services; Kafka in compose; k8s manifests / Helm on a local `kind` cluster.
- [x] **M6 — CI/CD + tests + observability.** Full CI pipeline; Prometheus + Grafana pipeline-health dashboard.
- [x] **M7 — Documentation + metrics capture.** README rewrite; every claimed figure backed by a measurement.

Each milestone landed in a working, committed, CI-passing state — one commit per
milestone, linear history.

---

## Development environment

Windows laptop, 12 GB RAM, with WSL2 (Ubuntu) hosting Docker, Kafka, Spark and
Kubernetes. The full stack does not run comfortably all at once, so components
are brought up in subsets locally; the literal everything-at-once proof runs in
CI, where there is no local LLM load. Machine-specific constraints and the
workarounds they forced are documented in [docs/METRICS.md](docs/METRICS.md)
rather than hidden.
