# Demo runbook

How to start, stop and present this project. Written for the case where someone
is watching, so it also covers the things that go wrong on a laptop in front of
an audience.

## Deployment matrix — what works where

Every feature now works on every path. What differs is speed and setup cost.

| Feature | Native (Windows) | Docker Compose | Kubernetes (kind) |
|---|---|---|---|
| Curated prices, ticks, lakehouse stats | ✅ | ✅ | ✅ |
| Streamlit console | ✅ | ✅ | ✅ |
| OpenAPI docs, `/metrics` | ✅ | ✅ | ✅ |
| **Retrieval-grounded briefings** | ✅ | ✅ `--profile llm` | ✅ `--set ollama.enabled=true` |
| Kafka → Spark → Delta streaming | ➖ manual | ✅ `--profile streaming` | ➖ not deployed |
| Prometheus + Grafana | ➖ | ✅ `--profile monitoring` | ➖ not deployed |
| Airflow | ➖ WSL2 | ✅ `--profile orchestration` | ➖ not deployed |
| Survives idle | ✅ | Only while a WSL window stays open | Same |

**Measured briefing speed**, because the paths are not equivalent:

| | Native | Containerised |
|---|---|---|
| Briefing, warm | **65–93 s** | 705.7 s |
| Briefing, cold | 480 s | 956.2 s |
| Retrieval | 4.5 s | **1.2 s (faster)** |

Containerised generation is ~1.6× slower per token because llama3 (6.2 GB
resident) against WSL2's 8 GB leaves the VM swapping. Retrieval is *faster*
there, because the embedding model shares an always-on Ollama with a 30-minute
keep-alive instead of being evicted by llama3 between calls. Full numbers and
the levers that were tried and did not work are in [METRICS.md](METRICS.md).

**For a live demo, use the native path** — briefings are 8× faster, which is the
difference between 90 seconds and 12 minutes in front of an audience. Show the
containerised path for the deployment story.

---

## Native path — recommended for demos

Three terminals, all from the repo root
(`C:\Users\ASUS\realtime-lakehouse-rag-pipeline`).

### Start

```powershell
# --- Terminal 1: Ollama (skip if it is already running) ---
$env:OLLAMA_HOST = "0.0.0.0:11434"
ollama serve
```

```powershell
# --- Terminal 2: the API ---
cd C:\Users\ASUS\realtime-lakehouse-rag-pipeline
$env:PYTHONPATH = "C:\Users\ASUS\realtime-lakehouse-rag-pipeline"
.venv\Scripts\python.exe -m uvicorn src.api.main:app --port 8000
```

```powershell
# --- Terminal 3: the console ---
cd C:\Users\ASUS\realtime-lakehouse-rag-pipeline
$env:PYTHONPATH = "C:\Users\ASUS\realtime-lakehouse-rag-pipeline"
$env:API_BRIEFING_TIMEOUT_SECONDS = "1800"
.venv\Scripts\python.exe -m streamlit run ui/streamlit_app.py
```

| Service | URL |
|---|---|
| Streamlit console | <http://localhost:8501> |
| FastAPI + Swagger | <http://localhost:8000/docs> |
| API metrics | <http://localhost:8000/metrics> |

### Stop

`Ctrl+C` in each terminal, or:

```powershell
Get-Process python,ollama -ErrorAction SilentlyContinue | Stop-Process -Force
```

### Check what is up

```powershell
curl.exe -s -o NUL -w "UI %{http_code}`n"     http://localhost:8501/_stcore/health
curl.exe -s -o NUL -w "API %{http_code}`n"    http://localhost:8000/health
curl.exe -s -o NUL -w "Ollama %{http_code}`n" http://localhost:11434/api/tags
```

---

## Warm the model before you present

**This is the single thing most likely to spoil a live demo.**

Briefing generation is memory-bound on this hardware (`llama3` is 4.7 GB):

| State | Time |
|---|---|
| Cold (model not loaded) | **~7 minutes** |
| Warm (model resident) | **~60-90 seconds** |

Check whether the model is currently loaded — an empty `models` list means the
next briefing will be cold:

```powershell
curl.exe -s http://localhost:11434/api/ps
```

Warm it ~2 minutes before presenting:

```powershell
curl.exe http://localhost:11434/api/generate -d '{\"model\":\"llama3\",\"prompt\":\"hi\",\"stream\":false}'
```

Ollama keeps the model resident for about 5 minutes after the last request, so
warm it shortly before the demo, not an hour earlier.

---

## Suggested demo order

1. **Sidebar — live lakehouse state.** Row counts and Delta versions for
   `ohlcv_raw`, `ohlcv_curated` and `ticks_raw`, read through the API. Worth
   saying: the UI does not open a Delta table, it has no `deltalake` installed
   at all; it asks the API.
2. **Chart.** The 20/50-day SMA lines were computed by the PySpark batch job and
   stored in the curated Delta table — the console is not recomputing them.
3. **<http://localhost:8000/docs>.** The OpenAPI contract. Run
   `GET /api/v1/lakehouse/stats` live from the Swagger page.
4. **Generate Briefing.** When it returns, point at the *"Grounded in: ..."*
   line: it lists earlier generated briefings alongside the static context docs,
   because every briefing is saved and re-indexed. The corpus feeds itself.
5. **Optional — <http://localhost:8000/metrics>.** Real Prometheus exposition,
   with the endpoint label as a route template (`/api/v1/prices/{ticker}`)
   rather than per-ticker, which would be unbounded cardinality.

If asked for numbers, [docs/METRICS.md](METRICS.md) has every measurement and
[docs/CV_NUMBERS.md](CV_NUMBERS.md) has the short versions with their caveats.

---

## Docker / Kubernetes path — the deployment story

```bash
wsl -d Ubuntu                                     # KEEP THIS WINDOW OPEN
cd /mnt/c/Users/ASUS/realtime-lakehouse-rag-pipeline/infra

docker compose up -d                              # kafka + api + ui
docker compose --profile monitoring up -d         # + prometheus, grafana, pushgateway
docker compose --profile streaming up -d          # + tick producer and Spark consumer
docker compose ps
```

| Service | URL |
|---|---|
| Streamlit console | <http://localhost:8501> |
| FastAPI + Swagger | <http://localhost:8000/docs> |
| Grafana (anonymous admin) | <http://localhost:3000> |
| Prometheus | <http://localhost:9090> |
| Pushgateway | <http://localhost:9091> |
| Airflow (`--profile orchestration`) | <http://localhost:8080> |

Stop:

```bash
docker compose --profile monitoring --profile streaming --profile orchestration down
```

Airflow's admin password, once started:

```bash
docker exec rlrp-airflow cat /opt/airflow/standalone_admin_password.txt
```

### Keep the WSL window open

This WSL distro tears down its services when no process holds it open, which
stops Docker and takes every container with it. Symptom: the links simply stop
responding, and the *next* Docker command fails with a cgroup/systemd scope
error that reads like a Docker configuration problem rather than "the distro
shut down". An interactive `wsl` window is enough to prevent it; so is any
long-running process inside the distro.

### Briefings in containers

Ollama runs as its own service in the stack, so briefings work here too:

```bash
docker compose --profile llm up -d       # + ollama, and pulls the models
```

The first start downloads ~4.7GB into a named volume; later starts reuse it.
The API reaches Ollama by service name over the compose network.

This replaced an arrangement that could never have worked: Ollama ran on
**Windows** while the containers run in **WSL2**, and reaching the Windows host
needs an inbound firewall rule on 11434 that requires administrator rights.
`POST /api/v1/briefings` simply returned 502.

**It requires `memory=8GB` in `.wslconfig`.** llama3 is 6.2GB resident; WSL2's
default allocation is 50% of host RAM (5.86GB here), which is not enough and
would page rather than fail cleanly. The repo's `.wslconfig` sets this.

**Expect ~12 minutes per briefing on this path**, versus ~90 seconds native —
see the deployment matrix above.

---

## Quick troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Links dead after a few minutes (Docker path) | WSL tore down its services | Keep a `wsl` window open, then `docker compose up -d` again |
| `ERR_CONNECTION_REFUSED` on 8501 | Nothing is listening — the process stopped | Restart the UI (native) or the stack (Docker) |
| Briefing returns 502 | Ollama not running | Native: `ollama serve`. Compose: `docker compose --profile llm up -d`. Check `curl http://localhost:11434/api/tags` |
| Briefing takes 7-16 minutes | Cold model load, or the containerised path | Warm it first; prefer the native path for demos |
| Prices endpoint returns 503 | A Delta table is missing | `python -m src.ingestion.fetch_market_data` |
| Port already in use | The other path is still running | `docker compose down`, or stop the native processes |
