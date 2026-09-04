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
| **Retrieval latency** (query embed + Chroma query) | **4.49-5.47s** typical (4 real samples: 4.487, 5.468, 4.505; one 10.559s outlier) | 2026-09-03/04 | `retriever.retrieve()`'s returned `latency_seconds`, real queries inside real `generate_briefing("MSFT", ...)` calls. The 10.56s outlier came immediately after `llama3` was loaded, which evicts `nomic-embed-text` on this 8GB box - so the query embed pays a model reload (see "model thrashing" below) | M3 |
| Index build (5 docs: 3 context + 2 generated briefings) | 12.14s, embedding_dim 768 | 2026-09-04 | `index_builder.build_index()`'s returned metrics | M3 |
| **Briefing generation latency, warm** (Ollama `llama3`, local; model already resident) | **65.64s and 92.57s** (2 real samples) for a 2-3 sentence briefing | 2026-09-04 | `generate_briefing()`'s returned `generation_latency_seconds`, runs 2 and 3 of a 3-run benchmark on an otherwise-normal machine (WSL2 shut down; ~1.4GB RAM available). **This is the representative number.** | M3 |
| Briefing generation latency, cold (includes `llama3` model load) | 480.28s | 2026-09-04 | Run 1 of the same benchmark. 5.2-7.3x the warm figure: the 4.7GB model has to be paged in against ~1.4GB available RAM (see below) | M3 |
| Briefing generation latency, under heavy contention | 537.93s (~9 min) | 2026-09-03 | Same call, but measured while the machine was down to ~177MB free RAM (~4 VS Code processes + multiple Claude Code processes). Kept here deliberately as the worst-case datapoint - the small gap to the 480.28s cold run is what identified this workload as memory-bound rather than CPU-bound | M3 |
| Local `llama3` token throughput (warm) | **2.73 tok/s** generation (44 tok in 16.09s), 3.26 tok/s prompt eval (20 tok in 6.14s), 0.26s model load | 2026-09-04 | Raw `ollama.chat()` response's own `eval_count`/`eval_duration`/`prompt_eval_*`/`load_duration` fields. For contrast, the same kind of call under the contention above managed 3 tokens in 44s (~0.07 tok/s) - a ~40x difference | M3 |
| **API latency, `GET /health`** | **p50 2.48ms, p95 3.11ms** (min 2.16, max 3.67) | 2026-09-04 | 50 real HTTP requests against a running `uvicorn` (not TestClient), after 5 warmup calls. No lakehouse dependency, so this is the framework floor | M4 |
| **API latency, `GET /api/v1/tickers`** | **p50 2.25ms, p95 2.58ms** | 2026-09-04 | Same harness. In-memory constant - matches the `/health` floor as expected | M4 |
| **API latency, `GET /api/v1/prices/{ticker}`** | **p50 24.26ms, p95 26.98ms** (`limit=100`); **p50 23.25ms, p95 25.60ms** (`limit=5`) | 2026-09-04 | Same harness, against the real 406-row `ohlcv_curated` table. Note `limit` barely moves the number - see "full-scan read path" below | M4 |
| **API latency, `GET /api/v1/ticks/{ticker}`** | **p50 55.93ms, p95 66.99ms** | 2026-09-04 | Same harness, against the real 765-row `ticks_raw` table. ~2.3x the `prices` endpoint: same code path, but this table is at Delta version 16 (17 commits from the streaming micro-batches) vs version 0, so there is more transaction log to replay | M4 |
| **API latency, `GET /api/v1/lakehouse/stats`** | **p50 86.83ms, p95 164.04ms** (min 57.36, max 278.24) | 2026-09-04 | Same harness. Reads all three tables in full, so it costs roughly the sum of the others - the widest p50/p95 spread of any endpoint | M4 |
| **API latency, `POST /api/v1/briefings/{ticker}`** | **521.48s end-to-end over HTTP** (retrieval 1.512s + generation 516.13s) | 2026-09-04 | Single real request to the running server; the full path (Delta read -> Chroma retrieval -> Ollama generation -> briefing saved to `data/briefings/`) returned 200. Cold model load again - `llama3` had been evicted by the time this ran, so it matches the 480.28s cold figure above, not the 65-93s warm one. No p50/p95: a single sample, and quoting percentiles off one call would be fake precision. Sources retrieved: 2 prior briefings + `tickers.md` + `schema_notes.md` | M4 |
| **Container image size, API** | **980 MB** (down from 1.17 GB before pruning) | 2026-09-04 | `docker images` on the built `rlrp-api:local`. The 190 MB drop is `pip uninstall kubernetes onnxruntime` in the install layer - chromadb transitive deps this project never uses (see below) | M5 |
| **Container image size, UI** | **1.09 GB** | 2026-09-04 | `docker images` on `rlrp-ui:local`. Contains no `deltalake`/`pyspark`/`chromadb`/`ollama` - verified by `importlib.util.find_spec` inside the running image, not just by reading the requirements file | M5 |
| **Container image size, streaming** | **1.75 GB** (before baking the Kafka JARs) | 2026-09-04 | `docker images` on `rlrp-streaming:local`. The largest by far and legitimately so: it is the only image carrying a JVM (openjdk-17-jre-headless) plus pyspark | M5 |
| **Image build time (cold, on this machine)** | **API 521s, UI 283s, streaming 847s** | 2026-09-04 | Wall clock around `docker compose build <svc>`. Dominated by the build context living on `/mnt/c`, which Docker-in-WSL2 reaches over the 9p filesystem - a dev-environment cost, not a CI number. A cached rebuild of an unchanged image is 6s | M5 |
| **Containerized streaming, end to end** | **918 new rows landed in 76s** (ticks_raw 765 -> 1683, Delta version 16 -> 22) | 2026-09-04 | `docker compose --profile streaming up`: tick producer *container* -> Kafka *container* -> Spark Structured Streaming consumer *container* -> Delta, read back through the API. Baseline row count read from the live API before starting (see note below on why that matters), so these are genuinely new rows, not pre-existing ones | M5 |
| **Compose stack cold start** (kafka + api + ui) | **27s** to all-healthy; API healthy in 1-9s, UI in 8s | 2026-09-04 | Wall clock from `docker compose up -d` to both `/health` and `/_stcore/health` returning 200 | M5 |
| **Runtime memory, default stack** (kafka + api + ui) | **~486 MB total**: API 89 MB, UI 46 MB, Kafka 352 MB | 2026-09-04 | `docker stats --no-stream` against WSL2's 3.76 GiB allocation | M5 |
| **Runtime memory, full streaming stack** | **~1.27 GB total**: Spark consumer 622 MB, Kafka 395 MB, API 116 MB, producer 89 MB, UI 49 MB | 2026-09-04 | `docker stats --no-stream` with the streaming profile up. The Spark consumer is the single largest component, as expected | M5 |
| **Spark Kafka JAR resolution in-container** | **0 kB downloaded, 11 artifacts already retrieved (24-267ms)** | 2026-09-04 | Ivy retrieve summary in the consumer's logs. The connector JARs are baked into the image at build time, so a cold container start needs no Maven access | M5 |
| **Kubernetes deploy, plain manifests** | **8 resources applied, both Deployments ready, 0 restarts** | 2026-09-04 | `kubectl apply -f infra/k8s/` on a real kind cluster (k8s v1.37.0): Namespace, ConfigMap, 2 Deployments, 4 Services. API reachable on localhost:8000 in 1s via NodePort, UI HTTP 200 on 8501 | M5 |
| **Kubernetes deploy, Helm chart** | **`helm install` STATUS deployed; 1/1 + 1/1 ready; 0 restarts** | 2026-09-04 | `helm install rlrp infra/k8s/helm/rlrp -n rlrp --create-namespace --wait`. Release rlrp-0.1.0, appVersion 1.0.0 | M5 |
| **Helm upgrade path** | **api scaled 1 -> 2 replicas, 2/2 ready; revision 1 superseded -> 2 deployed** | 2026-09-04 | `helm upgrade --set api.replicaCount=2 --wait`, confirmed with `helm history` | M5 |
| **UI -> API over in-cluster DNS** | **UI pod fetched real curated MSFT bars from `http://rlrp-api:8000`** | 2026-09-04 | `kubectl exec` into the UI pod, reading its own `API_BASE_URL`. Same substitution compose made with `http://api:8000` - the Milestone 4 split carried to k8s unchanged | M5 |
| **kind cluster stability (soak test)** | **23 min continuous, 0 pod restarts** - 21 samples at 60s intervals, every one healthy | 2026-09-04 | Sampled node status, pod readiness, `/health`, UI HTTP and restart counts every 60s for 20 min. Final state: 3/3 pods Running (ages 20-21m), helm release still `deployed` at revision 2, API still serving real curated MSFT bars. `dockerd` showed `NRestarts=0` throughout, confirming the distro was never torn down. Before the keep-alive fix the node died within 1-3 minutes | M5 |
| **kind cluster creation** | **~60-90s** to Ready (node image ~1 GB, pulled once) | 2026-09-04 | `kind create cluster --config infra/k8s/kind-cluster.yaml` | M5 |
| **`kind load docker-image`** | **48s (api), 57s (ui)** | 2026-09-04 | Required because there is no registry; images are 227 MB / 217 MB as stored by containerd in the node | M5 |
| CI pipeline duration (lint + test + build) | _tbd_ | | | M6 |

## Milestone 3 "Done when" proof: a briefing citing retrieved context

Real output from `python -m src.llm.briefing_generator MSFT`, 2026-09-03.
Retrieved sources: `tickers.md`, `schema_notes.md`, `methodology.md` (all 3
docs in the index at the time - only 3 existed yet). Saved to
`data/briefings/MSFT_20260903T195825.md`.

> Here's a concise market update for MSFT:
>
> Microsoft (MSFT) is consolidating its recent gains, with the stock
> hovering around the $510 level. The 20-day SMA is still above the 50-day
> SMA, indicating a bullish trend. However, the daily return has been
> trending sideways, suggesting a potential test of support around the $500
> mark, which could attract buyers if the stock can hold this level.
>
> **This view is grounded in MSFT's context as a mega-cap software/cloud
> company, sensitive to cloud growth, AI capex, and enterprise software
> demand.** The stock's recent price action is consistent with a
> consolidating trend, which may be driven by investors reassessing the
> company's growth prospects and valuing the stock relative to its peers.

The bolded sentence is a direct citation of `tickers.md`'s MSFT entry
("Mega-cap software/cloud (Azure); moves on cloud growth, AI capex, and
enterprise software demand") - the briefing is grounded in retrieved prior
context, not just the day's raw price data.

**Follow-up (2026-09-04)**: now that generated briefings have accumulated in
`data/briefings/` and been indexed, retrieval for the same query returns
`['MSFT_20260903T195825.md', 'tickers.md', 'MSFT_20260903T194419.md',
'schema_notes.md']` - i.e. two of the four retrieved passages are *prior
briefings*, so the loop the milestone describes (briefings feeding back into
the corpus that grounds later briefings) is closed and observable, not just
designed for. Worth recording honestly: in these later runs `llama3` produced
sound briefings but did **not** always emit an explicit citation sentence the
way the run above did - the retrieval step is deterministic and always
happens, but whether the model verbalises the attribution varies between
generations. The quoted run above remains the "Done when" evidence because it
shows the citation explicitly.

## Milestone 4 note: the full-scan read path

The measured `prices` numbers say something the pass/fail wouldn't: `limit=5`
(23.25ms p50) and `limit=100` (24.26ms p50) cost essentially the same. That's
because `src/api/lakehouse.py` calls `DeltaTable(...).to_pandas()` and *then*
filters by ticker and tail-slices in pandas - so every request materialises the
whole table regardless of how few rows it returns. Latency here tracks table
size and Delta history depth, not response size, which is also why `ticks_raw`
(765 rows, version 16) is ~2.3x `ohlcv_curated` (406 rows, version 0) through
identical code.

At current volumes this is comfortably fast enough and the simplicity is worth
it. It would not survive real data volumes, and the fix is understood rather
than hypothetical: push the predicate down into the Parquet scan
(`DeltaTable.to_pyarrow_dataset()` with a filter, or partition the tables by
`Ticker`) so the reader touches only the relevant files. Recorded here as a
known, measured limitation rather than presented as a finished result.

## Milestone 5 note: what the per-service dependency split actually bought

Worth recording honestly, because the obvious expectation was wrong. Splitting
`requirements.txt` into per-image files (`infra/requirements-*.txt`) was
expected to make the UI image much smaller than the API's. It didn't: **1.09 GB
vs 980 MB**, and before the API's dependency pruning the UI was actually the
*smaller* of the two by only ~7%. Both images are dominated by the same
scientific-Python core - pandas, numpy and pyarrow - which Streamlit pulls in
regardless of what the console does with it.

So the split's value is not primarily size. It is:

- **The streaming image is the only one carrying a JVM.** pyspark +
  openjdk-17-jre-headless is what makes it 1.75 GB; keeping that out of the API
  and UI is the real saving, and it is structural rather than incidental.
- **It makes the thin-client boundary physical.** The UI image has no
  `deltalake`, `pyspark`, `chromadb` or `ollama` at all, so a regression to
  reading data or running inference directly fails at import inside the
  container instead of silently working in dev.

The genuine size win came from somewhere else entirely: **chromadb pulls
`kubernetes` (83 MB) and `onnxruntime` (66 MB)** as transitive dependencies,
neither of which this project uses. Removing them took the API image from
1.17 GB to 980 MB (site-packages 739 MB -> 591 MB).

That removal is safe for a reason that traces back to Milestone 3:
`src/rag/_chromadb_compat.py` already stubs `sys.modules["onnxruntime"]` to
work around a Windows DLL conflict, so chromadb's default embedding function -
the only thing that wants onnxruntime - never touches the real package on any
platform. A Windows-specific workaround turned out to be what makes a
Linux image 190 MB smaller. Verified rather than assumed: chromadb import,
upsert, query and graceful degradation were all exercised with both packages
uninstalled, and the compose smoke test re-checks it inside the built image.

## Milestone 5 note: four bugs that only exist inside a container

None of these were visible in the dev venv, in CI, or in the test suite before
this milestone. They are recorded because they are the substance of what
containerization actually surfaced - and because in three of four cases the
naive health signal ("the container is Up") was green while the thing was
broken.

1. **`yfinance` missing in the streaming image.** `tick_producer.py` imported
   `TICKERS` from `ingestion/fetch_market_data.py`, which imports yfinance at
   module level. The producer invents ticks and never calls Yahoo Finance, so
   it had no business importing that module. Crash-looped with
   `ModuleNotFoundError`. Same coupling had already been fixed for the API -
   fixing one importer and not auditing the rest is what let it survive.
   `TICKERS` now lives in `src/config.py`.
2. **Spark cannot checkpoint onto a Windows-backed bind mount.** Spark chmods
   its checkpoint directory; on `/mnt/c` as a non-root user that fails with
   `chmod: ... Operation not permitted`, killing the query at startup. Fixed by
   moving checkpoints to a named volume, which is where runtime state belongs
   anyway.
3. **Named volumes are created root-owned.** Fixing (2) moved the error rather
   than removing it: `java.io.IOException: mkdir of file:/checkpoints/... failed`,
   because the container runs as uid 10001. Docker initialises an empty named
   volume from the image's directory at that path *including ownership*, so the
   fix is `mkdir /checkpoints && chown appuser` in the Dockerfile - not
   anything in compose. An already-created volume keeps its old ownership, so
   the stale volume had to be removed too.
4. **The producer logged nothing at all.** `run_producer` only printed metrics
   when it finished, which in run-until-stopped mode never happens - so
   `docker logs` was empty for five minutes and "is it publishing?" could not
   be answered. It now logs a startup line and a 30s heartbeat with count and
   rate.

**A verification bug worth recording too**, since it nearly produced a false
pass: the first version of the streaming check read its baseline row count
before starting the stack. The API was down, `curl` failed, and the script
fell back to `0` - which would have reported the 765 rows already written by
Milestone 2's *non-containerized* processes as newly landed, "proving"
containerized streaming worked without a single new row. The check now reads
the baseline only after the API is healthy and aborts outright if it cannot,
because a test whose baseline silently defaults to zero cannot fail.

Related: `found ... in central` in Ivy's output was initially misread as
evidence of a runtime Maven download. Ivy prints that even when resolving
entirely from local cache - "central" names the resolver that originally
supplied the module, not the source of this resolution. The decisive signal is
the retrieve summary's byte count (`0 kB`).

## Milestone 5 note: why the kind node kept dying (and the wrong first answer)

Worth recording because the first diagnosis was wrong, and the evidence that
corrected it was specific.

**Symptom.** The kind node container shut down on its own after roughly 1-3
minutes. Not memory: `OOMKilled=false`, ~5.3 GB of WSL2's 5.86 GB free,
`systemd-oomd` inactive. The node's own logs showed an orderly systemd
shutdown (`Reached target umount.target`), so something was asking it to stop.
Restarting it afterwards failed with:

```
runc create failed: unable to apply cgroup configuration:
error creating systemd unit `docker-<id>.scope`: got `failed`
```

**First (wrong) conclusion.** Docker here runs the systemd cgroup driver on
cgroup v2 under systemd PID 1 - the known-fragile combination for kind - so
this looked like it needed `native.cgroupdriver=cgroupfs` in
`/etc/docker/daemon.json` plus a Docker restart, i.e. root access this WSL user
does not have.

**What actually showed the real cause.** `systemctl show docker` reported
`ExecMainStartTimestamp` four seconds in the past with `NRestarts=0`, while WSL
itself had been up for 31 minutes. Docker had not *restarted*; it had been
stopped and freshly started at the moment of reconnecting. The distro's
services are torn down when no process holds the distro open, and the kind node
dies with them. The cgroup error is what happens *afterwards*, when Docker
tries to restart a container whose runtime state went away with the teardown -
a consequence, not the cause.

**Fix, no root required.** Hold a long-lived process open in the distro. The
same keep-alive technique this project already used in Milestone 2 to stop
WSL2 idling out from under a running Kafka broker.

**Verified by soak test**, not by finishing quickly: with the keep-alive in
place, the cluster was sampled every 60s for 20 minutes - node status, pod
readiness, API `/health`, UI HTTP, and restart counts. Every sample healthy,
0 pod restarts, versus a node that previously died inside 1-3 minutes.

Two smaller things worth knowing:

- `kubectl apply --dry-run=server` on `infra/k8s/deployment.yaml` reports
  `namespaces "rlrp" not found`. A server dry-run does not actually create the
  Namespace the same file defines, so the namespaced objects have nothing to
  validate against. It is a property of dry-run, not a manifest defect - the
  real apply creates all 8 resources cleanly.
- `/tmp` inside this WSL distro does not survive the service teardown described
  above, so verification scripts are fed to bash by process substitution from
  `/mnt/c` rather than staged in `/tmp`.

## Machine baseline (for context on all timings)

- Windows 10 Home, 8 GB RAM.
- **RAG (Milestone 3) runs natively on Windows** - unlike M1/M2, no
  Spark/Java involved (just Ollama HTTP calls + Chroma), so no WSL2 detour
  needed. Ollama `nomic-embed-text` (768-dim) for embeddings, `llama3` for
  generation, both already running locally for the ported baseline. One real
  Windows-only bug found and fixed: `chromadb`'s default embedding function
  is constructed at `import chromadb` time regardless of whether it's ever
  used, needing `onnxruntime` importable - but onnxruntime's native
  extension is unreliable in a process that's also loaded pandas/pyspark (a
  clean `ImportError` in isolation, a full Windows access violation when
  more of the project's native deps share the process, e.g. the full test
  suite). Fixed in `src/rag/_chromadb_compat.py`: unconditionally stub
  `sys.modules["onnxruntime"]` before `chromadb` is ever imported - safe
  because this project always supplies its own embeddings explicitly and
  never calls chromadb's default, so the real package was never needed.
- **Observed severe CPU contention during M3 verification**: this dev
  machine had ~4 VS Code processes and multiple Claude Code processes active
  simultaneously, at one point down to ~177MB free RAM out of 8GB. Ollama
  inference was 10-50x slower than the (also unremarkable) hardware should
  produce - confirmed genuine forward progress via climbing process CPU
  time, not a hang, but not representative of normal conditions. Recorded
  as-measured per this project's own rule (real numbers only, not
  estimates), with this context attached rather than being smoothed over.
- **Re-measured under normal conditions (2026-09-04)**, to sit alongside
  that worst case rather than replace it. Conditions at measurement time:
  WSL2 shut down (M3 doesn't need it), no Spark/Kafka running, ~1.4GB RAM
  available, total CPU ~36% busy across 4 logical cores. Three consecutive
  `generate_briefing("MSFT", ...)` runs: **480.28s cold, then 92.57s and
  65.64s warm**. Conclusion: this workload is **memory-bound, not
  CPU-bound**. `llama3` is a 4.7GB model and this is an 8GB machine whose
  normal working set (VS Code, browsers, editors) leaves ~1.4GB available,
  so the cold path is dominated by paging the model in - which is also why
  the heavily-contended 537.93s run was only ~12% worse than the "clean"
  480.28s cold run. Once the model is resident, generation is ~66-93s at
  2.73 tok/s, and that warm figure is the one to quote.
- **Two-model thrashing on 8GB**: the RAG path needs both
  `nomic-embed-text` (274MB) and `llama3` (4.7GB). They don't comfortably
  co-reside here, so loading one can evict the other - directly visible in
  the retrieval numbers (4.5s typical, but 10.6s on the query that ran right
  after `llama3` was loaded and had to pull the embedding model back in).
  Not a correctness problem; worth knowing before reading too much into any
  single latency sample.
- **Python 3.12 was uninstalled from this machine mid-milestone** (an
  unrelated installer run), which broke `.venv` - its interpreter was gone
  while `.venv/Lib/site-packages` stayed intact. Fixed by reinstalling
  CPython 3.12.10 to the original prefix
  (`%LocalAppData%\Programs\Python\Python312`), after which the existing
  venv worked unchanged with no package reinstall. Noted only because it
  explains the gap between the 2026-09-03 and 2026-09-04 measurements.
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
