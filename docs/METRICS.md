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
| API p50 / p95 latency per endpoint | _tbd_ | | | M4 / M6 |
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
