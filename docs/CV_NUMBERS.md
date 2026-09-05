# CV-ready numbers

Copy-pasteable bullets for a CV or interview, with the measurement behind each
one. Every figure traces to a row in [METRICS.md](METRICS.md) — nothing here is
rounded up, inferred, or invented.

The point of the "what you can defend" column is that these are numbers you can
be questioned on. If an interviewer asks *"how did you measure that?"*, the
answer is in METRICS.md, and in several cases the honest answer includes a
caveat. Those caveats are given here too, because being able to state the limits
of your own number is worth more than the number.

---

## Suggested CV bullets

> Built a hybrid batch + streaming Lakehouse (Airflow, PySpark, Kafka, Delta
> Lake) with a retrieval-grounded LLM layer, served via FastAPI and deployed to
> Kubernetes with Prometheus/Grafana observability and a full CI pipeline.

Then pick from the following, depending on the role's emphasis.

### Data engineering / pipelines

- **Ingested 17 tickers into a versioned Delta Lake table via an Airflow DAG,
  with the PySpark indicator transform completing in 53s and the full
  ingest→curate DAG in ~75s.**
  *Measured:* real `airflow dags test` run, DagRun timestamps, 406 rows in →
  406 out.

- **Delivered a Kafka → Spark Structured Streaming → Delta pipeline with zero
  message loss: 765/765 events landed across 17 micro-batches, consumer lag
  returning to 0 once caught up.**
  *Measured:* incremental row counts polled every 3s while the producer was
  still running, plus topic end-offsets queried directly.

- **Achieved p50 2.82s end-to-end streaming write latency against a 2-second
  micro-batch trigger.**
  *Measured:* per-row, from each event's own `event_time` to the Delta commit
  that landed it — not an approximation from batch boundaries.

### Backend / API

- **Designed a versioned FastAPI service with OpenAPI docs and Pydantic
  contracts, serving curated market data at p50 24ms / p95 27ms.**
  *Measured:* 50 real HTTP requests per endpoint against a running uvicorn, not
  a test client.

- **Split a monolithic Streamlit app into an API plus a thin HTTP client,
  enforced by tests that fail if the UI imports data or inference modules.**
  *Measured:* AST-based tests; the UI image also ships no `deltalake`/`pyspark`,
  so a regression fails at container import.

### ML / RAG

- **Built a retrieval-grounded briefing layer (Chroma + local embeddings) that
  cites its sources, with ~4.5s retrieval latency over a corpus that grows as
  generated briefings are re-indexed.**
  *Measured:* four real retrieval samples inside real generation calls.

### Platform / DevOps

- **Containerised five services and deployed to Kubernetes via a Helm chart,
  verified with a 23-minute stability soak: 21 health samples, zero pod
  restarts.**
  *Measured:* node status, pod readiness, API health and restart counts sampled
  every 60s.

- **Cut the API image by 190 MB (1.17 GB → 980 MB) by removing transitive
  dependencies the service never uses.**
  *Measured:* `docker images` before and after; site-packages 739 MB → 591 MB.

- **Built a 4-job CI pipeline that builds all images and runs the stack
  end-to-end — Kafka → Spark → Delta plus a real Kubernetes deploy — in 4.3
  minutes wall clock.**
  *Measured:* GitHub Actions job timestamps, 6 parallel jobs, 9.8 runner-minutes.

- **Instrumented the pipeline with Prometheus/Grafana across two collection
  paths (scrape for the long-lived API, push for short-lived batch jobs and the
  non-HTTP Spark driver), at ~90 MB of overhead.**
  *Measured:* 3/3 targets up, 218 requests recorded, Grafana querying through
  its provisioned datasource.

---

## Figures and what you can defend

| Figure | Source row in METRICS.md | Caveat worth volunteering |
|---|---|---|
| 17 tickers, 406 rows/run | M1 rows ingested | Row count varies 392–406 with available trading days |
| 53.28s PySpark transform | M1 transform runtime | Local-mode Spark on one machine, not a cluster |
| ~75s end-to-end DAG | M1 DAG runtime | Wall clock from DagRun start/end |
| 765/765 events, 0 lost | M2 throughput consumed | Simulated tick feed, not a market data subscription |
| 16.98 events/sec | M2 throughput published | Deliberately paced (17 tickers × 1 round/sec), not a throughput ceiling |
| Consumer lag max 136 → 0 | M2 consumer lag | Peak is cold-start catch-up, not steady state |
| p50 2.82s write latency | M2 write latency | Max 15.49s in the first batch reflects query cold start |
| p50 24ms / p95 27ms API | M4 prices latency | Native uvicorn; 3–6× slower containerised over a 9p mount |
| ~4.5s retrieval | M3 retrieval latency | One 10.6s outlier when the LLM evicted the embedding model |
| 65–93s briefing (warm) | M3 briefing warm | Memory-bound on 8 GB; ~480s cold. Not a model-quality claim |
| 918 rows in 76s containerised | M5 containerized streaming | Baseline read from the live API first, so these are genuinely new rows |
| 0 pod restarts, 23 min soak | M5 soak test | Needed a WSL2 keep-alive; the cluster is not otherwise stable on this host |
| 980 MB API image | M5 image size | Still large — dominated by pandas/pyarrow, not by anything removable |
| 4.3 min CI | M6 CI duration | Wall clock across parallel jobs; 9.8 runner-minutes total |
| 91 tests | test suite | 3 skip locally (need Spark/Ollama); all run in CI |
| ~90 MB monitoring stack | M6 monitoring memory | Idle footprint with a small time series volume |

---

## Numbers NOT to claim

Stated explicitly so they do not creep into a CV by accident:

- **No "X% faster than before"** — the original AI Market Terminal was never
  benchmarked, so any speedup claim would be invented.
- **No production uptime, SLA, or scale figures** — this runs on one laptop.
  "23 minutes with zero restarts" is what was measured; it is not uptime.
- **No cost savings** — nothing here was measured against a paid baseline.
- **No claim of real-time market data** — the tick stream is synthetic
  throughout, and saying otherwise would misrepresent the project.
- **No data volume beyond hundreds of rows per table.** The pipeline is
  production-*shaped*, not production-*scaled*, and the read path would need
  predicate pushdown before it was.

---

## If asked "what went wrong?"

Strong interview material, all documented in METRICS.md:

- **A Rust panic that `except Exception` could not catch.** delta-rs raises
  `pyo3_runtime.PanicException`, which inherits from `BaseException`, so the
  API returned a 500 instead of its designed 503. Found by CI, not locally —
  and the regression test had to be built carefully, because the obvious test
  case raises an ordinary `Exception` and passes with or without the fix.
- **Three container bugs that were green under "the container is `Up`."** A
  crash-looping Spark consumer reported `Up` the entire time.
- **A dependency present in `requirements.txt` but missing from the image**, so
  dev and CI passed while the container had no metrics client — presenting as a
  scrape failure rather than a missing package.
- **A verification script that nearly produced a false pass**, because it read
  its baseline before the API was up and silently defaulted to zero. A test
  whose baseline defaults to zero cannot fail.
- **A misdiagnosis, corrected by evidence.** A dying Kubernetes node looked like
  a Docker cgroup-driver problem needing root access; `systemctl show docker`
  revealed Docker had been *stopped and restarted* while WSL had been up for 31
  minutes — the distro was tearing down its services when nothing held it open.
  The real fix needed no root at all.
