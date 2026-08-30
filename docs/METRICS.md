# Measured metrics log

Real numbers, recorded as they are measured (never reconstructed from memory or
estimated). These feed the README metrics table and replace every `[INSERT]`
placeholder in the CV Projects section.

| Metric | Value | Measured on | How measured | Milestone |
|---|---|---|---|---|
| Rows ingested per batch run | _tbd_ | | | M1 |
| Total historical rows in Lakehouse | _tbd_ | | | M1 |
| End-to-end batch pipeline runtime (ingest -> transform -> index) | _tbd_ | | | M1 / M3 |
| Delta table versions produced per run | _tbd_ | | | M1 |
| Simulated tick throughput published (events/sec) | _tbd_ | | | M2 |
| Simulated tick throughput consumed (events/sec) | _tbd_ | | | M2 |
| Kafka consumer lag under normal load | _tbd_ | | | M2 / M6 |
| Streaming -> Delta write latency | _tbd_ | | | M2 |
| Retrieval latency (query -> retrieved context) | _tbd_ | | | M3 |
| Briefing generation latency | _tbd_ | | | M3 |
| API p50 / p95 latency per endpoint | _tbd_ | | | M4 / M6 |
| CI pipeline duration (lint + test + build) | _tbd_ | | | M6 |

## Machine baseline (for context on all timings)

- Windows 10 Home, 8 GB RAM, Python 3.12, Java 21, Docker Desktop (WSL2 backend).
- Components are run in subsets locally; the full-stack end-to-end proof runs in CI.
