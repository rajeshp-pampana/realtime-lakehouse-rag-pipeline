---
type: schema_notes
title: Lakehouse schema reference
---

# Lakehouse schema reference

## `data/delta/ohlcv_raw` (bronze, batch)

One row per ticker per trading day, appended daily by the Airflow DAG's ingest
task. Columns: `Date` (YYYY-MM-DD string), `Ticker`, `Open`, `High`, `Low`,
`Close` (float64), `Volume` (int64), `ingested_at_utc` (ISO timestamp string).

## `data/delta/ohlcv_curated` (silver, batch)

One row per ticker per trading day, overwritten by the PySpark transform on
every DAG run. Adds `SMA_20` and `SMA_50` (rolling mean of Close over the
trailing 20/50 trading days for that ticker), `daily_return` (fractional
change vs. the prior trading day's Close; null on a ticker's first row),
and `volatility_20d` (rolling standard deviation of `daily_return` over the
trailing 20 days; null until enough history exists).

## `data/delta/ticks_raw` (bronze, streaming)

One row per simulated intraday tick event, appended by the Spark Structured
Streaming consumer. Columns: `ticker`, `price`, `size`, `event_time` (producer
timestamp), `kafka_timestamp` (broker timestamp). Deliberately a separate
table from `ohlcv_raw`: ticks and daily bars are different natural grains
(event-level vs. one row per ticker per day), so they aren't unioned into one
table - the standard Lakehouse bronze-layer pattern.

## Interpreting the indicators

- **SMA_20 / SMA_50** (simple moving average): the trend a technical analyst
  reads first. Price above both SMAs and SMA_20 above SMA_50 is a bullish
  setup ("golden cross" territory); the reverse is bearish.
- **daily_return**: day-over-day percentage move. Persistent same-sign returns
  indicate a trend; alternating sign indicates consolidation/chop.
- **volatility_20d**: how noisy the last 20 days of returns have been. Rising
  volatility alongside a flat SMA often precedes a breakout in either
  direction; falling volatility suggests the stock is settling into a range.
