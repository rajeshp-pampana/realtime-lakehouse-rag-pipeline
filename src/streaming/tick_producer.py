"""Simulated intraday tick producer -> Kafka.

Milestone 2 implements this: generate a synthetic price event per ticker at a
fixed interval (a random walk seeded from each ticker's latest known Close in
the raw Delta table) and publish it as JSON to the ``market.ticks`` topic.

NOTE: this is a *simulated* event feed - a synthetic random walk, not a paid
real-time market data subscription. Called out here and in the README.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import UTC, datetime

from src import config
from src.ingestion.fetch_market_data import TICKERS

TOPIC = config.KAFKA_TICKS_TOPIC

# Per-tick move: uniform +/-30bps, tuned to look like plausible intraday noise
# without the walk exploding over a short demo run.
_MAX_TICK_MOVE = 0.003


def _load_last_closes() -> dict[str, float]:
    """Seed each ticker's walk from the latest Close in the batch raw Delta
    table. Falls back to a flat $100 seed for any ticker not found there (e.g.
    the batch DAG hasn't run yet) so the producer never hard-fails on this.
    """
    try:
        from deltalake import DeltaTable

        pdf = DeltaTable(config.DELTA_OHLCV_RAW).to_pandas()
        return pdf.sort_values("Date").groupby("Ticker")["Close"].last().to_dict()
    except Exception:
        return {}


def _make_producer():
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def run_producer(
    tickers: list[str] | None = None,
    duration_seconds: float = 30.0,
    interval_seconds: float | None = None,
) -> dict:
    """Publish simulated tick events for ``duration_seconds``, one round (all
    tickers) every ``interval_seconds``. Returns a metrics dict.
    """
    tickers = tickers or TICKERS
    interval_seconds = config.TICK_INTERVAL_SECONDS if interval_seconds is None else interval_seconds
    prices = {t: 100.0 for t in tickers}
    prices.update(_load_last_closes())

    producer = _make_producer()
    started = time.perf_counter()
    published = 0
    try:
        while time.perf_counter() - started < duration_seconds:
            round_started = time.perf_counter()
            for ticker in tickers:
                prices[ticker] *= 1 + random.uniform(-_MAX_TICK_MOVE, _MAX_TICK_MOVE)
                event = {
                    "ticker": ticker,
                    "price": round(prices[ticker], 4),
                    "size": random.randint(1, 500),
                    "event_time": datetime.now(UTC).isoformat(),
                }
                producer.send(TOPIC, key=ticker, value=event)
                published += 1
            producer.flush()
            sleep_for = interval_seconds - (time.perf_counter() - round_started)
            if sleep_for > 0:
                time.sleep(sleep_for)
    finally:
        producer.close()

    total_duration = time.perf_counter() - started
    metrics = {
        "events_published": published,
        "duration_seconds": round(total_duration, 2),
        "events_per_second": round(published / total_duration, 2) if total_duration > 0 else 0.0,
        "topic": TOPIC,
        "tickers": len(tickers),
    }
    print(f"[tick_producer] {metrics}")
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=30.0, help="seconds to run for")
    parser.add_argument("--interval", type=float, default=None, help="seconds between tick rounds")
    args = parser.parse_args()
    run_producer(duration_seconds=args.duration, interval_seconds=args.interval)
