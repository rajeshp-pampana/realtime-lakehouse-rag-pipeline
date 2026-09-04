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

# TICKERS comes from config, not from ingestion/fetch_market_data: importing it
# from there pulls yfinance into the streaming image, which doesn't ship it (the
# producer invents ticks, it never calls Yahoo Finance). Same coupling the API
# hit - see the note in src/config.py.
from src import config
from src.config import TICKERS

TOPIC = config.KAFKA_TICKS_TOPIC

# Per-tick move: uniform +/-30bps, tuned to look like plausible intraday noise
# without the walk exploding over a short demo run.
_MAX_TICK_MOVE = 0.003

# How often the run-until-stopped producer logs progress. Only used in that
# mode; a bounded run still reports its metrics once at the end.
_HEARTBEAT_SECONDS = 30.0


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

    A ``duration_seconds`` of 0 or less runs indefinitely. Milestone 2 only
    ever needed bounded verification runs, but Milestone 5 runs this as a
    long-lived container: a bounded run under ``restart: unless-stopped`` is a
    restart loop, not a service.
    """
    tickers = tickers or TICKERS
    interval_seconds = config.TICK_INTERVAL_SECONDS if interval_seconds is None else interval_seconds
    prices = {t: 100.0 for t in tickers}
    prices.update(_load_last_closes())

    producer = _make_producer()
    started = time.perf_counter()
    published = 0
    try:
        run_forever = duration_seconds <= 0
        if run_forever:
            # A service that only reports at the end reports nothing at all.
            # Without this the container logged absolutely nothing while
            # running, which made "is it publishing?" unanswerable from
            # `docker logs` during Milestone 5 debugging.
            print(
                f"[tick_producer] publishing to {TOPIC} every {interval_seconds}s "
                f"for {len(tickers)} tickers (run-until-stopped)",
                flush=True,
            )
        last_heartbeat = started
        while run_forever or time.perf_counter() - started < duration_seconds:
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

            now = time.perf_counter()
            if run_forever and now - last_heartbeat >= _HEARTBEAT_SECONDS:
                elapsed = now - started
                print(
                    f"[tick_producer] published={published} "
                    f"elapsed={elapsed:.0f}s rate={published / elapsed:.2f}/s",
                    flush=True,
                )
                last_heartbeat = now

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
