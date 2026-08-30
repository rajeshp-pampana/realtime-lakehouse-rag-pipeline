"""Simulated intraday tick producer -> Kafka.

Milestone 2 implements this: generate synthetic intraday price events (a random-walk
around each ticker's last close) and publish them to a Kafka topic at a short
interval.

NOTE: this is a *simulated* event feed, not a paid real-time market data feed.

Placeholder only until Milestone 2.
"""

from __future__ import annotations

TOPIC = "market.ticks"


def run_producer(*args, **kwargs):
    raise NotImplementedError("Milestone 2: tick producer not implemented yet")
