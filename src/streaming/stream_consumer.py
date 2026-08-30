"""Spark Structured Streaming consumer: Kafka -> Delta.

Milestone 2 implements this: consume the ``market.ticks`` topic and append
micro-batches into the same Delta tables the batch pipeline writes, so batch history
and near-real-time updates land in one Lakehouse.

Placeholder only until Milestone 2.
"""

from __future__ import annotations

TOPIC = "market.ticks"


def run_consumer(*args, **kwargs):
    raise NotImplementedError("Milestone 2: streaming consumer not implemented yet")
