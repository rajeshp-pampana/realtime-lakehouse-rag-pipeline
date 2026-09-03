"""Streaming producer/consumer tests.

`test_run_producer_publishes_bounded_random_walk` needs no Kafka/Spark at all
(the KafkaProducer is monkeypatched) and runs everywhere, including Windows.

`test_batch_writer_appends_to_delta` exercises the real foreachBatch write path
against a real (tiny) Spark DataFrame and a real Delta table. Skipped on
Windows for the same documented reason as the Milestone 1 transform test;
runs for real in CI.
"""

from __future__ import annotations

import importlib
import platform

import pytest


def test_streaming_modules_import():
    producer = importlib.import_module("src.streaming.tick_producer")
    consumer = importlib.import_module("src.streaming.stream_consumer")
    assert hasattr(producer, "run_producer")
    assert hasattr(consumer, "run_consumer")


class _FakeKafkaProducer:
    """Records (topic, key, value) instead of talking to a real broker."""

    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def flush(self):
        pass

    def close(self):
        pass


def test_run_producer_publishes_bounded_random_walk(monkeypatch):
    from src.streaming import tick_producer

    fake = _FakeKafkaProducer()
    monkeypatch.setattr(tick_producer, "_make_producer", lambda: fake)
    monkeypatch.setattr(tick_producer, "_load_last_closes", lambda: {"AAA": 100.0, "BBB": 50.0})

    metrics = tick_producer.run_producer(
        tickers=["AAA", "BBB"], duration_seconds=0.05, interval_seconds=0.01
    )

    assert metrics["events_published"] == len(fake.sent)
    assert metrics["events_published"] >= 2  # at least one round of both tickers
    assert metrics["topic"] == tick_producer.TOPIC

    last_price = {"AAA": 100.0, "BBB": 50.0}
    for _topic, key, event in fake.sent:
        assert event["ticker"] == key
        assert set(event) == {"ticker", "price", "size", "event_time"}
        # each tick moves at most _MAX_TICK_MOVE from the ticker's prior price
        max_move = last_price[key] * tick_producer._MAX_TICK_MOVE
        assert abs(event["price"] - last_price[key]) <= max_move + 1e-9
        last_price[key] = event["price"]


pytestmark_spark = pytest.mark.skipif(
    platform.system() == "Windows",
    reason="PySpark local-mode worker startup is broken on this dev machine; verified in CI/Docker instead",
)


@pytestmark_spark
def test_batch_writer_appends_to_delta(tmp_path, monkeypatch):
    from deltalake import DeltaTable
    from pyspark.sql import SparkSession

    from src.streaming import stream_consumer
    from src.streaming.stream_consumer import _make_batch_writer

    # No real Kafka broker in CI - stub the lag check out entirely rather than
    # let it hit the network. A real connection attempt to a closed/absent
    # port can take much longer to fail than expected depending on the
    # runner's network stack, which isn't something a unit test should ever
    # depend on; the real lag check is exercised in the WSL2/Docker demo run
    # documented in docs/METRICS.md, not here.
    monkeypatch.setattr(stream_consumer, "_consumer_lag", lambda *a, **k: None)

    spark = (
        SparkSession.builder.appName("test-batch-writer")
        .master("local[1]")
        .config("spark.driver.host", "localhost")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        batch_df = spark.createDataFrame(
            [("AAA", 101.5, 10, "2026-09-01T10:00:00+00:00")],
            ["ticker", "price", "size", "event_time"],
        )
        delta_path = str(tmp_path / "ticks_raw")
        metrics: dict = {}
        # no real Kafka broker in this test - the lag check inside the writer
        # fails fast (non-fatal, returns None) and just doesn't add a sample
        write_batch = _make_batch_writer(delta_path, metrics, "localhost:9092", "market.ticks")

        write_batch(batch_df, 0)

        assert metrics["batches"] == 1
        assert metrics["rows"] == 1
        assert len(metrics["write_latencies_seconds"]) == 1
        assert metrics["write_latencies_seconds"][0] >= 0
        result = DeltaTable(delta_path).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["ticker"] == "AAA"

        # a second, empty batch must not write a new version or double-count
        empty_df = spark.createDataFrame([], batch_df.schema)
        write_batch(empty_df, 1)
        assert metrics["batches"] == 1
        assert metrics["rows"] == 1
    finally:
        spark.stop()
