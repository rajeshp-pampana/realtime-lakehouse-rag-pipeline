"""Streaming producer/consumer tests. Real coverage lands in Milestone 2 / Milestone 6."""

import importlib


def test_streaming_modules_import():
    producer = importlib.import_module("src.streaming.tick_producer")
    consumer = importlib.import_module("src.streaming.stream_consumer")
    assert producer.TOPIC == consumer.TOPIC
