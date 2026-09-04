"""Spark Structured Streaming consumer: Kafka -> Delta.

Milestone 2 implements this: consume the ``market.ticks`` topic and append
micro-batches into a dedicated ``ticks_raw`` Delta table.

Design note on "the same Delta tables the batch job uses": ticks and the batch
DAG's daily OHLCV bars are different natural grains (event-level vs. one row
per ticker per day), so this writes a separate bronze table
(``data/delta/ticks_raw``) rather than unioning mismatched schemas into
``ohlcv_raw`` - the standard Lakehouse pattern (real trading/risk platforms keep
tick and bar tables separate, sometimes compacting ticks into bars downstream).
Both tables live in the same Lakehouse root and both are what "Done when: rows
land in the Delta table in near-real-time alongside the batch history" refers
to. See README.

Delta writes go through ``deltalake`` (delta-rs) inside ``foreachBatch``, not
Spark's Structured Streaming Delta sink - same reasoning as
``src/processing/transform_spark.py``: avoids the JVM ``delta-spark``
connector / Hadoop-on-Windows dependency while Spark still does the actual
streaming compute (Kafka consumption, JSON parsing, micro-batching).
"""

from __future__ import annotations

import argparse
import statistics
import time
from datetime import UTC, datetime

from src import config

_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def _consumer_lag(bootstrap_servers: str, topic: str, consumed_total: int) -> int | None:
    """Approximate Kafka consumer lag: (sum of each partition's latest offset)
    minus rows landed in Delta so far. Spark's Kafka source tracks offsets in
    its own checkpoint, not a committed Kafka consumer group, so there is no
    group for ``kafka-consumer-groups.sh --describe`` to report on - this
    queries the topic's own end offsets directly instead (metadata only, does
    not consume). Returns ``None`` if the topic isn't reachable yet.
    """
    try:
        from kafka import KafkaConsumer

        # api_version set explicitly to skip the auto-negotiation round trip;
        # short timeouts so this never hangs when no broker is reachable
        # (e.g. the CI unit test for the batch writer, which has no Kafka).
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            api_version=(3, 0, 0),
            api_version_auto_timeout_ms=2000,
            request_timeout_ms=3000,
            connections_max_idle_ms=10000,  # must be > request_timeout_ms
        )
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.close()
            return None
        from kafka import TopicPartition

        tps = [TopicPartition(topic, p) for p in partitions]
        end_offsets = consumer.end_offsets(tps)
        consumer.close()
        latest_total = sum(end_offsets.values())
        return max(0, latest_total - consumed_total)
    except Exception as exc:  # noqa: BLE001
        print(f"[stream_consumer] lag check failed (non-fatal): {exc}")
        return None


def _spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("tick-stream-consumer")
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", _KAFKA_PACKAGE)
        .getOrCreate()
    )


def _tick_schema():
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("ticker", StringType()),
            StructField("price", DoubleType()),
            StructField("size", LongType()),
            StructField("event_time", StringType()),
        ]
    )


def _make_batch_writer(delta_table_path: str, metrics: dict, bootstrap_servers: str, topic: str):
    import pandas as pd
    from deltalake import write_deltalake

    metrics.setdefault("write_latencies_seconds", [])
    metrics.setdefault("lag_samples", [])

    def _write_batch(batch_df, batch_id: int) -> None:
        pdf = batch_df.toPandas()
        if pdf.empty:
            return

        write_deltalake(delta_table_path, pdf, mode="append")
        committed_at = datetime.now(UTC)

        # Streaming-to-Delta write latency: wall-clock from each event's own
        # event_time (set by the producer) to the moment this batch's Delta
        # commit completed. Every row gets its own sample.
        event_times = pd.to_datetime(pdf["event_time"], utc=True)
        latencies = [(committed_at - t.to_pydatetime()).total_seconds() for t in event_times]
        metrics["write_latencies_seconds"].extend(latencies)

        metrics["batches"] = metrics.get("batches", 0) + 1
        metrics["rows"] = metrics.get("rows", 0) + len(pdf)

        lag = _consumer_lag(bootstrap_servers, topic, metrics["rows"])
        if lag is not None:
            metrics["lag_samples"].append(lag)

        print(
            f"[stream_consumer] batch {batch_id}: +{len(pdf)} rows (total {metrics['rows']}), "
            f"write latency avg={statistics.mean(latencies):.2f}s, lag={lag}"
        )

        # Push after EVERY micro-batch, not once at the end. In the
        # run-until-stopped mode the container uses, "the end" never arrives,
        # so end-of-run reporting would leave the consumer-lag and throughput
        # panels permanently empty - exactly the metrics that matter live.
        _push_batch_metrics(
            {
                "rlrp_stream_rows_total": metrics["rows"],
                "rlrp_stream_batches_total": metrics["batches"],
                "rlrp_stream_write_latency_avg_seconds": statistics.mean(latencies),
                "rlrp_stream_last_batch_rows": len(pdf),
                **({"rlrp_stream_consumer_lag": lag} if lag is not None else {}),
            }
        )

    return _write_batch


def _push_batch_metrics(values: dict) -> None:
    """Best-effort push of live streaming metrics.

    The Spark driver serves no HTTP, so nothing can scrape it; it pushes.
    Failures are swallowed - a metrics backend being down must never kill a
    streaming query.
    """
    try:
        from src.observability.metrics import push_job_metrics

        push_job_metrics("rlrp_stream_consumer", values)
    except Exception:  # noqa: BLE001
        pass


def run_consumer(
    bootstrap_servers: str | None = None,
    topic: str | None = None,
    delta_table_path: str | None = None,
    checkpoint_dir: str | None = None,
    timeout_seconds: float = 60.0,
) -> dict:
    """Consume ``topic`` from Kafka and append micro-batches into the ticks
    Delta table for up to ``timeout_seconds``, then stop and return metrics.

    A ``timeout_seconds`` of 0 or less runs until terminated. Milestone 2 only
    needed bounded verification runs; Milestone 5 runs this as a long-lived
    container, where a bounded run under ``restart: unless-stopped`` would be a
    restart loop rather than a service.
    """
    from pyspark.sql import functions as F

    bootstrap_servers = bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS
    topic = topic or config.KAFKA_TICKS_TOPIC
    delta_table_path = delta_table_path or config.DELTA_TICKS_RAW
    checkpoint_dir = checkpoint_dir or config.STREAM_CHECKPOINT_DIR

    spark = _spark_session()
    metrics: dict = {"batches": 0, "rows": 0}
    started = time.perf_counter()
    try:
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        parsed = raw.select(
            F.from_json(F.col("value").cast("string"), _tick_schema()).alias("event"),
            F.col("timestamp").alias("kafka_timestamp"),
        ).select("event.*", "kafka_timestamp")

        query = (
            parsed.writeStream.foreachBatch(
                _make_batch_writer(delta_table_path, metrics, bootstrap_servers, topic)
            )
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime="2 seconds")
            .start()
        )
        if timeout_seconds <= 0:
            query.awaitTermination()
        else:
            query.awaitTermination(timeout=timeout_seconds)
        query.stop()
    finally:
        spark.stop()

    metrics["duration_seconds"] = round(time.perf_counter() - started, 2)
    metrics["table_path"] = delta_table_path
    metrics["events_per_second"] = (
        round(metrics["rows"] / metrics["duration_seconds"], 2) if metrics["duration_seconds"] > 0 else 0.0
    )

    latencies = metrics.pop("write_latencies_seconds", [])
    if latencies:
        latencies_sorted = sorted(latencies)
        metrics["write_latency_avg_seconds"] = round(statistics.mean(latencies), 2)
        metrics["write_latency_p50_seconds"] = round(latencies_sorted[len(latencies_sorted) // 2], 2)
        metrics["write_latency_max_seconds"] = round(max(latencies), 2)

    lag_samples = metrics.pop("lag_samples", [])
    if lag_samples:
        metrics["consumer_lag_max"] = max(lag_samples)
        metrics["consumer_lag_final"] = lag_samples[-1]

    print(f"[stream_consumer] final: {metrics}")
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=60.0, help="seconds to consume for")
    args = parser.parse_args()
    run_consumer(timeout_seconds=args.timeout)
