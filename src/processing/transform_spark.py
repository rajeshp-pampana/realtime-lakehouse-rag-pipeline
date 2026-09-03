"""PySpark batch transforms (replaces the baseline pandas rolling-mean logic).

Milestone 1 implements this: read the raw OHLCV Delta table, compute technical
indicators (20/50-day SMA, daily return, 20-day return volatility) per ticker in
PySpark local mode, and overwrite a curated Delta table with an enforced schema.

Delta I/O goes through ``deltalake`` (delta-rs) rather than Spark's own Delta
connector: on Windows the JVM ``delta-spark`` connector needs a version-matched
JAR plus ``winutils.exe``/``hadoop.dll`` on ``HADOOP_HOME``, which is a well-known
source of breakage. Using delta-rs for the table boundary and PySpark purely for
the distributed computation keeps genuine Spark compute and genuine Delta
versioning/schema-enforcement/time-travel, without the fragile Windows Hadoop
setup. (Plain PySpark *local mode* startup on Windows still needs a minimal
``winutils.exe`` — see README "Running Milestone 1 locally".)
"""

from __future__ import annotations

import time

from deltalake import DeltaTable, write_deltalake

from src import config


def _spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("market-transform-local")
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def run_transform(
    raw_table_path: str | None = None, curated_table_path: str | None = None
) -> dict:
    """Read the raw Delta table, compute indicators in Spark, write the curated
    Delta table. Returns a metrics dict (rows in/out, duration, resulting version).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    raw_table_path = raw_table_path or config.DELTA_OHLCV_RAW
    curated_table_path = curated_table_path or config.DELTA_OHLCV_CURATED
    started = time.perf_counter()

    raw_pdf = DeltaTable(raw_table_path).to_pandas()
    rows_in = len(raw_pdf)
    if rows_in == 0:
        raise RuntimeError(f"Raw Delta table at {raw_table_path} is empty; nothing to transform")

    spark = _spark_session()
    try:
        sdf = spark.createDataFrame(raw_pdf)

        by_ticker_date = Window.partitionBy("Ticker").orderBy("Date")
        sma20_window = by_ticker_date.rowsBetween(-19, 0)
        sma50_window = by_ticker_date.rowsBetween(-49, 0)
        vol20_window = by_ticker_date.rowsBetween(-19, 0)

        curated = (
            sdf.withColumn("SMA_20", F.avg("Close").over(sma20_window))
            .withColumn("SMA_50", F.avg("Close").over(sma50_window))
            .withColumn("prev_close", F.lag("Close").over(by_ticker_date))
            .withColumn(
                "daily_return",
                F.when(
                    F.col("prev_close").isNotNull(),
                    (F.col("Close") - F.col("prev_close")) / F.col("prev_close"),
                ),
            )
            .withColumn("volatility_20d", F.stddev("daily_return").over(vol20_window))
            .drop("prev_close")
            .orderBy("Ticker", "Date")
        )

        curated_pdf = curated.toPandas()
    finally:
        spark.stop()

    write_deltalake(curated_table_path, curated_pdf, mode="overwrite")
    table_version = DeltaTable(curated_table_path).version()

    metrics = {
        "rows_in": int(rows_in),
        "rows_out": int(len(curated_pdf)),
        "duration_seconds": round(time.perf_counter() - started, 2),
        "table_path": curated_table_path,
        "table_version": table_version,
    }
    print(f"[transform] {metrics}")
    return metrics


if __name__ == "__main__":
    run_transform()
