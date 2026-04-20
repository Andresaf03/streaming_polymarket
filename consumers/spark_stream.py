#!/usr/bin/env python3
"""
spark_stream.py — Spark Structured Streaming job.

Reads polymarket.events + binance.trades + binance.book from Kafka, parses
the canonical envelope into a unified price-event view, and writes:

  * Parquet     → <output>/ticks/                  (raw events, for training)
  * Kafka       → topic `stats.windowed`           (windowed aggregates, for Grafana)
  * Console     → windowed aggregates (if --console)

Aggregations group by (window, source, ident, price_bucket). `ident` is the
Binance symbol or the Polymarket asset_id; `price_bucket` bins the continuous
price so we report min/max/mean/variance per range (rubric requirement).

Usage:
    spark-stream
    spark-stream --console --window "1 minute" --watermark "30 seconds"
    spark-stream --kafka-bootstrap localhost:9092 \\
                 --output-path data --checkpoint-path data/checkpoints/spark
"""

from __future__ import annotations

import argparse
import os

# Isolate pyspark from any system-wide Spark/Hadoop installs before importing.
# Homebrew's apache-spark + hadoop packages inject a different Spark version,
# a different Hadoop version, and an HDFS-defaulting core-site.xml into the
# classpath; any of those silently break the bundled pyspark runtime.
for _var in (
    "SPARK_HOME",
    "SPARK_CONF_DIR",
    "HADOOP_HOME",
    "HADOOP_CONF_DIR",
    "PYSPARK_SUBMIT_ARGS",
):
    os.environ.pop(_var, None)

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    floor,
    from_json,
    hour,
    lit,
    max as spark_max,
    min as spark_min,
    struct,
    to_json,
    variance,
    when,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_TOPICS = ["polymarket.events", "binance.trades", "binance.book"]
STATS_TOPIC = "stats.windowed"
FORECAST_TOPIC = "btc.forecast"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# Union of flat payload fields across all producers. Nested bids/asks arrays
# from depth20/book are intentionally omitted — Phase 5 will handle full
# microstructure features; Phase 2 only needs prices for aggregation.
PAYLOAD_SCHEMA = StructType([
    # binance aggTrade
    StructField("p", StringType()),          # price (JS numbers come as strings)
    StructField("q", StringType()),          # quantity
    StructField("m", BooleanType()),         # buyer-is-maker → sell aggressor
    StructField("T", LongType()),            # trade time ms
    # binance bookTicker
    StructField("b", StringType()),          # best bid
    StructField("B", StringType()),          # bid qty
    StructField("a", StringType()),          # best ask
    StructField("A", StringType()),          # ask qty
    # polymarket flat fields (price_change / last_trade_price)
    StructField("price", StringType()),
    StructField("size", StringType()),
    StructField("side", StringType()),
    StructField("best_bid", StringType()),
    StructField("best_ask", StringType()),
    # polymarket book's flat last_trade_price
    StructField("last_trade_price", StringType()),
])

MARKET_SCHEMA = StructType([
    StructField("question", StringType()),
    StructField("slug", StringType()),
    StructField("outcome", StringType()),   # "Yes" | "No" for binary markets
])

ENVELOPE_SCHEMA = StructType([
    StructField("source", StringType()),
    StructField("type", StringType()),
    StructField("recv_ts", DoubleType()),
    StructField("symbol", StringType()),
    StructField("asset_id", StringType()),
    StructField("market", MARKET_SCHEMA),
    StructField("payload", PAYLOAD_SCHEMA),
])


def build_session(app_name: str = "streaming-polymarket") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.session.timeZone", "UTC")
        # Binance bookTicker fields are single-letter case-sensitive (b/B bid
        # price/qty, a/A ask price/qty). Default case-insensitive resolution
        # causes AMBIGUOUS_REFERENCE_TO_FIELDS in from_json + col().
        .config("spark.sql.caseSensitive", "true")
        # Belt + suspenders: force local FS as the default for all Hadoop
        # operations in case a stray core-site.xml slips through the env
        # isolation above.
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, bootstrap: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", ",".join(KAFKA_TOPICS))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_envelope(raw: DataFrame) -> DataFrame:
    """Kafka value bytes → flat columns. Bad rows become null envelopes and drop."""
    return (
        raw.select(
            col("topic"),
            from_json(col("value").cast("string"), ENVELOPE_SCHEMA).alias("env"),
        )
        .where(col("env").isNotNull())
        .select("topic", "env.*")
    )


def extract_price_events(parsed: DataFrame) -> DataFrame:
    """Normalize into (source, type, ident, ts, price, price_bucket, date, hour).

    `price` is mid for bookTicker, trade price for aggTrade/last_trade_price,
    level price for Polymarket price_change.  Rows without a usable price are
    dropped (depth20 snapshots, `book` without last_trade_price, etc.).
    """
    price_col = (
        when(col("type") == "aggTrade", col("payload.p").cast("double"))
        .when(
            col("type") == "bookTicker",
            (col("payload.b").cast("double") + col("payload.a").cast("double")) / 2,
        )
        .when(col("type") == "price_change", col("payload.price").cast("double"))
        .when(col("type") == "last_trade_price", col("payload.price").cast("double"))
        .when(col("type") == "book", col("payload.last_trade_price").cast("double"))
    )

    bucket_col = (
        when(col("source") == "binance", (floor(col("price") / 100) * 100).cast("double"))
        .when(col("source") == "polymarket", (floor(col("price") * 10) / 10).cast("double"))
    )

    return (
        parsed
        .withColumn("ts", col("recv_ts").cast("timestamp"))
        .withColumn(
            "ident",
            when(col("source") == "binance", col("symbol")).otherwise(col("asset_id")),
        )
        .withColumn("price", price_col)
        .filter(col("price").isNotNull() & (col("price") > 0))
        .withColumn("price_bucket", bucket_col)
        .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"))
        .withColumn("hour", hour(col("ts")))
        .select(
            "source", "type", "ident", "ts",
            "price", "price_bucket",
            col("market.slug").alias("market_slug"),
            col("market.outcome").alias("market_outcome"),
            "date", "hour",
        )
    )


def forecast_stats(
    events: DataFrame, sigma: float, watermark: str, slug_prefix: str
) -> DataFrame:
    """Emit `{ts, btc_mid, poly_prob, drift, forecast, sigma}` per 1-second window.

    forecast = btc_mid + (2·poly_prob − 1) × σ

    Consumes the normalized `events` DataFrame so price extraction lives in
    exactly one place (extract_price_events). The two conditional aggregates
    avoid a proper stream-stream join between Binance and Polymarket.
    Seconds where only one of the two streams arrived produce partial (null)
    output; we let the gap show rather than forward-fill, so the dashboard
    honestly reflects "no active BTC 5-min market right now" windows.

    Polymarket binary markets use different outcome labels by market family:
      btc-updown-*   →  ["Up",  "Down"]
      most others    →  ["Yes", "No"]
    Both are accepted so the same filter works across market types.
    """
    btc_price = when(
        (col("source") == "binance")
        & (col("ident") == "btcusdt")
        & (col("type") == "bookTicker"),
        col("price"),
    )
    poly_prob = when(
        (col("source") == "polymarket")
        & col("market_slug").startswith(slug_prefix)
        & col("market_outcome").isin("Up", "Yes"),
        col("price"),
    )

    return (
        events
        .withColumn("_btc", btc_price)
        .withColumn("_poly", poly_prob)
        .withWatermark("ts", watermark)
        .groupBy(window(col("ts"), "1 second"))
        .agg(
            avg("_btc").alias("btc_mid"),
            avg("_poly").alias("poly_prob"),
        )
        .withColumn("drift", (col("poly_prob") * 2 - 1) * lit(sigma))
        .withColumn("forecast", col("btc_mid") + col("drift"))
        .withColumn("sigma", lit(sigma))
        .select(
            col("window.start").alias("ts"),
            col("btc_mid"),
            col("poly_prob"),
            col("drift"),
            col("forecast"),
            col("sigma"),
        )
    )


def windowed_stats(
    events: DataFrame, window_duration: str, watermark: str
) -> DataFrame:
    return (
        events
        .withWatermark("ts", watermark)
        .groupBy(
            window(col("ts"), window_duration),
            col("source"),
            col("ident"),
            col("price_bucket"),
        )
        .agg(
            spark_min("price").alias("price_min"),
            spark_max("price").alias("price_max"),
            avg("price").alias("price_avg"),
            variance("price").alias("price_var"),
            count("*").alias("n"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("source"),
            col("ident"),
            col("price_bucket"),
            col("price_min"),
            col("price_max"),
            col("price_avg"),
            col("price_var"),
            col("n"),
        )
    )


def start_parquet_sink(events: DataFrame, output_path: str, checkpoint_path: str):
    return (
        events.writeStream
        .format("parquet")
        .option("path", os.path.join(output_path, "ticks"))
        .option("checkpointLocation", os.path.join(checkpoint_path, "ticks"))
        .partitionBy("source", "date", "hour")
        .outputMode("append")
        .start()
    )


def start_kafka_stats_sink(stats: DataFrame, bootstrap: str, checkpoint_path: str):
    stats_json = stats.select(to_json(struct("*")).alias("value"))
    return (
        stats_json.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", STATS_TOPIC)
        .option("checkpointLocation", os.path.join(checkpoint_path, "stats"))
        .outputMode("append")
        .start()
    )


def start_kafka_forecast_sink(
    forecast: DataFrame, bootstrap: str, checkpoint_path: str
):
    # Write every forecast record to partition 0 so the Grafana Kafka plugin —
    # which queries a single partition per target — sees every message without
    # having to fan out three queries. At 1 msg/s the topic doesn't need
    # parallelism.
    value = forecast.select(
        lit(0).alias("partition"),
        to_json(struct("*")).alias("value"),
    )
    return (
        value.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", FORECAST_TOPIC)
        .option("checkpointLocation", os.path.join(checkpoint_path, "forecast"))
        .outputMode("append")
        .start()
    )


def start_console_sink(stats: DataFrame):
    return (
        stats.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="15 seconds")
        .start()
    )


def run(args: argparse.Namespace) -> None:
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_kafka(spark, args.kafka_bootstrap)
    parsed = parse_envelope(raw)
    events = extract_price_events(parsed)
    stats = windowed_stats(events, args.window, args.watermark)
    forecast = forecast_stats(
        events, args.sigma, args.forecast_watermark, args.forecast_slug_prefix
    )

    start_parquet_sink(events, args.output_path, args.checkpoint_path)
    start_kafka_stats_sink(stats, args.kafka_bootstrap, args.checkpoint_path)
    start_kafka_forecast_sink(forecast, args.kafka_bootstrap, args.checkpoint_path)
    if args.console:
        start_console_sink(stats)

    print(f"Spark UI: http://localhost:4040")
    spark.streams.awaitAnyTermination()


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming job")
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    parser.add_argument(
        "--output-path",
        default=os.environ.get("OUTPUT_PATH", "data"),
        help="Base dir for Parquet output (writes to <path>/ticks/)",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=os.environ.get("CHECKPOINT_PATH", "data/checkpoints/spark"),
    )
    parser.add_argument("--window", default="1 minute", help="Tumbling window size for stats")
    parser.add_argument(
        "--watermark",
        default="30 seconds",
        help="Watermark for the stats aggregation (matches the 1-min window cadence)",
    )
    parser.add_argument(
        "--forecast-watermark",
        default="10 seconds",
        help="Watermark for the live forecast query (shorter because windows are 1s; "
             "trades late-data tolerance for dashboard freshness)",
    )
    parser.add_argument(
        "--forecast-slug-prefix",
        default="btc-updown-5m-",
        help="Polymarket slug prefix used to filter events for the forecast. "
             "Change to btc-updown-15m- for the 15-min variant, etc.",
    )
    parser.add_argument(
        "--sigma",
        type=float,
        default=50.0,
        help="5-min BTC return stddev in $ (hardcoded for Phase 3; replace with "
             "rolling estimate in Phase 5)",
    )
    parser.add_argument("--console", action="store_true", help="Also print stats to console")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
