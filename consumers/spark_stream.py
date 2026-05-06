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
import asyncio
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path

# Isolate pyspark from any system-wide Spark/Hadoop installs before importing.
# Homebrew's apache-spark + hadoop packages inject a different Spark version,
# a different Hadoop version, and an HDFS-defaulting core-site.xml into the
# classpath; any of those silently break the bundled pyspark runtime.
for _var in (
    "SPARK_HOME",
    "SPARK_CONF_DIR",
    "HADOOP_CONF_DIR",
    "PYSPARK_SUBMIT_ARGS",
):
    os.environ.pop(_var, None)

if os.name == "nt":
    _repo_root = Path(__file__).resolve().parents[1]
    _local_hadoop = _repo_root / "infra" / "hadoop"
    _winutils = _local_hadoop / "bin" / "winutils.exe"
    if _winutils.exists():
        os.environ.setdefault("HADOOP_HOME", str(_local_hadoop))
        os.environ["PATH"] = f"{_local_hadoop / 'bin'};{os.environ.get('PATH', '')}"

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from aiokafka import AIOKafkaProducer  # noqa: E402
from pyspark.sql.functions import (
    avg,
    col,
    concat,
    count,
    current_timestamp,
    date_format,
    exp,
    floor,
    from_json,
    hour,
    lit,
    max as spark_max,
    min as spark_min,
    regexp_replace,
    unix_timestamp,
    struct,
    split,
    to_json,
    upper,
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
DEFAULT_FORECAST_TOPIC = "btc.forecast.clean"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
RAPIDS_PACKAGE = "com.nvidia:rapids-4-spark_2.12:26.04.0"
DEFAULT_SIGMA_LOG = 0.0005

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


def build_session(
    app_name: str = "streaming-polymarket",
    *,
    master: str | None = None,
    use_rapids: bool = False,
    rapids_package: str = RAPIDS_PACKAGE,
    rapids_jar: str | None = None,
    rapids_explain: str = "NOT_ON_GPU",
) -> SparkSession:
    packages = [KAFKA_PACKAGE]
    if use_rapids and not rapids_jar:
        packages.append(rapids_package)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        # Binance bookTicker fields are single-letter case-sensitive (b/B bid
        # price/qty, a/A ask price/qty). Default case-insensitive resolution
        # causes AMBIGUOUS_REFERENCE_TO_FIELDS in from_json + col().
        .config("spark.sql.caseSensitive", "true")
        # Belt + suspenders: force local FS as the default for all Hadoop
        # operations in case a stray core-site.xml slips through the env
        # isolation above.
        .config("spark.hadoop.fs.defaultFS", "file:///")
    )
    if master:
        builder = builder.master(master)
    if use_rapids:
        builder = (
            builder
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
            .config("spark.rapids.sql.enabled", "true")
            .config("spark.rapids.sql.explain", rapids_explain)
            .config("spark.rapids.sql.metrics.level", "MODERATE")
            .config("spark.rapids.memory.pinnedPool.size", "1G")
            .config("spark.sql.files.maxPartitionBytes", "256m")
        )
        if rapids_jar:
            builder = builder.config("spark.jars", rapids_jar)
    return builder.getOrCreate()


def load_sigma_log(features_path: str | None) -> float:
    if not features_path:
        return DEFAULT_SIGMA_LOG
    path = Path(features_path)
    if not path.exists():
        print(
            f"Feature metadata not found: {path}; using fallback "
            f"sigma_log={DEFAULT_SIGMA_LOG:.6f}"
        )
        return DEFAULT_SIGMA_LOG
    try:
        meta = json.loads(path.read_text())
        sigma_log = float(meta["sigma_log"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        print(
            f"Could not read sigma_log from {path}: {type(exc).__name__}; "
            f"using fallback sigma_log={DEFAULT_SIGMA_LOG:.6f}"
        )
        return DEFAULT_SIGMA_LOG
    return sigma_log


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
    asset_col = (
        when(col("source") == "binance", upper(regexp_replace(col("ident"), "(?i)usdt$", "")))
        .otherwise(upper(split(col("market.slug"), "-").getItem(0)))
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
        .withColumn("asset", asset_col)
        .withColumn(
            "series",
            when(col("source") == "binance", upper(col("ident")))
            .otherwise(concat(col("asset"), lit(" "), col("market.outcome"))),
        )
        .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"))
        .withColumn("hour", hour(col("ts")))
        .select(
            "source", "type", "ident", "asset", "series", "ts",
            "price", "price_bucket",
            col("market.slug").alias("market_slug"),
            col("market.outcome").alias("market_outcome"),
            "date", "hour",
        )
    )


def forecast_stats(
    events: DataFrame, sigma_log: float, watermark: str, slug_prefix: str
) -> DataFrame:
    """Emit `{ts, btc_mid, poly_prob, drift, forecast, sigma_log}` per 1-second window.

    log_return = (2·poly_prob − 1) × σ_log
    forecast   = btc_mid × exp(log_return)

    Consumes the normalized `events` DataFrame so price extraction lives in
    exactly one place (extract_price_events). The two conditional aggregates
    avoid a proper stream-stream join between Binance and Polymarket.
    Seconds where only one of the two streams arrived produce partial output.
    Grafana can still draw BTC smoothly while probability/forecast fields show
    gaps during rediscovery or source outages.

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
        .withColumn("log_return", (col("poly_prob") * 2 - 1) * lit(sigma_log))
        .withColumn("forecast", col("btc_mid") * exp(col("log_return")))
        .withColumn("drift", col("forecast") - col("btc_mid"))
        # Keep `sigma` as an approximate price-space stddev for older dashboards.
        .withColumn("sigma", col("btc_mid") * lit(sigma_log))
        .withColumn("sigma_log", lit(sigma_log))
        .select(
            unix_timestamp(col("window.start")).cast("double").alias("ts"),
            col("btc_mid"),
            col("poly_prob"),
            col("log_return"),
            col("drift"),
            col("forecast"),
            col("sigma"),
            col("sigma_log"),
        )
    )


def _finite_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _timestamp_seconds(value: object) -> float | None:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value.strip():
        return None

    raw = value.strip()
    normalized = f"{raw[:-1]}+00:00" if raw.endswith("Z") else raw
    try:
        return datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return _finite_float(raw)


def _format_timestamp(value: object) -> str | None:
    seconds = _timestamp_seconds(value)
    if seconds is None:
        return None
    return (
        datetime.fromtimestamp(seconds, timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _row_to_dict(row: object) -> dict:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    return dict(row)


def fill_forecast_records(
    rows: list[object],
    sigma_log: float,
    poly_stale_after_s: float,
    poly_state: dict[str, float | None] | None = None,
) -> list[dict]:
    """Carry the last Polymarket value across BTC-only forecast rows.

    Grafana's Kafka datasource reconnects from fresh Kafka messages. If the
    latest messages are BTC-only rows with null Polymarket fields, panels show
    `No data` even though the producer and Spark are healthy. Filling here keeps
    the live topic self-contained while still expiring stale Polymarket after a
    bounded interval.
    """
    ordered = sorted(
        (_row_to_dict(row) for row in rows),
        key=lambda item: _timestamp_seconds(item.get("ts")) or 0.0,
    )
    if poly_state is None:
        poly_state = {}
    last_poly_prob = poly_state.get("prob")
    last_poly_ts = poly_state.get("ts")
    last_btc_mid = poly_state.get("btc_mid")
    last_btc_ts = poly_state.get("btc_ts")
    btc_stale_after_s = min(poly_stale_after_s, 30.0)
    records: list[dict] = []

    for item in ordered:
        ts_raw = item.get("ts")
        ts_seconds = _timestamp_seconds(ts_raw)
        ts = _format_timestamp(ts_raw)
        if ts_seconds is None or ts is None:
            continue

        btc_mid = _finite_float(item.get("btc_mid"))
        if btc_mid is not None:
            last_btc_mid = btc_mid
            last_btc_ts = ts_seconds
        elif (
            last_btc_mid is not None
            and last_btc_ts is not None
            and ts_seconds - last_btc_ts <= btc_stale_after_s
        ):
            btc_mid = last_btc_mid

        poly_prob = _finite_float(item.get("poly_prob"))
        if poly_prob is not None:
            last_poly_prob = poly_prob
            last_poly_ts = ts_seconds
        elif (
            last_poly_prob is not None
            and last_poly_ts is not None
            and ts_seconds - last_poly_ts <= poly_stale_after_s
        ):
            poly_prob = last_poly_prob

        log_return = None
        forecast = None
        drift = None
        if btc_mid is not None and poly_prob is not None:
            log_return = (poly_prob * 2 - 1) * sigma_log
            forecast = btc_mid * math.exp(log_return)
            drift = forecast - btc_mid

        records.append(
            {
                "ts": ts,
                "btc_mid": btc_mid,
                "poly_prob": poly_prob,
                "log_return": log_return,
                "drift": drift,
                "forecast": forecast,
                "sigma": btc_mid * sigma_log if btc_mid is not None else None,
                "sigma_log": sigma_log,
            }
        )

    poly_state["prob"] = last_poly_prob
    poly_state["ts"] = last_poly_ts
    poly_state["btc_mid"] = last_btc_mid
    poly_state["btc_ts"] = last_btc_ts
    return records


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
            col("asset"),
            col("series"),
            col("market_outcome"),
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
            col("asset"),
            col("series"),
            col("market_outcome"),
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
    forecast: DataFrame,
    bootstrap: str,
    checkpoint_path: str,
    topic: str,
    sigma_log: float,
    poly_stale_after_s: float,
):
    # Write every forecast record to partition 0 so the Grafana Kafka plugin —
    # which queries a single partition per target — sees every message without
    # having to fan out three queries. At 1 msg/s the topic doesn't need
    # parallelism.
    poly_state: dict[str, float | None] = {}

    async def publish_records(records: list[dict[str, object]]) -> None:
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            for record in records:
                payload = json.dumps(record, separators=(",", ":"), allow_nan=False).encode("utf-8")
                await producer.send_and_wait(topic, payload, partition=0)
        finally:
            await producer.stop()

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        del batch_id
        records = fill_forecast_records(
            batch.collect(), sigma_log, poly_stale_after_s, poly_state
        )
        if not records:
            return
        asyncio.run(publish_records(records))

    return (
        forecast.writeStream
        .foreachBatch(write_batch)
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
    spark = build_session(
        master=args.master,
        use_rapids=args.rapids,
        rapids_package=args.rapids_package,
        rapids_jar=args.rapids_jar,
        rapids_explain=args.rapids_explain,
    )
    spark.sparkContext.setLogLevel("WARN")
    sigma_log = args.sigma_log
    if sigma_log is None:
        sigma_log = load_sigma_log(args.features_path)

    raw = read_kafka(spark, args.kafka_bootstrap)
    parsed = parse_envelope(raw)
    events = extract_price_events(parsed)
    stats = windowed_stats(events, args.window, args.watermark)
    forecast = forecast_stats(
        events, sigma_log, args.forecast_watermark, args.forecast_slug_prefix
    )

    start_parquet_sink(events, args.output_path, args.checkpoint_path)
    start_kafka_stats_sink(stats, args.kafka_bootstrap, args.checkpoint_path)
    start_kafka_forecast_sink(
        forecast,
        args.kafka_bootstrap,
        args.checkpoint_path,
        args.forecast_topic,
        sigma_log,
        args.poly_stale_after,
    )
    if args.console:
        start_console_sink(stats)

    print(f"Spark UI: http://localhost:4040")
    print(f"Polymarket-implied forecast topic={args.forecast_topic} sigma_log={sigma_log:.8f}")
    if args.rapids:
        print("RAPIDS Accelerator requested: check Spark UI SQL plans for Gpu* operators")
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
    parser.add_argument(
        "--master",
        default=os.environ.get("SPARK_MASTER"),
        help="Spark master override, e.g. local[*] or spark://host:7077",
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
        "--forecast-topic",
        default=os.environ.get("FORECAST_TOPIC", DEFAULT_FORECAST_TOPIC),
        help="Kafka topic for the dashboard-ready BTC forecast stream. "
             "Use a clean topic to avoid stale duplicate points from older Spark runs.",
    )
    parser.add_argument(
        "--features-path",
        default=os.environ.get("FEATURES_PATH", "data/feature_list.json"),
        help="Feature metadata JSON used to load sigma_log for the BTC forecast topic",
    )
    parser.add_argument(
        "--sigma-log",
        type=float,
        default=None,
        help="Override 5-min BTC log-return stddev for Polymarket-implied forecast",
    )
    parser.add_argument(
        "--poly-stale-after",
        type=float,
        default=float(os.environ.get("POLY_STALE_AFTER_S", "1800")),
        help="Seconds to carry the last Polymarket P(up) across BTC-only rows before expiring it",
    )
    parser.add_argument("--rapids", action="store_true", help="Enable RAPIDS Accelerator for Spark")
    parser.add_argument(
        "--rapids-package",
        default=os.environ.get("RAPIDS_SPARK_PACKAGE", RAPIDS_PACKAGE),
        help="Maven coordinate for the RAPIDS Spark plugin when --rapids-jar is not used",
    )
    parser.add_argument(
        "--rapids-jar",
        default=os.environ.get("RAPIDS_SPARK_JAR"),
        help="Local RAPIDS Spark plugin jar path; useful for CUDA 13/classifier jars",
    )
    parser.add_argument(
        "--rapids-explain",
        default=os.environ.get("RAPIDS_EXPLAIN", "NOT_ON_GPU"),
        choices=["ALL", "NONE", "NOT_ON_GPU"],
        help="RAPIDS explain logging level for GPU/fallback plan diagnostics",
    )
    parser.add_argument("--console", action="store_true", help="Also print stats to console")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
