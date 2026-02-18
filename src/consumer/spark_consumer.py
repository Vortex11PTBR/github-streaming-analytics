"""Spark Structured Streaming consumer for GitHub Events.

Reads from Kafka, performs windowed aggregations (1h, 24h) and writes
metric outputs to Parquet partitioned by date. Uses checkpointing for
fault-tolerance and supports basic commit-message sentiment analysis.

Run as a module:
    python -m src.consumer.spark_consumer

Requirements: `pyspark` installed in the environment.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count as _count,
    explode,
    expr,
    to_date,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
)

import logging
import structlog

from src.config import AppConfig
from src.tracing import (
    setup_tracer,
    kafka_headers_list_to_carrier,
)

cfg = AppConfig()
logger = structlog.get_logger()


def _init_logging() -> None:
    level = getattr(logging, cfg.log_level.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )


def _create_spark() -> SparkSession:
    builder = SparkSession.builder.appName(cfg.spark_app_name)
    if cfg.spark_master:
        builder = builder.master(cfg.spark_master)
    # reduce verbose logs
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _github_schema() -> StructType:
    # Minimal schema to parse necessary fields from GitHub Events payloads
    commit_schema = StructType(
        [
            StructField("sha", StringType(), True),
            StructField("message", StringType(), True),
            StructField(
                "author", StructType([StructField("name", StringType(), True)]), True
            ),
        ]
    )

    payload_schema = StructType(
        [
            StructField("commits", ArrayType(commit_schema), True),
            StructField("ref", StringType(), True),
        ]
    )

    actor_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("display_login", StringType(), True),
            StructField("url", StringType(), True),
        ]
    )
    repo_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True),
        ]
    )

    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("actor", actor_schema, True),
            StructField("repo", repo_schema, True),
            StructField("payload", payload_schema, True),
            StructField("created_at", StringType(), True),
        ]
    )


def read_from_kafka(spark: SparkSession) -> DataFrame:
    kafka_servers = ",".join(cfg.kafka_bootstrap_servers)
    # subscribe to topics with prefix
    topic_pattern = f"{cfg.kafka_topic_prefix}.*"
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribePattern", topic_pattern)
        .option("startingOffsets", "latest")
        .load()
    )

    # value is bytes -> cast to string
    # select headers too for trace propagation; headers format may vary by Spark version
    value_str = raw.selectExpr("CAST(value AS STRING) as value", "timestamp", "headers")
    schema = _github_schema()
    parsed = value_str.select(
        from_json(col("value"), schema).alias("event"), col("timestamp")
    )
    # flatten fields
    flattened = (
        parsed.select(
            col("event.id").alias("id"),
            col("event.type").alias("type"),
            col("event.actor.display_login").alias("actor_login"),
            col("event.repo.name").alias("repo_name"),
            col("event.payload.commits").alias("commits"),
            col("event.created_at").alias("created_at"),
            col("timestamp").alias("ingest_ts"),
            col("headers").alias("headers"),
        )
        .withColumn("event_ts", to_timestamp(col("created_at")))
        .withColumn("date", to_date(col("event_ts")))
    )
    return flattened


def _foreach_batch_tracing(df, epoch_id):
    # Setup tracer once
    try:
        setup_tracer(service_name="spark-consumer")
    except Exception:
        pass

    # Convert to pandas for simple per-row extraction (best-effort, small batches)
    try:
        pdf = df.select("id", "type", "repo_name", "event_ts", "headers").toPandas()
    except Exception:
        return

    from opentelemetry import propagate, trace

    tracer = trace.get_tracer(__name__)
    for _, row in pdf.iterrows():
        headers = row.get("headers")
        carrier = kafka_headers_list_to_carrier(headers)
        ctx = propagate.extract(carrier)
        with tracer.start_as_current_span("process.event", context=ctx) as span:
            span.set_attribute("event.id", str(row.get("id")))
            span.set_attribute("event.type", str(row.get("type")))
            span.set_attribute("repo", str(row.get("repo_name")))


def compute_metrics(stream_df: DataFrame, spark: SparkSession) -> None:
    # 1) Top 10 repos most active (events/hour) — window 1 hour
    events_by_repo_1h = stream_df.groupBy(
        window(col("event_ts"), "1 hour"), col("repo_name")
    ).agg(_count("id").alias("events_count"))

    out_repo_path_1h = f"{cfg.output_path}/events_by_repo_1h"
    query_repo_1h = (
        events_by_repo_1h.writeStream.outputMode("append")
        .option("checkpointLocation", f"{cfg.checkpoint_dir}/events_by_repo_1h")
        .trigger(processingTime=f"{cfg.consumer_poll_interval_seconds} seconds")
        .format("parquet")
        .option("path", out_repo_path_1h)
        .partitionBy("date")
        .start()
    )

    # Setup a lightweight foreachBatch stream that continues tracing contexts
    try:
        trace_stream = (
            stream_df.select("id", "type", "repo_name", "event_ts", "headers")
            .writeStream.foreachBatch(_foreach_batch_tracing)
            .outputMode("update")
            .option("checkpointLocation", f"{cfg.checkpoint_dir}/trace_foreach")
            .trigger(processingTime=f"{cfg.consumer_poll_interval_seconds} seconds")
            .start()
        )
    except Exception:
        trace_stream = None

    # 2) Event types distribution (1h and 24h)
    types_1h = stream_df.groupBy(window(col("event_ts"), "1 hour"), col("type")).agg(
        _count("id").alias("count")
    )
    types_24h = stream_df.groupBy(window(col("event_ts"), "24 hours"), col("type")).agg(
        _count("id").alias("count")
    )

    q_types_1h = (
        types_1h.writeStream.outputMode("append")
        .option("checkpointLocation", f"{cfg.checkpoint_dir}/types_1h")
        .trigger(processingTime=f"{cfg.consumer_poll_interval_seconds} seconds")
        .format("parquet")
        .option("path", f"{cfg.output_path}/types_1h")
        .partitionBy("date")
        .start()
    )

    q_types_24h = (
        types_24h.writeStream.outputMode("append")
        .option("checkpointLocation", f"{cfg.checkpoint_dir}/types_24h")
        .trigger(processingTime=f"{cfg.consumer_poll_interval_seconds} seconds")
        .format("parquet")
        .option("path", f"{cfg.output_path}/types_24h")
        .partitionBy("date")
        .start()
    )

    # 3) Languages trending (by commits) — best-effort: if commit messages exist,
    # attribute to repo (no language field is available in events by default)
    commits_exploded = stream_df.select(
        col("repo_name"), explode(col("commits")).alias("commit"), col("event_ts")
    )
    commits_with_msg = commits_exploded.select(
        col("repo_name"), col("commit.message").alias("message"), col("event_ts")
    )

    commits_1h = commits_with_msg.groupBy(
        window(col("event_ts"), "1 hour"), col("repo_name")
    ).agg(_count("message").alias("commits"))

    commits_1h_ckpt = f"{cfg.checkpoint_dir}/commits_1h"
    commits_1h_out = f"{cfg.output_path}/commits_1h"
    q_commits_1h = (
        commits_1h.writeStream.outputMode("append")
        .option("checkpointLocation", commits_1h_ckpt)
        .trigger(processingTime=f"{cfg.consumer_poll_interval_seconds} seconds")
        .format("parquet")
        .option("path", commits_1h_out)
        .partitionBy("date")
        .start()
    )

    # 4) Distribution by timezone — best-effort:
    # try extract timezone from actor_login (placeholder)
    tz_df = (
        stream_df.withColumn("timezone", expr("null"))
        .groupBy(window(col("event_ts"), "24 hours"), col("timezone"))
        .agg(_count("id").alias("count"))
    )

    q_tz = (
        tz_df.writeStream.outputMode("append")
        .option("checkpointLocation", f"{cfg.checkpoint_dir}/tz_24h")
        .trigger(processingTime=f"{cfg.consumer_poll_interval_seconds} seconds")
        .format("parquet")
        .option("path", f"{cfg.output_path}/tz_24h")
        .partitionBy("date")
        .start()
    )

    # 5) Commit message sentiment analysis (PushEvent commits)
    # Use a simple rule-based sentiment if vader is available.
    # Otherwise, fallback to neutral.
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        analyzer = SentimentIntensityAnalyzer()

        def sentiment_udf(message: str) -> str:
            if message is None:
                return "neutral"
            s = analyzer.polarity_scores(message)
            score = s.get("compound", 0.0)
            if score >= 0.05:
                return "positive"
            if score <= -0.05:
                return "negative"
            return "neutral"

        from pyspark.sql.functions import udf

        sentiment = udf(lambda m: sentiment_udf(m), StringType())
        commits_sent = commits_with_msg.withColumn(
            "sentiment", sentiment(col("message"))
        ).withColumn("date", to_date(col("event_ts")))

        sentiment_1h = commits_sent.groupBy(
            window(col("event_ts"), "1 hour"), col("sentiment")
        ).agg(_count("message").alias("count"))

        q_sent = (
            sentiment_1h.writeStream.outputMode("append")
            .option("checkpointLocation", f"{cfg.checkpoint_dir}/sentiment_1h")
            .trigger(processingTime=f"{cfg.consumer_poll_interval_seconds} seconds")
            .format("parquet")
            .option("path", f"{cfg.output_path}/sentiment_1h")
            .partitionBy("date")
            .start()
        )
    except Exception:
        logger.warning(
            "vader_not_available",
            note="Sentiment analysis disabled; vaderSentiment missing",
        )
        q_sent = None

    # Await termination for all queries
    queries = [
        q
        for q in [
            query_repo_1h,
            q_types_1h,
            q_types_24h,
            q_commits_1h,
            q_tz,
            q_sent,
            trace_stream,
        ]
        if q is not None
    ]
    for q in queries:
        q.awaitTermination()


def main() -> None:
    _init_logging()
    spark = _create_spark()
    df = read_from_kafka(spark)
    compute_metrics(df, spark)


if __name__ == "__main__":
    main()
