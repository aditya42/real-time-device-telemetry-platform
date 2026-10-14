from __future__ import annotations

import io
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Iterable

import boto3
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from telemetry_streaming.regression import RegressionThresholds, detect_regressions

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("telemetry-stream")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/checkpoints/telemetry")
S3_BUCKET = os.getenv("S3_BUCKET", "telemetry-lake")

SCHEMA = StructType([
    StructField("schemaVersion", StringType(), False),
    StructField("eventId", StringType(), False),
    StructField("eventType", StringType(), False),
    StructField("deviceId", StringType(), False),
    StructField("firmwareVersion", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("batteryLevel", DoubleType(), True),
    StructField("latencyMs", DoubleType(), True),
    StructField("connectionState", StringType(), True),
    StructField("errorCode", StringType(), True),
    StructField("region", StringType(), True),
    StructField("deviceType", StringType(), True),
    StructField("signalStrengthDbm", IntegerType(), True),
    StructField("uptimeSeconds", IntegerType(), True),
    StructField("sessionId", StringType(), True),
])


def pg_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "telemetry"),
        user=os.getenv("POSTGRES_USER", "telemetry"),
        password=os.getenv("POSTGRES_PASSWORD", "telemetry"),
        connect_timeout=5,
    )


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def upload_partitioned_parquet(df: DataFrame, batch_id: int) -> None:
    if df.rdd.isEmpty():
        return
    pdf = df.select(
        "schemaVersion", "eventId", "eventType", "deviceId", "firmwareVersion", "eventTime",
        "batteryLevel", "latencyMs", "connectionState", "errorCode", "region", "deviceType",
        "signalStrengthDbm", "uptimeSeconds", "sessionId"
    ).toPandas()
    if pdf.empty:
        return
    pdf["event_date"] = pdf["eventTime"].dt.strftime("%Y-%m-%d")
    pdf["event_hour"] = pdf["eventTime"].dt.strftime("%H")
    client = s3_client()
    for (event_date, event_hour, region, event_type), part in pdf.groupby(
        ["event_date", "event_hour", "region", "eventType"], dropna=False
    ):
        key = (
            f"curated/telemetry/event_date={event_date}/event_hour={event_hour}/"
            f"region={region}/event_type={event_type}/batch-{batch_id:020d}.parquet"
        )
        payload = io.BytesIO()
        parquet_part = part.drop(columns=["event_date", "event_hour", "region", "eventType"])
        pq.write_table(pa.Table.from_pandas(parquet_part, preserve_index=False), payload, compression="snappy")
        payload.seek(0)
        client.put_object(Bucket=S3_BUCKET, Key=key, Body=payload.getvalue(), ContentType="application/octet-stream")


def rows_as_dicts(df: DataFrame) -> list[dict]:
    return [row.asDict(recursive=True) for row in df.collect()]


def upsert_metrics(cur, table: str, columns: list[str], rows: Iterable[tuple]) -> None:
    rows = list(rows)
    if not rows:
        return
    values = ", ".join(columns)
    template = "(" + ",".join(["%s"] * len(columns)) + ")"
    execute_values(cur, f"INSERT INTO {table} ({values}) VALUES %s ON CONFLICT DO NOTHING", rows, template=template)


def process_batch(batch: DataFrame, batch_id: int) -> None:
    if batch.rdd.isEmpty():
        return
    started = datetime.now(timezone.utc)
    batch = batch.cache()
    valid = batch.filter(
        F.col("eventId").isNotNull()
        & F.col("deviceId").isNotNull()
        & F.col("eventTime").isNotNull()
        & F.col("batteryLevel").between(0.0, 100.0)
        & (F.col("latencyMs") >= 0.0)
        & F.col("eventType").isin("health", "usage", "error")
    ).cache()
    total = batch.count()
    valid_count = valid.count()
    rejected = total - valid_count

    if valid_count == 0:
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_quality(batch_id,total_records,valid_records,rejected_records,processed_at) VALUES (%s,%s,0,%s,%s) ON CONFLICT (batch_id) DO NOTHING",
                (batch_id, total, rejected, started),
            )
        batch.unpersist()
        valid.unpersist()
        return

    failure = F.when((F.col("errorCode").isNotNull()) | (F.col("connectionState") == "DISCONNECTED"), 1).otherwise(0)
    error_flag = F.when(F.col("errorCode").isNotNull(), 1).otherwise(0)
    conn_fail = F.when(F.col("connectionState") == "DISCONNECTED", 1).otherwise(0)

    firmware_df = valid.groupBy("firmwareVersion").agg(
        F.count("*").alias("event_count"),
        F.avg(failure).alias("failure_rate"),
        F.avg(error_flag).alias("error_rate"),
        F.sum(conn_fail).alias("connection_failures"),
        F.expr("percentile_approx(latencyMs, 0.95, 10000)").alias("p95_latency_ms"),
        F.avg("batteryLevel").alias("avg_battery_level"),
    )
    region_df = valid.groupBy("region").agg(
        F.count("*").alias("event_count"),
        F.avg(error_flag).alias("error_rate"),
        F.sum(conn_fail).alias("connection_failures"),
        F.expr("percentile_approx(latencyMs, 0.95, 10000)").alias("p95_latency_ms"),
    )
    health_df = valid.groupBy("deviceType").agg(
        F.count("*").alias("event_count"),
        F.avg("batteryLevel").alias("avg_battery_level"),
        F.avg(F.when(F.col("connectionState") == "CONNECTED", 1).otherwise(0)).alias("connected_rate"),
        F.expr("percentile_approx(latencyMs, 0.95, 10000)").alias("p95_latency_ms"),
    )
    fleet_row = valid.agg(
        F.avg(failure).alias("failure_rate"),
        F.expr("percentile_approx(latencyMs, 0.95, 10000)").alias("p95_latency_ms"),
    ).first()

    firmware_rows = rows_as_dicts(firmware_df)
    region_rows = rows_as_dicts(region_df)
    health_rows = rows_as_dicts(health_df)
    thresholds = RegressionThresholds(
        min_events=int(os.getenv("REGRESSION_MIN_EVENTS", "50")),
        failure_multiplier=float(os.getenv("REGRESSION_FAILURE_MULTIPLIER", "2.0")),
        failure_abs_threshold=float(os.getenv("REGRESSION_FAILURE_ABS_THRESHOLD", "0.05")),
        latency_multiplier=float(os.getenv("REGRESSION_LATENCY_MULTIPLIER", "1.5")),
    )
    alerts = detect_regressions(
        firmware_rows,
        float(fleet_row["failure_rate"] or 0.0),
        float(fleet_row["p95_latency_ms"] or 0.0),
        thresholds,
    )

    observed_at = datetime.now(timezone.utc)
    with pg_connect() as conn, conn.cursor() as cur:
        upsert_metrics(cur, "firmware_metrics",
            ["batch_id","observed_at","firmware_version","event_count","failure_rate","error_rate","connection_failures","p95_latency_ms","avg_battery_level"],
            [(
                batch_id, observed_at, r["firmwareVersion"], r["event_count"], r["failure_rate"], r["error_rate"],
                r["connection_failures"], r["p95_latency_ms"], r["avg_battery_level"]
            ) for r in firmware_rows],
        )
        upsert_metrics(cur, "region_metrics",
            ["batch_id","observed_at","region","event_count","error_rate","connection_failures","p95_latency_ms"],
            [(batch_id, observed_at, r["region"], r["event_count"], r["error_rate"], r["connection_failures"], r["p95_latency_ms"]) for r in region_rows],
        )
        upsert_metrics(cur, "device_health_metrics",
            ["batch_id","observed_at","device_type","event_count","avg_battery_level","connected_rate","p95_latency_ms"],
            [(batch_id, observed_at, r["deviceType"], r["event_count"], r["avg_battery_level"], r["connected_rate"], r["p95_latency_ms"]) for r in health_rows],
        )
        for alert in alerts:
            cur.execute(
                """
                INSERT INTO regression_alerts
                  (batch_id, observed_at, firmware_version, metric, metric_value, baseline_value, threshold_value, severity)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (batch_id, firmware_version, metric) DO NOTHING
                """,
                (batch_id, observed_at, alert["firmware_version"], alert["metric"], alert["metric_value"],
                 alert["baseline_value"], alert["threshold_value"], alert["severity"]),
            )
        cur.execute(
            """
            INSERT INTO pipeline_quality(batch_id,total_records,valid_records,rejected_records,processed_at)
            VALUES (%s,%s,%s,%s,%s) ON CONFLICT (batch_id) DO NOTHING
            """,
            (batch_id, total, valid_count, rejected, observed_at),
        )

    try:
        upload_partitioned_parquet(valid, batch_id)
    except Exception:
        LOG.exception("S3 parquet upload failed for batch %s", batch_id)
        raise
    finally:
        valid.unpersist()
        batch.unpersist()

    LOG.info("batch=%s total=%s valid=%s rejected=%s alerts=%s", batch_id, total, valid_count, rejected, len(alerts))


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("real-time-device-telemetry-platform")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", "telemetry.health,telemetry.usage,telemetry.error")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        source.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json("json", SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("eventTime", F.to_timestamp("timestamp"))
        .withWatermark("eventTime", "2 minutes")
        .dropDuplicates(["eventId", "eventTime"])
    )

    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds"))
        .start()
    )
    LOG.info("Streaming query started")
    query.awaitTermination()


if __name__ == "__main__":
    main()
