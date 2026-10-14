# Architecture

## Goals

1. Model a realistic connected-device telemetry domain at 10K-device fleet scale.
2. Support near-real-time operational analytics while retaining historical events in an analytical lake.
3. Demonstrate data correctness concerns: duplicates, malformed events, late events, retries, schema evolution, and idempotent sinks.
4. Make the entire platform runnable on a developer laptop.

## Data flow

1. **Telemetry Generator** maintains profiles for 10,000 devices and generates health, usage, and error events. Firmware `2.3.0` can be configured to simulate a bad rollout.
2. **Kafka** uses three topics with six partitions each. `deviceId` is the record key, preserving per-device partition ordering.
3. **Spark Structured Streaming** consumes all topics, parses the versioned JSON contract, applies event-time semantics, a two-minute watermark, deduplication, quality validation, and micro-batch aggregation.
4. **PostgreSQL** stores time-series aggregate snapshots and regression alerts for low-latency consumption.
5. **S3/MinIO** stores curated Parquet partitioned by event date, hour, region, and event type.
6. **Metrics API + Grafana** expose operational metrics and visualizations.
7. **Athena/Glue** provide the production-cloud analytical path over Parquet.

## Reliability and correctness

### Delivery semantics

Kafka producer idempotence and `acks=all` reduce duplicate production. Spark checkpointing tracks consumed offsets, and the local Compose stack persists checkpoints in a named Docker volume so restarts retain streaming progress. Because end-to-end exactly-once delivery across arbitrary external sinks is non-trivial, sinks are designed to be retry-friendly instead of claiming global exactly-once semantics.

PostgreSQL uses deterministic `(batch_id, dimension)` unique keys and `ON CONFLICT DO NOTHING`. S3 object names include the Spark batch ID and partition dimensions, so a retry overwrites the same deterministic key rather than multiplying objects.

### Late and duplicate data

`eventTime` is parsed from the event payload. Spark applies a two-minute watermark and deduplicates `(eventId, eventTime)` before business metrics are generated.

### Schema strategy

`schemaVersion` is mandatory. Version 1.0 is backward-compatible when optional fields are added. Removing/renaming fields or changing their types requires a new major schema version and a dual-reader migration period.

### Backpressure

The Kafka producer uses bounded buffering and polls when the queue is full. Spark micro-batches absorb short bursts. In production, monitor Kafka consumer lag and scale partitions/executors based on sustained lag and processing duration.

## Production extensions

- Schema Registry with Avro/Protobuf compatibility enforcement
- Kafka DLQ for invalid/unparseable records
- MSK Serverless + IAM/SASL auth
- EMR Serverless/Glue Streaming autoscaling
- Aurora PostgreSQL or Timestream for serving
- S3 lifecycle tiers and Iceberg table format
- Glue crawler/catalog partition projection
- CloudWatch/OpenTelemetry metrics and alarms
- Great Expectations/Deequ quality suites
