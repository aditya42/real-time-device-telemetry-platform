# Resume and GitHub Positioning

## GitHub description

**Production-style IoT telemetry data platform simulating 10K devices with Kafka, Spark Structured Streaming, PostgreSQL, S3/Parquet, Athena, Grafana, regression detection, CI and Terraform.**

## Resume project entry

**Real-Time Device Telemetry Platform — Kafka, PySpark, PostgreSQL, S3, Athena, Docker, Terraform**

- Engineered an event-driven telemetry platform modeling a **10,000-device fleet**, routing versioned health, usage, and error events through partitioned Apache Kafka topics keyed by device ID.
- Built **Spark Structured Streaming** pipelines with event-time processing, watermarks, deduplication, validation, and micro-batch aggregations for firmware failure rate, connection failures, regional error rate, P95 latency, and device-health trends.
- Implemented a dual-serving architecture that writes low-latency operational metrics to **PostgreSQL** and partitioned **Parquet** datasets to an S3-compatible data lake for Athena-style historical analytics.
- Developed automated **firmware regression detection** that compares per-version failure/latency metrics against fleet baselines with minimum-sample thresholds and publishes severity-ranked alerts.
- Containerized the complete platform with **Docker Compose**, provisioned Grafana dashboards and a FastAPI metrics service, and added unit tests, smoke tests, CI, operational runbooks, and AWS Terraform scaffolding.

## Claims to add only after measuring

Do not invent throughput or latency numbers. Run the stack on your machine and record measured values before adding bullets such as:

- Sustained **X events/sec** end-to-end with **Y-second** micro-batch processing latency.
- Reduced analytical scan volume by **Z%** using Parquet and partition pruning versus unpartitioned JSON.

Use `make benchmark` only for generator/serialization throughput. For end-to-end throughput, measure Kafka production rate, Spark input rows/sec, batch duration, and consumer lag under a controlled load.

## Interview narrative

Frame the project around four design decisions:

1. **Why Kafka?** Durable decoupling and scalable partitioned ingestion for asynchronous device events.
2. **Why Spark streaming?** Stateful/event-time processing, deduplication, and scalable aggregations.
3. **Why Postgres + S3?** Separate low-latency operational serving from cheap historical analytics.
4. **How do you detect bad firmware?** Compare each version with fleet baselines after enough samples; alert on statistically meaningful engineering thresholds rather than isolated errors.
