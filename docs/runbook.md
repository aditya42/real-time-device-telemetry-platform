# Operations Runbook

## No metrics in Grafana

1. `docker compose ps` — verify Kafka, PostgreSQL, API, generator, and Spark are running.
2. `docker compose logs --tail=200 generator` — confirm events are being produced.
3. `docker compose logs --tail=200 spark-streaming` — look for Kafka package download, parsing, PostgreSQL, or S3 errors.
4. `docker compose exec postgres psql -U telemetry -d telemetry -c 'select count(*) from firmware_metrics;'`

## Spark repeatedly restarts

The first start downloads the Spark Kafka connector from Maven repositories. Confirm Docker has internet access. If `spark-submit` reports a package-resolution error, retry after connectivity is restored.

## Kafka consumer lag is growing

- Lower `EVENTS_PER_SECOND` to establish whether the bottleneck is sustained load.
- Increase Kafka partitions and Spark shuffle partitions.
- Increase Spark executor resources in a multi-node deployment.
- Inspect batch duration in Spark logs; if S3 upload dominates, decouple lake persistence from serving aggregation or move to native S3A in production.

## PostgreSQL grows too large

The serving layer stores aggregate snapshots, not raw events. Add retention with a scheduled job, for example 30–90 days, while retaining historical raw/curated telemetry in S3.

## MinIO is healthy but no Parquet appears

Check Spark logs for `S3 parquet upload failed`. Verify `S3_ENDPOINT_URL`, access key, secret, and bucket. Run `make smoke` after at least one 10-second Spark micro-batch has completed.

## Reset the environment

```bash
make clean
cp .env.example .env
make up
```

`make clean` deletes Docker volumes and therefore local PostgreSQL and MinIO data.
