# AWS Production Mapping

The repository runs locally with Kafka, PostgreSQL, and MinIO. A production AWS deployment maps components as follows:

| Local | AWS production option |
|---|---|
| Kafka container | Amazon MSK / MSK Serverless |
| Spark container | EMR Serverless, AWS Glue Streaming, or EMR on EKS |
| PostgreSQL | Amazon RDS/Aurora PostgreSQL |
| MinIO | Amazon S3 |
| Local SQL files | AWS Glue Data Catalog + Athena |
| Grafana | Amazon Managed Grafana |
| Docker Compose | ECS/EKS + IaC pipeline |

## Security controls

- Private subnets for MSK, compute, and RDS
- IAM least-privilege service roles
- TLS in transit and KMS encryption at rest
- Secrets Manager for database credentials
- S3 Block Public Access + bucket policies
- CloudTrail and CloudWatch audit/operational logs
- VPC endpoints for S3/Glue/CloudWatch where appropriate

## Data lake layout

```text
s3://<bucket>/curated/telemetry/
  event_date=YYYY-MM-DD/
    event_hour=HH/
      region=<region>/
        event_type=<type>/
          batch-*.parquet
```

This layout enables partition pruning for time/region/type filters. At larger scale, migrate to Apache Iceberg to support schema evolution, compaction, partition evolution, and transactional table maintenance.
