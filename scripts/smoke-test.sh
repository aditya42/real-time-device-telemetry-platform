#!/usr/bin/env bash
set -euo pipefail

API="${API_URL:-http://localhost:8080}"

echo "[1/5] Checking containers..."
docker compose ps

echo "[2/5] Checking Kafka topics..."
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list | grep -E 'telemetry\.(health|usage|error)'

echo "[3/5] Checking API health..."
curl -fsS "$API/health"
echo

echo "[4/5] Checking that Spark produced serving metrics..."
for attempt in {1..18}; do
  count=$(docker compose exec -T postgres psql -U "${POSTGRES_USER:-telemetry}" -d "${POSTGRES_DB:-telemetry}" -tAc "SELECT count(*) FROM firmware_metrics" | tr -d '[:space:]')
  if [[ "${count:-0}" -gt 0 ]]; then
    echo "firmware_metrics rows: $count"
    break
  fi
  if [[ "$attempt" == "18" ]]; then
    echo "No firmware metrics found. Inspect: docker compose logs spark-streaming" >&2
    exit 1
  fi
  sleep 5
done

echo "[5/5] Checking data lake objects..."

for attempt in {1..18}; do
  if docker compose run --rm -T --no-deps \
    --entrypoint python minio-init -c '
import os
import sys
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT_URL"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    region_name="us-east-1",
)

response = s3.list_objects_v2(
    Bucket=os.environ["S3_BUCKET"],
    Prefix="curated/telemetry/",
    MaxKeys=1000,
)

objects = response.get("Contents", [])
parquet_files = [
    obj["Key"] for obj in objects
    if obj["Key"].endswith(".parquet")
]

if not parquet_files:
    sys.exit(1)

print("Parquet object found:", parquet_files[0])
'; then
    echo "Data lake verification passed."
    break
  fi

  if [[ "$attempt" == "18" ]]; then
    echo "No Parquet objects found. Inspect Spark logs." >&2
    exit 1
  fi

  echo "Waiting for Parquet objects..."
  sleep 5
done

echo "Smoke test passed."
