"""Wait for S3-compatible service and create an idempotent bucket."""
import os
import time
import boto3
from botocore.config import Config

client = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT_URL"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    region_name="us-east-1",
    config=Config(signature_version="s3v4", retries={"max_attempts": 2},
                  connect_timeout=3, read_timeout=3),
)
bucket = os.environ["S3_BUCKET"]
for attempt in range(60):
    try:
        client.list_buckets()
        try:
            client.head_bucket(Bucket=bucket)
        except client.exceptions.ClientError as exc:
            if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 404:
                raise
            client.create_bucket(Bucket=bucket)
        print(f"S3 bucket ready: {bucket}", flush=True)
        break
    except Exception as exc:
        if attempt == 59:
            raise
        print(f"Waiting for object storage ({attempt+1}/60): {exc}", flush=True)
        time.sleep(3)
