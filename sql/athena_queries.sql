-- Create the external table after copying the curated/ prefix to your AWS S3 bucket.
-- Replace ${BUCKET} before execution.
CREATE EXTERNAL TABLE IF NOT EXISTS telemetry_analytics.device_telemetry (
  schemaVersion string,
  eventId string,
  deviceId string,
  firmwareVersion string,
  eventTime timestamp,
  batteryLevel double,
  latencyMs double,
  connectionState string,
  errorCode string,
  signalStrengthDbm int,
  uptimeSeconds int,
  sessionId string
)
PARTITIONED BY (
  event_date string,
  event_hour string,
  region string,
  event_type string
)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/curated/telemetry/';

MSCK REPAIR TABLE telemetry_analytics.device_telemetry;

-- Failure rate by firmware.
SELECT firmwareVersion,
       count(*) AS events,
       avg(CASE WHEN errorCode IS NOT NULL OR connectionState='DISCONNECTED' THEN 1.0 ELSE 0.0 END) AS failure_rate,
       approx_percentile(latencyMs, 0.95) AS p95_latency_ms
FROM telemetry_analytics.device_telemetry
WHERE event_date >= date_format(current_date - interval '7' day, '%Y-%m-%d')
GROUP BY firmwareVersion
ORDER BY failure_rate DESC;

-- Error rate by region.
SELECT region, count(*) AS events,
       avg(CASE WHEN errorCode IS NOT NULL THEN 1.0 ELSE 0.0 END) AS error_rate
FROM telemetry_analytics.device_telemetry
GROUP BY region ORDER BY error_rate DESC;
