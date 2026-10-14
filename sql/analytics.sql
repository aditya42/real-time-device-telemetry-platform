-- Latest failure rate and P95 latency by firmware.
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY firmware_version ORDER BY observed_at DESC) AS rn
  FROM firmware_metrics
)
SELECT firmware_version, event_count, round(failure_rate::numeric, 4) AS failure_rate,
       round(p95_latency_ms::numeric, 2) AS p95_latency_ms
FROM ranked WHERE rn = 1 ORDER BY failure_rate DESC;

-- Error rate by region over the last 15 minutes.
SELECT region,
       sum(event_count) AS events,
       round((sum(error_rate * event_count) / NULLIF(sum(event_count),0))::numeric, 4) AS weighted_error_rate,
       sum(connection_failures) AS connection_failures
FROM region_metrics
WHERE observed_at >= now() - interval '15 minutes'
GROUP BY region
ORDER BY weighted_error_rate DESC;

-- Device health trend.
SELECT date_trunc('minute', observed_at) AS minute, device_type,
       avg(avg_battery_level) AS avg_battery,
       avg(connected_rate) AS connected_rate,
       avg(p95_latency_ms) AS p95_latency_ms
FROM device_health_metrics
WHERE observed_at >= now() - interval '1 hour'
GROUP BY 1,2 ORDER BY 1,2;

-- Recent firmware regressions.
SELECT observed_at, firmware_version, metric, metric_value, baseline_value,
       threshold_value, severity
FROM regression_alerts
ORDER BY observed_at DESC
LIMIT 100;
