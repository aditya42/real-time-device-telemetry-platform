CREATE TABLE IF NOT EXISTS firmware_metrics (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    firmware_version TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    failure_rate DOUBLE PRECISION NOT NULL,
    error_rate DOUBLE PRECISION NOT NULL,
    connection_failures BIGINT NOT NULL,
    p95_latency_ms DOUBLE PRECISION NOT NULL,
    avg_battery_level DOUBLE PRECISION NOT NULL,
    UNIQUE(batch_id, firmware_version)
);
CREATE INDEX IF NOT EXISTS idx_firmware_metrics_time ON firmware_metrics(observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_firmware_metrics_fw_time ON firmware_metrics(firmware_version, observed_at DESC);

CREATE TABLE IF NOT EXISTS region_metrics (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    region TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    error_rate DOUBLE PRECISION NOT NULL,
    connection_failures BIGINT NOT NULL,
    p95_latency_ms DOUBLE PRECISION NOT NULL,
    UNIQUE(batch_id, region)
);
CREATE INDEX IF NOT EXISTS idx_region_metrics_time ON region_metrics(observed_at DESC);

CREATE TABLE IF NOT EXISTS device_health_metrics (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    device_type TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    avg_battery_level DOUBLE PRECISION NOT NULL,
    connected_rate DOUBLE PRECISION NOT NULL,
    p95_latency_ms DOUBLE PRECISION NOT NULL,
    UNIQUE(batch_id, device_type)
);
CREATE INDEX IF NOT EXISTS idx_device_health_time ON device_health_metrics(observed_at DESC);

CREATE TABLE IF NOT EXISTS regression_alerts (
    id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    firmware_version TEXT NOT NULL,
    metric TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    baseline_value DOUBLE PRECISION NOT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('MEDIUM','HIGH')),
    UNIQUE(batch_id, firmware_version, metric)
);
CREATE INDEX IF NOT EXISTS idx_regression_alerts_time ON regression_alerts(observed_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_quality (
    batch_id BIGINT PRIMARY KEY,
    total_records BIGINT NOT NULL,
    valid_records BIGINT NOT NULL,
    rejected_records BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL
);
