from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegressionThresholds:
    min_events: int = 50
    failure_multiplier: float = 2.0
    failure_abs_threshold: float = 0.05
    latency_multiplier: float = 1.5


def detect_regressions(rows: list[dict], fleet_failure_rate: float, fleet_p95_latency: float, thresholds: RegressionThresholds) -> list[dict]:
    alerts: list[dict] = []
    for row in rows:
        if int(row["event_count"]) < thresholds.min_events:
            continue
        firmware_version = row.get("firmware_version", row.get("firmwareVersion"))
        if not firmware_version:
            continue
        failure_rate = float(row["failure_rate"] or 0.0)
        p95 = float(row["p95_latency_ms"] or 0.0)
        failure_limit = max(thresholds.failure_abs_threshold, fleet_failure_rate * thresholds.failure_multiplier)
        if failure_rate >= failure_limit:
            alerts.append({
                "firmware_version": firmware_version,
                "metric": "failure_rate",
                "metric_value": failure_rate,
                "baseline_value": fleet_failure_rate,
                "threshold_value": failure_limit,
                "severity": "HIGH" if failure_rate >= failure_limit * 1.5 else "MEDIUM",
            })
        if fleet_p95_latency > 0 and p95 >= fleet_p95_latency * thresholds.latency_multiplier:
            limit = fleet_p95_latency * thresholds.latency_multiplier
            alerts.append({
                "firmware_version": firmware_version,
                "metric": "p95_latency_ms",
                "metric_value": p95,
                "baseline_value": fleet_p95_latency,
                "threshold_value": limit,
                "severity": "HIGH" if p95 >= limit * 1.4 else "MEDIUM",
            })
    return alerts
