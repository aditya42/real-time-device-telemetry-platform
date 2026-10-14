import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from telemetry_streaming.regression import RegressionThresholds, detect_regressions


def test_detects_failure_rate_regression():
    rows = [
        {"firmware_version": "2.2.0", "event_count": 100, "failure_rate": 0.02, "p95_latency_ms": 50.0},
        {"firmware_version": "2.3.0", "event_count": 100, "failure_rate": 0.12, "p95_latency_ms": 55.0},
    ]
    alerts = detect_regressions(rows, 0.03, 50.0, RegressionThresholds())
    assert any(a["firmware_version"] == "2.3.0" and a["metric"] == "failure_rate" for a in alerts)


def test_respects_minimum_sample_count():
    rows = [{"firmware_version": "2.3.0", "event_count": 10, "failure_rate": 0.9, "p95_latency_ms": 500.0}]
    assert detect_regressions(rows, 0.02, 50.0, RegressionThresholds(min_events=50)) == []
