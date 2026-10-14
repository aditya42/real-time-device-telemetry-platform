import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from telemetry_generator.generator import TelemetryGenerator


def test_generator_emits_valid_contract():
    generator = TelemetryGenerator(device_count=50, seed=7)
    event = generator.next_event()
    payload = event.model_dump(by_alias=True)
    required = {
        "schemaVersion", "eventId", "eventType", "deviceId", "firmwareVersion", "timestamp",
        "batteryLevel", "latencyMs", "connectionState", "errorCode", "region", "deviceType",
    }
    assert required.issubset(payload)
    assert 0 <= payload["batteryLevel"] <= 100
    assert payload["latencyMs"] >= 0


def test_regression_firmware_has_higher_latency_distribution():
    os.environ["REGRESSION_FIRMWARE_VERSION"] = "2.3.0"
    os.environ["REGRESSION_LATENCY_MULTIPLIER_GENERATOR"] = "2.0"
    generator = TelemetryGenerator(device_count=500, seed=42)
    samples = [generator.next_event() for _ in range(8000)]
    reg = [e.latency_ms for e in samples if e.firmware_version == "2.3.0"]
    stable = [e.latency_ms for e in samples if e.firmware_version != "2.3.0"]
    assert sum(reg) / len(reg) > (sum(stable) / len(stable)) * 1.5
