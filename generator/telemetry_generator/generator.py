from __future__ import annotations

import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from .models import TelemetryEvent

REGIONS = ("us-east-1", "us-west-2", "eu-west-1", "ap-south-1")
DEVICE_TYPES = ("smart-speaker", "smart-display", "iot-accessory")
FIRMWARES = ("2.1.0", "2.2.0", "2.3.0")
ERROR_CODES = ("CONNECTION_TIMEOUT", "AUTH_FAILURE", "SENSOR_READ_ERROR", "SERVICE_UNAVAILABLE")


@dataclass(frozen=True)
class DeviceProfile:
    device_id: str
    firmware_version: str
    region: str
    device_type: str
    battery_bias: float
    latency_bias: float


class TelemetryGenerator:
    def __init__(self, device_count: int = 10_000, seed: int | None = None) -> None:
        self.random = random.Random(seed)
        self.regression_firmware = os.getenv("REGRESSION_FIRMWARE_VERSION", "2.3.0")
        self.error_multiplier = float(os.getenv("REGRESSION_ERROR_MULTIPLIER", "6.0"))
        self.latency_multiplier = float(os.getenv("REGRESSION_LATENCY_MULTIPLIER_GENERATOR", "1.8"))
        self.devices = self._build_devices(device_count)

    def _build_devices(self, count: int) -> list[DeviceProfile]:
        devices: list[DeviceProfile] = []
        for index in range(count):
            firmware = self.random.choices(FIRMWARES, weights=(0.35, 0.45, 0.20), k=1)[0]
            devices.append(
                DeviceProfile(
                    device_id=f"dev-{index:06d}",
                    firmware_version=firmware,
                    region=self.random.choice(REGIONS),
                    device_type=self.random.choice(DEVICE_TYPES),
                    battery_bias=self.random.uniform(-8.0, 8.0),
                    latency_bias=self.random.uniform(-8.0, 15.0),
                )
            )
        return devices

    def next_event(self) -> TelemetryEvent:
        d = self.random.choice(self.devices)
        event_type = self.random.choices(("health", "usage", "error"), weights=(0.76, 0.20, 0.04), k=1)[0]

        base_error_probability = 0.012
        error_probability = base_error_probability
        latency_multiplier = 1.0
        if d.firmware_version == self.regression_firmware:
            error_probability *= self.error_multiplier
            latency_multiplier = self.latency_multiplier

        has_error = event_type == "error" or self.random.random() < error_probability
        if has_error and event_type != "error" and self.random.random() < 0.55:
            event_type = "error"

        if has_error:
            connection_state = self.random.choices(
                ("DEGRADED", "DISCONNECTED", "CONNECTED"), weights=(0.45, 0.35, 0.20), k=1
            )[0]
            error_code = self.random.choice(ERROR_CODES)
        else:
            connection_state = self.random.choices(
                ("CONNECTED", "DEGRADED", "DISCONNECTED"), weights=(0.965, 0.03, 0.005), k=1
            )[0]
            error_code = None

        latency = max(1.0, self.random.lognormvariate(3.35, 0.38) + d.latency_bias)
        latency *= latency_multiplier
        if connection_state != "CONNECTED":
            latency *= self.random.uniform(1.4, 2.4)

        battery = max(0.0, min(100.0, self.random.gauss(72.0 + d.battery_bias, 17.0)))
        signal = int(max(-120, min(-20, self.random.gauss(-57 if connection_state == "CONNECTED" else -82, 11))))

        return TelemetryEvent(
            eventId=str(uuid.uuid4()),
            eventType=event_type,
            deviceId=d.device_id,
            firmwareVersion=d.firmware_version,
            timestamp=datetime.now(timezone.utc),
            batteryLevel=round(battery, 2),
            latencyMs=round(latency, 2),
            connectionState=connection_state,
            errorCode=error_code,
            region=d.region,
            deviceType=d.device_type,
            signalStrengthDbm=signal,
            uptimeSeconds=self.random.randint(10, 60 * 60 * 24 * 45),
            sessionId=str(uuid.uuid4()),
        )
