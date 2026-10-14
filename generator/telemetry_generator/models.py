from __future__ import annotations

from datetime import datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class TelemetryEvent(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_version: str = Field(default="1.0", alias="schemaVersion")
    event_id: str = Field(alias="eventId")
    event_type: Literal["health", "usage", "error"] = Field(alias="eventType")
    device_id: str = Field(alias="deviceId")
    firmware_version: str = Field(alias="firmwareVersion")
    timestamp: datetime
    battery_level: float = Field(ge=0.0, le=100.0, alias="batteryLevel")
    latency_ms: float = Field(ge=0.0, alias="latencyMs")
    connection_state: Literal["CONNECTED", "DEGRADED", "DISCONNECTED"] = Field(alias="connectionState")
    error_code: str | None = Field(default=None, alias="errorCode")
    region: str
    device_type: str = Field(alias="deviceType")
    signal_strength_dbm: int = Field(ge=-120, le=-20, alias="signalStrengthDbm")
    uptime_seconds: int = Field(ge=0, alias="uptimeSeconds")
    session_id: str = Field(alias="sessionId")
