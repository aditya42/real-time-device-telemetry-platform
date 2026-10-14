# Telemetry Event Data Contract

## Version 1.0

| Field | Type | Required | Rule |
|---|---|---:|---|
| `schemaVersion` | string | yes | `1.0` |
| `eventId` | UUID string | yes | unique event identifier |
| `eventType` | enum | yes | `health`, `usage`, `error` |
| `deviceId` | string | yes | Kafka key; stable device identifier |
| `firmwareVersion` | string | yes | semantic version-like string |
| `timestamp` | ISO-8601 UTC | yes | source event time |
| `batteryLevel` | double | yes | 0–100 |
| `latencyMs` | double | yes | >= 0 |
| `connectionState` | enum | yes | `CONNECTED`, `DEGRADED`, `DISCONNECTED` |
| `errorCode` | string/null | no | populated on failures |
| `region` | string | yes | simulated AWS-style region |
| `deviceType` | string | yes | device family |
| `signalStrengthDbm` | integer | yes | -120 to -20 |
| `uptimeSeconds` | integer | yes | >= 0 |
| `sessionId` | UUID string | yes | session correlation identifier |

## Compatibility policy

- Adding an optional field with a safe default is backward compatible.
- Changing a field type, semantic meaning, or requiredness is breaking.
- Breaking changes increment the major schema version.
- Producers should dual-write during breaking migrations; consumers should support both versions during the migration window.

## Data quality checks

Spark rejects records from business metrics when required identifiers/event time are missing, battery is outside 0–100, latency is negative, or event type is unknown. Rejected counts are written to `pipeline_quality` for operational monitoring.
