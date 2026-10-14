#!/usr/bin/env python3
from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "generator"))
from telemetry_generator.generator import TelemetryGenerator


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=100_000)
    args = parser.parse_args()
    generator = TelemetryGenerator(device_count=10_000, seed=42)
    latencies = []
    started = time.perf_counter()
    for _ in range(args.events):
        event = generator.next_event()
        latencies.append(event.latency_ms)
        event.model_dump_json(by_alias=True)
    elapsed = time.perf_counter() - started
    throughput = args.events / elapsed
    print(f"events={args.events}")
    print(f"elapsed_seconds={elapsed:.3f}")
    print(f"generation_and_serialization_eps={throughput:,.0f}")
    print(f"sample_mean_latency_ms={statistics.mean(latencies):.2f}")
    print("Note: this benchmarks generator+serialization only, not end-to-end Kafka/Spark throughput.")


if __name__ == "__main__":
    main()
