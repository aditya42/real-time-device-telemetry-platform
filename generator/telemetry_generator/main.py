from __future__ import annotations

import json
import logging
import os
import signal
import time
from collections import Counter
from threading import Event

from confluent_kafka import Producer

from .generator import TelemetryGenerator

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("telemetry-generator")
STOP = Event()


def handle_signal(*_: object) -> None:
    STOP.set()


def delivery_report(err, msg) -> None:
    if err is not None:
        LOG.error("Kafka delivery failed: %s", err)


def main() -> None:
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    device_count = int(os.getenv("DEVICE_COUNT", "10000"))
    eps = max(1, int(os.getenv("EVENTS_PER_SECOND", "500")))

    generator = TelemetryGenerator(device_count=device_count)
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "device-telemetry-generator",
            "compression.type": "snappy",
            "linger.ms": 20,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 200000,
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    LOG.info("Starting generator: devices=%d target_eps=%d bootstrap=%s", device_count, eps, bootstrap)
    counters: Counter[str] = Counter()
    report_at = time.monotonic() + 10
    interval = 1.0 / eps
    next_emit = time.monotonic()

    while not STOP.is_set():
        event = generator.next_event()
        payload = event.model_dump_json(by_alias=True)
        topic = f"telemetry.{event.event_type}"

        while True:
            try:
                producer.produce(
                    topic,
                    key=event.device_id,
                    value=payload,
                    headers={"schemaVersion": event.schema_version},
                    callback=delivery_report,
                )
                break
            except BufferError:
                producer.poll(0.05)

        counters[topic] += 1
        producer.poll(0)

        next_emit += interval
        sleep_for = next_emit - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        elif sleep_for < -1.0:
            next_emit = time.monotonic()

        if time.monotonic() >= report_at:
            LOG.info("Produced last interval: %s", json.dumps(dict(counters), sort_keys=True))
            counters.clear()
            report_at = time.monotonic() + 10

    LOG.info("Shutting down; flushing producer")
    producer.flush(10)


if __name__ == "__main__":
    main()
