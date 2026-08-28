import argparse
import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta

import orjson
from confluent_kafka import KafkaException, Producer

from mobility_flow.config import get_settings
from mobility_flow.simulator import MobilityEventSimulator

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce privacy-safe traffic observations")
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument("--rate", type=float, default=0, help="Messages per second; 0 is unlimited")
    parser.add_argument("--seed", type=int, default=20260828)
    parser.add_argument("--duplicate-rate", type=float, default=0.02)
    parser.add_argument("--late-rate", type=float, default=0.03)
    parser.add_argument("--invalid-rate", type=float, default=0.01)
    parser.add_argument("--start-time", type=datetime.fromisoformat)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    args = parse_args()
    if args.count <= 0:
        raise SystemExit("--count must be positive")
    for name in ("duplicate_rate", "late_rate", "invalid_rate"):
        value = getattr(args, name)
        if not 0 <= value < 1:
            raise SystemExit(f"--{name.replace('_', '-')} must be in [0, 1)")

    settings = get_settings()
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "mobility-traffic-producer",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 20,
        }
    )
    start_time = args.start_time or datetime.now(UTC) - timedelta(seconds=args.count)
    simulator = MobilityEventSimulator(seed=args.seed, start_time=start_time)
    delivery_errors: list[str] = []

    def on_delivery(error: KafkaException | None, _message: object) -> None:
        if error:
            delivery_errors.append(str(error))

    started = time.monotonic()
    interval = 1 / args.rate if args.rate > 0 else 0
    for event in simulator.events(
        args.count,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        invalid_rate=args.invalid_rate,
    ):
        while True:
            try:
                producer.produce(
                    topic=settings.kafka_topic,
                    key=str(event["segment_id"]).encode(),
                    value=orjson.dumps(event),
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        if interval:
            time.sleep(interval)

    undelivered = producer.flush(30)
    elapsed = time.monotonic() - started
    summary = {
        "topic": settings.kafka_topic,
        "requested": args.count,
        "delivered": args.count - undelivered - len(delivery_errors),
        "undelivered": undelivered,
        "delivery_errors": len(delivery_errors),
        "elapsed_seconds": round(elapsed, 3),
        "events_per_second": round(args.count / elapsed, 1) if elapsed else None,
        **simulator.stats,
    }
    print(json.dumps(summary, ensure_ascii=False))
    if undelivered or delivery_errors:
        LOGGER.error("Kafka delivery failed: %s", summary)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
