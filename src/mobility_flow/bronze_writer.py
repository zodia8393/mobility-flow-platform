import io
import logging
import os
import signal
import time
from datetime import UTC, datetime
from typing import Any

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pydantic import ValidationError

from mobility_flow.config import Settings, get_settings
from mobility_flow.schemas import DlqEnvelope, TrafficObservationEvent
from mobility_flow.storage import ObjectStore

LOGGER = logging.getLogger(__name__)

RECEIVED = Counter("mobility_bronze_received_total", "Kafka messages received")
ACCEPTED = Counter("mobility_bronze_accepted_total", "Schema-valid messages accepted")
DLQ = Counter("mobility_bronze_dlq_total", "Messages routed to DLQ", ["reason"])
BATCHES = Counter("mobility_bronze_batches_total", "Parquet batches uploaded")
UPLOAD_FAILURES = Counter("mobility_bronze_upload_failures_total", "Object-store upload failures")
BATCH_ROWS = Histogram("mobility_bronze_batch_rows", "Rows in uploaded bronze batches")
LAST_UPLOAD = Gauge("mobility_bronze_last_upload_unixtime", "Last successful upload time")


class BronzeWriter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = ObjectStore(settings)
        self.store.ensure_bucket()
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": settings.kafka_consumer_group,
                "client.id": "mobility-bronze-writer",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
            }
        )
        self.dlq_producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "mobility-dlq-producer",
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        self.records: list[dict[str, Any]] = []
        self.pending_offsets: dict[tuple[str, int], int] = {}
        self.last_flush = time.monotonic()
        self.running = True

    def stop(self, *_args: object) -> None:
        self.running = False

    def _track_offset(self, message: Message) -> None:
        key = (message.topic(), message.partition())
        self.pending_offsets[key] = max(self.pending_offsets.get(key, -1), message.offset() + 1)

    def _route_to_dlq(self, message: Message, error: Exception) -> None:
        reason = type(error).__name__
        envelope = DlqEnvelope(
            failed_at=datetime.now(UTC),
            error_type=reason,
            error_message=str(error)[:1000],
            source_topic=message.topic(),
            source_partition=message.partition(),
            source_offset=message.offset(),
            payload=(message.value() or b"").decode("utf-8", errors="replace")[:20_000],
        )
        self.dlq_producer.produce(
            self.settings.kafka_dlq_topic,
            key=message.key(),
            value=orjson.dumps(envelope.model_dump(mode="json")),
        )
        self.dlq_producer.poll(0)
        DLQ.labels(reason=reason).inc()

    def handle(self, message: Message) -> None:
        RECEIVED.inc()
        self._track_offset(message)
        try:
            payload = orjson.loads(message.value())
            event = TrafficObservationEvent.model_validate(payload)
        except (orjson.JSONDecodeError, ValidationError, TypeError) as exc:
            self._route_to_dlq(message, exc)
            return
        self.records.append(event.model_dump(mode="json"))
        ACCEPTED.inc()

    def should_flush(self) -> bool:
        return bool(self.pending_offsets) and (
            len(self.records) >= self.settings.bronze_batch_size
            or time.monotonic() - self.last_flush >= self.settings.bronze_flush_seconds
        )

    def flush(self) -> None:
        if not self.pending_offsets:
            return
        try:
            if self.records:
                table = pa.Table.from_pylist(self.records)
                output = io.BytesIO()
                pq.write_table(table, output, compression="zstd", write_statistics=True)
                now = datetime.now(UTC)
                first_offset = min(offset - 1 for offset in self.pending_offsets.values())
                key = (
                    "bronze/traffic_observations/"
                    f"ingest_date={now:%Y-%m-%d}/hour={now:%H}/"
                    f"batch-{now:%Y%m%dT%H%M%S%fZ}-{first_offset}.parquet"
                )
                self.store.upload_bytes(key, output.getvalue(), "application/vnd.apache.parquet")
                BATCHES.inc()
                BATCH_ROWS.observe(len(self.records))
                LAST_UPLOAD.set_to_current_time()

            self.dlq_producer.flush(10)
            offsets = [
                TopicPartition(topic, partition, offset)
                for (topic, partition), offset in self.pending_offsets.items()
            ]
            self.consumer.commit(offsets=offsets, asynchronous=False)
            LOGGER.info("Committed batch: rows=%d partitions=%d", len(self.records), len(offsets))
            self.records.clear()
            self.pending_offsets.clear()
            self.last_flush = time.monotonic()
        except Exception:
            UPLOAD_FAILURES.inc()
            LOGGER.exception("Batch upload failed; offsets were not committed")
            raise

    def run(self) -> None:
        self.consumer.subscribe([self.settings.kafka_topic])
        while self.running:
            message = self.consumer.poll(1.0)
            if message is not None:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        LOGGER.error("Kafka consumer error: %s", message.error())
                else:
                    self.handle(message)
            if self.should_flush():
                self.flush()
        self.flush()
        self.consumer.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    settings = get_settings()
    start_http_server(settings.metrics_port)
    writer = BronzeWriter(settings)
    signal.signal(signal.SIGTERM, writer.stop)
    signal.signal(signal.SIGINT, writer.stop)
    writer.run()


if __name__ == "__main__":
    main()
