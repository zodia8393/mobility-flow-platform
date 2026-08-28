import random
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

from mobility_flow.road_segments import ROAD_SEGMENTS


class MobilityEventSimulator:
    def __init__(self, seed: int = 20260828, start_time: datetime | None = None) -> None:
        self.seed = seed
        self.random = random.Random(seed)
        self.start_time = start_time or datetime.now(UTC).replace(microsecond=0)
        self.segment_speed = {
            segment.segment_id: segment.reference_speed_kph * self.random.uniform(0.22, 1.02)
            for segment in ROAD_SEGMENTS
        }
        self.stats = {"duplicates_injected": 0, "late_injected": 0, "invalid_injected": 0}
        self._previous: list[dict[str, object]] = []

    def events(
        self,
        count: int,
        *,
        duplicate_rate: float = 0.02,
        late_rate: float = 0.03,
        invalid_rate: float = 0.01,
    ) -> Iterator[dict[str, object]]:
        for index in range(count):
            if self._previous and self.random.random() < duplicate_rate:
                self.stats["duplicates_injected"] += 1
                yield dict(self.random.choice(self._previous))
                continue

            segment = ROAD_SEGMENTS[index % len(ROAD_SEGMENTS)]
            speed_delta = self.random.uniform(-5.5, 5.5)
            speed = min(
                segment.reference_speed_kph * 1.12,
                max(4.0, self.segment_speed[segment.segment_id] + speed_delta),
            )
            self.segment_speed[segment.segment_id] = speed
            traffic_volume = self.random.randint(120, 1_900)
            travel_time_seconds = segment.length_km / speed * 3600

            ingested_at = self.start_time + timedelta(seconds=index)
            observed_at = ingested_at
            if self.random.random() < late_rate:
                observed_at -= timedelta(minutes=self.random.randint(16, 90))
                self.stats["late_injected"] += 1

            event_id = str(
                uuid.uuid5(uuid.NAMESPACE_URL, f"mobility-flow-traffic:{self.seed}:{index}")
            )
            event: dict[str, object] = {
                "schema_version": "1.0",
                "event_id": event_id,
                "trace_id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"trace:{event_id}")),
                "segment_id": segment.segment_id,
                "road_name": segment.road_name,
                "latitude": segment.latitude,
                "longitude": segment.longitude,
                "observed_at": observed_at.isoformat(),
                "ingested_at": ingested_at.isoformat(),
                "source_system": segment.source_system,
                "vehicle_type": "PASSENGER",
                "segment_length_km": round(segment.length_km, 3),
                "speed_kph": round(speed, 3),
                "reference_speed_kph": round(segment.reference_speed_kph, 3),
                "traffic_volume": traffic_volume,
                "travel_time_seconds": round(travel_time_seconds, 3),
            }

            if self.random.random() < invalid_rate:
                event["speed_kph"] = -1
                self.stats["invalid_injected"] += 1
            else:
                self._previous.append(dict(event))
                if len(self._previous) > 500:
                    self._previous.pop(0)
            yield event
