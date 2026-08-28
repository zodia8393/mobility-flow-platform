from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from mobility_flow.schemas import TrafficObservationEvent


def valid_event() -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "schema_version": "1.0",
        "event_id": "event-123456",
        "trace_id": "trace-123456",
        "segment_id": "SEG-001",
        "road_name": "강변북로·마포",
        "latitude": 37.5498,
        "longitude": 126.9368,
        "observed_at": now.isoformat(),
        "ingested_at": now.isoformat(),
        "source_system": "TMAP",
        "vehicle_type": "PASSENGER",
        "segment_length_km": 4.8,
        "speed_kph": 24.0,
        "reference_speed_kph": 80.0,
        "traffic_volume": 1480,
        "travel_time_seconds": 720.0,
    }


def test_event_contract_accepts_valid_traffic_observation() -> None:
    event = TrafficObservationEvent.model_validate(valid_event())
    assert event.segment_id == "SEG-001"
    assert event.source_system == "TMAP"


@pytest.mark.parametrize(
    ("field", "value"),
    [("speed_kph", -1), ("segment_id", "bad-id"), ("schema_version", "2.0")],
)
def test_event_contract_rejects_invalid_values(field: str, value: object) -> None:
    payload = valid_event()
    payload[field] = value
    with pytest.raises(ValidationError):
        TrafficObservationEvent.model_validate(payload)


def test_event_contract_rejects_naive_timestamp() -> None:
    payload = valid_event()
    payload["observed_at"] = "2026-08-28T09:00:00"
    with pytest.raises(ValidationError, match="timezone"):
        TrafficObservationEvent.model_validate(payload)
