from datetime import UTC, datetime

from mobility_flow.schemas import TrafficObservationEvent
from mobility_flow.simulator import MobilityEventSimulator

START = datetime(2026, 8, 28, 0, 0, tzinfo=UTC)


def test_simulator_is_deterministic() -> None:
    first = list(
        MobilityEventSimulator(seed=7, start_time=START).events(
            100, duplicate_rate=0.05, late_rate=0.05, invalid_rate=0
        )
    )
    second = list(
        MobilityEventSimulator(seed=7, start_time=START).events(
            100, duplicate_rate=0.05, late_rate=0.05, invalid_rate=0
        )
    )
    assert first == second


def test_simulator_emits_contract_valid_events_when_faults_are_disabled() -> None:
    events = MobilityEventSimulator(seed=11, start_time=START).events(
        200, duplicate_rate=0, late_rate=0, invalid_rate=0
    )
    validated = [TrafficObservationEvent.model_validate(event) for event in events]
    assert len(validated) == 200
    assert len({event.segment_id for event in validated}) == 20
    assert {event.source_system for event in validated} == {"TMAP", "TCS", "VDS", "GPS"}


def test_simulator_injects_observable_failure_cases() -> None:
    simulator = MobilityEventSimulator(seed=23, start_time=START)
    events = list(
        simulator.events(500, duplicate_rate=0.10, late_rate=0.10, invalid_rate=0.10)
    )
    assert len(events) == 500
    assert simulator.stats["duplicates_injected"] > 0
    assert simulator.stats["late_injected"] > 0
    assert simulator.stats["invalid_injected"] > 0
