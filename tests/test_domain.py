import pytest

from mobility_flow.domain import CongestionLevel, classify_congestion


@pytest.mark.parametrize(
    ("speed", "reference_speed", "expected"),
    [
        (0, 80, CongestionLevel.SEVERE),
        (24, 80, CongestionLevel.SEVERE),
        (25, 80, CongestionLevel.CONGESTED),
        (40, 80, CongestionLevel.CONGESTED),
        (41, 80, CongestionLevel.SLOW),
        (56, 80, CongestionLevel.SLOW),
        (57, 80, CongestionLevel.SMOOTH),
    ],
)
def test_congestion_boundaries(
    speed: float, reference_speed: float, expected: CongestionLevel
) -> None:
    assert classify_congestion(speed, reference_speed) == expected


def test_congestion_rejects_invalid_reference_speed() -> None:
    with pytest.raises(ValueError, match="positive"):
        classify_congestion(20, 0)


def test_congestion_rejects_negative_speed() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        classify_congestion(-1, 80)
