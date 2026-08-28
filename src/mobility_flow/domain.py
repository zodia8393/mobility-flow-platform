from enum import StrEnum


class CongestionLevel(StrEnum):
    SEVERE = "SEVERE"
    CONGESTED = "CONGESTED"
    SLOW = "SLOW"
    SMOOTH = "SMOOTH"


def classify_congestion(speed_kph: float, reference_speed_kph: float) -> CongestionLevel:
    if speed_kph < 0:
        raise ValueError("speed_kph must be non-negative")
    if reference_speed_kph <= 0:
        raise ValueError("reference_speed_kph must be positive")
    speed_index = speed_kph / reference_speed_kph
    if speed_index <= 0.30:
        return CongestionLevel.SEVERE
    if speed_index <= 0.50:
        return CongestionLevel.CONGESTED
    if speed_index <= 0.70:
        return CongestionLevel.SLOW
    return CongestionLevel.SMOOTH
