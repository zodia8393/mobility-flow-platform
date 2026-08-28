from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RoadSegment:
    segment_id: str
    road_name: str
    latitude: float
    longitude: float
    length_km: float
    reference_speed_kph: float
    source_system: str


# Privacy-safe synthetic segments near familiar Seoul corridors. They are not copied from a
# client source and do not represent a live traffic feed.
ROAD_SEGMENTS: tuple[RoadSegment, ...] = (
    RoadSegment("SEG-001", "강변북로·마포", 37.5498, 126.9368, 4.8, 80, "TMAP"),
    RoadSegment("SEG-002", "강변북로·용산", 37.5298, 126.9648, 5.1, 80, "TMAP"),
    RoadSegment("SEG-003", "강변북로·성수", 37.5365, 127.0559, 4.4, 80, "VDS"),
    RoadSegment("SEG-004", "올림픽대로·여의도", 37.5185, 126.9245, 5.6, 80, "TCS"),
    RoadSegment("SEG-005", "올림픽대로·반포", 37.5111, 127.0049, 4.2, 80, "VDS"),
    RoadSegment("SEG-006", "올림픽대로·잠실", 37.5156, 127.1001, 5.0, 80, "TMAP"),
    RoadSegment("SEG-007", "경부고속도로·서초", 37.4766, 127.0210, 6.2, 90, "TCS"),
    RoadSegment("SEG-008", "서부간선도로·가산", 37.4816, 126.8826, 4.7, 70, "VDS"),
    RoadSegment("SEG-009", "서부간선도로·구로", 37.4935, 126.9015, 3.9, 70, "TMAP"),
    RoadSegment("SEG-010", "내부순환로·홍제", 37.5890, 126.9430, 4.5, 70, "GPS"),
    RoadSegment("SEG-011", "내부순환로·성북", 37.6020, 127.0250, 5.3, 70, "VDS"),
    RoadSegment("SEG-012", "동부간선도로·성동", 37.5612, 127.0371, 4.1, 70, "TCS"),
    RoadSegment("SEG-013", "동부간선도로·중랑", 37.5850, 127.0692, 5.8, 70, "TMAP"),
    RoadSegment("SEG-014", "북부간선도로·신내", 37.6130, 127.1000, 4.6, 70, "VDS"),
    RoadSegment("SEG-015", "남부순환로·사당", 37.4766, 126.9816, 3.5, 60, "GPS"),
    RoadSegment("SEG-016", "남부순환로·대치", 37.4979, 127.0550, 4.0, 60, "TMAP"),
    RoadSegment("SEG-017", "공항대로·마곡", 37.5669, 126.8274, 4.9, 60, "TCS"),
    RoadSegment("SEG-018", "통일로·은평", 37.6050, 126.9220, 3.7, 60, "GPS"),
    RoadSegment("SEG-019", "양재대로·송파", 37.4900, 127.1000, 4.3, 60, "VDS"),
    RoadSegment("SEG-020", "한강대로·서울역", 37.5547, 126.9706, 2.8, 50, "TMAP"),
)
