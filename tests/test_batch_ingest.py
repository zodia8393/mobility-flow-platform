import csv

import pytest

from mobility_flow.batch_ingest import _read_mapping, ingest_file


def test_file_ingest_dry_run_maps_columns_and_quarantines_invalid_rows(tmp_path) -> None:
    source = tmp_path / "tcs.csv"
    columns = [
        "link_id",
        "road_label",
        "lat",
        "lon",
        "measure_time",
        "length_km",
        "avg_speed",
        "reference_speed",
        "volume_15m",
        "travel_seconds",
    ]
    with source.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerow(
            {
                "link_id": "SEG-001",
                "road_label": "강변북로·마포",
                "lat": 37.5498,
                "lon": 126.9368,
                "measure_time": "2026-08-28T09:00:00+09:00",
                "length_km": 4.8,
                "avg_speed": 24,
                "reference_speed": 80,
                "volume_15m": 1480,
                "travel_seconds": 720,
            }
        )
        writer.writerow(
            {
                "link_id": "SEG-002",
                "road_label": "강변북로·용산",
                "lat": 37.5298,
                "lon": 126.9648,
                "measure_time": "2026-08-28T09:00:00+09:00",
                "length_km": 5.1,
                "avg_speed": -1,
                "reference_speed": 80,
                "volume_15m": 1320,
                "travel_seconds": 437.1,
            }
        )

    mapping = {
        "segment_id": "link_id",
        "road_name": "road_label",
        "latitude": "lat",
        "longitude": "lon",
        "observed_at": "measure_time",
        "segment_length_km": "length_km",
        "speed_kph": "avg_speed",
        "reference_speed_kph": "reference_speed",
        "traffic_volume": "volume_15m",
        "travel_time_seconds": "travel_seconds",
    }
    report = ingest_file(source, source_system="TCS", mapping=mapping, dry_run=True)

    assert report["status"] == "VALIDATED"
    assert report["input_rows"] == 2
    assert report["accepted_rows"] == 1
    assert report["rejected_rows"] == 1
    assert report["reject_counts"] == {"INVALID_SPEED": 1}
    assert report["accepted_object"] is None


def test_column_map_rejects_unknown_canonical_field() -> None:
    with pytest.raises(ValueError, match="unknown canonical fields"):
        _read_mapping('{"unknown": "source"}')
