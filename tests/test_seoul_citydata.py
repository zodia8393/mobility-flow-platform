import gzip
from datetime import UTC, datetime

import pyarrow.parquet as pq

from mobility_flow.config import Settings
from mobility_flow.seoul_citydata import sync_seoul_citydata


class FakeClient:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def fetch_area(self, _area: str) -> dict[str, object]:
        return self.payload


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def ensure_bucket(self) -> None:
        pass

    def upload_bytes(self, key: str, body: bytes, content_type: str) -> None:
        assert content_type in {"application/gzip", "application/vnd.apache.parquet"}
        self.objects[key] = body


def citydata_payload(*, xylist: str | None = None) -> dict[str, object]:
    return {
        "AREA_NM": "광화문·덕수궁",
        "AREA_CD": "POI009",
        "ROAD_TRAFFIC_STTS": {
            "ROAD_TRAFFIC_STTS": [
                {
                    "LINK_ID": "1000000301",
                    "ROAD_NM": "종로",
                    "SPD": 26.0,
                    "IDX": "서행",
                    "DIST": "46.0",
                    "XYLIST": xylist
                    or "126.979380523_37.570149411|126.979899983_37.570153560",
                }
            ]
        },
    }


def test_live_source_is_preserved_and_normalized_without_invented_metrics(tmp_path) -> None:
    store = MemoryStore()
    snapshot = tmp_path / "seoul-live.parquet"
    report = sync_seoul_citydata(
        areas=("광화문·덕수궁",),
        settings=Settings(seoul_open_data_api_key="not-used-by-fake"),
        store=store,
        client=FakeClient(citydata_payload()),
        retrieved_at=datetime(2026, 8, 28, 7, 13, 42, tzinfo=UTC),
        snapshot_output=snapshot,
    )

    assert report["status"] == "SYNCED"
    assert report["live_api"] is True
    assert report["input_rows"] == report["accepted_rows"] == 1
    assert report["rejected_rows"] == 0
    assert report["congestion_counts"] == {"SLOW": 1}
    assert len(report["raw_objects"]) == len(report["bronze_objects"]) == 1

    raw_key = report["raw_objects"][0]
    assert b'"LINK_ID":"1000000301"' in gzip.decompress(store.objects[raw_key])
    table = pq.read_table(snapshot)
    row = table.to_pylist()[0]
    assert row["segment_id"] == "1000000301"
    assert row["source_system"] == "SEOUL_TOPIS"
    assert row["source_congestion_level"] == "SLOW"
    assert row["reference_speed_kph"] is None
    assert row["traffic_volume"] is None
    assert row["observed_at"] == datetime(2026, 8, 28, 7, 10, tzinfo=UTC)
    assert len(row["source_payload_sha256"]) == 64


def test_invalid_official_coordinate_is_quarantined() -> None:
    store = MemoryStore()
    report = sync_seoul_citydata(
        areas=("광화문·덕수궁",),
        settings=Settings(seoul_open_data_api_key="not-used-by-fake"),
        store=store,
        client=FakeClient(citydata_payload(xylist="0_0|1_1")),
        retrieved_at=datetime(2026, 8, 28, 7, 15, tzinfo=UTC),
    )

    assert report["accepted_rows"] == 0
    assert report["rejected_rows"] == 1
    assert not report["bronze_objects"]
    assert any(key.startswith("quarantine/seoul_citydata/") for key in store.objects)
