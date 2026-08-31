from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from mobility_flow.config import Settings, get_settings
from mobility_flow.schemas import TrafficObservationEvent
from mobility_flow.storage import ObjectStore

SEOUL_CITYDATA_API_BASE = "http://openapi.seoul.go.kr:8088"
SEOUL_CITYDATA_DATASET_URL = (
    "https://data.seoul.go.kr/dataList/OA-21285/A/1/datasetView.do"
)
SEOUL_CITYDATA_SOURCE_URL = "https://data.seoul.go.kr/SeoulRtd/"
SEOUL_CITYDATA_LICENSE = "서울특별시 공공데이터 이용정책·공공누리 제1유형(출처표시)"

CONGESTION_MAP = {
    "정체": "CONGESTED",
    "서행": "SLOW",
    "원활": "SMOOTH",
    "정보없음": "UNKNOWN",
}

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("trace_id", pa.string(), nullable=False),
        pa.field("segment_id", pa.string(), nullable=False),
        pa.field("road_name", pa.string(), nullable=False),
        pa.field("area_code", pa.string()),
        pa.field("area_name", pa.string()),
        pa.field("latitude", pa.float64(), nullable=False),
        pa.field("longitude", pa.float64(), nullable=False),
        pa.field("start_latitude", pa.float64()),
        pa.field("start_longitude", pa.float64()),
        pa.field("end_latitude", pa.float64()),
        pa.field("end_longitude", pa.float64()),
        pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("source_system", pa.string(), nullable=False),
        pa.field("vehicle_type", pa.string(), nullable=False),
        pa.field("segment_length_km", pa.float64(), nullable=False),
        pa.field("speed_kph", pa.float64(), nullable=False),
        pa.field("reference_speed_kph", pa.float64()),
        pa.field("traffic_volume", pa.int64()),
        pa.field("travel_time_seconds", pa.float64(), nullable=False),
        pa.field("source_congestion_level", pa.string()),
        pa.field("source_payload_sha256", pa.string()),
    ]
)


class ObjectStoreProtocol(Protocol):
    def ensure_bucket(self) -> None: ...

    def upload_bytes(self, key: str, body: bytes, content_type: str) -> None: ...


class SeoulCityDataError(RuntimeError):
    pass


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _snapshot_time(value: datetime) -> datetime:
    value = value.astimezone(UTC)
    return value.replace(minute=value.minute - value.minute % 5, second=0, microsecond=0)


def _coordinate_points(raw: str) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    for item in raw.split("|"):
        if not item:
            continue
        longitude_raw, latitude_raw = item.split("_", 1)
        longitude = float(longitude_raw)
        latitude = float(latitude_raw)
        if not (126.0 <= longitude <= 128.0 and 36.0 <= latitude <= 38.5):
            raise ValueError("coordinate outside the Seoul service boundary")
        points.append((longitude, latitude))
    if len(points) < 2:
        raise ValueError("XYLIST must contain at least two coordinate points")
    return points


def _normalize_road(
    road: dict[str, Any],
    *,
    area_code: str,
    area_name: str,
    snapshot_at: datetime,
    retrieved_at: datetime,
    payload_sha256: str,
) -> dict[str, Any]:
    points = _coordinate_points(str(road.get("XYLIST") or ""))
    start_longitude, start_latitude = points[0]
    end_longitude, end_latitude = points[-1]
    longitude = sum(point[0] for point in points) / len(points)
    latitude = sum(point[1] for point in points) / len(points)
    segment_id = str(road.get("LINK_ID") or "").strip()
    road_name = str(road.get("ROAD_NM") or "").strip()
    length_km = float(road.get("DIST")) / 1000
    speed_kph = float(road.get("SPD"))
    if speed_kph <= 0:
        raise ValueError("SPD must be positive")
    congestion_level = CONGESTION_MAP.get(str(road.get("IDX") or ""), "UNKNOWN")
    stable_key = f"SEOUL_CITYDATA:{area_code}:{segment_id}:{snapshot_at.isoformat()}"
    event_id = "evt-" + hashlib.sha256(stable_key.encode()).hexdigest()
    trace_id = "sync-" + payload_sha256[:32]
    travel_time_seconds = length_km / speed_kph * 3600
    event = TrafficObservationEvent.model_validate(
        {
            "schema_version": "1.1",
            "event_id": event_id,
            "trace_id": trace_id,
            "segment_id": segment_id,
            "road_name": road_name,
            "area_code": area_code,
            "area_name": area_name,
            "latitude": latitude,
            "longitude": longitude,
            "start_latitude": start_latitude,
            "start_longitude": start_longitude,
            "end_latitude": end_latitude,
            "end_longitude": end_longitude,
            "observed_at": snapshot_at,
            "ingested_at": retrieved_at,
            "source_system": "SEOUL_TOPIS",
            "vehicle_type": "ALL",
            "segment_length_km": length_km,
            "speed_kph": speed_kph,
            "reference_speed_kph": None,
            "traffic_volume": None,
            "travel_time_seconds": travel_time_seconds,
            "source_congestion_level": congestion_level,
            "source_payload_sha256": payload_sha256,
        }
    )
    return event.model_dump(mode="python")


def _parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=PARQUET_SCHEMA)
    output = pa.BufferOutputStream()
    pq.write_table(table, output, compression="zstd")
    return output.getvalue().to_pybytes()


class SeoulCityDataClient:
    def __init__(self, api_key: str, *, timeout_seconds: float = 20.0) -> None:
        if not api_key:
            raise SeoulCityDataError("SEOUL_OPEN_DATA_API_KEY is required")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def fetch_area(self, area: str) -> dict[str, Any]:
        url = (
            f"{SEOUL_CITYDATA_API_BASE}/{self.api_key}/json/citydata/1/5/"
            f"{quote(area, safe='')}"
        )
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = httpx.get(
                    url,
                    timeout=self.timeout_seconds,
                    headers={"User-Agent": "MobilityFlow/1.0 (+public-data-ingestion)"},
                )
                response.raise_for_status()
                payload = response.json()
                city = payload.get("CITYDATA")
                if not isinstance(city, dict):
                    result = payload.get("RESULT") or {}
                    message = result.get("MESSAGE") or "CITYDATA is missing"
                    raise SeoulCityDataError(f"Seoul API rejected area {area!r}: {message}")
                roads = city.get("ROAD_TRAFFIC_STTS", {}).get("ROAD_TRAFFIC_STTS")
                if not isinstance(roads, list):
                    raise SeoulCityDataError(f"ROAD_TRAFFIC_STTS is missing for {area!r}")
                return {
                    "AREA_NM": city.get("AREA_NM"),
                    "AREA_CD": city.get("AREA_CD"),
                    "ROAD_TRAFFIC_STTS": {"ROAD_TRAFFIC_STTS": roads},
                }
            except (httpx.HTTPError, ValueError, SeoulCityDataError) as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(2**attempt)
        raise SeoulCityDataError(f"Seoul citydata request failed for {area!r}: {last_error}")


def sync_seoul_citydata(
    *,
    areas: tuple[str, ...] | None = None,
    settings: Settings | None = None,
    store: ObjectStoreProtocol | None = None,
    client: SeoulCityDataClient | None = None,
    retrieved_at: datetime | None = None,
    upload: bool = True,
    snapshot_output: Path | None = None,
) -> dict[str, Any]:
    settings = settings or get_settings()
    selected_areas = areas or settings.seoul_areas
    if not selected_areas:
        raise SeoulCityDataError("at least one Seoul citydata area is required")
    client = client or SeoulCityDataClient(
        settings.seoul_open_data_api_key or "",
        timeout_seconds=settings.seoul_citydata_timeout_seconds,
    )
    object_store = store or ObjectStore(settings)
    if upload:
        object_store.ensure_bucket()

    retrieved_at = (retrieved_at or datetime.now(UTC)).astimezone(UTC)
    snapshot_at = _snapshot_time(retrieved_at)
    started = time.monotonic()
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    area_reports: list[dict[str, Any]] = []
    raw_objects: list[str] = []
    bronze_objects: list[str] = []

    for requested_area in selected_areas:
        payload = client.fetch_area(requested_area)
        payload_bytes = _json_bytes(payload)
        payload_sha256 = _sha256(payload_bytes)
        area_name = str(payload.get("AREA_NM") or requested_area)
        area_code = str(payload.get("AREA_CD") or "UNKNOWN")
        roads = payload["ROAD_TRAFFIC_STTS"]["ROAD_TRAFFIC_STTS"]
        area_rows: list[dict[str, Any]] = []
        area_rejects: list[dict[str, Any]] = []
        for road in roads:
            try:
                area_rows.append(
                    _normalize_road(
                        road,
                        area_code=area_code,
                        area_name=area_name,
                        snapshot_at=snapshot_at,
                        retrieved_at=retrieved_at,
                        payload_sha256=payload_sha256,
                    )
                )
            except (KeyError, TypeError, ValueError, ValidationError) as exc:
                area_rejects.append(
                    {
                        "area_code": area_code,
                        "link_id": road.get("LINK_ID"),
                        "reason": type(exc).__name__,
                        "message": str(exc)[:500],
                    }
                )

        observed_partition = snapshot_at.strftime("%Y-%m-%d/%H%M")
        raw_key = (
            "landing/seoul_citydata/"
            f"retrieved_date={retrieved_at:%Y-%m-%d}/area_code={area_code}/"
            f"snapshot-{snapshot_at:%Y%m%dT%H%MZ}-{payload_sha256[:12]}.json.gz"
        )
        bronze_key = (
            "bronze/seoul_citydata/"
            f"observed_date={observed_partition[:10]}/area_code={area_code}/"
            f"snapshot-{snapshot_at:%Y%m%dT%H%MZ}-{payload_sha256[:12]}.parquet"
        )
        if upload:
            object_store.upload_bytes(
                raw_key,
                gzip.compress(payload_bytes, compresslevel=9),
                "application/gzip",
            )
            if area_rows:
                object_store.upload_bytes(
                    bronze_key,
                    _parquet_bytes(area_rows),
                    "application/vnd.apache.parquet",
                )
            if area_rejects:
                quarantine_key = (
                    "quarantine/seoul_citydata/"
                    f"retrieved_date={retrieved_at:%Y-%m-%d}/area_code={area_code}/"
                    f"snapshot-{snapshot_at:%Y%m%dT%H%MZ}-{payload_sha256[:12]}.json.gz"
                )
                object_store.upload_bytes(
                    quarantine_key,
                    gzip.compress(_json_bytes({"rejected": area_rejects}), compresslevel=9),
                    "application/gzip",
                )
            raw_objects.append(raw_key)
            if area_rows:
                bronze_objects.append(bronze_key)

        accepted.extend(area_rows)
        rejected.extend(area_rejects)
        area_reports.append(
            {
                "area_code": area_code,
                "area_name": area_name,
                "source_rows": len(roads),
                "accepted_rows": len(area_rows),
                "rejected_rows": len(area_rejects),
                "payload_sha256": payload_sha256,
            }
        )

    snapshot_sha256: str | None = None
    snapshot_bytes: int | None = None
    if snapshot_output:
        snapshot_output.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pylist(accepted, schema=PARQUET_SCHEMA),
            snapshot_output,
            compression="zstd",
        )
        snapshot_body = snapshot_output.read_bytes()
        snapshot_sha256 = _sha256(snapshot_body)
        snapshot_bytes = len(snapshot_body)

    status_counts = Counter(row["source_congestion_level"] for row in accepted)
    report_core = {
        "source": "서울시 실시간 도시데이터·도로소통",
        "dataset_url": SEOUL_CITYDATA_DATASET_URL,
        "source_url": SEOUL_CITYDATA_SOURCE_URL,
        "license": SEOUL_CITYDATA_LICENSE,
        "retrieved_at": retrieved_at.isoformat(),
        "snapshot_at": snapshot_at.isoformat(),
        "areas": area_reports,
        "input_rows": sum(report["source_rows"] for report in area_reports),
        "accepted_rows": len(accepted),
        "rejected_rows": len(rejected),
        "congestion_counts": dict(sorted(status_counts.items())),
        "raw_objects": raw_objects,
        "bronze_objects": bronze_objects,
        "snapshot_written": snapshot_output is not None,
        "snapshot_sha256": snapshot_sha256,
        "snapshot_bytes": snapshot_bytes,
        "duration_seconds": round(time.monotonic() - started, 3),
        "live_api": True,
    }
    return {
        "status": "SYNCED" if upload else "VALIDATED",
        "sync_id": "seoul-" + _sha256(_json_bytes(report_core))[:20],
        **report_core,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch and ingest live road traffic from Seoul Real-Time City Data"
    )
    parser.add_argument("--area", action="append", dest="areas")
    parser.add_argument("--no-upload", action="store_true")
    parser.add_argument("--snapshot-output", type=Path)
    parser.add_argument("--report", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = sync_seoul_citydata(
        areas=tuple(args.areas) if args.areas else None,
        upload=not args.no_upload,
        snapshot_output=args.snapshot_output,
    )
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
