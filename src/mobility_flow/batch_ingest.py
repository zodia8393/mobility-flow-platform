from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import duckdb

from mobility_flow.config import get_settings
from mobility_flow.storage import ObjectStore

REQUIRED_FIELDS = (
    "segment_id",
    "road_name",
    "latitude",
    "longitude",
    "observed_at",
    "segment_length_km",
    "speed_kph",
    "reference_speed_kph",
    "traffic_volume",
    "travel_time_seconds",
)
OPTIONAL_FIELDS = ("event_id", "trace_id", "ingested_at", "vehicle_type")
CANONICAL_COLUMNS = (
    "schema_version",
    "event_id",
    "trace_id",
    "segment_id",
    "road_name",
    "latitude",
    "longitude",
    "observed_at",
    "ingested_at",
    "source_system",
    "vehicle_type",
    "segment_length_km",
    "speed_kph",
    "reference_speed_kph",
    "traffic_volume",
    "travel_time_seconds",
)


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_mapping(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    rendered = Path(raw[1:]).read_text(encoding="utf-8") if raw.startswith("@") else raw
    parsed = json.loads(rendered)
    if not isinstance(parsed, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in parsed.items()
    ):
        raise ValueError("column map must be a JSON object of canonical_field -> source_column")
    unknown = set(parsed) - set(REQUIRED_FIELDS) - set(OPTIONAL_FIELDS)
    if unknown:
        raise ValueError(f"unknown canonical fields in column map: {sorted(unknown)}")
    return parsed


def _reader(path: Path) -> str:
    if path.suffix.lower() == ".parquet":
        return f"read_parquet({_literal(path)})"
    if path.suffix.lower() in {".csv", ".gz"} or path.name.lower().endswith(".csv.gz"):
        return (
            f"read_csv_auto({_literal(path)}, header=true, all_varchar=true, "
            "sample_size=-1, ignore_errors=false)"
        )
    raise ValueError("supported input formats: .csv, .csv.gz, .parquet")


def _source_expression(
    canonical: str,
    mapping: dict[str, str],
    source_columns: set[str],
    *,
    default: str = "NULL",
) -> str:
    source_name = mapping.get(canonical, canonical)
    return _identifier(source_name) if source_name in source_columns else default


def _prepare_tables(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    *,
    source_system: str,
    vehicle_type: str,
    timezone: str,
    mapping: dict[str, str],
    fingerprint: str,
) -> None:
    connection.execute("SET TimeZone = ?", [timezone])
    reader = _reader(path)
    described = connection.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
    source_columns = {str(row[0]) for row in described}
    missing = [
        field
        for field in REQUIRED_FIELDS
        if mapping.get(field, field) not in source_columns
    ]
    if missing:
        raise ValueError(f"missing required canonical fields: {missing}")

    expr = {
        field: _source_expression(field, mapping, source_columns)
        for field in (*REQUIRED_FIELDS, *OPTIONAL_FIELDS)
    }
    event_id = (
        f"coalesce(nullif(trim(cast({expr['event_id']} as varchar)), ''), "
        f"md5({_literal(fingerprint)} || ':' || cast(_row_number as varchar)))"
    )
    trace_id = (
        f"coalesce(nullif(trim(cast({expr['trace_id']} as varchar)), ''), "
        f"md5('trace:' || {_literal(fingerprint)} || ':' || cast(_row_number as varchar)))"
    )
    ingested_at = (
        "coalesce(try_cast(" + expr["ingested_at"] + " as timestamptz), current_timestamp)"
    )
    vehicle = (
        "coalesce(nullif(upper(trim(cast(" + expr["vehicle_type"]
        + f" as varchar))), ''), {_literal(vehicle_type)})"
    )

    connection.execute(
        f"""
        CREATE TEMP TABLE normalized AS
        WITH numbered AS (
            SELECT *, row_number() OVER () AS _row_number
            FROM {reader}
        )
        SELECT
            '1.0'::VARCHAR AS schema_version,
            {event_id} AS event_id,
            {trace_id} AS trace_id,
            trim(cast({expr['segment_id']} AS VARCHAR)) AS segment_id,
            trim(cast({expr['road_name']} AS VARCHAR)) AS road_name,
            try_cast({expr['latitude']} AS DOUBLE) AS latitude,
            try_cast({expr['longitude']} AS DOUBLE) AS longitude,
            try_cast({expr['observed_at']} AS TIMESTAMPTZ) AS observed_at,
            {ingested_at} AS ingested_at,
            {_literal(source_system)}::VARCHAR AS source_system,
            {vehicle} AS vehicle_type,
            try_cast({expr['segment_length_km']} AS DOUBLE) AS segment_length_km,
            try_cast({expr['speed_kph']} AS DOUBLE) AS speed_kph,
            try_cast({expr['reference_speed_kph']} AS DOUBLE) AS reference_speed_kph,
            try_cast({expr['traffic_volume']} AS INTEGER) AS traffic_volume,
            try_cast({expr['travel_time_seconds']} AS DOUBLE) AS travel_time_seconds,
            _row_number
        FROM numbered
        """
    )
    connection.execute(
        """
        CREATE TEMP VIEW classified AS
        SELECT *,
            CASE
                WHEN NOT regexp_full_match(segment_id, 'SEG-[0-9]{3}') THEN 'INVALID_SEGMENT_ID'
                WHEN road_name IS NULL OR road_name = '' THEN 'MISSING_ROAD_NAME'
                WHEN latitude IS NULL OR latitude NOT BETWEEN -90 AND 90 THEN 'INVALID_LATITUDE'
                WHEN longitude IS NULL
                    OR longitude NOT BETWEEN -180 AND 180 THEN 'INVALID_LONGITUDE'
                WHEN observed_at IS NULL THEN 'INVALID_OBSERVED_AT'
                WHEN vehicle_type NOT IN ('PASSENGER', 'BUS', 'TRUCK', 'ALL')
                    THEN 'INVALID_VEHICLE_TYPE'
                WHEN segment_length_km IS NULL
                    OR segment_length_km <= 0
                    OR segment_length_km > 100 THEN 'INVALID_SEGMENT_LENGTH'
                WHEN speed_kph IS NULL OR speed_kph <= 0 OR speed_kph > 180 THEN 'INVALID_SPEED'
                WHEN reference_speed_kph IS NULL
                    OR reference_speed_kph <= 0
                    OR reference_speed_kph > 180 THEN 'INVALID_REFERENCE_SPEED'
                WHEN traffic_volume IS NULL
                    OR traffic_volume < 0
                    OR traffic_volume > 20000 THEN 'INVALID_TRAFFIC_VOLUME'
                WHEN travel_time_seconds IS NULL
                    OR travel_time_seconds <= 0
                    OR travel_time_seconds > 7200 THEN 'INVALID_TRAVEL_TIME'
                ELSE NULL
            END AS reject_reason
        FROM normalized
        """
    )


def ingest_file(
    path: Path,
    *,
    source_system: str,
    vehicle_type: str = "ALL",
    timezone: str = "Asia/Seoul",
    mapping: dict[str, str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    source_system = source_system.upper()
    vehicle_type = vehicle_type.upper()
    if source_system not in {"TMAP", "TCS", "VDS", "GPS"}:
        raise ValueError("source_system must be one of TMAP, TCS, VDS, GPS")
    if vehicle_type not in {"PASSENGER", "BUS", "TRUCK", "ALL"}:
        raise ValueError("vehicle_type must be one of PASSENGER, BUS, TRUCK, ALL")

    mapping = mapping or {}
    input_sha256 = _sha256(path)
    mapping_payload = json.dumps(mapping, ensure_ascii=False, sort_keys=True)
    fingerprint = hashlib.sha256(
        f"{input_sha256}:{source_system}:{vehicle_type}:{mapping_payload}".encode()
    ).hexdigest()

    connection = duckdb.connect()
    try:
        _prepare_tables(
            connection,
            path,
            source_system=source_system,
            vehicle_type=vehicle_type,
            timezone=timezone,
            mapping=mapping,
            fingerprint=fingerprint,
        )
        input_rows, accepted_rows, rejected_rows = connection.execute(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE reject_reason IS NULL),
                   count(*) FILTER (WHERE reject_reason IS NOT NULL)
            FROM classified
            """
        ).fetchone()
        reject_counts = dict(
            connection.execute(
                """
                SELECT reject_reason, count(*)
                FROM classified
                WHERE reject_reason IS NOT NULL
                GROUP BY reject_reason
                ORDER BY reject_reason
                """
            ).fetchall()
        )

        accepted_key: str | None = None
        rejected_key: str | None = None
        if not dry_run:
            now = datetime.now(UTC)
            store = ObjectStore(get_settings())
            store.ensure_bucket()
            with TemporaryDirectory(prefix="mobility-file-ingest-") as temporary:
                temporary_dir = Path(temporary)
                if accepted_rows:
                    accepted_path = temporary_dir / "accepted.parquet"
                    columns = ", ".join(_identifier(column) for column in CANONICAL_COLUMNS)
                    connection.execute(
                        f"COPY (SELECT {columns} FROM classified WHERE reject_reason IS NULL) "
                        f"TO {_literal(accepted_path)} (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                    accepted_key = (
                        "bronze/traffic_observations/"
                        f"ingest_date={now:%Y-%m-%d}/source_system={source_system}/"
                        f"file-{fingerprint[:16]}.parquet"
                    )
                    store.upload_file(accepted_path, accepted_key)
                if rejected_rows:
                    rejected_path = temporary_dir / "rejected.parquet"
                    connection.execute(
                        "COPY (SELECT * EXCLUDE (_row_number) FROM classified "
                        f"WHERE reject_reason IS NOT NULL) TO {_literal(rejected_path)} "
                        "(FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                    rejected_key = (
                        "quarantine/traffic_observations/"
                        f"ingest_date={now:%Y-%m-%d}/source_system={source_system}/"
                        f"file-{fingerprint[:16]}.parquet"
                    )
                    store.upload_file(rejected_path, rejected_key)
    finally:
        connection.close()

    return {
        "status": "VALIDATED" if dry_run else "INGESTED",
        "source_file": path.name,
        "source_system": source_system,
        "vehicle_type": vehicle_type,
        "timezone": timezone,
        "input_bytes": path.stat().st_size,
        "input_sha256": input_sha256,
        "ingest_fingerprint": fingerprint,
        "input_rows": input_rows,
        "accepted_rows": accepted_rows,
        "rejected_rows": rejected_rows,
        "reject_counts": reject_counts,
        "accepted_object": accepted_key,
        "quarantine_object": rejected_key,
        "duration_seconds": round(time.monotonic() - started, 3),
        "dry_run": dry_run,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and ingest a TMAP/TCS/VDS/GPS CSV, CSV.GZ or Parquet file"
    )
    parser.add_argument("path", type=Path)
    parser.add_argument("--source-system", required=True, choices=["TMAP", "TCS", "VDS", "GPS"])
    parser.add_argument(
        "--vehicle-type", default="ALL", choices=["PASSENGER", "BUS", "TRUCK", "ALL"]
    )
    parser.add_argument("--timezone", default="Asia/Seoul")
    parser.add_argument(
        "--column-map",
        help="JSON canonical_field -> source_column, or @path/to/mapping.json",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate without S3/MinIO upload")
    parser.add_argument("--output", type=Path, help="Optional JSON report path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = ingest_file(
        args.path,
        source_system=args.source_system,
        vehicle_type=args.vehicle_type,
        timezone=args.timezone,
        mapping=_read_mapping(args.column_map),
        dry_run=args.dry_run,
    )
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
