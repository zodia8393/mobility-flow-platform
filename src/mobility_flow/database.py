import json
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from mobility_flow.config import Settings, get_settings

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS raw.traffic_observation (
    event_id TEXT PRIMARY KEY,
    trace_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    road_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    source_system TEXT NOT NULL,
    vehicle_type TEXT NOT NULL,
    segment_length_km DOUBLE PRECISION NOT NULL CHECK (segment_length_km > 0),
    speed_kph DOUBLE PRECISION NOT NULL CHECK (speed_kph > 0),
    reference_speed_kph DOUBLE PRECISION NOT NULL CHECK (reference_speed_kph > 0),
    traffic_volume INTEGER NOT NULL CHECK (traffic_volume >= 0),
    travel_time_seconds DOUBLE PRECISION NOT NULL CHECK (travel_time_seconds > 0),
    schema_version TEXT NOT NULL,
    is_late BOOLEAN NOT NULL,
    speed_index DOUBLE PRECISION NOT NULL CHECK (speed_index > 0),
    congestion_level TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_traffic_observation_observed_at
    ON raw.traffic_observation (observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_traffic_observation_segment_observed
    ON raw.traffic_observation (segment_id, observed_at DESC);

ALTER TABLE raw.traffic_observation ADD COLUMN IF NOT EXISTS area_code TEXT;
ALTER TABLE raw.traffic_observation ADD COLUMN IF NOT EXISTS area_name TEXT;
ALTER TABLE raw.traffic_observation ADD COLUMN IF NOT EXISTS start_latitude DOUBLE PRECISION;
ALTER TABLE raw.traffic_observation ADD COLUMN IF NOT EXISTS start_longitude DOUBLE PRECISION;
ALTER TABLE raw.traffic_observation ADD COLUMN IF NOT EXISTS end_latitude DOUBLE PRECISION;
ALTER TABLE raw.traffic_observation ADD COLUMN IF NOT EXISTS end_longitude DOUBLE PRECISION;
ALTER TABLE raw.traffic_observation ADD COLUMN IF NOT EXISTS source_congestion_level TEXT;
ALTER TABLE raw.traffic_observation ADD COLUMN IF NOT EXISTS source_payload_sha256 TEXT;
ALTER TABLE raw.traffic_observation ALTER COLUMN reference_speed_kph DROP NOT NULL;
ALTER TABLE raw.traffic_observation ALTER COLUMN traffic_volume DROP NOT NULL;
ALTER TABLE raw.traffic_observation ALTER COLUMN speed_index DROP NOT NULL;

CREATE TABLE IF NOT EXISTS ops.object_manifest (
    object_key TEXT PRIMARY KEY,
    etag TEXT NOT NULL,
    run_id TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    source_objects INTEGER NOT NULL DEFAULT 0,
    input_rows BIGINT NOT NULL DEFAULT 0,
    accepted_rows BIGINT NOT NULL DEFAULT 0,
    duplicates_removed BIGINT NOT NULL DEFAULT 0,
    late_rows BIGINT NOT NULL DEFAULT 0,
    dlq_rows BIGINT NOT NULL DEFAULT 0,
    output_objects INTEGER NOT NULL DEFAULT 0,
    job_duration_seconds DOUBLE PRECISION,
    dbt_status TEXT,
    error_message TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);
"""

TRAFFIC_COLUMNS = (
    "event_id",
    "trace_id",
    "segment_id",
    "road_name",
    "area_code",
    "area_name",
    "latitude",
    "longitude",
    "start_latitude",
    "start_longitude",
    "end_latitude",
    "end_longitude",
    "observed_at",
    "ingested_at",
    "source_system",
    "vehicle_type",
    "segment_length_km",
    "speed_kph",
    "reference_speed_kph",
    "traffic_volume",
    "travel_time_seconds",
    "source_congestion_level",
    "source_payload_sha256",
    "schema_version",
    "is_late",
    "speed_index",
    "congestion_level",
)


def connect(settings: Settings | None = None) -> psycopg.Connection[Any]:
    cfg = settings or get_settings()
    return psycopg.connect(cfg.database_url, row_factory=dict_row)


def ensure_schema(settings: Settings | None = None) -> None:
    with connect(settings) as connection:
        connection.execute(SCHEMA_SQL)


def processed_object_keys(
    keys: Sequence[str], settings: Settings | None = None
) -> set[str]:
    if not keys:
        return set()
    with connect(settings) as connection:
        rows = connection.execute(
            "SELECT object_key FROM ops.object_manifest WHERE object_key = ANY(%s)", (list(keys),)
        ).fetchall()
    return {row["object_key"] for row in rows}


def start_pipeline_run(run_id: str, settings: Settings | None = None) -> None:
    now = datetime.now(UTC)
    with connect(settings) as connection:
        connection.execute(
            """
            INSERT INTO ops.pipeline_runs (run_id, started_at, status)
            VALUES (%s, %s, 'RUNNING')
            ON CONFLICT (run_id) DO UPDATE
            SET started_at = EXCLUDED.started_at,
                finished_at = NULL,
                status = 'RUNNING',
                error_message = NULL
            """,
            (run_id, now),
        )


def finish_pipeline_run(
    run_id: str,
    *,
    status: str,
    metrics: dict[str, Any] | None = None,
    dbt_status: str | None = None,
    error_message: str | None = None,
    details: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> None:
    metrics = metrics or {}
    with connect(settings) as connection:
        connection.execute(
            """
            UPDATE ops.pipeline_runs
            SET finished_at = NOW(),
                status = %s,
                source_objects = COALESCE(%s, source_objects),
                input_rows = COALESCE(%s, input_rows),
                accepted_rows = COALESCE(%s, accepted_rows),
                duplicates_removed = COALESCE(%s, duplicates_removed),
                late_rows = COALESCE(%s, late_rows),
                dlq_rows = COALESCE(%s, dlq_rows),
                output_objects = COALESCE(%s, output_objects),
                job_duration_seconds = COALESCE(%s, job_duration_seconds),
                dbt_status = COALESCE(%s, dbt_status),
                error_message = %s,
                details = details || %s::jsonb
            WHERE run_id = %s
            """,
            (
                status,
                metrics.get("source_objects"),
                metrics.get("input_rows"),
                metrics.get("accepted_rows"),
                metrics.get("duplicates_removed"),
                metrics.get("late_rows"),
                metrics.get("dlq_rows"),
                metrics.get("output_objects"),
                metrics.get("job_duration_seconds"),
                dbt_status,
                error_message,
                json.dumps(details or {}),
                run_id,
            ),
        )


def load_traffic_rows(
    rows: Iterable[Sequence[Any]],
    *,
    object_records: Sequence[tuple[str, str, str, int]],
    settings: Settings | None = None,
) -> int:
    row_count = 0
    columns = ", ".join(TRAFFIC_COLUMNS)
    with connect(settings) as connection:
        connection.execute(
            "CREATE TEMP TABLE traffic_observation_stage "
            "(LIKE raw.traffic_observation INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        with connection.cursor().copy(
            f"COPY traffic_observation_stage ({columns}) FROM STDIN"
        ) as copy:
            for row in rows:
                copy.write_row(row)
                row_count += 1

        updates = ", ".join(
            f"{column} = EXCLUDED.{column}" for column in TRAFFIC_COLUMNS if column != "event_id"
        )
        connection.execute(
            f"""
            INSERT INTO raw.traffic_observation ({columns})
            SELECT {columns} FROM traffic_observation_stage
            ON CONFLICT (event_id) DO UPDATE SET {updates}, loaded_at = NOW()
            """
        )
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO ops.object_manifest (object_key, etag, run_id, row_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (object_key) DO UPDATE
                SET etag = EXCLUDED.etag,
                    run_id = EXCLUDED.run_id,
                    row_count = EXCLUDED.row_count,
                    processed_at = NOW()
                """,
                object_records,
            )
    return row_count
