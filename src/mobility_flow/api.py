import hashlib
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import Gauge, Histogram, make_asgi_app

from mobility_flow.config import get_settings
from mobility_flow.database import connect, ensure_schema, finish_pipeline_run
from mobility_flow.schemas import PipelineRunUpdate, SeoulSyncRequest
from mobility_flow.seoul_citydata import (
    SEOUL_CITYDATA_DATASET_URL,
    SEOUL_CITYDATA_LICENSE,
    SEOUL_CITYDATA_SOURCE_URL,
    SeoulCityDataError,
    sync_seoul_citydata,
)
from mobility_flow.storage import ObjectStore

LOGGER = logging.getLogger(__name__)
SETTINGS = get_settings()
STORE = ObjectStore(SETTINGS)
STATIC_DIR = Path(__file__).parent / "static"

FRESHNESS = Gauge("mobility_pipeline_freshness_seconds", "Age of newest observed event")
RAW_ROWS = Gauge("mobility_warehouse_raw_rows", "Rows loaded to raw warehouse")
CONGESTED = Gauge("mobility_congested_segments", "Latest road segments at severe congestion")
LAST_JOB_DURATION = Gauge("mobility_last_job_duration_seconds", "Duration of last pipeline job")
LAST_RUN_SUCCESS = Gauge("mobility_last_run_success", "1 when the last pipeline run succeeded")
DBT_TEST_SUCCESS = Gauge("mobility_dbt_test_success", "1 when the last dbt build succeeded")
HTTP_DURATION = Histogram(
    "mobility_api_request_duration_seconds", "Dashboard API latency", ["method", "path"]
)

@asynccontextmanager
async def lifespan(_app: FastAPI) -> Any:
    STORE.ensure_bucket()
    ensure_schema(SETTINGS)
    yield


app = FastAPI(
    title="MobilityFlow Traffic DataOps API",
    version="0.1.0",
    description="Operational view of Seoul public real-time road traffic ingestion and quality.",
    lifespan=lifespan,
)
app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")
app.mount("/metrics", make_asgi_app())


@app.middleware("http")
async def observe_requests(request: Request, call_next: Any) -> Any:
    started = time.monotonic()
    response = await call_next(request)
    path = request.url.path if request.url.path.startswith("/api/") else "other"
    HTTP_DURATION.labels(method=request.method, path=path).observe(time.monotonic() - started)
    return response


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "environment": SETTINGS.app_env}


@app.get("/ready")
def ready() -> dict[str, str]:
    with connect(SETTINGS) as connection:
        connection.execute("SELECT 1").fetchone()
    STORE.count_objects("bronze/")
    return {"status": "ready"}


def _last_run(connection: Any) -> dict[str, Any] | None:
    return connection.execute(
        """
        SELECT run_id, started_at, finished_at, status, source_objects, input_rows,
               accepted_rows, duplicates_removed, late_rows, dlq_rows, output_objects,
               job_duration_seconds, dbt_status, error_message, details
        FROM ops.pipeline_runs
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()


def _last_successful_run(connection: Any) -> dict[str, Any] | None:
    return connection.execute(
        """
        SELECT run_id, started_at, finished_at, status, source_objects, input_rows,
               accepted_rows, duplicates_removed, late_rows, dlq_rows, output_objects,
               job_duration_seconds, dbt_status, error_message, details
        FROM ops.pipeline_runs
        WHERE status = 'SUCCESS'
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()


def build_delivery_manifest(run: dict[str, Any] | None) -> dict[str, Any]:
    if not run:
        return {
            "delivery_status": "WAITING",
            "run_id": None,
            "checks": [],
            "artifacts": [],
            "message": "성공한 pipeline run을 기다리는 중입니다.",
        }
    details = run.get("details") or {}
    artifacts = details.get("delivery_artifacts") or []
    checks = [
        {
            "name": "SOURCE_OBJECTS_PRESENT",
            "status": "PASS" if run.get("source_objects", 0) > 0 else "FAIL",
            "value": run.get("source_objects", 0),
        },
        {
            "name": "ROW_RECONCILIATION",
            "status": (
                "PASS"
                if run.get("input_rows", 0)
                == run.get("accepted_rows", 0) + run.get("duplicates_removed", 0)
                else "FAIL"
            ),
            "value": (
                f"{run.get('input_rows', 0)} = {run.get('accepted_rows', 0)} + "
                f"{run.get('duplicates_removed', 0)}"
            ),
        },
        {
            "name": "DBT_QUALITY_GATE",
            "status": "PASS" if run.get("dbt_status") == "PASS" else "FAIL",
            "value": run.get("dbt_status") or "WAITING",
        },
        {
            "name": "ARTIFACT_HASH_MANIFEST",
            "status": (
                "PASS"
                if artifacts
                and len(artifacts) == run.get("output_objects", 0)
                and all(item.get("sha256") for item in artifacts)
                else "FAIL"
            ),
            "value": len(artifacts),
        },
    ]
    delivery_status = "READY" if all(item["status"] == "PASS" for item in checks) else "BLOCKED"
    manifest_core = {
        "run_id": run.get("run_id"),
        "delivery_status": delivery_status,
        "checks": checks,
        "artifacts": artifacts,
    }
    manifest_id = hashlib.sha256(
        json.dumps(manifest_core, sort_keys=True, ensure_ascii=False, default=str).encode()
    ).hexdigest()
    return {
        **manifest_core,
        "manifest_id": manifest_id,
        "generated_at": datetime.now(UTC),
        "late_rows_preserved": run.get("late_rows", 0),
        "claim_boundary": (
            "READY는 이 run의 기술적 품질 gate 통과이며 "
            "정책 승인·납품 승인을 대체하지 않습니다."
        ),
    }


@app.get("/api/overview")
def overview() -> dict[str, Any]:
    with connect(SETTINGS) as connection:
        traffic = connection.execute(
            """
            WITH latest AS (
                SELECT DISTINCT ON (segment_id)
                       segment_id, area_code, observed_at, congestion_level, source_system
                FROM raw.traffic_observation
                WHERE source_system = 'SEOUL_TOPIS'
                ORDER BY segment_id, observed_at DESC
            )
            SELECT
                (SELECT COUNT(*) FROM raw.traffic_observation
                 WHERE source_system = 'SEOUL_TOPIS') AS raw_rows,
                COUNT(*) AS segment_count,
                COUNT(DISTINCT source_system) AS source_count,
                COUNT(DISTINCT area_code) AS area_count,
                COUNT(*) FILTER (
                    WHERE congestion_level IN ('SEVERE', 'CONGESTED')
                ) AS congested_segments,
                MAX(observed_at) AS latest_observed_at
            FROM latest
            """
        ).fetchone()
        last_run = _last_run(connection)
        last_successful_run = _last_successful_run(connection)

    latest = traffic["latest_observed_at"]
    freshness_seconds = max(0, (datetime.now(UTC) - latest).total_seconds()) if latest else None
    raw_rows = traffic["raw_rows"] or 0
    congested_segments = traffic["congested_segments"] or 0
    success = bool(last_run and last_run["status"] == "SUCCESS")
    dbt_success = bool(last_successful_run and last_successful_run["dbt_status"] == "PASS")

    RAW_ROWS.set(raw_rows)
    CONGESTED.set(congested_segments)
    LAST_RUN_SUCCESS.set(int(success))
    DBT_TEST_SUCCESS.set(int(dbt_success))
    if freshness_seconds is not None:
        FRESHNESS.set(freshness_seconds)
    if last_run and last_run["job_duration_seconds"] is not None:
        LAST_JOB_DURATION.set(last_run["job_duration_seconds"])

    if not last_run:
        platform_status = "WAITING"
    elif last_run["status"] == "FAILED":
        platform_status = "FAILED"
    elif freshness_seconds is not None and freshness_seconds > 1800:
        platform_status = "STALE"
    elif last_run["status"] in {"SUCCESS", "TRANSFORMED", "NO_DATA"} and latest:
        platform_status = "HEALTHY"
    else:
        platform_status = last_run["status"]

    return {
        "platform_status": platform_status,
        "raw_rows": raw_rows,
        "segment_count": traffic["segment_count"] or 0,
        "source_count": traffic["source_count"] or 0,
        "area_count": traffic["area_count"] or 0,
        "congested_segments": congested_segments,
        "freshness_seconds": round(freshness_seconds, 1) if freshness_seconds is not None else None,
        "latest_observed_at": latest,
        "last_run": last_run,
        "last_successful_run": last_successful_run,
        "live_data": bool(raw_rows),
        "data_provider": "서울특별시·서울 열린데이터광장",
    }


@app.get("/api/segments")
def segments() -> list[dict[str, Any]]:
    with connect(SETTINGS) as connection:
        return connection.execute(
            """
            SELECT DISTINCT ON (segment_id)
                   segment_id, road_name, area_code, area_name,
                   latitude, longitude, start_latitude, start_longitude,
                   end_latitude, end_longitude, observed_at,
                   source_system, vehicle_type, segment_length_km,
                   speed_kph, reference_speed_kph, traffic_volume,
                   travel_time_seconds, speed_index, congestion_level, is_late,
                   source_payload_sha256
            FROM raw.traffic_observation
            WHERE source_system = 'SEOUL_TOPIS'
            ORDER BY segment_id, observed_at DESC
            """
        ).fetchall()


@app.get("/api/source")
def source_status() -> dict[str, Any]:
    with connect(SETTINGS) as connection:
        row = connection.execute(
            """
            WITH latest_snapshot AS (
                SELECT MAX(observed_at) AS observed_at
                FROM raw.traffic_observation
                WHERE source_system = 'SEOUL_TOPIS'
            )
            SELECT
                latest_snapshot.observed_at AS snapshot_at,
                MAX(item.ingested_at) AS retrieved_at,
                COUNT(item.event_id) AS records,
                COUNT(DISTINCT item.segment_id) AS unique_links,
                ARRAY_AGG(DISTINCT item.area_name ORDER BY item.area_name)
                    FILTER (WHERE item.area_name IS NOT NULL) AS areas,
                COUNT(*) FILTER (WHERE item.congestion_level = 'CONGESTED') AS congested,
                COUNT(*) FILTER (WHERE item.congestion_level = 'SLOW') AS slow,
                COUNT(*) FILTER (WHERE item.congestion_level = 'SMOOTH') AS smooth,
                ARRAY_AGG(
                    DISTINCT item.source_payload_sha256
                    ORDER BY item.source_payload_sha256
                ) FILTER (WHERE item.source_payload_sha256 IS NOT NULL) AS payload_sha256s
            FROM latest_snapshot
            LEFT JOIN raw.traffic_observation item
              ON item.observed_at = latest_snapshot.observed_at
             AND item.source_system = 'SEOUL_TOPIS'
            GROUP BY latest_snapshot.observed_at
            """
        ).fetchone()
    return {
        "provider": "서울특별시·서울 열린데이터광장",
        "dataset": "서울시 실시간 도시데이터·도로소통",
        "dataset_url": SEOUL_CITYDATA_DATASET_URL,
        "source_url": SEOUL_CITYDATA_SOURCE_URL,
        "license": SEOUL_CITYDATA_LICENSE,
        "update_interval_minutes": 5,
        "api_key_configured": bool(SETTINGS.seoul_open_data_api_key),
        **(row or {}),
    }


@app.get("/api/runs")
def runs(limit: int = 10) -> list[dict[str, Any]]:
    limit = min(max(limit, 1), 50)
    with connect(SETTINGS) as connection:
        return connection.execute(
            """
            SELECT run_id, started_at, finished_at, status, source_objects,
                   input_rows, accepted_rows, duplicates_removed, late_rows,
                   output_objects, job_duration_seconds, dbt_status, error_message, details
            FROM ops.pipeline_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        ).fetchall()


@app.get("/api/delivery/latest")
def latest_delivery() -> dict[str, Any]:
    with connect(SETTINGS) as connection:
        return build_delivery_manifest(_last_successful_run(connection))


@app.get("/api/services")
def services() -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    try:
        with connect(SETTINGS) as connection:
            connection.execute("SELECT 1")
        result["postgres"] = {"status": "UP", "role": "Warehouse"}
    except Exception as exc:
        LOGGER.warning("Postgres health check failed: %s", exc)
        result["postgres"] = {"status": "DOWN", "role": "Warehouse"}
    try:
        STORE.count_objects("bronze/")
        result["object_store"] = {
            "status": "UP",
            "role": "AWS S3" if SETTINGS.is_aws_object_store else "MinIO S3",
        }
    except Exception as exc:
        LOGGER.warning("Object-store health check failed: %s", exc)
        result["object_store"] = {"status": "DOWN", "role": "Object Storage"}
    try:
        metadata = AdminClient(
            {"bootstrap.servers": SETTINGS.kafka_bootstrap_servers}
        ).list_topics(timeout=2)
        topic_ok = SETTINGS.kafka_topic in metadata.topics
        result["kafka"] = {"status": "UP" if topic_ok else "DEGRADED", "role": "Redpanda"}
    except Exception as exc:
        LOGGER.warning("Kafka health check failed: %s", exc)
        result["kafka"] = {"status": "DOWN", "role": "Redpanda"}
    try:
        source = source_status()
        if source.get("records"):
            result["seoul_open_data"] = {"status": "UP", "role": "Live source"}
        elif SETTINGS.seoul_open_data_api_key:
            result["seoul_open_data"] = {"status": "WAITING", "role": "Live source"}
        else:
            result["seoul_open_data"] = {"status": "NOT_CONFIGURED", "role": "Live source"}
    except Exception as exc:
        LOGGER.warning("Seoul source status check failed: %s", exc)
        result["seoul_open_data"] = {"status": "DOWN", "role": "Live source"}
    return result


@app.post("/ops/sources/seoul-citydata/sync")
def sync_seoul_source(
    request: SeoulSyncRequest,
    x_ops_token: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    if x_ops_token != SETTINGS.ops_token:
        raise HTTPException(status_code=401, detail="invalid operations token")
    try:
        return sync_seoul_citydata(
            areas=tuple(request.areas) if request.areas else None,
            settings=SETTINGS,
            store=STORE,
        )
    except SeoulCityDataError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.patch("/ops/runs/{run_id}")
def update_run(
    run_id: str,
    update: PipelineRunUpdate,
    x_ops_token: Annotated[str | None, Header()] = None,
) -> dict[str, str]:
    if x_ops_token != SETTINGS.ops_token:
        raise HTTPException(status_code=401, detail="invalid operations token")
    finish_pipeline_run(
        run_id,
        status=update.status,
        dbt_status=update.dbt_status,
        error_message=update.error_message,
        details=update.details,
        settings=SETTINGS,
    )
    return {"run_id": run_id, "status": update.status}
