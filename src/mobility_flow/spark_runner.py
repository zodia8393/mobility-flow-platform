import hashlib
import logging
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Histogram, make_asgi_app
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from mobility_flow.config import get_settings
from mobility_flow.database import (
    TRAFFIC_COLUMNS,
    ensure_schema,
    finish_pipeline_run,
    load_traffic_rows,
    processed_object_keys,
    start_pipeline_run,
)
from mobility_flow.schemas import TransformRequest, TransformResult
from mobility_flow.storage import ObjectInfo, ObjectStore

LOGGER = logging.getLogger(__name__)
SETTINGS = get_settings()
STORE = ObjectStore(SETTINGS)
JOB_LOCK = threading.Lock()

JOB_TOTAL = Counter("mobility_spark_jobs_total", "Spark jobs", ["status"])
JOB_DURATION = Histogram("mobility_spark_job_duration_seconds", "Spark transform duration")
JOB_ROWS = Counter("mobility_spark_rows_total", "Rows processed by Spark", ["kind"])

@asynccontextmanager
async def lifespan(_app: FastAPI) -> Any:
    STORE.ensure_bucket()
    ensure_schema(SETTINGS)
    yield


app = FastAPI(
    title="MobilityFlow Spark Job Runner",
    version="0.1.0",
    description="Idempotent bronze-to-silver Spark job endpoint used by Airflow.",
    lifespan=lifespan,
)
app.mount("/metrics", make_asgi_app())


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "spark_master": SETTINGS.spark_master}


@app.get("/bronze/stats")
def bronze_stats(prefix: str = "bronze/traffic_observations/") -> dict[str, int | str]:
    objects = list(STORE.list_objects(prefix))
    return {
        "prefix": prefix,
        "objects": len(objects),
        "bytes": sum(item.size for item in objects),
    }


def _new_run_id() -> str:
    return f"run-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def _spark_session(run_id: str) -> SparkSession:
    return (
        SparkSession.builder.appName(f"mobility-flow-{run_id}")
        .master(SETTINGS.spark_master)
        .config("spark.driver.memory", SETTINGS.spark_driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )


def _download(objects: list[ObjectInfo], directory: Path) -> list[Path]:
    def fetch(item: ObjectInfo) -> Path:
        destination = directory / "bronze" / item.key.replace("/", "__")
        STORE.download_file(item.key, destination)
        return destination

    workers = min(8, max(1, len(objects)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(fetch, objects))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _traffic_rows(frame: Any) -> Any:
    for row in frame.select(*TRAFFIC_COLUMNS).toLocalIterator():
        yield tuple(row[column] for column in TRAFFIC_COLUMNS)


def _run_transform(request: TransformRequest) -> TransformResult:
    run_id = request.run_id or _new_run_id()
    started = time.monotonic()
    start_pipeline_run(run_id, SETTINGS)
    spark: SparkSession | None = None
    try:
        all_objects = list(STORE.list_objects(request.prefix))
        if request.force_reprocess:
            objects = all_objects
        else:
            already_processed = processed_object_keys([item.key for item in all_objects], SETTINGS)
            objects = [item for item in all_objects if item.key not in already_processed]

        if not objects:
            duration = time.monotonic() - started
            metrics = {"source_objects": 0, "job_duration_seconds": duration}
            finish_pipeline_run(run_id, status="NO_DATA", metrics=metrics, settings=SETTINGS)
            JOB_TOTAL.labels(status="no_data").inc()
            return TransformResult(
                run_id=run_id,
                status="NO_DATA",
                job_duration_seconds=round(duration, 3),
                details={"available_bronze_objects": len(all_objects)},
            )

        with tempfile.TemporaryDirectory(prefix="mobility-flow-") as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            local_files = _download(objects, temp_dir)
            object_rows = {
                item.key: pq.ParquetFile(path).metadata.num_rows
                for item, path in zip(objects, local_files, strict=True)
            }

            spark = _spark_session(run_id)
            frame = spark.read.parquet(*[str(path) for path in local_files])
            frame = (
                frame.withColumn("observed_at", F.to_timestamp("observed_at"))
                .withColumn("ingested_at", F.to_timestamp("ingested_at"))
                .withColumn("segment_length_km", F.col("segment_length_km").cast("double"))
                .withColumn("speed_kph", F.col("speed_kph").cast("double"))
                .withColumn("reference_speed_kph", F.col("reference_speed_kph").cast("double"))
                .withColumn("traffic_volume", F.col("traffic_volume").cast("int"))
                .withColumn("travel_time_seconds", F.col("travel_time_seconds").cast("double"))
            )
            input_rows = frame.count()

            latest_event = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())
            deduplicated = (
                frame.withColumn("_row_number", F.row_number().over(latest_event))
                .filter(F.col("_row_number") == 1)
                .drop("_row_number")
            )
            accepted_rows = deduplicated.count()
            duplicates_removed = input_rows - accepted_rows

            speed_index = F.col("speed_kph") / F.col("reference_speed_kph")
            transformed = (
                deduplicated.withColumn(
                    "is_late",
                    (F.unix_timestamp("ingested_at") - F.unix_timestamp("observed_at")) > 900,
                )
                .withColumn("speed_index", F.round(speed_index, 6))
                .withColumn(
                    "congestion_level",
                    F.when(speed_index <= 0.30, F.lit("SEVERE"))
                    .when(speed_index <= 0.50, F.lit("CONGESTED"))
                    .when(speed_index <= 0.70, F.lit("SLOW"))
                    .otherwise(F.lit("SMOOTH")),
                )
                .cache()
            )
            late_rows = transformed.filter(F.col("is_late")).count()

            silver_dir = temp_dir / "silver"
            transformed.select(*TRAFFIC_COLUMNS).write.mode("overwrite").option(
                "compression", "zstd"
            ).parquet(str(silver_dir))
            output_files = sorted(silver_dir.glob("part-*.parquet"))
            delivery_artifacts = []
            for output_file in output_files:
                object_key = (
                    f"silver/traffic_observations/run_id={run_id}/{output_file.name}"
                )
                STORE.upload_file(output_file, object_key)
                delivery_artifacts.append(
                    {
                        "object_key": object_key,
                        "size_bytes": output_file.stat().st_size,
                        "row_count": pq.ParquetFile(output_file).metadata.num_rows,
                        "sha256": _sha256(output_file),
                    }
                )

            manifest_rows = [
                (item.key, item.etag, run_id, object_rows[item.key]) for item in objects
            ]
            loaded_rows = load_traffic_rows(
                _traffic_rows(transformed), object_records=manifest_rows, settings=SETTINGS
            )
            transformed.unpersist()

        duration = time.monotonic() - started
        metrics = {
            "source_objects": len(objects),
            "input_rows": input_rows,
            "accepted_rows": loaded_rows,
            "duplicates_removed": duplicates_removed,
            "late_rows": late_rows,
            "output_objects": len(output_files),
            "job_duration_seconds": duration,
        }
        finish_pipeline_run(
            run_id,
            status="TRANSFORMED",
            metrics=metrics,
            details={
                "bronze_prefix": request.prefix,
                "spark_master": SETTINGS.spark_master,
                "row_reconciliation": input_rows == loaded_rows + duplicates_removed,
                "delivery_artifacts": delivery_artifacts,
                "source_manifest": [
                    {
                        "object_key": item.key,
                        "etag": item.etag,
                        "row_count": object_rows[item.key],
                    }
                    for item in objects
                ],
            },
            settings=SETTINGS,
        )
        JOB_TOTAL.labels(status="transformed").inc()
        JOB_ROWS.labels(kind="input").inc(input_rows)
        JOB_ROWS.labels(kind="accepted").inc(loaded_rows)
        JOB_DURATION.observe(duration)
        rounded_metrics = {
            key: round(value, 3) if key == "job_duration_seconds" else value
            for key, value in metrics.items()
        }
        return TransformResult(
            run_id=run_id,
            status="TRANSFORMED",
            **rounded_metrics,
            details={"spark_master": SETTINGS.spark_master},
        )
    except Exception as exc:
        duration = time.monotonic() - started
        LOGGER.exception("Spark transform failed: run_id=%s", run_id)
        finish_pipeline_run(
            run_id,
            status="FAILED",
            metrics={"job_duration_seconds": duration},
            error_message=str(exc)[:2000],
            settings=SETTINGS,
        )
        JOB_TOTAL.labels(status="failed").inc()
        raise
    finally:
        if spark is not None:
            spark.stop()


@app.post("/jobs/bronze-to-silver", response_model=TransformResult)
def bronze_to_silver(request: TransformRequest) -> TransformResult:
    if not JOB_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="another Spark job is already running")
    try:
        return _run_transform(request)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Spark job failed: {exc}") from exc
    finally:
        JOB_LOCK.release()
