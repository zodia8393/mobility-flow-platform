"""15-minute traffic DataOps orchestration with retries, replay and dbt gates."""

import os
import re
import subprocess
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

SPARK_RUNNER_URL = os.getenv("SPARK_RUNNER_URL", "http://spark-runner:8081")
CONTROL_API_URL = os.getenv("CONTROL_API_URL", "http://api:8000")
OPS_TOKEN = os.getenv("OPS_TOKEN", "local-ops-token")
DBT_BIN = "/opt/dbt/bin/dbt"
DBT_PROJECT_DIR = "/opt/dbt-project"


def _safe_run_id(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]", "-", value)[:100]


def _update_run(run_id: str, payload: dict[str, object]) -> None:
    response = requests.patch(
        f"{CONTROL_API_URL}/ops/runs/{run_id}",
        json=payload,
        headers={"X-Ops-Token": OPS_TOKEN},
        timeout=15,
    )
    response.raise_for_status()


@dag(
    dag_id="mobility_flow_15m",
    description="Seoul live traffic → Spark silver → dbt marts with quality gates",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=12),
    },
    tags=["seoul-open-data", "traffic", "spark", "dbt", "data-quality"],
    doc_md="""
    ### MobilityFlow — Seoul road traffic pipeline
    1. Fetch current road traffic for configured Seoul citydata areas.
    2. Preserve the source response and write canonical Bronze Parquet.
    3. Trigger idempotent Spark deduplication and official congestion mapping.
    4. Build and test dbt marts, then publish a run manifest to the Control Room.

    The API response is preserved before canonical Bronze creation. Spark uses an object
    manifest and `event_id` upsert, so a retry can safely replay an interrupted batch.
    """,
)
def mobility_flow_pipeline() -> None:
    @task(retries=2, retry_delay=timedelta(seconds=30))
    def sync_seoul_live_traffic() -> dict[str, object]:
        response = requests.post(
            f"{CONTROL_API_URL}/ops/sources/seoul-citydata/sync",
            json={"areas": None},
            headers={"X-Ops-Token": OPS_TOKEN},
            timeout=120,
        )
        response.raise_for_status()
        return response.json()

    @task(retries=3, retry_delay=timedelta(seconds=20))
    def wait_for_bronze(sync_report: dict[str, object]) -> dict[str, object]:
        response = requests.get(
            f"{SPARK_RUNNER_URL}/bronze/stats",
            params={"prefix": "bronze/seoul_citydata/"},
            timeout=10,
        )
        response.raise_for_status()
        stats = response.json()
        if stats["objects"] < 1:
            raise RuntimeError("No bronze object is available yet")
        return {**stats, "sync_id": sync_report["sync_id"]}

    @task
    def spark_transform(_bronze_stats: dict[str, object]) -> dict[str, object]:
        context = get_current_context()
        run_id = _safe_run_id(context["dag_run"].run_id)
        response = requests.post(
            f"{SPARK_RUNNER_URL}/jobs/bronze-to-silver",
            json={
                "run_id": run_id,
                "force_reprocess": False,
                "prefix": "bronze/seoul_citydata/",
            },
            timeout=600,
        )
        response.raise_for_status()
        return response.json()

    @task
    def dbt_quality_gate(transform: dict[str, object]) -> dict[str, object]:
        run_id = str(transform["run_id"])
        if transform["status"] == "NO_DATA":
            _update_run(run_id, {"status": "NO_DATA", "dbt_status": "SKIPPED"})
            return {"status": "SKIPPED", "run_id": run_id}
        try:
            build = subprocess.run(
                [
                    DBT_BIN,
                    "build",
                    "--project-dir",
                    DBT_PROJECT_DIR,
                    "--profiles-dir",
                    DBT_PROJECT_DIR,
                    "--target",
                    "local",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=420,
            )
            freshness = subprocess.run(
                [
                    DBT_BIN,
                    "source",
                    "freshness",
                    "--project-dir",
                    DBT_PROJECT_DIR,
                    "--profiles-dir",
                    DBT_PROJECT_DIR,
                    "--target",
                    "local",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=120,
            )
        except subprocess.CalledProcessError as exc:
            error = (exc.stdout or "")[-1500:] + (exc.stderr or "")[-1500:]
            _update_run(
                run_id,
                {"status": "FAILED", "dbt_status": "FAIL", "error_message": error[-2000:]},
            )
            raise
        return {
            "status": "PASS",
            "run_id": run_id,
            "build_tail": build.stdout[-800:],
            "freshness_tail": freshness.stdout[-800:],
        }

    @task
    def publish_success(
        transform: dict[str, object],
        quality: dict[str, object],
        sync_report: dict[str, object],
    ) -> None:
        if quality["status"] == "SKIPPED":
            return
        _update_run(
            str(transform["run_id"]),
            {
                "status": "SUCCESS",
                "dbt_status": "PASS",
                "details": {
                    "orchestrator": "Apache Airflow",
                    "quality_gate": "dbt build + source freshness",
                    "source": "서울시 실시간 도시데이터·도로소통",
                    "source_sync_id": sync_report["sync_id"],
                    "source_rows": sync_report["input_rows"],
                    "source_areas": [
                        area["area_name"] for area in sync_report.get("areas", [])
                    ],
                },
            },
        )

    source_sync = sync_seoul_live_traffic()
    bronze = wait_for_bronze(source_sync)
    transformed = spark_transform(bronze)
    quality = dbt_quality_gate(transformed)
    publish_success(transformed, quality, source_sync)


mobility_flow_pipeline()
