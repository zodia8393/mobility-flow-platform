import argparse
from datetime import UTC, datetime

import boto3
import httpx

from mobility_flow.config import get_settings


def metric_data(overview: dict[str, object]) -> list[dict[str, object]]:
    last_run = overview.get("last_run") or {}
    return [
        {
            "MetricName": "FreshnessSeconds",
            "Value": float(overview.get("freshness_seconds") or 0),
            "Unit": "Seconds",
        },
        {
            "MetricName": "DataQualityFailures",
            "Value": 0.0 if last_run.get("dbt_status") == "PASS" else 1.0,
            "Unit": "Count",
        },
        {
            "MetricName": "JobDurationSeconds",
            "Value": float(last_run.get("job_duration_seconds") or 0),
            "Unit": "Seconds",
        },
        {
            "MetricName": "WarehouseRows",
            "Value": float(overview.get("raw_rows") or 0),
            "Unit": "Count",
        },
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish measured pipeline evidence to CloudWatch")
    parser.add_argument("--base-url", default="http://localhost:18000")
    parser.add_argument("--namespace", default="MobilityFlow/Pipeline")
    args = parser.parse_args()

    settings = get_settings()
    overview = httpx.get(f"{args.base_url}/api/overview", timeout=15).raise_for_status().json()
    dimensions = [{"Name": "Environment", "Value": settings.app_env}]
    timestamp = datetime.now(UTC)
    metrics = [
        {**metric, "Timestamp": timestamp, "Dimensions": dimensions}
        for metric in metric_data(overview)
    ]
    boto3.client("cloudwatch", region_name=settings.aws_region).put_metric_data(
        Namespace=args.namespace,
        MetricData=metrics,
    )
    print(f"published={len(metrics)} namespace={args.namespace} region={settings.aws_region}")


if __name__ == "__main__":
    main()
