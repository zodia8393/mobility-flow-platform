import json
from datetime import UTC, datetime
from typing import Protocol

import boto3

from mobility_flow.config import get_settings


class S3Client(Protocol):
    def list_objects_v2(self, **kwargs: object) -> dict[str, object]: ...

    def get_object(self, **kwargs: object) -> dict[str, object]: ...


class CloudWatchClient(Protocol):
    def put_metric_data(self, **kwargs: object) -> dict[str, object]: ...


def verify_runtime(
    s3_client: S3Client,
    cloudwatch_client: CloudWatchClient,
    *,
    bucket: str,
    environment: str,
) -> dict[str, object]:
    response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    contents = response.get("Contents")
    if not isinstance(contents, list) or not contents:
        raise RuntimeError("runtime check failed: no data lake object is readable")

    first_object = contents[0]
    if not isinstance(first_object, dict) or not isinstance(first_object.get("Key"), str):
        raise RuntimeError("runtime check failed: S3 listing returned no readable object key")

    object_response = s3_client.get_object(
        Bucket=bucket,
        Key=first_object["Key"],
        Range="bytes=0-0",
    )
    body = object_response.get("Body")
    if body is None or not getattr(body, "read", None) or not body.read(1):
        raise RuntimeError("runtime check failed: listed S3 object could not be read")

    cloudwatch_client.put_metric_data(
        Namespace="MobilityFlow/Pipeline",
        MetricData=[
            {
                "MetricName": "CloudRuntimeHealthy",
                "Value": 1.0,
                "Unit": "Count",
                "Timestamp": datetime.now(UTC),
                "Dimensions": [{"Name": "Environment", "Value": environment}],
            }
        ],
    )
    return {
        "status": "PASS",
        "s3_readable_objects": 1,
        "cloudwatch_metric": "CloudRuntimeHealthy",
        "environment": environment,
    }


def main() -> None:
    settings = get_settings()
    result = verify_runtime(
        boto3.client("s3", region_name=settings.aws_region),
        boto3.client("cloudwatch", region_name=settings.aws_region),
        bucket=settings.s3_bucket,
        environment=settings.app_env,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
