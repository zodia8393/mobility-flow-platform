from datetime import datetime

import pytest

from mobility_flow.runtime_check import verify_runtime


class FakeS3:
    def __init__(self, key_count: int) -> None:
        self.key_count = key_count
        self.get_calls: list[dict[str, object]] = []

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        assert kwargs["Bucket"] == "private-lake"
        assert kwargs["MaxKeys"] == 1
        contents = [{"Key": "raw/traffic.json"}] if self.key_count else []
        return {"KeyCount": self.key_count, "Contents": contents}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.get_calls.append(kwargs)
        return {"Body": FakeBody(b"{")}


class FakeBody:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self, amount: int) -> bytes:
        return self.payload[:amount]


class FakeCloudWatch:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def put_metric_data(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        return {}


def test_runtime_check_reads_s3_and_publishes_health_metric() -> None:
    cloudwatch = FakeCloudWatch()
    s3 = FakeS3(1)
    result = verify_runtime(
        s3,
        cloudwatch,
        bucket="private-lake",
        environment="dev",
    )

    assert result == {
        "status": "PASS",
        "s3_readable_objects": 1,
        "cloudwatch_metric": "CloudRuntimeHealthy",
        "environment": "dev",
    }
    assert s3.get_calls == [
        {"Bucket": "private-lake", "Key": "raw/traffic.json", "Range": "bytes=0-0"}
    ]
    assert cloudwatch.calls[0]["Namespace"] == "MobilityFlow/Pipeline"
    metric = cloudwatch.calls[0]["MetricData"][0]
    assert metric["MetricName"] == "CloudRuntimeHealthy"
    assert isinstance(metric["Timestamp"], datetime)


def test_runtime_check_fails_closed_when_lake_is_empty() -> None:
    with pytest.raises(RuntimeError, match="no data lake object"):
        verify_runtime(
            FakeS3(0),
            FakeCloudWatch(),
            bucket="private-lake",
            environment="dev",
        )


def test_runtime_check_fails_closed_when_object_body_is_empty() -> None:
    class EmptyBodyS3(FakeS3):
        def get_object(self, **kwargs: object) -> dict[str, object]:
            return {"Body": FakeBody(b"")}

    with pytest.raises(RuntimeError, match="could not be read"):
        verify_runtime(
            EmptyBodyS3(1),
            FakeCloudWatch(),
            bucket="private-lake",
            environment="dev",
        )
