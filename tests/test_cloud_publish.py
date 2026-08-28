from mobility_flow.cloud_publish import metric_data


def test_cloudwatch_metrics_are_derived_from_run_evidence() -> None:
    overview = {
        "freshness_seconds": 42.5,
        "raw_rows": 1234,
        "last_run": {"dbt_status": "PASS", "job_duration_seconds": 8.2},
    }
    metrics = {item["MetricName"]: item["Value"] for item in metric_data(overview)}
    assert metrics == {
        "FreshnessSeconds": 42.5,
        "DataQualityFailures": 0.0,
        "JobDurationSeconds": 8.2,
        "WarehouseRows": 1234.0,
    }
