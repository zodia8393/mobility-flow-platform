from mobility_flow.api import build_delivery_manifest


def test_delivery_manifest_is_ready_only_after_reconciliation_dbt_and_hashes() -> None:
    run = {
        "run_id": "run-1",
        "source_objects": 2,
        "input_rows": 100,
        "accepted_rows": 95,
        "duplicates_removed": 5,
        "late_rows": 3,
        "output_objects": 1,
        "dbt_status": "PASS",
        "details": {
            "delivery_artifacts": [
                {"object_key": "silver/run-1/part.parquet", "sha256": "a" * 64}
            ]
        },
    }

    manifest = build_delivery_manifest(run)

    assert manifest["delivery_status"] == "READY"
    assert all(check["status"] == "PASS" for check in manifest["checks"])
    assert len(manifest["manifest_id"]) == 64


def test_delivery_manifest_blocks_row_mismatch() -> None:
    run = {
        "run_id": "run-2",
        "source_objects": 1,
        "input_rows": 100,
        "accepted_rows": 90,
        "duplicates_removed": 5,
        "output_objects": 1,
        "dbt_status": "PASS",
        "details": {"delivery_artifacts": [{"sha256": "b" * 64}]},
    }

    assert build_delivery_manifest(run)["delivery_status"] == "BLOCKED"
