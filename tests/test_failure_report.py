import argparse
import json

from mobility_flow.failure_report import build_report


def test_failure_report_requires_recovered_backlog_dlq_and_noop_replay() -> None:
    args = argparse.Namespace(
        first_result=json.dumps({"status": "TRANSFORMED", "input_rows": 900}),
        replay_result=json.dumps({"status": "NO_DATA", "input_rows": 0}),
        recovery_seconds=12.5,
        buffered_events=1000,
        dlq_total=50,
        output="unused.json",
    )
    report = build_report(args)
    assert report["passed"] is True
    assert all(report["assertions"].values())
