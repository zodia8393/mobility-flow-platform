import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a machine-readable failure drill report")
    parser.add_argument("--first-result", required=True)
    parser.add_argument("--replay-result", required=True)
    parser.add_argument("--recovery-seconds", type=float, required=True)
    parser.add_argument("--buffered-events", type=int, required=True)
    parser.add_argument("--dlq-total", type=float, required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    first = json.loads(args.first_result)
    replay = json.loads(args.replay_result)
    assertions = {
        "backlog_recovered": first.get("input_rows", 0) > 0,
        "invalid_events_quarantined": args.dlq_total > 0,
        "replay_is_idempotent": replay.get("status") == "NO_DATA",
    }
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "drill": "bronze-writer outage → Kafka backlog → recovery → replay",
        "buffered_events": args.buffered_events,
        "recovery_seconds": round(args.recovery_seconds, 3),
        "dlq_total": args.dlq_total,
        "first_transform": first,
        "immediate_replay": replay,
        "assertions": assertions,
        "passed": all(assertions.values()),
    }


def main() -> None:
    args = parse_args()
    report = build_report(args)
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(path), "passed": report["passed"]}))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
