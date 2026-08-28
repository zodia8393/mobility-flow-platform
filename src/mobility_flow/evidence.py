import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export the latest operational run manifest")
    parser.add_argument("--base-url", default="http://api:8000")
    parser.add_argument("--output", default="-")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with httpx.Client(base_url=args.base_url, timeout=20) as client:
        payload = {
            "generated_at": datetime.now(UTC).isoformat(),
            "project": "MobilityFlow DataOps",
            "data_scope": "Seoul public real-time citydata road traffic observations",
            "source": client.get("/api/source").raise_for_status().json(),
            "overview": client.get("/api/overview").raise_for_status().json(),
            "services": client.get("/api/services").raise_for_status().json(),
            "delivery": client.get("/api/delivery/latest").raise_for_status().json(),
            "recent_runs": client.get("/api/runs?limit=5").raise_for_status().json(),
            "latest_segments": client.get("/api/segments").raise_for_status().json(),
        }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    if args.output == "-":
        print(rendered)
    else:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
