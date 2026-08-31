#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

if [[ -z "${SEOUL_OPEN_DATA_API_KEY:-}" ]] && ! grep -Eq '^SEOUL_OPEN_DATA_API_KEY=.+$' .env; then
  echo "SEOUL_OPEN_DATA_API_KEY is required. Add the free Seoul Open Data Plaza key to .env." >&2
  exit 2
fi

evidence_dir="$project_dir/docs/evidence/generated"
mkdir -p "$evidence_dir"

docker compose up -d --build
bash scripts/wait_for_url.sh http://localhost:18000/ready 120
bash scripts/wait_for_url.sh http://localhost:18081/health 120
bash scripts/wait_for_url.sh http://localhost:18080/health 120

import_errors="$(docker compose exec -T airflow-scheduler airflow dags list-import-errors --output json)"
if [[ "$import_errors" != "[]" ]]; then
  echo "$import_errors" >&2
  exit 1
fi

run_id="live__$(date -u +%Y%m%dT%H%M%SZ)"
docker compose exec -T airflow-scheduler airflow dags trigger \
  --run-id "$run_id" mobility_flow_15m >/dev/null

RUN_ID="$run_id" python3 - <<'PY'
import json
import os
import subprocess
import time

run_id = os.environ["RUN_ID"]
for _ in range(48):
    output = subprocess.check_output(
        [
            "docker", "compose", "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "list-runs", "-d", "mobility_flow_15m", "-o", "json",
        ],
        text=True,
    )
    runs = json.loads(output[output.find("["):])
    run = next((item for item in runs if item["run_id"] == run_id), None)
    if run and run["state"] in {"success", "failed"}:
        if run["state"] != "success":
            raise SystemExit(f"Airflow run failed: {run_id}")
        print(f"Airflow run succeeded: {run_id}")
        break
    time.sleep(5)
else:
    raise SystemExit(f"Airflow run timed out: {run_id}")
PY

docker compose run --rm -T producer mobility-evidence \
  --base-url http://api:8000 --output - > "$evidence_dir/live_run.json"

printf 'Control Room: http://localhost:18000\n'
printf 'Airflow:      http://localhost:18080\n'
printf 'Grafana:      http://localhost:13000\n'
printf 'Evidence manifest generated.\n'
