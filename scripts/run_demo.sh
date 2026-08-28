#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

event_count="${EVENT_COUNT:-5000}"
evidence_dir="$project_dir/docs/evidence/generated"
mkdir -p "$evidence_dir"

docker compose build api spark-runner
docker compose up -d \
  postgres redpanda minio minio-init redpanda-init bronze-writer spark-runner api prometheus grafana
docker compose stop airflow-scheduler airflow-webserver >/dev/null 2>&1 || true

bash scripts/wait_for_url.sh http://localhost:18000/ready 90
bash scripts/wait_for_url.sh http://localhost:18081/health 90

docker compose run --rm producer mobility-ingest-file \
  /app/examples/tcs_15min_sample.csv \
  --source-system TCS \
  --column-map @/app/examples/tcs_column_map.json

docker compose run --rm producer mobility-producer \
  --count "$event_count" \
  --duplicate-rate 0.04 \
  --late-rate 0.05 \
  --invalid-rate 0.02

for _attempt in $(seq 1 60); do
  bronze_objects="$(curl --fail --silent http://localhost:18081/bronze/stats | python3 -c 'import json,sys; print(json.load(sys.stdin)["objects"])')"
  if (( bronze_objects > 0 )); then
    break
  fi
  sleep 2
done
if (( bronze_objects == 0 )); then
  echo "Bronze writer did not create a Parquet object" >&2
  exit 1
fi

docker compose up airflow-init
logical_date="$(date -u +%Y-%m-%dT%H:%M:00+00:00)"
docker compose run --rm airflow-scheduler airflow dags test mobility_flow_15m "$logical_date"

docker compose up -d airflow-webserver airflow-scheduler
bash scripts/wait_for_url.sh http://localhost:18080/health 90

docker compose run --rm -T producer mobility-evidence \
  --base-url http://api:8000 --output - > "$evidence_dir/run_evidence.json"

printf 'Control Room: http://localhost:18000\n'
printf 'Airflow:      http://localhost:18080\n'
printf 'Grafana:      http://localhost:13000\n'
printf 'Evidence:     %s\n' "$evidence_dir/run_evidence.json"
