#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"

buffered_events="${DRILL_EVENT_COUNT:-2000}"
evidence_file="$project_dir/docs/evidence/generated/failure_drill.json"
mkdir -p "$(dirname "$evidence_file")"

bash scripts/wait_for_url.sh http://localhost:18081/health 30
before_objects="$(curl --fail --silent http://localhost:18081/bronze/stats | python3 -c 'import json,sys; print(json.load(sys.stdin)["objects"])')"
started_at="$(date +%s)"

docker compose stop bronze-writer
docker compose run --rm producer mobility-producer \
  --count "$buffered_events" --seed 20260829 \
  --duplicate-rate 0.10 --late-rate 0.10 --invalid-rate 0.05
docker compose start bronze-writer

stable_count=0
last_objects="$before_objects"
for _attempt in $(seq 1 90); do
  current_objects="$(curl --fail --silent http://localhost:18081/bronze/stats | python3 -c 'import json,sys; print(json.load(sys.stdin)["objects"])')"
  if (( current_objects > before_objects )) && [[ "$current_objects" == "$last_objects" ]]; then
    stable_count=$((stable_count + 1))
  else
    stable_count=0
  fi
  if (( stable_count >= 3 )); then
    break
  fi
  last_objects="$current_objects"
  sleep 2
done
if (( stable_count < 3 )); then
  echo "Backlog recovery did not stabilize" >&2
  exit 1
fi

run_id="failure-drill-$(date -u +%Y%m%dT%H%M%SZ)"
first_result="$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"run_id\":\"$run_id\",\"force_reprocess\":false}" \
  http://localhost:18081/jobs/bronze-to-silver)"
replay_result="$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  -d "{\"run_id\":\"$run_id-replay\",\"force_reprocess\":false}" \
  http://localhost:18081/jobs/bronze-to-silver)"

finished_at="$(date +%s)"
recovery_seconds="$((finished_at - started_at))"
dlq_total="$(curl --fail --silent http://localhost:19101/metrics | awk '/^mobility_bronze_dlq_total/{sum += $NF} END {print sum + 0}')"

uv run mobility-failure-report \
  --first-result "$first_result" \
  --replay-result "$replay_result" \
  --recovery-seconds "$recovery_seconds" \
  --buffered-events "$buffered_events" \
  --dlq-total "$dlq_total" \
  --output "$evidence_file"
