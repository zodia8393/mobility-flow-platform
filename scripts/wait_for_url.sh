#!/usr/bin/env bash
set -euo pipefail

url="${1:?URL is required}"
attempts="${2:-60}"

for ((attempt = 1; attempt <= attempts; attempt++)); do
  if curl --fail --silent --show-error "$url" >/dev/null 2>&1; then
    exit 0
  fi
  sleep 2
done

echo "Timed out waiting for $url" >&2
exit 1
