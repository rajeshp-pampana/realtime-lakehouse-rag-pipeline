#!/usr/bin/env bash
# Wait for the containerised API to become healthy, or fail loudly with logs.
set -uo pipefail

API="${API_URL:-http://localhost:8000}"
DEADLINE="${DEADLINE_SECONDS:-90}"

for i in $(seq 1 "$DEADLINE"); do
  if curl -sf "${API}/health" >/dev/null 2>&1; then
    echo "API healthy after ${i}s"
    exit 0
  fi
  sleep 1
done

echo "API never became healthy within ${DEADLINE}s"
docker compose -f infra/docker-compose.yml logs api | tail -40
exit 1
