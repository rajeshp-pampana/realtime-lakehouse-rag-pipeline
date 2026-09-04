#!/usr/bin/env bash
# The real streaming proof: ticks published by the producer container must
# reach Delta through the Spark consumer container and show up as rows that did
# not exist before.
#
# The baseline is read from the live API and the script ABORTS if it cannot be
# read. An earlier version of this check (run locally during Milestone 5) read
# its baseline before the API was up, silently fell back to 0, and would have
# reported pre-existing rows as newly landed - a test whose baseline defaults to
# zero cannot fail.
set -uo pipefail

API="${API_URL:-http://localhost:8000}"
ATTEMPTS="${ATTEMPTS:-30}"
INTERVAL="${INTERVAL:-10}"

ticks_rows() {
  curl -sf "${API}/api/v1/lakehouse/stats" 2>/dev/null | python3 -c "
import sys, json
tables = json.load(sys.stdin)['tables']
row = next((t for t in tables if t['name'] == 'ticks_raw'), None)
print(row['rows'] if row and row.get('available') else 0)
" 2>/dev/null
}

before="$(ticks_rows)"
if [ -z "${before:-}" ]; then
  echo "FATAL: could not read a baseline tick count from ${API}"
  echo "refusing to run a test whose result would be meaningless"
  exit 1
fi
echo "ticks_raw rows BEFORE (verified readable): ${before}"

for i in $(seq 1 "$ATTEMPTS"); do
  sleep "$INTERVAL"
  now="$(ticks_rows)"
  if [ -z "${now:-}" ]; then
    echo "  t+$((i * INTERVAL))s  (stats unreadable)"
    continue
  fi
  echo "  t+$((i * INTERVAL))s  ticks_raw=${now}  (baseline ${before})"
  if [ "$now" -gt "$before" ] 2>/dev/null; then
    echo "NEW ROWS LANDED: $((now - before)) after $((i * INTERVAL))s"
    exit 0
  fi
done

echo "FAILED: no new tick rows landed after $((ATTEMPTS * INTERVAL))s"
docker compose -f infra/docker-compose.yml logs stream-consumer | tail -60
echo "--- producer ---"
docker compose -f infra/docker-compose.yml logs tick-producer | tail -20
exit 1
