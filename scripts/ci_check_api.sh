#!/usr/bin/env bash
# Check the containerised API serves real lakehouse data and exposes metrics.
set -euo pipefail

API="${API_URL:-http://localhost:8000}"

echo "--- /health ---"
curl -sf "${API}/health"
echo

echo "--- curated bars ---"
curl -sf "${API}/api/v1/prices/MSFT?limit=2" -o /tmp/prices.json
cat /tmp/prices.json
echo
grep -q '"rows":2' /tmp/prices.json
# The first bar of a ticker has no previous close. NaN is not valid JSON, so
# this is the check that the API emits null instead of something unparseable.
python3 -c "import json; d=json.load(open('/tmp/prices.json')); assert d['rows']==2; print('bars parse as valid JSON:', [b['Date'] for b in d['bars']])"

echo "--- Prometheus exposition ---"
curl -sf "${API}/metrics" -o /tmp/metrics.txt
for metric in rlrp_api_requests_total rlrp_api_request_duration_seconds; do
  if grep -q "$metric" /tmp/metrics.txt; then
    echo "  found $metric"
  else
    echo "  MISSING $metric"
    exit 1
  fi
done

# The endpoint label must be the route template, not the raw path - labelling
# per-ticker would create unbounded cardinality.
if grep -q 'endpoint="/api/v1/prices/{ticker}"' /tmp/metrics.txt; then
  echo "  endpoint label uses the route template (bounded cardinality)"
else
  echo "  WARNING: expected templated endpoint label not found"
  grep 'rlrp_api_requests_total{' /tmp/metrics.txt | head -5
fi

echo "API checks passed"
