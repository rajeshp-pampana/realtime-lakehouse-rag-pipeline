#!/usr/bin/env bash
# In-cluster check of the service split's core claim: the UI reaches the API by
# Kubernetes Service name, using only the API_BASE_URL its ConfigMap gives it.
#
# This is the same substitution compose makes with http://api:8000, which is
# why API_BASE_URL was kept separate from the API's own bind address back in
# Milestone 4 - the split carries to k8s with a config change and no code change.
#
# On timing: `kubectl rollout status` returning does NOT mean the Service is
# routable. Endpoint programming is eventually consistent, and a Service with no
# ready endpoints yet gets its connections actively REJECTED by kube-proxy -
# which surfaces as "Connection refused" from inside the pod and looks exactly
# like a broken API. CI hit this with a demonstrably healthy API pod (uvicorn up,
# serving 200s to its own probe) purely because the check raced endpoint setup.
# So: wait for a ready endpoint first, then retry the request itself.
set -uo pipefail

NS="${NAMESPACE:-rlrp}"
ENDPOINT_TIMEOUT="${ENDPOINT_TIMEOUT:-90}"
REQUEST_ATTEMPTS="${REQUEST_ATTEMPTS:-12}"

echo "=== waiting for the API Service to have a ready endpoint ==="
ready=0
for i in $(seq 1 "$ENDPOINT_TIMEOUT"); do
  addresses=$(kubectl -n "$NS" get endpoints rlrp-api \
    -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null)
  if [ -n "${addresses:-}" ]; then
    echo "  ready endpoint(s) after ${i}s: ${addresses}"
    ready=1
    break
  fi
  sleep 1
done

if [ "$ready" = "0" ]; then
  echo "FAILED: rlrp-api Service still has no ready endpoints after ${ENDPOINT_TIMEOUT}s"
  kubectl -n "$NS" get endpoints,svc,pods -o wide
  kubectl -n "$NS" describe endpoints rlrp-api
  exit 1
fi

UI_POD="$(kubectl -n "$NS" get pod -l app.kubernetes.io/component=ui \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"

if [ -z "${UI_POD:-}" ]; then
  echo "no Running UI pod found in namespace ${NS}"
  kubectl -n "$NS" get pods -o wide
  exit 1
fi
echo "ui pod: ${UI_POD}"

echo
echo "=== UI pod -> API Service over cluster DNS ==="
for attempt in $(seq 1 "$REQUEST_ATTEMPTS"); do
  if kubectl -n "$NS" exec "$UI_POD" -- python -c "
import os, urllib.request, sys
base = os.environ.get('API_BASE_URL')
if not base:
    print('API_BASE_URL is not set in the UI pod')
    sys.exit(1)
print('UI sees API_BASE_URL=' + base)
with urllib.request.urlopen(base + '/health', timeout=15) as r:
    body = r.read().decode()
print('API replied: ' + body)
assert '\"status\":\"ok\"' in body, 'unexpected health response'
print('UI -> API over cluster DNS: OK')
"; then
    exit 0
  fi
  echo "  attempt ${attempt}/${REQUEST_ATTEMPTS} failed; retrying in 5s"
  sleep 5
done

echo "FAILED: the UI pod could not reach the API Service"
echo "--- endpoints ---";  kubectl -n "$NS" get endpoints -o wide
echo "--- services ---";   kubectl -n "$NS" get svc -o wide
echo "--- pods ---";       kubectl -n "$NS" get pods -o wide
echo "--- api logs ---";   kubectl -n "$NS" logs -l app.kubernetes.io/component=api --tail 40
exit 1
