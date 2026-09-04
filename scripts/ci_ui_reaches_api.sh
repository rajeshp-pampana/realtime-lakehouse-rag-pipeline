#!/usr/bin/env bash
# In-cluster check of the service split's core claim: the UI reaches the API by
# Kubernetes Service name, using only the API_BASE_URL its ConfigMap gives it.
#
# This is the same substitution compose makes with http://api:8000, which is
# why API_BASE_URL was kept separate from the API's own bind address back in
# Milestone 4 - the split carries to k8s with a config change and no code change.
set -euo pipefail

NS="${NAMESPACE:-rlrp}"

UI_POD="$(kubectl -n "$NS" get pod -l app.kubernetes.io/component=ui \
  -o jsonpath='{.items[0].metadata.name}')"

if [ -z "$UI_POD" ]; then
  echo "no UI pod found in namespace ${NS}"
  kubectl -n "$NS" get pods
  exit 1
fi
echo "ui pod: ${UI_POD}"

kubectl -n "$NS" exec "$UI_POD" -- python -c "
import os, urllib.request, sys
base = os.environ.get('API_BASE_URL')
if not base:
    print('API_BASE_URL is not set in the UI pod')
    sys.exit(1)
print('UI sees API_BASE_URL=' + base)
with urllib.request.urlopen(base + '/health', timeout=30) as r:
    body = r.read().decode()
print('API replied: ' + body)
assert '\"status\":\"ok\"' in body, 'unexpected health response'
print('UI -> API over cluster DNS: OK')
"
