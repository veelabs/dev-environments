#!/usr/bin/env bash
set -euo pipefail

chart="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT

helm lint "$chart" \
  --set-string baseDomain=example.test \
  --set-string clusterDNS=10.43.0.10
helm template dev-environments "$chart" --namespace dev-environments \
  --set-string baseDomain=example.test \
  --set-string clusterDNS=10.43.0.10 >"$rendered"

if grep -E '^[[:space:]]+image: ' "$rendered" | grep -vF '@sha256:'; then
  echo "rendered image is not pinned by digest" >&2
  exit 1
fi
