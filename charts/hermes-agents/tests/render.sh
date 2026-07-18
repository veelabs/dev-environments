#!/usr/bin/env bash
set -euo pipefail

chart="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT

helm lint "$chart" --set-string baseDomain=example.test
helm template hermes-agents "$chart" --namespace hermes-agents \
  --set-string baseDomain=example.test >"$rendered"

grep -Fq 'value: hermes' "$rendered"
grep -Fq 'app: hermes-agent' "$rendered"
grep -Fq 'port: 9119' "$rendered"
grep -Fq 'port: 8642' "$rendered"
grep -Fq 'docker.io/nousresearch/hermes-agent:v2026.7.7.2@sha256:3db34ce19adfa080736a2a3feb0316dbcccc588faa9afe7fd8ae1c03b4f1a53a' "$rendered"
grep -Fq '/apis/agents.x-k8s.io/v1beta1' "$rendered"
