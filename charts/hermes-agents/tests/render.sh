#!/usr/bin/env bash
set -euo pipefail

chart="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT

helm lint "$chart" --set-string baseDomain=example.test
helm template hermes-agents "$chart" --namespace hermes-agents \
  --set-string baseDomain=example.test >"$rendered"

assert_rendered() {
  grep -Fq "$1" "$rendered" || {
    echo "missing rendered contract: $1" >&2
    return 1
  }
}

assert_rendered 'value: hermes'
assert_rendered 'app: hermes-agent'
assert_rendered 'port: 9119'
assert_rendered 'port: 8642'
assert_rendered 'docker.io/nousresearch/hermes-agent:v2026.7.7.2@sha256:3db34ce19adfa080736a2a3feb0316dbcccc588faa9afe7fd8ae1c03b4f1a53a'
assert_rendered '/apis/agents.x-k8s.io/v1beta1'
assert_rendered 'resources: ["pods"]'
assert_rendered 'command: ["kubectl"]'
assert_rendered 'test -s /secret/key'
assert_rendered 'secretName: "hermes-api"'
assert_rendered 'runAsUser: 65532'
assert_rendered 'runAsGroup: 65532'
assert_rendered 'name: hermes-landing'
assert_rendered 'value: "hermes"'
assert_rendered 'host: "agents.example.test"'
assert_rendered 'path: /healthz'
assert_rendered 'image: ghcr.io/veelabs/dev-environments-landing:0.4.0'
assert_rendered 'image: ghcr.io/veelabs/dev-environments-provisioner:0.5.0'
assert_rendered 'verbs: ["get"]'
assert_rendered 'verbs: ["get", "list", "watch", "create", "delete", "update"]'
