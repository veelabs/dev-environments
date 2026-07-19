#!/usr/bin/env bash
set -euo pipefail

tests="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chart="$(cd "$tests/.." && pwd)"
rendered="$tests/.rendered-prometheus-rules.yaml"
trap 'rm -f "$rendered"' EXIT

helm template hermes-agents "$chart" --namespace hermes-agents \
  --show-only templates/monitoring.yaml \
  --set-string baseDomain=example.test \
  --set-string router.hostname=homelab-server.example.ts.net \
  --set-string backup.repository=sftp:user@nas:/repo | \
  python3 -c '
import pathlib, sys
document = sys.stdin.read()
spec = document.split("\nspec:\n", 1)[1]
pathlib.Path(sys.argv[1]).write_text("\n".join(line[2:] if line.startswith("  ") else line for line in spec.splitlines()) + "\n")
' "$rendered"

if [[ -n "${PROMTOOL:-}" ]]; then
  "$PROMTOOL" test rules "$tests/prometheus-rules.test.yaml"
elif command -v promtool >/dev/null; then
  promtool test rules "$tests/prometheus-rules.test.yaml"
else
  docker run --rm -v "$tests:/work" -w /work --entrypoint promtool \
    prom/prometheus:v3.13.1 test rules prometheus-rules.test.yaml
fi
