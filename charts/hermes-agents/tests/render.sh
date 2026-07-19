#!/usr/bin/env bash
set -euo pipefail

chart="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rendered="$(mktemp)"
rendered_custom="$(mktemp)"
rendered_no_monitoring="$(mktemp)"
trap 'rm -f "$rendered" "$rendered_custom" "$rendered_no_monitoring"' EXIT

helm lint "$chart" --set-string baseDomain=example.test \
  --set-string router.hostname=homelab-server.example.ts.net \
  --set-string backup.repository=sftp:user@nas:/repo \
  --set-string backup.maintenance.schedule='30 6 * * *'
helm template hermes-agents "$chart" --namespace hermes-agents \
	--set-string baseDomain=example.test \
	--set-string router.hostname=homelab-server.example.ts.net \
	--set-string backup.repository=sftp:user@nas:/repo \
	--set-string backup.maintenance.schedule='30 6 * * *' >"$rendered"
helm template hermes-agents "$chart" --namespace hermes-agents \
	--set-string baseDomain=example.test \
	--set-string router.hostname=homelab-server.example.ts.net \
	--set-string backup.repository=sftp:user@nas:/repo \
	--set-string backup.maintenance.schedule='30 6 * * *' \
	--set-string 'hermes.gitAllowedHosts[0]=github.com' \
	--set-string 'hermes.gitAllowedHosts[1]=gitlab.com' >"$rendered_custom"
helm template hermes-agents "$chart" --namespace hermes-agents \
	--set-string baseDomain=example.test \
	--set-string router.hostname=homelab-server.example.ts.net \
	--set-string backup.repository=sftp:user@nas:/repo \
	--set-string backup.maintenance.schedule='30 6 * * *' \
	--set monitoring.enabled=false >"$rendered_no_monitoring"

assert_rendered() {
  grep -Fq "$1" "${2:-$rendered}" || {
    echo "missing rendered contract: $1" >&2
    return 1
  }
}

assert_not_rendered() {
  if grep -Fq "$1" "${2:-$rendered}"; then
    echo "unexpected rendered contract: $1" >&2
    return 1
  fi
}

assert_images_pinned() {
  if grep -E '^[[:space:]]+image: ' "$1" | grep -vF '@sha256:'; then
    echo "rendered image is not pinned by digest" >&2
    return 1
  fi
}

assert_rendered 'value: hermes'
assert_rendered 'app: hermes-agent'
assert_rendered 'port: 9119'
assert_rendered 'port: 8642'
assert_rendered 'docker.io/nousresearch/hermes-agent:v2026.7.7.2@sha256:3db34ce19adfa080736a2a3feb0316dbcccc588faa9afe7fd8ae1c03b4f1a53a'
assert_rendered '/apis/agents.x-k8s.io/v1beta1'
assert_rendered '/apis/traefik.io/v1alpha1'
assert_rendered 'resources: ["pods"]'
assert_rendered 'resources: ["pods/log"]'
assert_rendered 'command: ["kubectl"]'
assert_rendered 'test -s /secret/key'
assert_rendered 'secretName: "hermes-api"'
assert_rendered 'runAsUser: 65532'
assert_rendered 'runAsGroup: 65532'
assert_rendered 'name: hermes-landing'
assert_rendered 'value: "hermes"'
assert_rendered 'host: "agents.example.test"'
assert_rendered 'path: /healthz'
assert_rendered 'image: ghcr.io/veelabs/dev-environments-landing:0.9.0@sha256:1e7d4a164d3f04b1c7796050dbfca3b646ed25d5befda982a5936d5aa1b9d0bf'
assert_rendered 'image: ghcr.io/veelabs/dev-environments-provisioner:0.10.0@sha256:416fe0ee177c7acf5a16d922f646bf933df35b22d7b8c522b960028d4c29c5c6'
assert_rendered 'verbs: ["get", "list"]'
assert_rendered 'verbs: ["get", "list", "watch", "create", "delete", "update"]'
assert_rendered 'resources: ["persistentvolumeclaims"]'
assert_rendered 'name: hermes-api-router'
assert_rendered 'app: hermes-api-router'
assert_rendered 'nodePort: 30864'
assert_rendered 'externalTrafficPolicy: Local'
assert_rendered 'node-role.kubernetes.io/control-plane: "true"'
assert_rendered 'cidr: 100.64.0.0/10'
assert_rendered 'value: ".hermes-agents.svc.cluster.local:8642"'
assert_rendered 'value: "http://homelab-server.example.ts.net:30864"'
assert_rendered 'name: HERMES_GIT_ALLOWED_HOSTS'
assert_rendered 'value: "github.com"'
assert_rendered 'value: "github.com,gitlab.com"' "$rendered_custom"
assert_rendered 'mountPath: /tmp'
assert_rendered 'sizeLimit: 64Mi'
assert_rendered 'ephemeral-storage: 16Mi'
assert_rendered 'ephemeral-storage: 64Mi'
assert_rendered 'resources: ["jobs"]'
assert_rendered 'verbs: ["get", "list", "watch", "create", "delete"]'
assert_rendered 'verbs: ["get", "list", "create", "delete"]'
assert_rendered 'name: hermes-bootstrap-deny-all'
assert_rendered 'renala.dev/hermes-bootstrap: "true"'
assert_rendered 'policyTypes: ["Ingress", "Egress"]'
assert_rendered 'docker.io/restic/restic:0.19.1@sha256:136600b6ff6843d61d355f7f71f460a166429f35de6fd11b568fece3c9a4d510'
assert_rendered 'name: HERMES_BACKUP_REPOSITORY'
assert_rendered 'value: "sftp:user@nas:/repo"'
assert_rendered 'test -s /secret/RESTIC_PASSWORD && test -s /secret/ssh-privatekey'
assert_rendered 'secretName: "hermes-backup"'
assert_rendered 'name: HERMES_BACKUP_HOUR_UTC'
assert_rendered 'name: HERMES_BACKUP_STAGGER_MINUTES'
assert_rendered 'resources: ["cronjobs"]'
assert_rendered 'name: hermes-backup-maintenance'
assert_rendered 'schedule: "30 6 * * *"'
assert_rendered 'concurrencyPolicy: Forbid'
assert_rendered 'forget --tag hermes-agent --group-by host --keep-daily 7 --keep-weekly 4 --keep-monthly 6'
assert_rendered 'restic_nas prune'
assert_rendered 'restic_nas check'
assert_not_rendered 'forget --tag scheduled'
assert_rendered 'kind: PrometheusRule'
assert_rendered 'release: monitoring'
assert_rendered 'alert: HermesBackupJobFailed'
assert_rendered 'kube_job_status_failed'
assert_rendered 'alert: HermesBackupStale'
assert_rendered 'kube_cronjob_status_last_successful_time'
assert_rendered 'kube_cronjob_created'
assert_rendered '26 * 3600'
assert_rendered 'unless on(namespace, cronjob)'
assert_rendered '/apis/monitoring.coreos.com/v1'
assert_rendered 'args: ["get", "--raw", "/apis/monitoring.coreos.com/v1"]'
assert_rendered 'args: ["get", "--raw", "/apis/traefik.io/v1alpha1"]'
assert_rendered 'args: ["wait", "nodes", "--selector=node-role.kubernetes.io/control-plane=true", "--for=condition=Ready", "--timeout=30s"]'
assert_not_rendered 'kind: PrometheusRule' "$rendered_no_monitoring"
assert_not_rendered '/apis/monitoring.coreos.com/v1' "$rendered_no_monitoring"
assert_images_pinned "$rendered"
