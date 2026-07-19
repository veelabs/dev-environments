# Hermes Agents Runbook

The `charts/hermes-agents` release manages persistent Hermes agents as durable
Temporal entity workflows. Each agent has a stable `agent-...` identity and a
retained PVC; its Sandbox is disposable runtime. Use the landing page for
normal operations. Use Temporal and Kubernetes directly only for diagnosis.

Environment-specific edge, tailnet, backup, and certification procedures belong
in the deploying cluster's runbook.

## Create an agent

Open `https://agents.<baseDomain>` and submit a DNS-safe name plus optional
`SOUL.md` text. The landing page proposes an adjective-animal name. Creation
progresses through storage, optional profile seeding, Sandbox startup, routing,
and health verification before reporting `running`.

An advanced profile source may be either:

- a public HTTPS Git repository on a `hermes.gitAllowedHosts` hostname; or
- a UTF-8 text-only Hermes distribution ZIP up to 1 MiB and 250 files.

Git clones are shallow and do not fetch submodules or Git LFS content. ZIPs may
contain native Hermes distribution files such as `SOUL.md`, `config.yaml`, MCP
configuration, skills, and cron definitions. Credentials, memories, sessions,
databases, logs, caches, binaries, symlinks, traversal paths, and malformed or
conflicting entries are rejected.

The form is authoritative: its name always selects the Kubernetes identity and
a non-empty form `SOUL.md` replaces the distribution's `SOUL.md`. The source is
applied only while creating the new PVC. Start, restore, and image upgrades do
not fetch or reapply it. To verify seed-once behavior, change a seeded file from
the dashboard, stop and start the agent, and confirm the changed value remains.

Configure model-provider credentials only after launch through the Hermes
dashboard. Never put provider keys in the creation form, Temporal input, or an
operator drill record.

## Credentials

Each dashboard keeps generated Hermes basic-auth credentials in its Kubernetes
Secret. Use **Reveal credentials** on the Access-gated landing page, then sign
in at `https://<agent-id>.<baseDomain>`. Use **Rotate password** after suspected
disclosure; existing sessions and the old password must stop working.

The landing page also reveals the platform-wide API Bearer token and a client
example. That token authorizes every agent API, so store it as a secret and
rotate the configured API Secret if it is exposed. Dashboard passwords, API
tokens, provider keys, cookies, and sensitive chat content must not enter Git,
workflow input, issue comments, or drill evidence.

## Private API clients

The router has one environment-specific tailnet URL. Every request supplies the
stable agent ID and shared Bearer token:

```sh
api=http://<tailnet-router-host>:<node-port>
agent=agent-calm-fox
token='<revealed bearer token>'

curl -fsS \
  -H "X-Hermes-Agent: $agent" \
  -H "Authorization: Bearer $token" \
  "$api/health/detailed"

curl -N \
  -H "X-Hermes-Agent: $agent" \
  -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' \
  --data '{"model":"<configured-model>","messages":[{"role":"user","content":"Reply with: stream-ok"}],"stream":true}' \
  "$api/v1/chat/completions"
```

`400 missing-agent` means the routing header is absent. `400 invalid-agent`
means it is not an exact managed identity. `503 agent-unavailable` means the
agent is stopped, unknown, or unhealthy. Public edge traffic and sources outside
`router.tailnetCIDR` must not reach this endpoint.

## Lifecycle

| Action | Runtime | PVC | Dashboard Secret | Backup schedule | Catalog entity | NAS snapshots |
|---|---|---|---|---|---|---|
| Stop | Removed | Retained | Retained | Retained | Retained | Retained |
| Start | Recreated | Reused | Reused | Retained | Retained | Retained |
| Delete data | Removed | Deleted after final backup | Retained | Removed | `backup-only` | Retained |
| Force delete data | Removed | Deleted without a new successful backup | Retained | Removed | `backup-only` | Retained |
| Restore | Recreated after import | Fresh 5 GiB | Reused | Recreated | Retained | Unchanged |
| Forget | Must be absent | Must be absent | Deleted | Absent | Removed | Retained |
| Temporal cancellation | Best-effort runtime cleanup | Retained | Retained | Retained | Closed | Retained |
| Temporal termination | No cleanup | May be orphaned | Retained | Retained | Closed | Retained |

Use **Stop**, not Temporal cancellation, for normal shutdown. Stopped agents
remain listed and protected by scheduled backups. **Start** reuses the PVC and
credentials, creates runtime with the chart's current Hermes image, and waits
for dashboard and API health.

Literal cancellation is an emergency close operation that attempts runtime
cleanup and removes the agent from the active Temporal catalog. Literal
termination skips workflow cleanup by definition and is unsupported. After a
termination, inspect and remove orphaned Sandbox, Service, and Ingress resources
before deciding whether to recover or forget the retained data.

## Image upgrades

All deployed images must use stable tags plus OCI digests. Update the image tag
and digest together in chart values, run both chart render tests and image smoke
tests, then deploy normally. Running agents are not rolled. Record their current
image, stop and start one canary agent, verify its persisted state and new image,
then upgrade other agents at operator-selected boundaries.

## Capacity and Pending agents

There is no application concurrency limit. Kubernetes schedules each agent with
the chart's CPU and memory requests; excess agents remain Pending. Diagnose the
actual constraint instead of deleting retained data:

```sh
kubectl -n hermes-agents get sandboxes,pods,pvc
kubectl -n hermes-agents describe pod <pending-agent-pod>
kubectl get nodes
```

Common causes are insufficient CPU or memory, an unbound local-path PVC, image
pull failure, and node affinity. A bound local-path PVC is node-local; adding a
node does not move an existing agent's volume. Stop another runtime, increase
cluster capacity, or restore from NAS to replace lost local storage.

## Backups and alerts

Every retained PVC has one daily CronJob, including stopped agents. A job uses
native `hermes backup`, verifies the archive, and uploads it to restic with host
`<agent-id>` and tags `hermes-agent` and `agent:<agent-id>`. Maintenance keeps 7
daily, 4 weekly, and 6 monthly snapshots grouped by host.

The landing card reports the latest scheduled attempt, success, failure, and
next run. `HermesBackupJobFailed` detects failed Jobs and `HermesBackupStale`
detects a schedule with no success for 26 hours, including one that has never
succeeded. After changing schedules, update the stale threshold or explicitly
accept the mismatch.

For a safe alert test, create a disposable Job from one backup CronJob with an
intentionally invalid repository override, confirm the failed-job alert becomes
pending/firing, then delete only that test Job. Test staleness with the chart's
Prometheus rule tests rather than suspending a real agent's protection for 26
hours.

Never run `restic unlock` while any data-tier, Hermes backup, restore, or
maintenance writer is active. Restic host and tag values are selectors, not
authorization boundaries.

## Delete, restore, and Forget

**Delete data** requires the full agent identity. It stops runtime, creates and
verifies a final backup, then removes the PVC and schedule while retaining the
entity, dashboard Secret, and snapshots in `backup-only`. If the final backup
fails, repair the reported archive/NAS problem and retry.

Use **Force delete data** only after a failed final backup and only when changes
newer than the last successful snapshot may be lost. It requires the full
identity again.

In `backup-only`, select a dated snapshot by its full ID and restore. Restore
creates a fresh PVC, downloads that exact agent-scoped snapshot, validates its
ZIP and SQLite files, runs native `hermes import`, then starts and health-checks
the current image. A failed restore removes partial PVC data, never starts
Hermes, and leaves the source snapshot untouched for retry.

Before deletion, write a harmless sentinel and record the visible SOUL/provider
configuration without secrets. After restore, verify dashboard login, private
API health, the sentinel, and a real chat.

**Forget** is available only without runtime or PVC. Type the exact identity to
delete the dashboard Secret and close/remove the durable catalog entity. Forget
does not delete NAS snapshots. Closed Temporal history remains only for the
server's configured retention period.

## Accepted risks

- Agent egress is unrestricted, so tools can reach public, cluster, LAN, and
  tailnet destinations. Service-account tokens are disabled; destination auth
  and ingress policy remain the barriers.
- Each PVC uses local-path storage and can be lost with its node. Scheduled NAS
  backup sets the recovery point, not the live volume.
- One platform API token authorizes every agent.
- The landing page is single-operator and trusts the external Access gate.
- Shared restic credentials can read, alter, or delete unrelated snapshots in
  the same repository.
- Dynamic agent resources are workflow-owned rather than Argo-owned. Literal
  workflow termination can orphan them.
