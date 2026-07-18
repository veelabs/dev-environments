# dev-environments

On-demand cloud dev environments: [OpenCode](https://opencode.ai) +
[OpenChamber](https://openchamber.dev) sandboxes on Kubernetes, provisioned by
a [Temporal](https://temporal.io) workflow, running on
[Agent Sandbox](https://agent-sandbox.sigs.k8s.io) CRDs.

Each environment is an ephemeral, TTL-bound pod reachable at
`https://oc-<id>.<baseDomain>`; any HTTP server started inside it is instantly
public at `https://oc-<id>-<port>.<baseDomain>` (Codespaces-style, incl. the
`Host: localhost` rewrite so Vite/Rails/etc. need zero config).

```
landing page (https://oc.<baseDomain>) ─┐
temporal CLI ───────────────────────────┴▶ Temporal ──▶ provisioner worker (Go)
                                │ SandboxClaim ──▶ agent-sandbox ──▶ pod (+ headless svc)
                                ├ per-env Service + Ingress  (oc-<id>.<baseDomain>)
                                └ durable TTL timer ──▶ teardown
port-router (nginx) ◀── HostRegexp IngressRoute (oc-<id>-<port>.<baseDomain>)
```

## Claim a devbox (landing page)

`https://oc.<baseDomain>` (`landing.subdomain`, inside the existing wildcard
edge route) serves a one-click "claim a devbox" page (`landing` Deployment).
The button starts a `ProvisionDevEnvironment` workflow with a
short TTL (`landing.claimTTL`, default 1h) and streams the workflow's `status`
query as live progress steps until the environment URL appears. A capacity
gate (`landing.maxConcurrent`) refuses new claims while too many environments
are running. Disable with `landing.enabled: false`.

## Deploy

Helm chart at `charts/dev-environments`. Designed for Argo CD (the preflight
dependency check is an Argo `PreSync` hook); plain `helm install` works too
(hooks map to Helm pre-install).

```yaml
# Argo CD Application source
source:
  repoURL: https://github.com/veelabs/dev-environments.git
  targetRevision: main
  path: charts/dev-environments
  helm:
    valuesObject:
      baseDomain: example.com     # REQUIRED
      clusterDNS: 10.43.0.10      # REQUIRED (k3s default shown)
```

## Cluster dependencies (consumed, not installed)

Verified by the PreSync `preflight-check` Job unless noted:

| Dependency | Requirement |
|---|---|
| [agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox) | ≥ v0.5.0, core **and** extensions manifests; `SandboxClaim` v1alpha1 served (the worker uses it — see `docs/runbook.md` for why) |
| Temporal | frontend gRPC reachable at `temporal.hostPort`; worker uses task queue `temporal.taskQueue` in namespace `temporal.namespace` |
| Traefik | default ingress class; `IngressRoute` CRD (`traefik.io/v1alpha1`); reachable at `healthProbeURL` for provision-time health checks |
| Edge (⚠ not machine-checked) | wildcard `*.<baseDomain>` DNS + route to the ingress; TLS terminated upstream (pods speak plain HTTP) |
| CNI (⚠ not machine-checked) | enforces NetworkPolicy incl. `endPort` (Kubernetes ≥ 1.25) |
| Registry | anonymous pull of `ghcr.io/veelabs/dev-environments-{sandbox,provisioner,landing}` |

Security note: environments have no built-in auth. Gate `oc-*.<baseDomain>`
at the edge (e.g. Cloudflare Access) or accept public exposure knowingly.

## Operate

```sh
temporal workflow start --task-queue dev-environments \
  --type ProvisionDevEnvironment --workflow-id "dev-env-$(date +%s)" \
  --input '{"ttl":"8h"}'                      # provision (result/query has the URL)
temporal workflow query  --workflow-id ... --type status    # get URL/phase
temporal workflow cancel --workflow-id ...                  # early teardown
```

Full operational guide: [`docs/runbook.md`](docs/runbook.md).

## Persistent Hermes tracer

The independent `charts/hermes-agents` release runs a dedicated Temporal
worker for persistent Hermes agents. Start and inspect the first tracer from
the Temporal CLI:

```sh
temporal workflow start --task-queue hermes-agents \
  --type ProvisionHermesAgent --workflow-id agent-calm-fox \
  --input '{"name":"calm-fox","soul":"# Calm Fox"}'
temporal workflow query --workflow-id agent-calm-fox --type status
temporal workflow cancel --workflow-id agent-calm-fox
```

Cancellation removes the Sandbox, Service, and Ingress. The `5Gi` PVC and
generated dashboard credential Secret remain in `hermes-agents`; retrieve the
password with `kubectl -n hermes-agents get secret agent-calm-fox -o jsonpath='{.data.password}' | base64 -d`.

## Layout

| Path | What |
|---|---|
| `charts/dev-environments/` | Helm chart (the deployable) |
| `charts/hermes-agents/` | Independent persistent Hermes tracer chart |
| `provisioner/` | Go module: Temporal worker (workflows + k8s activities) and landing server |
| `images/sandbox/` | OpenCode + OpenChamber sandbox image |
| `images/provisioner/` | worker image |
| `images/landing/` | landing page image |
| `docs/` | runbook + design notes |

## Versions

Images are pinned by tag in `values.yaml`; CI (`.github/workflows/images.yaml`)
builds `dev-environments-sandbox:<opencode>-<openchamber>`,
`dev-environments-provisioner:<appVersion>`, and
`dev-environments-landing:<appVersion>` on change.
