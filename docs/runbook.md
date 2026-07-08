# Dev Environments Runbook

On-demand OpenCode + OpenChamber environments (ADR-025). A Temporal workflow
provisions a Sandbox (Agent Sandbox CRDs) plus an Ingress, keeps it alive for a
TTL, and tears it down. Public at `oc-<id>.renala.dev`, behind Cloudflare Access.

## Provision

From a tailnet machine (the Temporal frontend rides the NodePort, no auth —
tailnet trust boundary):

```sh
temporal --address homelab-server.cuscus-pompano.ts.net:30233 \
  workflow start \
  --task-queue dev-environments \
  --type ProvisionDevEnvironment \
  --workflow-id "dev-env-$(date +%s)" \
  --input '{"ttl": "8h"}'
```

Input fields (all optional): `name` (DNS label, default `oc-<unix-ts>`) and
`ttl` (Go duration string like `"8h"` or `"90m"`; default 8h, capped at 24h).
Omitting `--input` entirely gives the defaults.

Get the URL while it runs:

```sh
temporal --address ... workflow query \
  --workflow-id dev-env-<...> --type status
# → {"phase":"ready","envId":"oc-1234567890","url":"https://oc-1234567890.renala.dev", ...}
```

The same URL is also the workflow result at completion, and in the workflow's
logger output (`temporal.renala.dev` UI → workflow → history).

## Tear down early

```sh
temporal --address ... workflow cancel --workflow-id dev-env-<...>
```

Cancellation wakes the TTL timer; the workflow deletes the Ingress and the
SandboxClaim (which cascades to Sandbox → Pod → Service) before completing.

## Orphan cleanup

If an environment's resources exist but its workflow is gone (terminated, not
cancelled), run the idempotent cleanup workflow:

```sh
temporal --address ... workflow start \
  --task-queue dev-environments \
  --type DeprovisionDevEnvironment \
  --workflow-id "deprovision-oc-<id>" \
  --input '"oc-<id>"'
```

Everything the provisioner creates is labelled
`app.kubernetes.io/managed-by=homelab-provisioner`, so a manual sweep is:

```sh
kubectl -n dev-environments get sandboxclaims,ingresses \
  -l app.kubernetes.io/managed-by=homelab-provisioner
```

## Inspect a broken environment

```sh
kubectl -n dev-environments get sandboxclaims,sandboxes,pods,svc,ingress
kubectl -n dev-environments describe sandboxclaim oc-<id>   # conditions
kubectl -n dev-environments logs oc-<id>                    # openchamber logs
kubectl -n dev-environments logs deploy/provisioner         # worker logs
kubectl -n agent-sandbox-system logs deploy/agent-sandbox-controller
```

Common failure modes:

- **Workflow stuck in AwaitSandboxReady** — usually image pull (the sandbox
  image is ~large on first pull per node) or memory pressure. Check pod events.
- **Ingress created but 404/blank page** — Traefik can't reach the pod: check
  the shared NetworkPolicy (`kubectl -n dev-environments get netpol`) still
  admits `kube-system` on 3000, and that the headless Service has endpoints.
- **UI loads but terminal/live updates dead** — WebSocket/SSE breakage at the
  edge; verify the Cloudflare compression rule for `oc-*` hosts (ADR-025) and
  that nothing buffers `/api/event*` routes.
- **503 from Cloudflare** — wildcard hostname or DNS CNAME missing (see
  `docs/runbooks/cloudflare.md`).

## Capacity

Nodes are 2×4GB. Each environment requests 512Mi (limit 1.5Gi). Realistic
ceiling is **2–3 concurrent environments**; beyond that, raise
`K3S_AGENT_COUNT` in `homelab.env` and `make recreate-nodes`.

## Exposing app ports (port-router)

Any HTTP server inside a sandbox is publicly reachable at
`https://oc-<id>-<port>.renala.dev` — no provisioning step:

```sh
# inside the env (OpenChamber terminal):
npx vite --host          # binds 0.0.0.0:5173
# → https://oc-<id>-5173.renala.dev is live immediately
```

Mechanics: Traefik `HostRegexp` routes the `<env>-<port>` hostname shape to the
`port-router` nginx (`charts/dev-environments/templates/port-router.yaml`), which
proxies to the sandbox pod via its headless-Service DNS on the parsed port.
The sandbox NetworkPolicy admits the router on all ports (nothing binds <1024
as non-root; `127.0.0.1`-bound listeners are unreachable regardless — the
router targets the pod IP, so only deliberate `0.0.0.0` listeners are public).

Rules of thumb:

- **Bind `0.0.0.0`**, not `127.0.0.1` (`vite --host`, `next dev -H 0.0.0.0`, …).
- The router rewrites `Host` to `localhost:<port>` (Codespaces-style), so
  Vite/Rails/etc. need **no allowedHosts config**. Apps that build absolute
  URLs should honor `X-Forwarded-Host` (the real public hostname).
- Any unprivileged port (1024+); OpenChamber sits on **1982**, so framework defaults like 3000 are free.
- `502` = nothing listening on that port yet.
- HTTP/WebSocket/SSE only — raw TCP needs a quick tunnel from inside the env
  (`cloudflared tunnel --url http://localhost:<port>`, egress is open).
- Exposed apps share the env's auth posture (currently: none — Access snoozed).
- Don't give envs custom names ending in `-<digits>` — the router would parse
  the tail as a port. Generated names (`oc-<unix-ts>`) are safe.

## Users & credentials

No LLM keys are injected. Each user authenticates inside their environment
(OpenChamber terminal → `opencode auth login`). Credentials live in the pod's
ephemeral filesystem and die with the TTL — by design.

## One-time edge setup (DR checklist)

1. Tunnel public hostname `*.renala.dev` → `http://traefik.kube-system:80`.
2. DNS: `*.renala.dev` CNAME → `<tunnel-id>.cfargotunnel.com`, proxied.
3. Access application for `oc-*.renala.dev` (same policy as argocd/temporal).
4. Optional: disable compression for `oc-*.renala.dev` (SSE double-compression).

## Version bumps

- **opencode/openchamber:** bump `ARG`s in `images/sandbox/Dockerfile`
  and `SANDBOX_TAG` in `.github/workflows/images.yaml`, then the image
  tag in `charts/dev-environments (values: sandbox.image)`. Running sandboxes
  are unaffected; new claims pick up the new template.
- **agent-sandbox:** re-vendor both release files into
  `the infra repo (homelab-k8s: manifests/agent-sandbox/)` preserving the LOCAL MODIFICATIONS noted in their
  headers (dedupe the controller Deployment, keep resources); read the upstream
  API migration guide first.
- **provisioner:** code change → bump `PROVISIONER_TAG` in the workflow file and
  the image tag in `charts/dev-environments (values: provisioner.image)`.
