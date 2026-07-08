# Plan: Port-exposure DX for dev environments

**Status:** FOR REVIEW — prompted by Vite's `allowedHosts` block on
`oc-<id>-<port>.renala.dev` hostnames.

## The actual problem

Modern dev servers (Vite ≥5, Rails HostAuthorization, webpack-dev-server…)
implement DNS-rebinding protection: they reject requests whose `Host` header
isn't on an allowlist. Our port-router forwards the public hostname as-is, so
every framework demands per-project config (`server.allowedHosts`,
`config.hosts`, …). That's the DX papercut — **it has nothing to do with where
the port lives in the URL**. A true `https://host:3000` URL would carry
`Host: host:3000` and be blocked identically.

## How GitHub Codespaces actually does it

Codespaces' public URL is `https://<name>-<port>.app.github.dev` — **port in
the hostname, same scheme as ours**. The DX magic is elsewhere: the forwarding
proxy **rewrites `Host` to `localhost:<port>`** (real host in
`X-Forwarded-Host`), which every framework's rebinding protection allows by
default, zero config. Two more reasons they chose hostname-encoding over
port-in-URL for a production product:

1. **Corporate/hotel/guest networks commonly allow egress only on 80/443.**
   `https://foo:5173` is unreachable for a large slice of real users; a
   443-only hostname scheme always works.
2. One wildcard cert + one edge port covers everything; no port-range
   listener infrastructure.

## Fix A (recommended, ~30 min): Codespaces-style Host rewrite

Change the port-router nginx:

```nginx
proxy_set_header Host localhost:$port;          # was: $host
proxy_set_header X-Forwarded-Host $host;        # real public host
proxy_set_header X-Forwarded-Proto https;
```

- Vite/Rails/Next/etc. work with **zero project config** — identical to
  Codespaces.
- Trade-off: apps that build absolute URLs from `Host` will generate
  `localhost:<port>` links/redirects. Same trade-off Codespaces makes; apps
  that care read `X-Forwarded-Host`. For preview-a-dev-server usage this is
  the right default.
- Optional belt-and-braces: template env hints for frameworks that support
  them (`RAILS_DEVELOPMENT_HOSTS=.renala.dev`), harmless and additive.
- OpenChamber's own hostname (base host, port 1982) is untouched — the
  rewrite lives only in the port-router vhost.

## Thought experiment: true `https://oc-<id>.renala.dev:<port>` without Cloudflare

Asked and answered for understanding — **not proposed**. What it takes when no
edge constrains ports:

```
browser :5173 ─▶ public IP (edge VPS)              [DNS: *.renala.dev A → VPS, DNS-only]
                 nftables: redirect tcp 1024-65535 → :8443 (single socket)
                 L7 proxy (Envoy/Go):
                   • TLS terminate: wildcard *.renala.dev cert (Let's Encrypt DNS-01)
                   • recover ORIGINAL dst port (SO_ORIGINAL_DST / Envoy original_dst)
                   • parse env from SNI/Host
                 ─▶ tailnet ─▶ in-cluster router (PROXY protocol v2 or
                               X-Target-Port header carries the port)
                 ─▶ pod IP : original port
```

Piece by piece:

1. **Port-range listening.** Nothing binds 64k sockets sanely; you funnel with
   `nftables redirect`/TPROXY into one listener and recover the client's
   intended port from the kernel (`SO_ORIGINAL_DST`; Envoy's `original_dst`
   listener filter exists precisely for this).
2. **TLS is the easy part.** SNI carries only the hostname — one wildcard cert
   covers every port. (This is also why Cloudflare *could* support this if
   they wanted; their fixed port set is a product/anycast-routing decision.)
3. **Kubernetes can't help you at the edge.** `Service` ports are enumerated,
   NodePorts are capped to 30000–32767 — a port-range ingress **must bypass
   kube networking** (hostNetwork + node nftables, or an external box). The
   natural host in this cluster is `homelab-edge-1` (Hetzner VPS, fixed IPv4,
   already doing L4 mail ingress per ADR-021/022).
4. **Carrying the port to the cluster.** The edge reaches the cluster over the
   tailnet on one port; the intended dst port travels as metadata (PROXY v2
   header or an HTTP header), and an in-cluster router (our existing
   port-router, minus the hostname parsing) dials `pod-ip:port`.

**What it costs:**

- Cloudflare is *fully* out of the data path for these hosts: no DDoS
  absorption, no WAF, and **no Access** — ports 1024–65535 of the VPS become
  raw public surface. A whole new security posture to own.
- Cert lifecycle (DNS-01 automation on the VPS), edge HA, conntrack limits,
  and the VPS becomes a bandwidth bottleneck/cost center for all preview
  traffic.
- **And after all that, Vite still blocks you** — `Host: oc-x.renala.dev:5173`
  isn't allowlisted. You end up doing the Host rewrite anyway.
- Unreachable from port-restricted networks (the Codespaces reason).

Conclusion of the experiment: port-in-URL buys nothing the hostname scheme
lacks, costs an edge redesign, and doesn't even fix the papercut that
motivated it. The hostname scheme + Host rewrite *is* the production-grade
answer — it's what the production products converged on.

## Recommendation

1. Implement **Fix A** (Host rewrite + `X-Forwarded-Host` in port-router).
2. Add `RAILS_DEVELOPMENT_HOSTS=.renala.dev` style env hints to the template
   (optional, cheap).
3. Do not pursue port-in-URL; keep this document as the rationale.
