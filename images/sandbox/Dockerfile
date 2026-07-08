# Dev-environment sandbox: OpenCode (headless AI coding agent) + OpenChamber (web UI).
# Served by Agent Sandbox as a stateful singleton pod; OpenChamber listens on :3000
# and manages its own OpenCode server process (documented default behavior).
FROM node:22-bookworm-slim

# Pinned app versions (repo rule: pin everything)
ARG OPENCODE_VERSION=1.17.15
ARG OPENCHAMBER_VERSION=1.14.1

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      git \
      jq \
      openssh-client \
      procps \
      ripgrep \
      tini \
      unzip \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g --omit=dev \
      @openchamber/web@${OPENCHAMBER_VERSION} \
    && npm cache clean --force

# Reuse the node user (uid 1000) shipped by the base image as the dev user.
RUN mkdir -p /workspace && chown node:node /workspace
USER node
ENV HOME=/home/node

# OpenCode via the official installer, AS THE RUNTIME USER, into ~/.opencode/bin:
# `opencode upgrade` then writes to a user-owned dir and works inside the
# sandbox (root-owned npm -g made self-update fail with EACCES / exit 243).
RUN curl -fsSL https://opencode.ai/install | VERSION=${OPENCODE_VERSION} bash
ENV PATH=/home/node/.opencode/bin:$PATH

WORKDIR /workspace

EXPOSE 3000

# tini: openchamber spawns/manages an opencode child process — reap zombies properly.
ENTRYPOINT ["tini", "--"]
CMD ["openchamber", "--foreground", "--host", "0.0.0.0", "--port", "3000"]
