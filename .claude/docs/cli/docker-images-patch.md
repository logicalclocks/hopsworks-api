# docker-images Dockerfile patch (Phase 4 switchover)

Cross-repo Dockerfile edits were denied by the sandbox, so here is the exact
diff to apply by hand on the `cli` branch of `docker-images` once the
`hopsworks` pip package includes the Python CLI (this repo's `cli` branch, Phase 0–4).

## Context

Currently every terminal image downloads the Go SDK, clones the `hopsworks-cli` repo,
and builds a `hops` binary — costing ~200MB per layer and a toolchain in the image.
Once `pip install hopsworks` ships the `hops` console script, the build step is
unnecessary: the binary lands at `/srv/hops/venv/bin/hops` already.

## Three files, one identical diff block each

Replace the "Install hopsworks CLI" RUN in all three of:

- `base-image/terminal-server/Dockerfile` (lines 151-155)
- `base-image/terminal-gpu/Dockerfile` (lines 172-176)
- `base-image/terminal-spark/Dockerfile` (lines 166-170)

**Before** (identical in all three):

```dockerfile
# Install hopsworks CLI (the 'hops' command) — Go SDK downloaded, used, and removed in one layer
RUN curl -fsSL https://go.dev/dl/go1.24.1.linux-amd64.tar.gz | tar -C /usr/local -xz \
    && git clone --depth 1 --branch v0.8.8 https://github.com/logicalclocks/hopsworks-cli /tmp/hopsworks-cli \
    && cd /tmp/hopsworks-cli && /usr/local/go/bin/go build -o /usr/local/bin/hops . \
    && rm -rf /tmp/hopsworks-cli /usr/local/go
```

**After**:

```dockerfile
# The 'hops' CLI ships with the hopsworks pip package already installed in
# /srv/hops/venv, so no separate build step is needed. Symlink it onto PATH
# for users who activate a different venv.
RUN ln -sf /srv/hops/venv/bin/hops /usr/local/bin/hops \
    && /srv/hops/venv/bin/hops --version
```

## Verification

Build each image and confirm:

```bash
docker build -f base-image/terminal-server/Dockerfile -t terminal-server:test .
docker run --rm terminal-server:test hops --version
# hops, version 5.0.0.dev1 (or later)
```

The `hops init` call that already lives in each image's startup script
(around line 380 / 394 / 447) continues to work unchanged.

## Expected savings

- Per image: ~200 MB (Go toolchain), ~1 min build time, one fewer external fetch.
- No change to user-facing behavior: same `hops` command, same config file, same
  auto-login inside pods.

## Sequencing

1. Land the CLI on `hopsworks-api` `cli` branch (done — Phase 0–4 in-tree).
2. Ship a `hopsworks` release (or dev wheel) that includes the new CLI.
3. Confirm the base image's `internal-base` picks up that release (check
   `base-image/internal-base/python_install.sh`).
4. Apply this patch on the `cli` branch of `docker-images`.
5. Build and smoke-test all three terminal images end-to-end against a live cluster.
