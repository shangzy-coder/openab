# CubeSandbox Deployment

OpenAB can run inside a CubeSandbox MicroVM without a remote ACP bridge. This
deployment model mirrors the existing Kubernetes agent images:

- one sandbox contains `openab` and one coding-agent runtime;
- CubeSandbox starts `envd` on port `49983`;
- the operator injects `config.toml` and credentials;
- authentication and `openab run` are started manually inside the sandbox.

The first supported template targets are:

| Docker target | Agent command | Authentication command |
|---|---|---|
| `cubesandbox-opencode` | `opencode acp` | `opencode auth login` |
| `cubesandbox-claude` | `claude-agent-acp` | `claude auth login` |
| `cubesandbox-codex` | `codex-acp` | `codex login --device-auth` |

Other OpenAB agent variants are intentionally out of scope for the first
CubeSandbox rollout.

## Architecture

```text
CubeSandbox MicroVM
├── envd :49983                 # PID 1, Cube control/data plane
├── openab                      # started manually by the operator
└── agent ACP process           # child of openab
    ├── opencode acp
    ├── claude-agent-acp
    └── codex-acp
```

Each OCI image contains only one of the three agents. This keeps credentials,
configuration, upgrades, and failures isolated in the same way as separate
Kubernetes Deployments.

## Prerequisites

- Docker with BuildKit/buildx.
- An OCI registry reachable by every Cube compute node.
- `cubemastercli` configured for the target Cube cluster.
- Cube compute nodes matching the built architecture. The commands below use
  `linux/arm64`; change the platform only when the cluster architecture differs.

The Cube `envd` source image is pinned by digest in `Dockerfile.unified`. Override
`CUBESANDBOX_ENVD_IMAGE` only when intentionally upgrading CubeSandbox.

## Build and push images

Set the registry prefix once:

```bash
export OPENAB_CUBE_REGISTRY=registry.example.com/openab
export OPENAB_CUBE_TAG=0.10.0
```

Build and push the three initial images:

```bash
docker buildx build --platform linux/arm64 \
  --file Dockerfile.unified \
  --target cubesandbox-opencode \
  --tag "${OPENAB_CUBE_REGISTRY}:${OPENAB_CUBE_TAG}-opencode" \
  --push .

docker buildx build --platform linux/arm64 \
  --file Dockerfile.unified \
  --target cubesandbox-claude \
  --tag "${OPENAB_CUBE_REGISTRY}:${OPENAB_CUBE_TAG}-claude" \
  --push .

docker buildx build --platform linux/arm64 \
  --file Dockerfile.unified \
  --target cubesandbox-codex \
  --tag "${OPENAB_CUBE_REGISTRY}:${OPENAB_CUBE_TAG}-codex" \
  --push .
```

For a local smoke build, replace `--push` with `--load`. A local image is not
enough to create a Cube template: the Cube nodes must be able to pull it.

## Register Cube templates through CubeAPI

Create one Cube template per image with the management-plane REST API. The
writable layer holds configuration, agent credentials, session state, and
checked-out repositories. `POST /templates` returns a build job; poll the
returned `jobID` until the template status is `READY`.

```bash
export CUBE_API_URL="http://192.168.104.116:3000"
export CUBE_API_KEY="e2b_000000"
export OPENAB_CUBE_REGISTRY="192.168.111.90:30002/open/openab"
export OPENAB_CUBE_TAG="<git-commit>"

curl -fsS -H "Authorization: Bearer ${CUBE_API_KEY}" \
  -H 'Content-Type: application/json' \
  -d "$(jq -n \
    --arg image "${OPENAB_CUBE_REGISTRY}:${OPENAB_CUBE_TAG}-opencode" \
    '{image:$image,writableLayerSize:"4G",exposedPorts:[49983],probePort:49983,probePath:"/health",command:["/usr/bin/envd"],args:["-port","49983"]}')" \
  "${CUBE_API_URL}/templates"
```

Repeat the request with `-claude` and `-codex`. The API returns `templateID`
and `jobID`; inspect progress with:

```bash
curl -fsS -H "Authorization: Bearer ${CUBE_API_KEY}" \
  "${CUBE_API_URL}/templates/<template-id>/builds/<job-id>/status"
```

Do not create sandboxes from a template until its status is `READY`. If the
registry is private, pass `registryUsername` and `registryPassword` in the
same JSON request (or publish to a registry reachable by the Cube node).

## Register the same templates with `cubemastercli`

`cubemastercli` is the direct CubeMaster client. It does not use the
CubeAPI bearer key; point its global `--address`/`--port` flags at the
CubeMaster service (the default port is `8089`). The command below is
equivalent to the REST request above and submits asynchronously:

```bash
cubemastercli \
  --address 192.168.104.116 \
  --port 8089 \
  tpl create-from-image \
  --image "${OPENAB_CUBE_REGISTRY}:${OPENAB_CUBE_TAG}-opencode" \
  --writable-layer-size 4G \
  --expose-port 49983 \
  --probe 49983 \
  --probe-path /health \
  --cmd /usr/bin/envd \
  --arg=-port \
  --arg=49983 \
  --detach
```

Repeat with `-claude` and `-codex`. For a private registry, add
`--registry-username "$REGISTRY_USERNAME"` and
`--registry-password "$REGISTRY_PASSWORD"`. The CLI prints `job_id` and
`template_id`; monitor and inspect them with:

```bash
cubemastercli --address 192.168.104.116 --port 8089 \
  tpl watch --job-id <job-id>
cubemastercli --address 192.168.104.116 --port 8089 \
  tpl info <template-id> --json --include-request
cubemastercli --address 192.168.104.116 --port 8089 tpl list -o wide
```

The important CLI-to-API field mapping is: `--image` → `image`, repeated
`--expose-port` → `exposedPorts`, `--probe`/`--probe-path` → `probePort`/
`probePath`, `--cmd`/`--arg` → `command`/`args`, and
`--writable-layer-size` → `writableLayerSize`.

## Configure a sandbox

Create a sandbox from the chosen template with `POST /sandboxes` (or use the
SDK/Dashboard). Keep platform tokens in sandbox environment variables or a
secret injector, not in the image.

```bash
curl -fsS -H "Authorization: Bearer ${CUBE_API_KEY}" \
  -H 'Content-Type: application/json' \
  -d '{"templateID":"<template-id>","timeout":3600}' \
  "${CUBE_API_URL}/sandboxes"
```

The response contains the sandbox ID and data-plane access information. Use
CubeProxy at `CUBE_PROXY_NODE_IP:CUBE_PROXY_PORT_HTTP` for the envd command and
filesystem APIs; no SSH access is required.

Example:

```toml
[discord]
bot_token = "${DISCORD_BOT_TOKEN}"
allowed_channels = ["123456789"]

[agent]
working_dir = "/workspace"

[pool]
max_sessions = 3
session_ttl_hours = 1

[reactions]
enabled = true
```

The image-provided `OPENAB_AGENT_COMMAND` selects the correct ACP runtime, so an
explicit `[agent].command` is unnecessary unless intentionally overriding it.

## Authenticate and start manually

Open a Cube terminal for the sandbox and run the image-specific auth command:

```bash
# OpenCode template
opencode auth login

# Claude Code template
claude auth login

# Codex template
codex login --device-auth
```

For unattended Claude deployments, prefer a long-lived token generated with
`claude setup-token` and inject it as `CLAUDE_CODE_OAUTH_TOKEN`.

Start OpenAB in the foreground:

```bash
openab run -c /workspace/config.toml
```

For a manually supervised background process:

```bash
nohup openab run -c /workspace/config.toml \
  >/workspace/openab.log 2>&1 </dev/null &
```

CubeSandbox keeps `envd` as PID 1. Stopping OpenAB does not destroy the sandbox;
restart it with the same command after updating configuration or authentication.

## Verification

Inside each new sandbox, run the common checks:

```bash
curl -fsS -o /dev/null http://127.0.0.1:49983/health
openab --version
git --version
rg --version
```

Then verify the selected agent:

```bash
# OpenCode
opencode --version
timeout 3s opencode acp </dev/null || test $? -eq 124

# Claude Code
claude --version
timeout 3s claude-agent-acp </dev/null || test $? -eq 124

# Codex
codex --version
timeout 3s codex-acp </dev/null || test $? -eq 124
```

ACP processes are long-running JSON-RPC servers. Exit code `124` means the
three-second smoke window expired while the process remained alive; that is the
expected result.

## Adversarial checks

Before using a template in production, verify:

1. A missing or invalid bot token makes OpenAB fail closed without exposing it
   in logs.
2. A failed OpenAB start leaves `envd` healthy and allows a corrected restart.
3. Two sandboxes created from the same template do not share `/workspace` or
   agent credential state.
4. Deleting a sandbox removes its writable state; no credential is baked into
   the OCI image or template snapshot.
5. A template built for the wrong CPU architecture fails validation and is not
   promoted.
6. OpenAB can be stopped and restarted without creating duplicate platform
   consumers from an older background process.
