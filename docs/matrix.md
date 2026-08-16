# Matrix Adapter

OpenAB can connect directly to a Matrix homeserver through the Matrix Client-Server API. It receives events with `/sync`, maps Matrix threads to ACP sessions, and sends replies, edits, and reactions as Matrix events.

## Security model and current limits

- Use a dedicated Matrix account for OpenAB. Its access token grants the same room access as that account; never place the token in the image or commit it to config.
- HTTPS is required for non-loopback homeservers by default. `allow_insecure_http = true` is an explicit escape hatch for trusted private networks only.
- Room and user access are deny-by-default. Configure `allowed_rooms` and `allowed_users`, or explicitly opt into `allow_all_rooms` / `allow_all_users`.
- Room invitations are not accepted by default. Set `auto_join_invites = true` only when the room policy safely limits which invitations may be accepted.
- The current adapter supports **unencrypted rooms only**. If a joined room has `m.room.encryption` state, OpenAB refuses to process events or send plaintext there.
- Matrix does not provide a standard `is_bot` field. Put every known bot MXID in `bot_user_ids`; put bots allowed to trigger OpenAB in `trusted_bot_ids` when using a bot allowlist.
- Unencrypted `m.image`, `m.file`, `m.audio`, and `m.video` events are supported. Encrypted media payloads (`content.file`) remain unsupported and are rejected rather than silently downgraded.

## Create the bot account

1. Create a dedicated account such as `@openab:example.com` using your homeserver's normal account-provisioning process.
2. Obtain an access token for that account and store it in your secret manager as `MATRIX_ACCESS_TOKEN`.
3. Invite the account only to the rooms it needs.
4. Ensure those rooms are not encrypted. Many clients enable encryption by default for private rooms, so verify room security settings before deployment.
5. Copy the canonical room ID (`!…:server`), not a room alias (`#…:server`), into `allowed_rooms`.

OpenAB calls `/_matrix/client/v3/account/whoami` at startup. Setting `user_id` is recommended because it makes a token/account mismatch fail immediately.

## Configuration

```toml
[matrix]
homeserver_url = "https://matrix.example.com"
access_token = "${MATRIX_ACCESS_TOKEN}"
# allow_insecure_http = false
user_id = "@openab:example.com"

allowed_rooms = ["!engineering:example.com"]
allow_all_rooms = false
auto_join_invites = false
allowed_users = ["@alice:example.com", "@bob:example.com"]
allow_all_users = false

# Matrix events have no standard bot marker.
bot_user_ids = ["@release-bot:example.com"]
allow_bot_messages = "off"
trusted_bot_ids = []
allow_user_messages = "multibot-mentions"

sync_timeout_seconds = 30
thread_replies = true
# outbound_file_root = "/workspace"
streaming = true

[agent]
command = "kiro-cli"
args = ["acp", "--trust-all-tools"]
working_dir = "/workspace"
```

The homeserver URL may contain a deployment prefix, for example `https://example.com/matrix`; OpenAB appends `/_matrix/client/v3` to it. It must not contain embedded credentials.

When `auto_join_invites = true`, OpenAB accepts invitations only when the room ID passes `allowed_rooms` or `allow_all_rooms`. The adapter learns room encryption state on the following `/sync`; encrypted rooms remain fail-closed and receive no plaintext.

## Message and thread behavior

- A top-level room message must mention the OpenAB MXID unless the room is marked as a direct room in the bot account's `m.direct` account data.
- With `thread_replies = true` (the default), top-level triggers receive replies in a Matrix thread using `m.relates_to.rel_type = "m.thread"` with a fallback reply relation.
- With `thread_replies = false`, top-level triggers receive top-level room replies and use a room-scoped logical session, so subsequent top-level `/model`, `/cancel`, and mentioned prompts reach the same session. Messages received inside an existing Matrix thread still use that thread root and receive in-thread replies.
- Existing thread messages follow `allow_user_messages`, matching Discord and Slack behavior.
- Streaming uses an initial event followed by `m.replace` edits. It is disabled after another configured bot appears in the thread.
- Matrix event IDs are used as message and thread IDs. Session keys are namespaced as `matrix:<thread-root-event-id>`.
- On startup, OpenAB performs a full-state sync to learn encryption and direct-room state, records the returned cursor, and does not dispatch the historical timeline from that initial sync.

## Attachments and media

Matrix uses authenticated media downloads through `/_matrix/client/v1/media/download`. The access token is sent only to the configured homeserver; the remote server name and media ID from `mxc://` are encoded as path segments, so an event cannot redirect the bearer token to another host.

Inbound handling follows the same shared `media`, `stt`, and optional `filestore` paths used by Slack and Discord:

- `m.image`: download, validate PNG/JPEG/GIF/WebP bytes, resize/compress, and send an ACP image block. The image cap is 10 MB.
- Text `m.file`: inline recognized text formats up to 512 KB.
- Other `m.file`: upload to the configured S3/R2 `[filestore]` and pass a temporary presigned URL to the agent. Without filestore, the user receives an explicit warning.
- `m.audio`: transcribe through `[stt]` when enabled and optionally echo the transcript. The audio cap is 25 MB; when STT is disabled, OpenAB adds the configured microphone status reaction and does not invent a transcript.
- `m.video`: pass filename, MIME, size, and MXC metadata to the agent, matching the current Slack behavior.
- Encrypted attachment objects are rejected because the Matrix MVP has no Olm/Megolm or attachment-key store.

### Agent-generated outbound files

Matrix has an adapter-local, opt-in upload path that does not modify the shared `ChatAdapter` contract. Set `outbound_file_root` to a directory such as the agent workspace. OpenAB then tells the agent that it can request delivery with an internal `<openab-send-file>…</openab-send-file>` marker, strips the marker from the visible reply, validates the requested path, uploads the bytes through `/_matrix/media/v3/upload`, and sends `m.file`, `m.image`, `m.audio`, or `m.video` in the current room/thread.

Paths may be relative to `outbound_file_root` or absolute beneath it. Canonicalization rejects `..` and symlink escapes, only regular files are accepted, at most five files may be requested per reply, and each file is capped at 50 MiB. The feature is disabled when the field is omitted. Enabling it lets any admitted user persuade the agent to send files from that root, so use a dedicated workspace and trusted admission policy outside disposable test environments.

Other adapters retain the existing text-only ACP output behavior described in [sendfiles.md](sendfiles.md); Matrix implements this locally rather than changing their shared core contract.

## Control commands

In a room or thread admitted by the room/user policy:

- `/cancel` stops the active ACP turn and leaves buffered messages intact.
- `/cancel-all` stops the active turn and clears buffered messages for the Matrix thread.

An optional exact bot MXID prefix is accepted, for example:

```text
@openab:example.com /cancel-all
```

Human control commands retain the room/user trust policy without requiring a mention. Bot-authored control commands must also pass `allow_bot_messages` and `trusted_bot_ids` admission.

## ACP model selection

Matrix reuses the same ACP `configOptions` model selector as the Gateway adapter. In an active Matrix conversation thread:

```text
/model
/models
/model list
/model set 2
/model set sino-bridge-test/qwen3.8:27b
```

`/model` and `/models` list the models advertised by the active ACP session and mark the current selection. `/model set` calls the standard ACP `session/set_config_option` method; it does not rewrite Pi files or use provider-specific logic. A session must already exist in that Matrix thread, so send an initial prompt before using the command.

## Verification

Start OpenAB and verify the startup log reports the expected MXID:

```bash
MATRIX_ACCESS_TOKEN='…' openab run -c config.toml
```

Then check:

1. A listed user in a listed unencrypted room can mention the bot and receives a threaded reply.
2. An unlisted user and an unlisted room are ignored.
3. Restarting OpenAB does not replay old timeline messages as prompts.
4. `/cancel` works in an active thread.
5. An encrypted room produces a refusal warning and never receives plaintext from OpenAB.
6. A sender in `bot_user_ids` cannot trigger the bot while `allow_bot_messages = "off"`.

## Cron targets

Matrix cron jobs use canonical room IDs as `channel` values:

```toml
[[cron.jobs]]
name = "matrix-daily-summary"
schedule = "0 0 9 * * *"
timezone = "UTC"
message = "Summarize today's repository activity."
channel = "!engineering:example.com"
platform = "matrix"
```

The bot must already be joined to the room, and the initial sync must have confirmed that the room is unencrypted before a cron message can be sent.
