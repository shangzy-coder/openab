# Mattermost Bot Setup Guide

OpenAB connects directly to Mattermost: inbound posts arrive over the Mattermost WebSocket API, while replies, edits, deletes, and reactions use the v4 REST API. No public webhook endpoint is required.

## 1. Create a bot account

1. In Mattermost, open **Integrations → Bot Accounts**.
2. Select **Add Bot Account**.
3. Choose a username. Users mention this exact username, for example `@openab`.
4. Copy the generated access token and store it as `MATTERMOST_BOT_TOKEN`.
5. Add the bot account to the required team and channels.

If **Bot Accounts** is unavailable, a Mattermost system administrator must enable bot account creation in the System Console. The bot must be a member of every private channel it should read or reply in.

## 2. Configure OpenAB

```toml
[mattermost]
server_url = "${MATTERMOST_SERVER_URL}"
bot_token = "${MATTERMOST_BOT_TOKEN}"

# Omit these flags to infer the behavior from the lists:
# non-empty list = restricted; empty list = allow all.
allowed_channels = ["channel-id"]
allowed_users = ["user-id"]

allow_bot_messages = "off"            # off | mentions | all
trusted_bot_ids = []
allow_user_messages = "multibot-mentions" # multibot-mentions | involved | mentions
streaming = true
```

Set the credentials before starting OpenAB:

```bash
export MATTERMOST_SERVER_URL="https://chat.example.com"
export MATTERMOST_BOT_TOKEN="your-bot-token"
openab run -c config.toml
```

`server_url` should normally be the site root. Subpath installations such as `https://example.com/mattermost` are supported, and an accidental trailing `/api/v4` is normalized automatically.

## 3. Find channel and user IDs

- **Channel ID:** open the channel menu and use **Copy Link**. The channel ID is also available through Mattermost's API and administrative tooling.
- **User ID:** open the user's profile and use **Copy ID** when enabled, or query the Mattermost v4 users API as an administrator.

When `allow_all_channels` or `allow_all_users` is omitted, OpenAB follows the same compatibility rule as Discord and Slack:

- a non-empty allowlist enables restriction;
- an empty or absent allowlist allows all.

Set the corresponding flag explicitly to `false` with an empty list for deny-all behavior.

## 4. Message behavior

- In public or private channels, start a conversation with `@bot_username your request`.
- In direct and group messages, the mention is implicit.
- A top-level trigger becomes a new Mattermost thread; OpenAB replies with `root_id` equal to the trigger post ID.
- Existing thread replies continue the same `root_id`.
- With the default `allow_user_messages = "multibot-mentions"`, follow-up messages do not need another mention while OpenAB is involved, unless another bot has joined the thread.
- OpenAB's own posts are always ignored. Other bot posts are controlled by `allow_bot_messages`, `trusted_bot_ids`, and the bot-turn safety limits.

With `streaming = true`, OpenAB creates a placeholder post and updates it using Mattermost's post patch API. Set `streaming = false` to post only the final response.

## 5. Required access

The token's bot account must be able to:

- read posts in the configured channels;
- create posts and thread replies;
- edit and delete its own posts;
- add and remove its own reactions;
- read a thread when checking whether the bot already participated.

These operations normally work with ordinary bot membership and channel permissions; no incoming webhook or OAuth application is required.

## 6. Current limitation

The initial Mattermost adapter handles text posts only. If a post contains both text and files, its text is processed. Attachment-only posts are ignored until Mattermost file ingestion is added.

## Troubleshooting

### Authentication fails or the socket reconnects continuously

- Verify `MATTERMOST_SERVER_URL` points to the same site that issued the token.
- Regenerate the bot token if it was revoked.
- Confirm `/api/v4/users/me` is reachable with `Authorization: Bearer <token>`.
- Confirm reverse proxies allow WebSocket upgrades for `/api/v4/websocket`.

### The bot sees DMs but not a channel

- Add the bot account to the team and channel.
- Check `allowed_channels` / `allow_all_channels`.
- Use the Mattermost channel ID, not its display name.
- Mention the bot username on top-level channel posts.

### Follow-up messages require another mention after restart

OpenAB checks the Mattermost thread through the REST API when its in-memory participation cache is cold. Ensure the bot can read the thread and that the root post has not been deleted.
