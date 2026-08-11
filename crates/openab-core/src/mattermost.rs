//! Mattermost adapter using the v4 REST API and outbound WebSocket API.
//!
//! Mattermost threads are reply chains: `channel_id` stays constant and
//! `root_id` identifies the conversation. A top-level `@bot` post becomes the
//! root for a new OpenAB session; replies continue the existing `root_id`.

use crate::adapter::{ChannelRef, ChatAdapter, MessageRef, SenderContext};
use crate::bot_turns::{BotTurnTracker, TurnAction, TurnSeverity};
use crate::config::{AllowBots, AllowUsers};
use crate::trust::l3_gate_applies;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use regex::Regex;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, OnceCell};
use tokio_tungstenite::tungstenite;
use tracing::{debug, error, info, warn};

const MATTERMOST_MESSAGE_LIMIT: usize = 16_000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const AUTH_TIMEOUT: Duration = Duration::from_secs(15);
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const CACHE_MAX_ENTRIES: usize = 1_000;

type MattermostWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone, Debug)]
struct BotIdentity {
    id: String,
    username: String,
}

#[derive(Debug, Deserialize)]
struct UserResponse {
    id: String,
    username: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
struct MattermostPost {
    #[serde(default)]
    id: String,
    #[serde(default)]
    user_id: String,
    #[serde(default)]
    channel_id: String,
    #[serde(default)]
    root_id: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    create_at: i64,
    #[serde(default)]
    props: Map<String, Value>,
    #[serde(default)]
    file_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct PostedEvent {
    post: MattermostPost,
    channel_type: String,
    sender_name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MattermostControlCommand {
    /// Stop the current ACP turn, preserving messages already queued for the
    /// next turn.
    Cancel,
    /// Stop the current ACP turn and drop all messages currently queued for
    /// this Mattermost thread.
    CancelAll,
}

/// Runtime gates copied out of config before the adapter task is spawned.
#[derive(Clone)]
pub struct MattermostRunConfig {
    pub allow_all_channels: bool,
    pub allow_all_users: bool,
    pub allowed_channels: HashSet<String>,
    pub allowed_users: HashSet<String>,
    pub allow_bot_messages: AllowBots,
    pub trusted_bot_ids: HashSet<String>,
    pub allow_user_messages: AllowUsers,
    pub max_bot_turns: u32,
}

pub struct MattermostAdapter {
    client: reqwest::Client,
    api_base: String,
    websocket_url: String,
    bot_token: String,
    identity: OnceCell<BotIdentity>,
    channel_types: tokio::sync::Mutex<HashMap<String, String>>,
    participated_threads: tokio::sync::Mutex<HashMap<String, tokio::time::Instant>>,
    multibot_threads: tokio::sync::Mutex<HashMap<String, tokio::time::Instant>>,
    session_ttl: Duration,
    streaming: bool,
}

impl MattermostAdapter {
    pub fn new(
        server_url: String,
        bot_token: String,
        session_ttl: Duration,
        streaming: bool,
    ) -> Result<Self> {
        let (api_base, websocket_url) = normalize_server_urls(&server_url)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(concat!("openab/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build Mattermost HTTP client")?;

        Ok(Self {
            client,
            api_base,
            websocket_url,
            bot_token,
            identity: OnceCell::new(),
            channel_types: tokio::sync::Mutex::new(HashMap::new()),
            participated_threads: tokio::sync::Mutex::new(HashMap::new()),
            multibot_threads: tokio::sync::Mutex::new(HashMap::new()),
            session_ttl,
            streaming,
        })
    }

    /// Validate credentials at startup and cache the bot identity used by
    /// mention detection, self-message filtering, and reaction deletion.
    pub async fn initialize(&self) -> Result<()> {
        let identity = self.bot_identity().await?;
        info!(
            bot_user_id = %identity.id,
            bot_username = %identity.username,
            "Mattermost credentials validated"
        );
        Ok(())
    }

    async fn bot_identity(&self) -> Result<&BotIdentity> {
        self.identity
            .get_or_try_init(|| async {
                let value = self.api_json(Method::GET, "/users/me", None).await?;
                let user: UserResponse = serde_json::from_value(value)
                    .context("Mattermost /users/me returned an invalid user")?;
                if user.id.is_empty() || user.username.is_empty() {
                    return Err(anyhow!(
                        "Mattermost /users/me response is missing id or username"
                    ));
                }
                Ok(BotIdentity {
                    id: user.id,
                    username: user.username,
                })
            })
            .await
    }

    async fn api_json(&self, method: Method, path: &str, body: Option<&Value>) -> Result<Value> {
        let url = format!("{}{}", self.api_base, path);
        let mut request = self
            .client
            .request(method.clone(), &url)
            .bearer_auth(&self.bot_token);
        if let Some(body) = body {
            request = request.json(body);
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Mattermost API {method} {path} request failed"))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .with_context(|| format!("failed to read Mattermost API {method} {path} response"))?;
        if !status.is_success() {
            let detail = String::from_utf8_lossy(&bytes);
            return Err(anyhow!(
                "Mattermost API {method} {path} returned {status}: {detail}"
            ));
        }
        if bytes.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&bytes)
            .with_context(|| format!("Mattermost API {method} {path} returned invalid JSON"))
    }

    async fn resolve_channel_type(&self, channel_id: &str, event_type: &str) -> String {
        if !event_type.is_empty() {
            return event_type.to_string();
        }
        if let Some(channel_type) = self.channel_types.lock().await.get(channel_id).cloned() {
            return channel_type;
        }

        let path = format!("/channels/{channel_id}");
        match self.api_json(Method::GET, &path, None).await {
            Ok(value) => {
                let channel_type = value["type"].as_str().unwrap_or_default().to_string();
                if !channel_type.is_empty() {
                    let mut cache = self.channel_types.lock().await;
                    if cache.len() >= CACHE_MAX_ENTRIES {
                        cache.clear();
                    }
                    cache.insert(channel_id.to_string(), channel_type.clone());
                }
                channel_type
            }
            Err(err) => {
                warn!(channel_id, error = %err, "failed to resolve Mattermost channel type");
                String::new()
            }
        }
    }

    async fn note_participated(&self, root_id: &str) {
        let mut cache = self.participated_threads.lock().await;
        prune_timed_cache(&mut cache, self.session_ttl);
        cache.insert(root_id.to_string(), tokio::time::Instant::now());
    }

    async fn has_participated(&self, root_id: &str) -> bool {
        {
            let mut cache = self.participated_threads.lock().await;
            prune_timed_cache(&mut cache, self.session_ttl);
            if cache.contains_key(root_id) {
                return true;
            }
        }

        let identity = match self.bot_identity().await {
            Ok(identity) => identity,
            Err(err) => {
                warn!(error = %err, "cannot determine Mattermost thread participation");
                return false;
            }
        };
        let path = format!("/posts/{root_id}/thread");
        let value = match self.api_json(Method::GET, &path, None).await {
            Ok(value) => value,
            Err(err) => {
                warn!(root_id, error = %err, "failed to inspect Mattermost thread participation");
                return false;
            }
        };
        let participated = value["posts"].as_object().is_some_and(|posts| {
            posts
                .values()
                .any(|post| post["user_id"].as_str() == Some(identity.id.as_str()))
        });
        if participated {
            self.note_participated(root_id).await;
        }
        participated
    }

    async fn note_other_bot(&self, root_id: &str) {
        let mut cache = self.multibot_threads.lock().await;
        prune_timed_cache(&mut cache, self.session_ttl);
        cache.insert(root_id.to_string(), tokio::time::Instant::now());
    }

    async fn other_bot_present(&self, root_id: &str) -> bool {
        let mut cache = self.multibot_threads.lock().await;
        prune_timed_cache(&mut cache, self.session_ttl);
        cache.contains_key(root_id)
    }
}

#[async_trait]
impl ChatAdapter for MattermostAdapter {
    fn platform(&self) -> &'static str {
        "mattermost"
    }

    fn message_limit(&self) -> usize {
        MATTERMOST_MESSAGE_LIMIT
    }

    async fn send_message(&self, channel: &ChannelRef, content: &str) -> Result<MessageRef> {
        let body =
            build_create_post_body(&channel.channel_id, channel.thread_id.as_deref(), content);
        let value = self.api_json(Method::POST, "/posts", Some(&body)).await?;
        let post_id = value["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| anyhow!("Mattermost create post response is missing id"))?;
        if let Some(root_id) = channel.thread_id.as_deref() {
            self.note_participated(root_id).await;
        }
        Ok(MessageRef {
            channel: channel.clone(),
            message_id: post_id.to_string(),
        })
    }

    async fn create_thread(
        &self,
        channel: &ChannelRef,
        trigger_msg: &MessageRef,
        _title: &str,
    ) -> Result<ChannelRef> {
        Ok(ChannelRef {
            platform: "mattermost".into(),
            channel_id: channel.channel_id.clone(),
            thread_id: Some(trigger_msg.message_id.clone()),
            parent_id: None,
            origin_event_id: None,
        })
    }

    async fn add_reaction(&self, msg: &MessageRef, emoji: &str) -> Result<()> {
        let identity = self.bot_identity().await?;
        let body = json!({
            "user_id": identity.id,
            "post_id": msg.message_id,
            "emoji_name": unicode_to_mattermost_emoji(emoji),
        });
        match self.api_json(Method::POST, "/reactions", Some(&body)).await {
            Ok(_) => Ok(()),
            Err(err) if err.to_string().to_ascii_lowercase().contains("already") => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn remove_reaction(&self, msg: &MessageRef, emoji: &str) -> Result<()> {
        let identity = self.bot_identity().await?;
        let emoji_name = unicode_to_mattermost_emoji(emoji);
        let path = format!(
            "/users/{}/posts/{}/reactions/{emoji_name}",
            identity.id, msg.message_id
        );
        match self.api_json(Method::DELETE, &path, None).await {
            Ok(_) => Ok(()),
            Err(err) if err.to_string().contains("404") => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn edit_message(&self, msg: &MessageRef, content: &str) -> Result<()> {
        let path = format!("/posts/{}/patch", msg.message_id);
        self.api_json(Method::PUT, &path, Some(&json!({ "message": content })))
            .await?;
        Ok(())
    }

    async fn delete_message(&self, msg: &MessageRef) -> Result<()> {
        let path = format!("/posts/{}", msg.message_id);
        self.api_json(Method::DELETE, &path, None).await?;
        Ok(())
    }

    fn use_streaming(&self, other_bot_present: bool) -> bool {
        self.streaming && !other_bot_present
    }

    fn renders_native_tables(&self, _platform: &str) -> bool {
        true
    }
}

/// Run the Mattermost WebSocket receiver. Authentication happens on every
/// reconnect using the same bot token as the REST client.
pub async fn run_mattermost_adapter(
    adapter: Arc<MattermostAdapter>,
    router: Arc<crate::adapter::AdapterRouter>,
    config: MattermostRunConfig,
    mut shutdown_rx: watch::Receiver<bool>,
    dispatcher: Arc<crate::dispatch::Dispatcher>,
) -> Result<()> {
    adapter.initialize().await?;
    let bot_turns = Arc::new(tokio::sync::Mutex::new(BotTurnTracker::new(
        config.max_bot_turns,
    )));
    let mut backoff_secs = 1u64;

    loop {
        if *shutdown_rx.borrow() {
            info!("Mattermost adapter shutting down");
            return Ok(());
        }

        let websocket = match connect_and_authenticate(&adapter).await {
            Ok(websocket) => websocket,
            Err(err) => {
                error!(error = %err, backoff = backoff_secs, "Mattermost WebSocket connection failed");
                match wait_backoff_or_shutdown(backoff_secs, &mut shutdown_rx).await {
                    Some(next) => backoff_secs = next,
                    None => return Ok(()),
                }
                continue;
            }
        };

        backoff_secs = 1;
        info!(url = %adapter.websocket_url, "Mattermost WebSocket connected");
        let (mut write, mut read) = websocket.split();
        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Consume the immediate first tick; the connection was just used for auth.
        ping_interval.tick().await;

        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        let _ = write.send(tungstenite::Message::Close(None)).await;
                        info!("Mattermost adapter shutting down");
                        return Ok(());
                    }
                }
                _ = ping_interval.tick() => {
                    match tokio::time::timeout(
                        SEND_TIMEOUT,
                        write.send(tungstenite::Message::Ping(Vec::new())),
                    ).await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            warn!(error = %err, "Mattermost WebSocket ping failed; reconnecting");
                            break;
                        }
                        Err(_) => {
                            warn!("Mattermost WebSocket ping timed out; reconnecting");
                            break;
                        }
                    }
                }
                frame = read.next() => {
                    let Some(frame) = frame else {
                        warn!("Mattermost WebSocket closed; reconnecting");
                        break;
                    };
                    match frame {
                        Ok(tungstenite::Message::Text(text)) => {
                            let value: Value = match serde_json::from_str(&text) {
                                Ok(value) => value,
                                Err(err) => {
                                    debug!(error = %err, "ignoring malformed Mattermost WebSocket message");
                                    continue;
                                }
                            };
                            let event = match parse_posted_event(&value) {
                                Ok(Some(event)) => event,
                                Ok(None) => continue,
                                Err(err) => {
                                    warn!(error = %err, "ignoring invalid Mattermost posted event");
                                    continue;
                                }
                            };
                            let adapter = adapter.clone();
                            let router = router.clone();
                            let config = config.clone();
                            let bot_turns = bot_turns.clone();
                            let dispatcher = dispatcher.clone();
                            tokio::spawn(async move {
                                handle_posted_event(
                                    event,
                                    adapter,
                                    router,
                                    config,
                                    bot_turns,
                                    dispatcher,
                                ).await;
                            });
                        }
                        Ok(tungstenite::Message::Ping(payload)) => {
                            match tokio::time::timeout(
                                SEND_TIMEOUT,
                                write.send(tungstenite::Message::Pong(payload)),
                            ).await {
                                Ok(Ok(())) => {}
                                Ok(Err(err)) => {
                                    warn!(error = %err, "Mattermost WebSocket pong failed; reconnecting");
                                    break;
                                }
                                Err(_) => {
                                    warn!("Mattermost WebSocket pong timed out; reconnecting");
                                    break;
                                }
                            }
                        }
                        Ok(tungstenite::Message::Pong(_)) => {}
                        Ok(tungstenite::Message::Close(frame)) => {
                            warn!(?frame, "Mattermost WebSocket closed by server");
                            break;
                        }
                        Ok(tungstenite::Message::Binary(_))
                        | Ok(tungstenite::Message::Frame(_)) => {}
                        Err(err) => {
                            warn!(error = %err, "Mattermost WebSocket read failed; reconnecting");
                            break;
                        }
                    }
                }
            }
        }

        match wait_backoff_or_shutdown(backoff_secs, &mut shutdown_rx).await {
            Some(next) => backoff_secs = next,
            None => return Ok(()),
        }
    }
}

async fn handle_posted_event(
    event: PostedEvent,
    adapter: Arc<MattermostAdapter>,
    router: Arc<crate::adapter::AdapterRouter>,
    config: MattermostRunConfig,
    bot_turns: Arc<tokio::sync::Mutex<BotTurnTracker>>,
    dispatcher: Arc<crate::dispatch::Dispatcher>,
) {
    let post = event.post;
    if post.id.is_empty() || post.user_id.is_empty() || post.channel_id.is_empty() {
        return;
    }
    if !config.allow_all_channels && !config.allowed_channels.contains(&post.channel_id) {
        return;
    }

    let identity = match adapter.bot_identity().await {
        Ok(identity) => identity.clone(),
        Err(err) => {
            error!(error = %err, "Mattermost bot identity unavailable");
            return;
        }
    };
    let is_bot = is_bot_post(&post);
    let is_own_bot = post.user_id == identity.id;
    let logical_thread_id = logical_thread_id(&post).to_string();

    if is_bot && !is_own_bot {
        adapter.note_other_bot(&logical_thread_id).await;
    }

    let turn_action = {
        let mut tracker = bot_turns.lock().await;
        if is_bot {
            tracker.classify_bot_message(&logical_thread_id)
        } else {
            tracker.on_human_message(&logical_thread_id);
            TurnAction::Continue
        }
    };
    match turn_action {
        TurnAction::Continue => {}
        TurnAction::SilentStop => return,
        TurnAction::WarnAndStop {
            severity,
            turns,
            user_message,
        } => {
            match severity {
                TurnSeverity::Hard => {
                    warn!(thread_id = %logical_thread_id, turns, "hard Mattermost bot turn limit reached")
                }
                TurnSeverity::Soft => {
                    info!(thread_id = %logical_thread_id, turns, "Mattermost bot turn limit reached")
                }
            }
            if !is_own_bot {
                let channel = ChannelRef {
                    platform: "mattermost".into(),
                    channel_id: post.channel_id.clone(),
                    thread_id: Some(logical_thread_id),
                    parent_id: None,
                    origin_event_id: None,
                };
                if let Err(err) = adapter.send_message(&channel, &user_message).await {
                    warn!(error = %err, "failed to send Mattermost bot turn warning");
                }
            }
            return;
        }
    }

    if is_own_bot {
        return;
    }

    let channel_type = adapter
        .resolve_channel_type(&post.channel_id, &event.channel_type)
        .await;
    let is_dm = matches!(channel_type.as_str(), "D" | "G");

    if !is_bot && !config.allow_all_users && !config.allowed_users.contains(&post.user_id) {
        info!(user_id = %post.user_id, "denied Mattermost user, ignoring");
        let denied = MessageRef {
            channel: ChannelRef {
                platform: "mattermost".into(),
                channel_id: post.channel_id.clone(),
                thread_id: non_empty(&post.root_id).map(str::to_string),
                parent_id: None,
                origin_event_id: None,
            },
            message_id: post.id.clone(),
        };
        let _ = adapter.add_reaction(&denied, "🚫").await;
        return;
    }

    if l3_gate_applies(is_bot) {
        let decision = router.gate_incoming("mattermost", &post.channel_id, is_dm, &post.user_id);
        if !decision.is_allowed() {
            info!(
                user_id = %post.user_id,
                channel_id = %post.channel_id,
                ?decision,
                "Mattermost message denied by trust gate"
            );
            return;
        }
    }

    // Mattermost does not have a native slash-command interaction in this
    // adapter. Treat the two session-control commands as exact plain-text
    // commands (optionally prefixed by an @mention) before normal mention and
    // follow-up routing. This lets a user stop a turn from the same thread
    // without accidentally sending the command to the ACP agent as a prompt.
    if let Some(command) = parse_control_command(&post.message, &identity.username) {
        handle_control_command(
            command,
            &post,
            &adapter,
            &router,
            &dispatcher,
            &logical_thread_id,
        )
        .await;
        return;
    }

    let mentions_bot = mentions_bot(&post.message, &identity.username);
    let other_bot_present = adapter.other_bot_present(&logical_thread_id).await;
    let participated = if is_dm || mentions_bot {
        true
    } else if post.root_id.is_empty() {
        false
    } else {
        adapter.has_participated(&post.root_id).await
    };

    let should_process = if is_bot {
        let trusted = config.trusted_bot_ids.contains(&post.user_id);
        if !config.trusted_bot_ids.is_empty() && !trusted {
            false
        } else if trusted && mentions_bot {
            true
        } else {
            match config.allow_bot_messages {
                AllowBots::Off => false,
                AllowBots::Mentions => is_dm || mentions_bot,
                AllowBots::All => {
                    is_dm || mentions_bot || (!post.root_id.is_empty() && participated)
                }
            }
        }
    } else if is_dm {
        true
    } else if post.root_id.is_empty() {
        mentions_bot
    } else {
        match config.allow_user_messages {
            AllowUsers::Mentions => mentions_bot,
            AllowUsers::Involved => mentions_bot || participated,
            AllowUsers::MultibotMentions => mentions_bot || (participated && !other_bot_present),
        }
    };
    if !should_process {
        return;
    }

    let prompt = strip_bot_mention(&post.message, &identity.username);
    if prompt.is_empty() {
        if !post.file_ids.is_empty() {
            debug!(
                post_id = %post.id,
                files = post.file_ids.len(),
                "Mattermost attachment-only post ignored (attachments are not supported yet)"
            );
        }
        return;
    }

    // Mark intent before dispatch so follow-up replies arriving while the first
    // ACP turn is running are admitted into the same thread buffer.
    adapter.note_participated(&logical_thread_id).await;

    let original_root = non_empty(&post.root_id).map(str::to_string);
    let sender_name = if event.sender_name.trim().is_empty() {
        post.user_id.clone()
    } else {
        event.sender_name
    };
    let sender = SenderContext {
        schema: "openab.sender.v1".into(),
        sender_id: post.user_id.clone(),
        sender_name: sender_name.clone(),
        display_name: sender_name,
        channel: "mattermost".into(),
        channel_id: post.channel_id.clone(),
        thread_id: original_root.clone(),
        is_bot,
        timestamp: Some(crate::timestamp::unix_millis_to_iso8601(post.create_at)),
        message_id: Some(post.id.clone()),
        receiver_id: Some(identity.id),
    };
    let sender_json = mattermost_sender_json(&sender);

    let trigger_msg = MessageRef {
        channel: ChannelRef {
            platform: "mattermost".into(),
            channel_id: post.channel_id.clone(),
            thread_id: original_root,
            parent_id: None,
            origin_event_id: None,
        },
        message_id: post.id,
    };
    let thread_channel = ChannelRef {
        platform: "mattermost".into(),
        channel_id: post.channel_id,
        thread_id: Some(logical_thread_id.clone()),
        parent_id: None,
        origin_event_id: None,
    };
    let thread_key = dispatcher.key("mattermost", &logical_thread_id, &sender.sender_id);
    let estimated_tokens = crate::dispatch::estimate_tokens(&prompt, &[]);
    let buffered = crate::dispatch::BufferedMessage {
        sender_json,
        sender_name: sender.sender_name,
        prompt,
        extra_blocks: Vec::new(),
        trigger_msg,
        arrived_at: std::time::Instant::now(),
        estimated_tokens,
        other_bot_present,
        recipient: None,
    };
    let adapter_dyn: Arc<dyn ChatAdapter> = adapter;
    if let Err(err) = dispatcher
        .submit(thread_key, thread_channel, adapter_dyn, buffered)
        .await
    {
        error!(error = %err, "Mattermost dispatcher submit failed");
    }
}

async fn handle_control_command(
    command: MattermostControlCommand,
    post: &MattermostPost,
    adapter: &MattermostAdapter,
    router: &crate::adapter::AdapterRouter,
    dispatcher: &crate::dispatch::Dispatcher,
    logical_thread_id: &str,
) {
    let session_key = format!("mattermost:{logical_thread_id}");
    let reply_channel = ChannelRef {
        platform: "mattermost".into(),
        channel_id: post.channel_id.clone(),
        // Keep a control acknowledgement in the existing Mattermost reply
        // chain. A top-level control post has no root, so its acknowledgement
        // remains top-level as well.
        thread_id: non_empty(&post.root_id).map(str::to_string),
        parent_id: None,
        origin_event_id: None,
    };

    let message = match command {
        MattermostControlCommand::Cancel => {
            match router.pool().cancel_session(&session_key).await {
                Ok(()) => "🛑 Cancel signal sent.".to_string(),
                Err(err) => format!("⚠️ {err}"),
            }
        }
        MattermostControlCommand::CancelAll => {
            // Remove dispatcher handles before sending the ACP signal. This
            // makes a concurrent post start a fresh queue rather than landing
            // on the consumer being torn down.
            let dropped = dispatcher.cancel_buffered_thread("mattermost", logical_thread_id);
            let cancel_result = router.pool().cancel_session(&session_key).await;
            match (cancel_result, dropped) {
                (Ok(()), 0) => "🛑 Cancel signal sent.".to_string(),
                (Ok(()), _) => "🛑 Cancel signal sent. Buffered messages cleared.".to_string(),
                (Err(_), 0) => {
                    "⚠️ Nothing to cancel — no active session and no buffered messages.".to_string()
                }
                (Err(_), _) => {
                    "🛑 Buffered messages cleared. No active session to cancel.".to_string()
                }
            }
        }
    };

    if let Err(err) = adapter.send_message(&reply_channel, &message).await {
        warn!(error = %err, command = ?command, "failed to send Mattermost control-command acknowledgement");
    }
}

async fn connect_and_authenticate(adapter: &MattermostAdapter) -> Result<MattermostWebSocket> {
    let (mut websocket, _) = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async(&adapter.websocket_url),
    )
    .await
    .map_err(|_| anyhow!("Mattermost WebSocket connect timed out"))?
    .context("Mattermost WebSocket handshake failed")?;

    websocket
        .send(tungstenite::Message::Text(authentication_message(
            &adapter.bot_token,
        )))
        .await
        .context("failed to send Mattermost WebSocket authentication challenge")?;

    tokio::time::timeout(AUTH_TIMEOUT, async {
        loop {
            let frame = websocket
                .next()
                .await
                .ok_or_else(|| anyhow!("Mattermost WebSocket closed during authentication"))?
                .context("Mattermost WebSocket authentication read failed")?;
            match frame {
                tungstenite::Message::Text(text) => {
                    let value: Value = serde_json::from_str(&text)
                        .context("Mattermost sent invalid JSON during authentication")?;
                    if value["seq_reply"].as_i64() == Some(1) {
                        if value["status"].as_str() == Some("OK") {
                            return Ok(websocket);
                        }
                        return Err(anyhow!(
                            "Mattermost WebSocket authentication failed: {}",
                            value
                        ));
                    }
                    // `hello` can arrive before the authentication response.
                }
                tungstenite::Message::Ping(payload) => {
                    websocket
                        .send(tungstenite::Message::Pong(payload))
                        .await
                        .context("failed to answer Mattermost auth-time ping")?;
                }
                tungstenite::Message::Close(frame) => {
                    return Err(anyhow!(
                        "Mattermost WebSocket closed during authentication: {frame:?}"
                    ));
                }
                tungstenite::Message::Binary(_)
                | tungstenite::Message::Pong(_)
                | tungstenite::Message::Frame(_) => {}
            }
        }
    })
    .await
    .map_err(|_| anyhow!("Mattermost WebSocket authentication timed out"))?
}

async fn wait_backoff_or_shutdown(
    backoff_secs: u64,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Option<u64> {
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(backoff_secs)) => {
            Some((backoff_secs.saturating_mul(2)).min(30))
        }
        _ = shutdown_rx.changed() => None,
    }
}

fn normalize_server_urls(server_url: &str) -> Result<(String, String)> {
    let mut url = reqwest::Url::parse(server_url.trim())
        .with_context(|| format!("invalid Mattermost server_url: {server_url}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(anyhow!(
            "Mattermost server_url must use http or https, got {}",
            url.scheme()
        ));
    }
    url.set_query(None);
    url.set_fragment(None);

    let mut path = url.path().trim_end_matches('/').to_string();
    if path.ends_with("/api/v4") {
        path.truncate(path.len() - "/api/v4".len());
    }
    url.set_path(&path);
    let root = url.as_str().trim_end_matches('/').to_string();
    let api_base = format!("{root}/api/v4");

    let mut websocket = url;
    let websocket_scheme = if websocket.scheme() == "https" {
        "wss"
    } else {
        "ws"
    };
    websocket
        .set_scheme(websocket_scheme)
        .map_err(|_| anyhow!("failed to build Mattermost WebSocket URL"))?;
    let websocket_root = websocket.as_str().trim_end_matches('/');
    Ok((api_base, format!("{websocket_root}/api/v4/websocket")))
}

fn authentication_message(token: &str) -> String {
    json!({
        "seq": 1,
        "action": "authentication_challenge",
        "data": { "token": token },
    })
    .to_string()
}

fn parse_posted_event(value: &Value) -> Result<Option<PostedEvent>> {
    if value["event"].as_str() != Some("posted") {
        return Ok(None);
    }
    let raw_post = &value["data"]["post"];
    let post = if let Some(raw) = raw_post.as_str() {
        serde_json::from_str(raw).context("Mattermost posted event has invalid post JSON")?
    } else if raw_post.is_object() {
        serde_json::from_value(raw_post.clone())
            .context("Mattermost posted event has invalid post object")?
    } else {
        return Err(anyhow!("Mattermost posted event is missing data.post"));
    };
    Ok(Some(PostedEvent {
        post,
        channel_type: value["data"]["channel_type"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        sender_name: value["data"]["sender_name"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
    }))
}

fn is_bot_post(post: &MattermostPost) -> bool {
    match post.props.get("from_bot") {
        Some(Value::Bool(value)) => *value,
        Some(Value::String(value)) => value.eq_ignore_ascii_case("true") || value == "1",
        Some(Value::Number(value)) => value.as_i64() == Some(1),
        _ => false,
    }
}

fn logical_thread_id(post: &MattermostPost) -> &str {
    non_empty(&post.root_id).unwrap_or(&post.id)
}

fn non_empty(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn bot_mention_regex(username: &str) -> Option<Regex> {
    if username.is_empty() {
        return None;
    }
    // Mattermost usernames may contain letters, digits, dots, dashes, and
    // underscores. Capture the trailing delimiter because Rust regexes do not
    // support look-around; replacing with `$1` preserves punctuation/spacing.
    Regex::new(&format!(
        r"(?i)@{}([^A-Za-z0-9._-]|$)",
        regex::escape(username)
    ))
    .ok()
}

fn mentions_bot(message: &str, username: &str) -> bool {
    bot_mention_regex(username).is_some_and(|regex| regex.is_match(message))
}

fn strip_bot_mention(message: &str, username: &str) -> String {
    let Some(regex) = bot_mention_regex(username) else {
        return message.trim().to_string();
    };
    regex.replace_all(message, "$1").trim().to_string()
}

fn parse_control_command(message: &str, username: &str) -> Option<MattermostControlCommand> {
    let normalized = strip_bot_mention(message, username);
    match normalized.trim() {
        "/cancel" => Some(MattermostControlCommand::Cancel),
        "/cancel-all" => Some(MattermostControlCommand::CancelAll),
        _ => None,
    }
}

fn build_create_post_body(channel_id: &str, root_id: Option<&str>, content: &str) -> Value {
    let mut body = json!({
        "channel_id": channel_id,
        "message": content,
    });
    if let Some(root_id) = root_id.filter(|root_id| !root_id.is_empty()) {
        body["root_id"] = Value::String(root_id.to_string());
    }
    body
}

fn mattermost_sender_json(sender: &SenderContext) -> String {
    let mut value = serde_json::to_value(sender).unwrap_or_else(|_| json!({}));
    if let Some(object) = value.as_object_mut() {
        if let Some(thread_id) = object.remove("thread_id") {
            object.insert("root_id".to_string(), thread_id);
        }
    }
    value.to_string()
}

fn unicode_to_mattermost_emoji(emoji: &str) -> &str {
    match emoji {
        "👀" => "eyes",
        "🤔" => "thinking_face",
        "🔥" => "fire",
        "👨\u{200d}💻" => "technologist",
        "⚡" => "zap",
        "🆗" => "ok",
        "😱" => "scream",
        "🚫" => "no_entry_sign",
        "😊" => "blush",
        "😎" => "sunglasses",
        "🫡" => "saluting_face",
        "🤓" => "nerd_face",
        "😏" => "smirk",
        "✌\u{fe0f}" => "v",
        "💪" => "muscle",
        "🦾" => "mechanical_arm",
        "🥱" => "yawning_face",
        "😨" => "fearful",
        "✅" => "white_check_mark",
        "❌" => "x",
        "🔧" => "wrench",
        "🎤" => "microphone",
        custom if custom.starts_with(':') && custom.ends_with(':') && custom.len() > 2 => {
            &custom[1..custom.len() - 1]
        }
        _ => "grey_question",
    }
}

fn prune_timed_cache(cache: &mut HashMap<String, tokio::time::Instant>, ttl: Duration) {
    cache.retain(|_, inserted| inserted.elapsed() < ttl);
    if cache.len() >= CACHE_MAX_ENTRIES {
        let mut entries: Vec<(String, tokio::time::Instant)> = cache
            .iter()
            .map(|(key, instant)| (key.clone(), *instant))
            .collect();
        entries.sort_by_key(|(_, instant)| *instant);
        for (key, _) in entries.into_iter().take(cache.len() / 2) {
            cache.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_post() -> MattermostPost {
        MattermostPost {
            id: "post1".into(),
            user_id: "user1".into(),
            channel_id: "channel1".into(),
            root_id: String::new(),
            message: "@openab hello".into(),
            create_at: 1_714_204_397_123,
            props: Map::new(),
            file_ids: Vec::new(),
        }
    }

    #[test]
    fn normalizes_root_and_api_urls() {
        let (api, websocket) = normalize_server_urls("https://chat.example.com/").unwrap();
        assert_eq!(api, "https://chat.example.com/api/v4");
        assert_eq!(websocket, "wss://chat.example.com/api/v4/websocket");

        let (api, websocket) =
            normalize_server_urls("http://host.local/mattermost/api/v4/").unwrap();
        assert_eq!(api, "http://host.local/mattermost/api/v4");
        assert_eq!(websocket, "ws://host.local/mattermost/api/v4/websocket");
    }

    #[test]
    fn rejects_non_http_server_url() {
        assert!(normalize_server_urls("ftp://chat.example.com").is_err());
    }

    #[test]
    fn authentication_payload_matches_mattermost_protocol() {
        let value: Value = serde_json::from_str(&authentication_message("token-1")).unwrap();
        assert_eq!(value["seq"], 1);
        assert_eq!(value["action"], "authentication_challenge");
        assert_eq!(value["data"]["token"], "token-1");
    }

    #[test]
    fn parses_posted_event_with_string_post() {
        let post = serde_json::to_string(&sample_post()).unwrap();
        let value = json!({
            "event": "posted",
            "data": {
                "post": post,
                "channel_type": "O",
                "sender_name": "Alice"
            }
        });
        let parsed = parse_posted_event(&value).unwrap().unwrap();
        assert_eq!(parsed.post.id, "post1");
        assert_eq!(parsed.channel_type, "O");
        assert_eq!(parsed.sender_name, "Alice");
    }

    #[test]
    fn parses_posted_event_with_object_post() {
        let value = json!({
            "event": "posted",
            "data": {
                "post": sample_post(),
                "channel_type": "D"
            }
        });
        let parsed = parse_posted_event(&value).unwrap().unwrap();
        assert_eq!(parsed.post.channel_id, "channel1");
        assert_eq!(parsed.channel_type, "D");
    }

    #[test]
    fn ignores_non_posted_events() {
        assert!(parse_posted_event(&json!({ "event": "hello" }))
            .unwrap()
            .is_none());
    }

    #[test]
    fn mention_detection_and_cleanup_preserve_other_text() {
        assert!(mentions_bot("@OpenAB, please review @alice", "openab"));
        assert_eq!(
            strip_bot_mention("@OpenAB, please review @alice", "openab"),
            ", please review @alice"
        );
        assert_eq!(
            strip_bot_mention("@openab @openab run tests", "openab"),
            "run tests"
        );
        assert!(!mentions_bot("@openab-helper run tests", "openab"));
    }

    #[test]
    fn parses_control_commands_with_or_without_a_mention() {
        assert_eq!(
            parse_control_command("/cancel", "openab"),
            Some(MattermostControlCommand::Cancel)
        );
        assert_eq!(
            parse_control_command("@openab /cancel", "openab"),
            Some(MattermostControlCommand::Cancel)
        );
        assert_eq!(
            parse_control_command("@OpenAB /cancel-all", "openab"),
            Some(MattermostControlCommand::CancelAll)
        );
        assert_eq!(parse_control_command("/cancel now", "openab"), None);
        assert_eq!(parse_control_command("please /cancel", "openab"), None);
    }

    #[test]
    fn root_id_selects_existing_thread_otherwise_post_id() {
        let mut post = sample_post();
        assert_eq!(logical_thread_id(&post), "post1");
        post.root_id = "root1".into();
        assert_eq!(logical_thread_id(&post), "root1");
    }

    #[test]
    fn detects_bot_prop_in_supported_shapes() {
        let mut post = sample_post();
        post.props
            .insert("from_bot".into(), Value::String("true".into()));
        assert!(is_bot_post(&post));
        post.props.insert("from_bot".into(), Value::Bool(false));
        assert!(!is_bot_post(&post));
    }

    #[test]
    fn post_payload_includes_root_only_for_threads() {
        assert_eq!(
            build_create_post_body("c1", None, "hello"),
            json!({ "channel_id": "c1", "message": "hello" })
        );
        assert_eq!(
            build_create_post_body("c1", Some("root1"), "hello"),
            json!({ "channel_id": "c1", "root_id": "root1", "message": "hello" })
        );
    }

    #[test]
    fn reaction_names_use_mattermost_shortcodes() {
        assert_eq!(unicode_to_mattermost_emoji("👀"), "eyes");
        assert_eq!(
            unicode_to_mattermost_emoji(":party_parrot:"),
            "party_parrot"
        );
        assert_eq!(unicode_to_mattermost_emoji("unknown"), "grey_question");
    }
}
