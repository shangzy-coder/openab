//! Matrix adapter using the Client-Server API.
//!
//! The MVP deliberately supports unencrypted rooms only. Matrix end-to-end
//! encryption requires device keys, Olm/Megolm session persistence, and device
//! verification; sending plaintext to an encrypted room is therefore refused.

use crate::acp::ContentBlock;
use crate::adapter::{ChannelRef, ChatAdapter, MessageRef, SenderContext};
use crate::bot_turns::{BotTurnTracker, TurnAction, TurnSeverity};
use crate::config::{AllowBots, AllowUsers, MatrixConfig, SttConfig};
use crate::gateway::handle_config_command;
use crate::media;
use crate::trust::l3_gate_applies;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use reqwest::{Method, RequestBuilder, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex, OnceCell};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const MATRIX_MESSAGE_LIMIT: usize = 8_000;
const CACHE_MAX_ENTRIES: usize = 1_000;
const SEEN_EVENT_LIMIT: usize = 10_000;
const MAX_ERROR_BODY_CHARS: usize = 1_000;
const MATRIX_OUTBOUND_FILE_LIMIT: u64 = 50 * 1024 * 1024;

#[derive(Clone)]
pub struct MatrixRunConfig {
    pub allow_all_rooms: bool,
    pub auto_join_invites: bool,
    pub allow_all_users: bool,
    pub allowed_rooms: HashSet<String>,
    pub allowed_users: HashSet<String>,
    pub bot_user_ids: HashSet<String>,
    pub allow_bot_messages: AllowBots,
    pub trusted_bot_ids: HashSet<String>,
    pub allow_user_messages: AllowUsers,
    pub max_bot_turns: u32,
    pub thread_replies: bool,
}

impl MatrixRunConfig {
    pub fn from_config(config: &MatrixConfig) -> Self {
        Self {
            allow_all_rooms: config.allow_all_rooms,
            auto_join_invites: config.auto_join_invites,
            allow_all_users: config.allow_all_users,
            allowed_rooms: config.allowed_rooms.iter().cloned().collect(),
            allowed_users: config.allowed_users.iter().cloned().collect(),
            bot_user_ids: config.bot_user_ids.iter().cloned().collect(),
            allow_bot_messages: config.allow_bot_messages,
            trusted_bot_ids: config.trusted_bot_ids.iter().cloned().collect(),
            allow_user_messages: config.allow_user_messages,
            max_bot_turns: config.max_bot_turns,
            thread_replies: config.thread_replies,
        }
    }

    fn is_bot(&self, sender: &str, own_user_id: &str) -> bool {
        sender == own_user_id
            || self.bot_user_ids.contains(sender)
            || self.trusted_bot_ids.contains(sender)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatrixControlCommand {
    Cancel,
    CancelAll,
}

#[derive(Debug, Deserialize)]
struct WhoAmIResponse {
    user_id: String,
}

#[derive(Debug, Deserialize)]
struct SendEventResponse {
    event_id: String,
}

#[derive(Debug, Deserialize)]
struct JoinRoomResponse {
    room_id: String,
}

#[derive(Debug, Deserialize)]
struct MediaUploadResponse {
    content_uri: String,
}

#[derive(Debug, Default, Deserialize)]
struct MatrixSyncResponse {
    next_batch: String,
    #[serde(default)]
    rooms: SyncRooms,
    #[serde(default)]
    account_data: EventList,
}

#[derive(Debug, Default, Deserialize)]
struct SyncRooms {
    #[serde(default)]
    join: HashMap<String, JoinedRoom>,
    #[serde(default)]
    invite: HashMap<String, Value>,
    #[serde(default)]
    leave: HashMap<String, Value>,
}

#[derive(Debug, Default, Deserialize)]
struct JoinedRoom {
    #[serde(default)]
    state: EventList,
    #[serde(default)]
    timeline: Timeline,
}

#[derive(Debug, Default, Deserialize)]
struct Timeline {
    #[serde(default)]
    events: Vec<MatrixEvent>,
}

#[derive(Debug, Default, Deserialize)]
struct EventList {
    #[serde(default)]
    events: Vec<MatrixEvent>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MatrixEvent {
    #[serde(rename = "type", default)]
    event_type: String,
    #[serde(default)]
    event_id: String,
    #[serde(default)]
    sender: String,
    #[serde(default)]
    origin_server_ts: i64,
    #[serde(default)]
    content: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatrixAttachmentKind {
    Image,
    Audio,
    Video,
    File,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MatrixAttachment {
    kind: MatrixAttachmentKind,
    filename: String,
    mime_type: String,
    size: u64,
    mxc_uri: String,
}

#[derive(Default)]
struct MatrixAttachmentOutcome {
    blocks: Vec<ContentBlock>,
    echo_entries: Vec<crate::stt::EchoEntry>,
    warning: Option<String>,
    audio_skipped: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ReactionKey {
    room_id: String,
    event_id: String,
    emoji: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct OutboundFileKey {
    room_id: String,
    anchor_event_id: String,
    path: String,
}

#[derive(Default)]
struct SeenEvents {
    ids: HashSet<String>,
    order: VecDeque<String>,
}

impl SeenEvents {
    /// Returns true only for the first observation of a non-empty event ID.
    fn insert(&mut self, event_id: &str) -> bool {
        if event_id.is_empty() || self.ids.contains(event_id) {
            return false;
        }
        let event_id = event_id.to_string();
        self.ids.insert(event_id.clone());
        self.order.push_back(event_id);
        while self.order.len() > SEEN_EVENT_LIMIT {
            if let Some(oldest) = self.order.pop_front() {
                self.ids.remove(&oldest);
            }
        }
        true
    }
}

pub struct MatrixAdapter {
    client: reqwest::Client,
    api_base: Url,
    access_token: String,
    expected_user_id: Option<String>,
    user_id: OnceCell<String>,
    sync_timeout: Duration,
    session_ttl: Duration,
    thread_replies: bool,
    streaming: bool,
    outbound_file_root: Option<std::path::PathBuf>,
    participated_threads: Mutex<HashMap<String, tokio::time::Instant>>,
    multibot_threads: Mutex<HashMap<String, tokio::time::Instant>>,
    direct_rooms: Mutex<HashSet<String>>,
    /// `room_id -> encrypted`; absence means security state is unknown and
    /// outbound plaintext must fail closed.
    room_security: Mutex<HashMap<String, bool>>,
    reaction_events: Mutex<HashMap<ReactionKey, String>>,
    outbound_file_events: Mutex<HashMap<OutboundFileKey, String>>,
    seen_events: Mutex<SeenEvents>,
}

impl MatrixAdapter {
    pub fn new(config: &MatrixConfig, session_ttl: Duration) -> Result<Self> {
        anyhow::ensure!(
            !config.access_token.trim().is_empty(),
            "matrix.access_token is empty"
        );
        let api_base = normalize_homeserver_url(&config.homeserver_url)?;
        anyhow::ensure!(
            api_base.scheme() == "https"
                || config.allow_insecure_http
                || url_host_is_loopback(&api_base),
            "Matrix homeserver bearer authentication requires HTTPS; set matrix.allow_insecure_http=true only for a trusted private network"
        );
        let sync_timeout = Duration::from_secs(config.sync_timeout_seconds);
        let outbound_file_root = config
            .outbound_file_root
            .as_deref()
            .map(std::fs::canonicalize)
            .transpose()
            .context("failed to resolve matrix.outbound_file_root")?;
        if let Some(root) = outbound_file_root.as_ref() {
            anyhow::ensure!(
                root.is_dir(),
                "matrix.outbound_file_root must be a directory"
            );
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(20))
            .timeout(sync_timeout + Duration::from_secs(15))
            .user_agent(concat!("openab/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build Matrix HTTP client")?;

        Ok(Self {
            client,
            api_base,
            access_token: config.access_token.clone(),
            expected_user_id: config.user_id.clone(),
            user_id: OnceCell::new(),
            sync_timeout,
            session_ttl,
            thread_replies: config.thread_replies,
            streaming: config.streaming,
            outbound_file_root,
            participated_threads: Mutex::new(HashMap::new()),
            multibot_threads: Mutex::new(HashMap::new()),
            direct_rooms: Mutex::new(HashSet::new()),
            room_security: Mutex::new(HashMap::new()),
            reaction_events: Mutex::new(HashMap::new()),
            outbound_file_events: Mutex::new(HashMap::new()),
            seen_events: Mutex::new(SeenEvents::default()),
        })
    }

    /// Validate the token identity and establish the initial full-state sync cursor.
    /// Call before spawning the long-running receiver so startup failures are fatal.
    pub async fn initialize(&self, run_config: &MatrixRunConfig) -> Result<String> {
        let whoami: WhoAmIResponse = self
            .execute_json(
                self.authorized(Method::GET, self.endpoint(&["account", "whoami"])?)?,
                "Matrix whoami",
            )
            .await?;
        if let Some(expected) = self.expected_user_id.as_deref() {
            anyhow::ensure!(
                expected == whoami.user_id,
                "matrix.user_id mismatch: configured {expected}, homeserver returned {}",
                whoami.user_id
            );
        }
        self.user_id
            .set(whoami.user_id.clone())
            .map_err(|_| anyhow!("Matrix adapter initialized more than once"))?;

        // Establish a fresh sync cursor without dispatching historical timeline
        // events. full_state guarantees room encryption state is known before
        // any outbound plaintext can be sent.
        let initial = self.sync(None, true).await?;
        self.apply_sync_metadata(&initial).await;
        self.seed_initial_state(&initial, run_config, &whoami.user_id)
            .await;
        self.join_invited_rooms(&initial, run_config).await;
        info!(user_id = %whoami.user_id, "Matrix adapter authenticated");
        Ok(initial.next_batch)
    }

    fn endpoint(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.api_base.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow!("Matrix homeserver URL cannot be a base URL"))?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }

    fn media_download_url(&self, mxc_uri: &str) -> Result<Url> {
        let mxc = Url::parse(mxc_uri)
            .with_context(|| format!("invalid Matrix content URI: {mxc_uri}"))?;
        anyhow::ensure!(
            mxc.scheme() == "mxc",
            "Matrix attachment URL must use mxc://"
        );
        anyhow::ensure!(
            mxc.username().is_empty()
                && mxc.password().is_none()
                && mxc.query().is_none()
                && mxc.fragment().is_none(),
            "Matrix content URI must not contain credentials, query, or fragment"
        );
        let host = mxc
            .host_str()
            .ok_or_else(|| anyhow!("Matrix content URI has no server name"))?;
        let server_name = mxc
            .port()
            .map_or_else(|| host.to_string(), |port| format!("{host}:{port}"));
        let media_id = mxc.path().trim_start_matches('/');
        anyhow::ensure!(
            !media_id.is_empty() && !media_id.contains('/'),
            "Matrix content URI has an invalid media ID"
        );

        // Authenticated media downloads use the Matrix Client API v1 path even
        // while the rest of the adapter uses Client API v3.
        let mut url = self.api_base.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| anyhow!("Matrix homeserver URL cannot be a base URL"))?;
        segments.pop_if_empty();
        segments.pop(); // remove "v3"
        segments.extend(["v1", "media", "download", &server_name, media_id]);
        drop(segments);
        Ok(url)
    }

    fn media_upload_url(&self) -> Result<Url> {
        let mut url = self.api_base.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| anyhow!("Matrix homeserver URL cannot be a base URL"))?;
        segments.pop_if_empty();
        segments.pop(); // remove "v3"
        segments.pop(); // remove "client"
        segments.extend(["media", "v3", "upload"]);
        drop(segments);
        Ok(url)
    }

    fn authorized(&self, method: Method, url: Url) -> Result<RequestBuilder> {
        anyhow::ensure!(
            !self.access_token.is_empty(),
            "matrix.access_token is empty"
        );
        Ok(self
            .client
            .request(method, url)
            .bearer_auth(&self.access_token))
    }

    async fn execute_json<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        operation: &str,
    ) -> Result<T> {
        let retry_request = request.try_clone();
        let mut request = request;
        let mut retried_rate_limit = false;
        loop {
            let response = request
                .send()
                .await
                .with_context(|| format!("{operation} request failed"))?;
            let status = response.status();
            let body = response
                .text()
                .await
                .with_context(|| format!("{operation} response body failed"))?;
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS && !retried_rate_limit {
                if let Some(retry) = retry_request.as_ref().and_then(RequestBuilder::try_clone) {
                    let retry_ms = serde_json::from_str::<Value>(&body)
                        .ok()
                        .and_then(|value| value["retry_after_ms"].as_u64())
                        .unwrap_or(1_000)
                        .clamp(100, 30_000);
                    warn!(
                        operation,
                        retry_ms, "Matrix rate limit reached; retrying once"
                    );
                    tokio::time::sleep(Duration::from_millis(retry_ms)).await;
                    request = retry;
                    retried_rate_limit = true;
                    continue;
                }
            }
            if !status.is_success() {
                let body: String = body.chars().take(MAX_ERROR_BODY_CHARS).collect();
                return Err(anyhow!("{operation} failed with HTTP {status}: {body}"));
            }
            return serde_json::from_str(&body)
                .with_context(|| format!("{operation} returned invalid JSON"));
        }
    }

    async fn sync(&self, since: Option<&str>, full_state: bool) -> Result<MatrixSyncResponse> {
        let timeout_ms = if since.is_some() {
            self.sync_timeout.as_millis().to_string()
        } else {
            "0".to_string()
        };
        let mut request = self.authorized(Method::GET, self.endpoint(&["sync"])?)?;
        request = request.query(&[("timeout", timeout_ms.as_str())]);
        if let Some(since) = since {
            request = request.query(&[("since", since)]);
        }
        if full_state {
            request = request.query(&[("full_state", "true")]);
        }
        let response: MatrixSyncResponse = self.execute_json(request, "Matrix sync").await?;
        anyhow::ensure!(
            !response.next_batch.is_empty(),
            "Matrix sync returned an empty next_batch cursor"
        );
        Ok(response)
    }

    async fn join_invited_rooms(&self, sync: &MatrixSyncResponse, run_config: &MatrixRunConfig) {
        if !run_config.auto_join_invites {
            return;
        }
        for (room_id, invite) in &sync.rooms.invite {
            if !run_config.allow_all_rooms && !run_config.allowed_rooms.contains(room_id) {
                info!(room_id, "ignoring Matrix invite denied by room policy");
                continue;
            }
            let request = match self.authorized(
                Method::POST,
                match self.endpoint(&["rooms", room_id, "join"]) {
                    Ok(url) => url,
                    Err(err) => {
                        warn!(room_id, error = %err, "failed to build Matrix join URL");
                        continue;
                    }
                },
            ) {
                Ok(request) => request.json(&json!({})),
                Err(err) => {
                    warn!(room_id, error = %err, "failed to build Matrix join request");
                    continue;
                }
            };
            match self
                .execute_json::<JoinRoomResponse>(request, "Matrix join room")
                .await
            {
                Ok(response) if response.room_id == *room_id => {
                    info!(room_id, "joined invited Matrix room");
                    if let Some(peer) = matrix_direct_inviter(invite) {
                        if let Err(err) = self.remember_direct_room(peer, room_id).await {
                            warn!(room_id, peer, error = %err, "joined direct Matrix invite but failed to persist m.direct metadata");
                        }
                    }
                }
                Ok(response) => {
                    warn!(room_id, joined_room_id = %response.room_id, "Matrix join returned an unexpected room ID");
                }
                Err(err) => {
                    warn!(room_id, error = %err, "failed to join invited Matrix room");
                }
            }
        }
    }

    async fn remember_direct_room(&self, peer: &str, room_id: &str) -> Result<()> {
        self.direct_rooms.lock().await.insert(room_id.to_string());
        let own_user_id = self
            .user_id
            .get()
            .ok_or_else(|| anyhow!("Matrix adapter identity is not initialized"))?;
        let url = self.endpoint(&["user", own_user_id, "account_data", "m.direct"])?;
        let response = self
            .authorized(Method::GET, url.clone())?
            .send()
            .await
            .context("Matrix m.direct read request failed")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("Matrix m.direct read response body failed")?;
        let mut mapping = if status == reqwest::StatusCode::NOT_FOUND {
            serde_json::Map::new()
        } else {
            anyhow::ensure!(
                status.is_success(),
                "Matrix m.direct read failed with HTTP {status}: {}",
                body.chars().take(MAX_ERROR_BODY_CHARS).collect::<String>()
            );
            serde_json::from_str::<Value>(&body)
                .context("Matrix m.direct read returned invalid JSON")?
                .as_object()
                .cloned()
                .ok_or_else(|| anyhow!("Matrix m.direct account data is not an object"))?
        };
        let rooms = mapping
            .entry(peer.to_string())
            .or_insert_with(|| Value::Array(Vec::new()));
        if !rooms.is_array() {
            *rooms = Value::Array(Vec::new());
        }
        let rooms = rooms
            .as_array_mut()
            .expect("m.direct peer entry was normalized to an array");
        if !rooms.iter().any(|value| value.as_str() == Some(room_id)) {
            rooms.push(Value::String(room_id.to_string()));
        }
        let _: Value = self
            .execute_json(
                self.authorized(Method::PUT, url)?
                    .json(&Value::Object(mapping)),
                "Matrix m.direct update",
            )
            .await?;
        info!(room_id, peer, "recorded direct Matrix room");
        Ok(())
    }

    async fn apply_sync_metadata(&self, sync: &MatrixSyncResponse) {
        for event in &sync.account_data.events {
            if event.event_type == "m.direct" {
                let mut direct_rooms = HashSet::new();
                if let Some(mapping) = event.content.as_object() {
                    for rooms in mapping.values().filter_map(Value::as_array) {
                        for room_id in rooms.iter().filter_map(Value::as_str) {
                            direct_rooms.insert(room_id.to_string());
                        }
                    }
                }
                *self.direct_rooms.lock().await = direct_rooms;
            }
        }

        let mut security = self.room_security.lock().await;
        for (room_id, room) in &sync.rooms.join {
            let newly_encrypted = room
                .state
                .events
                .iter()
                .chain(room.timeline.events.iter())
                .any(|event| event.event_type == "m.room.encryption");
            let encrypted = security.get(room_id).copied().unwrap_or(false) || newly_encrypted;
            if newly_encrypted && security.get(room_id) != Some(&true) {
                warn!(
                    room_id,
                    "Matrix encrypted room detected; plaintext adapter disabled for room"
                );
            }
            security.insert(room_id.clone(), encrypted);
        }
        for room_id in sync.rooms.leave.keys() {
            security.remove(room_id);
        }
    }

    async fn seed_initial_state(
        &self,
        sync: &MatrixSyncResponse,
        run_config: &MatrixRunConfig,
        own_user_id: &str,
    ) {
        for room in sync.rooms.join.values() {
            for event in &room.timeline.events {
                self.seen_events.lock().await.insert(&event.event_id);
                let Some(thread_id) = event_thread_root(event) else {
                    continue;
                };
                if event.sender == own_user_id {
                    self.note_participated(thread_id).await;
                } else if run_config.is_bot(&event.sender, own_user_id) {
                    self.note_other_bot(thread_id).await;
                }
            }
        }
    }

    async fn room_is_direct(&self, room_id: &str) -> bool {
        self.direct_rooms.lock().await.contains(room_id)
    }

    async fn ensure_plaintext_room(&self, room_id: &str) -> Result<()> {
        match self.room_security.lock().await.get(room_id).copied() {
            Some(false) => Ok(()),
            Some(true) => Err(anyhow!(
                "Matrix room {room_id} is encrypted; E2EE is not supported"
            )),
            None => Err(anyhow!(
                "Matrix room {room_id} security state is unknown; refusing plaintext send"
            )),
        }
    }

    fn outbound_file_enabled(&self) -> bool {
        self.outbound_file_root.is_some()
    }

    async fn load_outbound_file(&self, requested: &str) -> Result<(String, String, Vec<u8>)> {
        let root = self
            .outbound_file_root
            .as_ref()
            .ok_or_else(|| anyhow!("Matrix outbound file delivery is disabled"))?;
        let requested_path = std::path::Path::new(requested);
        let candidate = if requested_path.is_absolute() {
            requested_path.to_path_buf()
        } else {
            root.join(requested_path)
        };
        let path = tokio::fs::canonicalize(&candidate)
            .await
            .with_context(|| format!("cannot resolve outbound file `{requested}`"))?;
        anyhow::ensure!(
            path.starts_with(root),
            "outbound file must stay within matrix.outbound_file_root"
        );
        let metadata = tokio::fs::metadata(&path).await?;
        anyhow::ensure!(metadata.is_file(), "outbound path is not a regular file");
        anyhow::ensure!(
            metadata.len() <= MATRIX_OUTBOUND_FILE_LIMIT,
            "outbound file exceeds the 50 MiB limit"
        );
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| anyhow!("outbound file has no valid UTF-8 filename"))?
            .to_string();
        let data = tokio::fs::read(path).await?;
        let media_type = matrix_outbound_media_type(&filename).to_string();
        Ok((filename, media_type, data))
    }

    async fn send_outbound_file(
        &self,
        channel: &ChannelRef,
        requested: &str,
    ) -> Result<MessageRef> {
        self.ensure_plaintext_room(&channel.channel_id).await?;
        let (filename, media_type, data) = self.load_outbound_file(requested).await?;
        let size = data.len() as u64;
        let request = self
            .authorized(Method::POST, self.media_upload_url()?)?
            .query(&[("filename", filename.as_str())])
            .header(reqwest::header::CONTENT_TYPE, media_type.as_str())
            .body(data);
        let uploaded: MediaUploadResponse =
            self.execute_json(request, "Matrix media upload").await?;
        anyhow::ensure!(
            uploaded.content_uri.starts_with("mxc://"),
            "Matrix media upload returned an invalid content_uri"
        );
        let content = matrix_file_content(
            &filename,
            &media_type,
            size,
            &uploaded.content_uri,
            channel.thread_id.as_deref(),
        );
        let event_id = self
            .send_room_event(&channel.channel_id, "m.room.message", &content)
            .await?;
        Ok(MessageRef {
            channel: channel.clone(),
            message_id: event_id,
        })
    }

    async fn deliver_output_files(
        &self,
        channel: &ChannelRef,
        anchor_event_id: Option<&str>,
        requested_paths: &[String],
    ) -> Result<Option<MessageRef>> {
        let mut last = None;
        for requested in requested_paths {
            let key = anchor_event_id.map(|anchor| OutboundFileKey {
                room_id: channel.channel_id.clone(),
                anchor_event_id: anchor.to_string(),
                path: requested.clone(),
            });
            if let Some(key) = key.as_ref() {
                if let Some(event_id) = self.outbound_file_events.lock().await.get(key).cloned() {
                    last = Some(MessageRef {
                        channel: channel.clone(),
                        message_id: event_id,
                    });
                    continue;
                }
            }
            let sent = self.send_outbound_file(channel, requested).await?;
            if let Some(key) = key {
                let mut cache = self.outbound_file_events.lock().await;
                if cache.len() >= CACHE_MAX_ENTRIES {
                    cache.clear();
                }
                cache.insert(key, sent.message_id.clone());
            }
            last = Some(sent);
        }
        Ok(last)
    }

    async fn note_participated(&self, thread_id: &str) {
        let mut cache = self.participated_threads.lock().await;
        prune_timed_cache(&mut cache, self.session_ttl);
        cache.insert(thread_id.to_string(), tokio::time::Instant::now());
    }

    async fn has_participated(&self, thread_id: &str) -> bool {
        let mut cache = self.participated_threads.lock().await;
        prune_timed_cache(&mut cache, self.session_ttl);
        cache.contains_key(thread_id)
    }

    async fn note_other_bot(&self, thread_id: &str) {
        let mut cache = self.multibot_threads.lock().await;
        prune_timed_cache(&mut cache, self.session_ttl);
        cache.insert(thread_id.to_string(), tokio::time::Instant::now());
    }

    async fn other_bot_present(&self, thread_id: &str) -> bool {
        let mut cache = self.multibot_threads.lock().await;
        prune_timed_cache(&mut cache, self.session_ttl);
        cache.contains_key(thread_id)
    }

    async fn send_room_event(
        &self,
        room_id: &str,
        event_type: &str,
        content: &Value,
    ) -> Result<String> {
        self.ensure_plaintext_room(room_id).await?;
        let txn_id = format!("openab-{}", Uuid::new_v4());
        let request = self
            .authorized(
                Method::PUT,
                self.endpoint(&["rooms", room_id, "send", event_type, &txn_id])?,
            )?
            .json(content);
        let response: SendEventResponse = self.execute_json(request, "Matrix send event").await?;
        anyhow::ensure!(
            !response.event_id.is_empty(),
            "Matrix send event returned an empty event_id"
        );
        Ok(response.event_id)
    }

    async fn redact_event(&self, room_id: &str, event_id: &str, reason: &str) -> Result<()> {
        self.ensure_plaintext_room(room_id).await?;
        let txn_id = format!("openab-{}", Uuid::new_v4());
        let request = self
            .authorized(
                Method::PUT,
                self.endpoint(&["rooms", room_id, "redact", event_id, &txn_id])?,
            )?
            .json(&json!({ "reason": reason }));
        let _: SendEventResponse = self.execute_json(request, "Matrix redact event").await?;
        Ok(())
    }

    async fn send_text(
        &self,
        channel: &ChannelRef,
        content: &str,
        reply_to: Option<&str>,
    ) -> Result<MessageRef> {
        let (requested_files, visible_content) = parse_matrix_file_directives(content);
        let sent_file = self
            .deliver_output_files(channel, None, &requested_files)
            .await?;
        if visible_content.trim().is_empty() {
            return sent_file.ok_or_else(|| anyhow!("Matrix message content is empty"));
        }
        let body = matrix_message_content(&visible_content, channel.thread_id.as_deref(), reply_to);
        let event_id = self
            .send_room_event(&channel.channel_id, "m.room.message", &body)
            .await?;
        if let Some(thread_id) = channel.thread_id.as_deref() {
            self.note_participated(thread_id).await;
        }
        Ok(MessageRef {
            channel: channel.clone(),
            message_id: event_id,
        })
    }
}

#[async_trait]
impl ChatAdapter for MatrixAdapter {
    fn platform(&self) -> &'static str {
        "matrix"
    }

    fn message_limit(&self) -> usize {
        MATRIX_MESSAGE_LIMIT
    }

    async fn send_message(&self, channel: &ChannelRef, content: &str) -> Result<MessageRef> {
        self.send_text(channel, content, None).await
    }

    async fn create_thread(
        &self,
        channel: &ChannelRef,
        trigger_msg: &MessageRef,
        _title: &str,
    ) -> Result<ChannelRef> {
        let thread_id = channel
            .thread_id
            .clone()
            .or_else(|| self.thread_replies.then(|| trigger_msg.message_id.clone()));
        Ok(ChannelRef {
            platform: "matrix".into(),
            channel_id: channel.channel_id.clone(),
            thread_id,
            parent_id: None,
            origin_event_id: channel.origin_event_id.clone(),
        })
    }

    async fn add_reaction(&self, msg: &MessageRef, emoji: &str) -> Result<()> {
        let key = ReactionKey {
            room_id: msg.channel.channel_id.clone(),
            event_id: msg.message_id.clone(),
            emoji: emoji.to_string(),
        };
        if self.reaction_events.lock().await.contains_key(&key) {
            return Ok(());
        }
        let content = json!({
            "m.relates_to": {
                "rel_type": "m.annotation",
                "event_id": msg.message_id,
                "key": emoji,
            }
        });
        let reaction_event = self
            .send_room_event(&msg.channel.channel_id, "m.reaction", &content)
            .await?;
        let mut reactions = self.reaction_events.lock().await;
        if reactions.len() >= CACHE_MAX_ENTRIES {
            reactions.clear();
        }
        reactions.insert(key, reaction_event);
        Ok(())
    }

    async fn remove_reaction(&self, msg: &MessageRef, emoji: &str) -> Result<()> {
        let key = ReactionKey {
            room_id: msg.channel.channel_id.clone(),
            event_id: msg.message_id.clone(),
            emoji: emoji.to_string(),
        };
        let Some(reaction_event) = self.reaction_events.lock().await.get(&key).cloned() else {
            return Ok(());
        };
        self.redact_event(
            &msg.channel.channel_id,
            &reaction_event,
            "OpenAB status update",
        )
        .await?;
        self.reaction_events.lock().await.remove(&key);
        Ok(())
    }

    async fn edit_message(&self, msg: &MessageRef, content: &str) -> Result<()> {
        let (requested_files, visible_content) = parse_matrix_file_directives(content);
        self.deliver_output_files(&msg.channel, Some(&msg.message_id), &requested_files)
            .await?;
        let visible_content = if visible_content.trim().is_empty() && !requested_files.is_empty() {
            "📎 File sent.".to_string()
        } else {
            visible_content
        };
        let body = matrix_edit_content(
            &visible_content,
            &msg.message_id,
            msg.channel.thread_id.as_deref(),
        );
        self.send_room_event(&msg.channel.channel_id, "m.room.message", &body)
            .await?;
        Ok(())
    }

    async fn send_message_with_reply(
        &self,
        channel: &ChannelRef,
        content: &str,
        reply_to_message_id: &str,
    ) -> Result<MessageRef> {
        self.send_text(channel, content, Some(reply_to_message_id))
            .await
    }

    async fn delete_message(&self, msg: &MessageRef) -> Result<()> {
        self.redact_event(
            &msg.channel.channel_id,
            &msg.message_id,
            "OpenAB removed a streaming placeholder",
        )
        .await
    }

    fn use_streaming(&self, other_bot_present: bool) -> bool {
        self.streaming && !other_bot_present
    }
}

async fn process_matrix_attachment(
    adapter: &MatrixAdapter,
    attachment: &MatrixAttachment,
    stt_config: &SttConfig,
    #[cfg(feature = "filestore")] filestore: Option<&crate::filestore::Filestore>,
) -> MatrixAttachmentOutcome {
    let mut outcome = MatrixAttachmentOutcome::default();
    let download_url = match adapter.media_download_url(&attachment.mxc_uri) {
        Ok(url) => url,
        Err(err) => {
            outcome.warning = Some(format!(
                "⚠️ I couldn't process `{}`: invalid Matrix media URI ({err}).",
                attachment.filename
            ));
            return outcome;
        }
    };
    let download_url = download_url.as_str();
    let token = Some(adapter.access_token.as_str());

    if attachment.kind == MatrixAttachmentKind::Audio {
        if stt_config.enabled {
            match media::download_and_transcribe(
                download_url,
                &attachment.filename,
                &attachment.mime_type,
                attachment.size,
                stt_config,
                token,
            )
            .await
            {
                Some(transcript) => {
                    outcome.blocks.push(ContentBlock::Text {
                        text: format!("[Voice message transcript]: {transcript}"),
                    });
                    outcome
                        .echo_entries
                        .push(crate::stt::EchoEntry::Success(transcript));
                }
                None => {
                    outcome.echo_entries.push(crate::stt::EchoEntry::Failed);
                    outcome.warning = Some(format!(
                        "⚠️ I couldn't transcribe `{}`.",
                        attachment.filename
                    ));
                }
            }
        } else {
            outcome.audio_skipped = true;
        }
        return outcome;
    }

    if attachment.kind == MatrixAttachmentKind::File
        && media::is_text_file(&attachment.filename, Some(&attachment.mime_type))
    {
        #[cfg(feature = "filestore")]
        let result = media::download_and_read_text_file(
            download_url,
            &attachment.filename,
            attachment.size,
            token,
            filestore,
        )
        .await;
        #[cfg(not(feature = "filestore"))]
        let result = media::download_and_read_text_file(
            download_url,
            &attachment.filename,
            attachment.size,
            token,
        )
        .await;
        if let Some((block, _)) = result {
            outcome.blocks.push(block);
        } else {
            outcome.warning = Some(format!(
                "⚠️ I couldn't read `{}`; it may be too large or unavailable.",
                attachment.filename
            ));
        }
        return outcome;
    }

    if attachment.kind == MatrixAttachmentKind::Image {
        match media::download_and_encode_image(
            download_url,
            (!attachment.mime_type.is_empty()).then_some(attachment.mime_type.as_str()),
            &attachment.filename,
            attachment.size,
            token,
        )
        .await
        {
            Ok(block) => outcome.blocks.push(block),
            Err(err) => {
                warn!(filename = %attachment.filename, error = %err, "Matrix image processing failed");
                outcome.warning = Some(format!(
                    "⚠️ I couldn't process image `{}`: {err}.",
                    attachment.filename
                ));
            }
        }
        return outcome;
    }

    if attachment.kind == MatrixAttachmentKind::Video {
        outcome.blocks.push(ContentBlock::Text {
            text: format!(
                "[Video attachment]\nfilename: {}\ncontent_type: {}\nsize_bytes: {}\nmxc_uri: {}",
                attachment.filename, attachment.mime_type, attachment.size, attachment.mxc_uri
            ),
        });
        return outcome;
    }

    #[cfg(feature = "filestore")]
    if let Some(filestore) = filestore {
        if let Some((block, _)) = media::download_and_upload_any_file(
            download_url,
            &attachment.filename,
            attachment.size,
            (!attachment.mime_type.is_empty()).then_some(attachment.mime_type.as_str()),
            token,
            filestore,
        )
        .await
        {
            outcome.blocks.push(block);
            return outcome;
        }
    }

    outcome.warning = Some(format!(
        "⚠️ I couldn't make `{}` available to the agent; configure [filestore] for binary files.",
        attachment.filename
    ));
    outcome
}

#[allow(clippy::too_many_arguments)]
pub async fn run_matrix_adapter(
    adapter: Arc<MatrixAdapter>,
    router: Arc<crate::adapter::AdapterRouter>,
    config: MatrixRunConfig,
    mut since: String,
    stt_config: SttConfig,
    mut shutdown_rx: watch::Receiver<bool>,
    dispatcher: Arc<crate::dispatch::Dispatcher>,
    #[cfg(feature = "filestore")] filestore: Option<Arc<crate::filestore::Filestore>>,
) -> Result<()> {
    let bot_turns = Arc::new(Mutex::new(BotTurnTracker::new(config.max_bot_turns)));
    let mut backoff_secs = 1u64;

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }
        let sync = tokio::select! {
            result = adapter.sync(Some(&since), false) => result,
            _ = shutdown_rx.changed() => return Ok(()),
        };
        let sync = match sync {
            Ok(sync) => sync,
            Err(err) => {
                warn!(error = %err, backoff_secs, "Matrix sync failed; retrying");
                match wait_backoff_or_shutdown(backoff_secs, &mut shutdown_rx).await {
                    Some(next) => backoff_secs = next,
                    None => return Ok(()),
                }
                continue;
            }
        };
        backoff_secs = 1;
        adapter.apply_sync_metadata(&sync).await;
        adapter.join_invited_rooms(&sync, &config).await;

        for (room_id, room) in &sync.rooms.join {
            for event in &room.timeline.events {
                if !adapter.seen_events.lock().await.insert(&event.event_id) {
                    continue;
                }
                handle_matrix_event(
                    room_id,
                    event,
                    adapter.clone(),
                    router.clone(),
                    &config,
                    bot_turns.clone(),
                    &stt_config,
                    dispatcher.clone(),
                    #[cfg(feature = "filestore")]
                    filestore.as_deref(),
                )
                .await;
            }
        }
        since = sync.next_batch;
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_matrix_event(
    room_id: &str,
    event: &MatrixEvent,
    adapter: Arc<MatrixAdapter>,
    router: Arc<crate::adapter::AdapterRouter>,
    config: &MatrixRunConfig,
    bot_turns: Arc<Mutex<BotTurnTracker>>,
    stt_config: &SttConfig,
    dispatcher: Arc<crate::dispatch::Dispatcher>,
    #[cfg(feature = "filestore")] filestore: Option<&crate::filestore::Filestore>,
) {
    if event.event_type != "m.room.message"
        || event.event_id.is_empty()
        || event.sender.is_empty()
        || is_replacement_event(event)
    {
        return;
    }
    if !config.allow_all_rooms && !config.allowed_rooms.contains(room_id) {
        return;
    }
    if let Err(err) = adapter.ensure_plaintext_room(room_id).await {
        debug!(room_id, error = %err, "ignoring Matrix event from unavailable room");
        return;
    }

    let Some(raw_prompt) = matrix_event_prompt(event) else {
        return;
    };
    let (attachment, attachment_error) = match matrix_attachment(event) {
        Ok(attachment) => (attachment, None),
        Err(err) => (None, Some(err)),
    };
    let Some(own_user_id) = adapter.user_id.get() else {
        error!("Matrix identity unavailable after initialization");
        return;
    };
    let is_own_bot = event.sender == *own_user_id;
    let is_bot = config.is_bot(&event.sender, own_user_id);
    let logical_thread_id = matrix_logical_session_id(room_id, event, config.thread_replies);

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
                    warn!(thread_id = %logical_thread_id, turns, "hard Matrix bot turn limit reached")
                }
                TurnSeverity::Soft => {
                    info!(thread_id = %logical_thread_id, turns, "Matrix bot turn limit reached")
                }
            }
            if !is_own_bot && adapter.has_participated(&logical_thread_id).await {
                let channel = matrix_thread_channel(room_id, &logical_thread_id);
                if let Err(err) = adapter.send_message(&channel, &user_message).await {
                    warn!(error = %err, "failed to send Matrix bot turn warning");
                }
            }
            return;
        }
    }
    if is_own_bot {
        return;
    }

    let is_direct = adapter.room_is_direct(room_id).await;
    if !is_bot && !config.allow_all_users && !config.allowed_users.contains(&event.sender) {
        info!(user_id = %event.sender, room_id, "denied Matrix user, ignoring");
        return;
    }
    if l3_gate_applies(is_bot) {
        let decision = router.gate_incoming("matrix", room_id, is_direct, &event.sender);
        if !decision.is_allowed() {
            info!(user_id = %event.sender, room_id, ?decision, "Matrix message denied by trust gate");
            return;
        }
    }

    let mentions_bot = event_mentions_user(event, own_user_id);
    let other_bot_present = adapter.other_bot_present(&logical_thread_id).await;
    let in_thread = event_thread_root(event).is_some();
    let participated = is_direct
        || mentions_bot
        || (in_thread && adapter.has_participated(&logical_thread_id).await);
    let should_process = should_process_message(
        is_bot,
        is_direct,
        mentions_bot,
        in_thread,
        participated,
        other_bot_present,
        config.allow_bot_messages,
        config.allow_user_messages,
        !config.trusted_bot_ids.is_empty(),
        config.trusted_bot_ids.contains(&event.sender),
    );
    let thread_key = dispatcher.key("matrix", &logical_thread_id, &event.sender);

    if let Some(command) =
        matrix_text_body(event).and_then(|body| parse_control_command(body, own_user_id))
    {
        if is_bot && !should_process {
            return;
        }
        handle_control_command(
            command,
            room_id,
            event,
            &adapter,
            &router,
            &dispatcher,
            &logical_thread_id,
        )
        .await;
        return;
    }

    if let Some(body) = matrix_text_body(event) {
        let config_command = strip_user_mention(body, own_user_id);
        if let Some(response) = handle_config_command(&config_command, &router, &thread_key).await {
            if is_bot && !should_process {
                return;
            }
            let channel = ChannelRef {
                platform: "matrix".into(),
                channel_id: room_id.to_string(),
                thread_id: event_thread_root(event).map(str::to_string),
                parent_id: None,
                origin_event_id: None,
            };
            if let Err(err) = adapter.send_message(&channel, &response).await {
                warn!(error = %err, "failed to send Matrix config command response");
            }
            return;
        }
    }
    if !should_process {
        return;
    }

    let prompt = strip_user_mention(&raw_prompt, own_user_id);
    if prompt.is_empty() && attachment.is_none() && attachment_error.is_none() {
        return;
    }
    adapter.note_participated(&logical_thread_id).await;

    let original_thread = event_thread_root(event).map(str::to_string);
    let sender = SenderContext {
        schema: "openab.sender.v1".into(),
        sender_id: event.sender.clone(),
        sender_name: event.sender.clone(),
        display_name: event.sender.clone(),
        channel: "matrix".into(),
        channel_id: room_id.to_string(),
        thread_id: original_thread.clone(),
        is_bot,
        timestamp: Some(crate::timestamp::unix_millis_to_iso8601(
            event.origin_server_ts,
        )),
        message_id: Some(event.event_id.clone()),
        receiver_id: Some(own_user_id.clone()),
    };
    let sender_json = serde_json::to_string(&sender).unwrap_or_else(|_| "{}".into());
    let trigger_msg = MessageRef {
        channel: ChannelRef {
            platform: "matrix".into(),
            channel_id: room_id.to_string(),
            thread_id: original_thread,
            parent_id: None,
            origin_event_id: None,
        },
        message_id: event.event_id.clone(),
    };
    let thread_channel = matrix_response_channel(
        room_id,
        &logical_thread_id,
        in_thread || config.thread_replies,
    );
    let mut extra_blocks = Vec::new();
    if adapter.outbound_file_enabled() {
        extra_blocks.push(matrix_file_delivery_instruction());
    }
    let mut echo_entries = Vec::new();
    let mut warning =
        attachment_error.map(|err| format!("⚠️ I couldn't process this Matrix attachment: {err}."));
    let mut audio_skipped = false;
    if let Some(attachment) = attachment.as_ref() {
        let outcome = process_matrix_attachment(
            &adapter,
            attachment,
            stt_config,
            #[cfg(feature = "filestore")]
            filestore,
        )
        .await;
        extra_blocks = outcome.blocks;
        echo_entries = outcome.echo_entries;
        warning = warning.or(outcome.warning);
        audio_skipped = outcome.audio_skipped;
    }
    if let Some(warning) = warning {
        if let Err(err) = adapter.send_message(&thread_channel, &warning).await {
            warn!(error = %err, "failed to send Matrix attachment warning");
        }
    }
    if audio_skipped {
        let _ = adapter.add_reaction(&trigger_msg, "🎤").await;
    }

    let adapter_dyn: Arc<dyn ChatAdapter> = adapter.clone();
    crate::stt::post_echo(
        &adapter_dyn,
        &thread_channel,
        &trigger_msg,
        &echo_entries,
        stt_config,
    )
    .await;

    if prompt.is_empty() && extra_blocks.is_empty() {
        return;
    }
    let estimated_tokens = crate::dispatch::estimate_tokens(&prompt, &extra_blocks);
    let buffered = crate::dispatch::BufferedMessage {
        sender_json,
        sender_name: sender.sender_name,
        prompt,
        extra_blocks,
        trigger_msg,
        arrived_at: std::time::Instant::now(),
        estimated_tokens,
        other_bot_present,
        recipient: None,
    };
    if let Err(err) = dispatcher
        .submit(thread_key, thread_channel, adapter_dyn, buffered)
        .await
    {
        error!(error = %err, "Matrix dispatcher submit failed");
    }
}

async fn handle_control_command(
    command: MatrixControlCommand,
    room_id: &str,
    event: &MatrixEvent,
    adapter: &MatrixAdapter,
    router: &crate::adapter::AdapterRouter,
    dispatcher: &crate::dispatch::Dispatcher,
    logical_thread_id: &str,
) {
    let session_key = format!("matrix:{logical_thread_id}");
    let dropped = if command == MatrixControlCommand::CancelAll {
        dispatcher.cancel_buffered_thread("matrix", logical_thread_id)
    } else {
        0
    };
    let result = router.pool().cancel_session(&session_key).await;
    let message = match (command, result, dropped) {
        (MatrixControlCommand::Cancel, Ok(()), _) => "🛑 Cancel signal sent.".to_string(),
        (MatrixControlCommand::Cancel, Err(err), _) => format!("⚠️ {err}"),
        (MatrixControlCommand::CancelAll, Ok(()), 0) => "🛑 Cancel signal sent.".to_string(),
        (MatrixControlCommand::CancelAll, Ok(()), _) => {
            "🛑 Cancel signal sent. Buffered messages cleared.".to_string()
        }
        (MatrixControlCommand::CancelAll, Err(_), 0) => {
            "⚠️ Nothing to cancel — no active session and no buffered messages.".to_string()
        }
        (MatrixControlCommand::CancelAll, Err(_), _) => {
            "🛑 Buffered messages cleared. No active session to cancel.".to_string()
        }
    };
    let channel = ChannelRef {
        platform: "matrix".into(),
        channel_id: room_id.to_string(),
        thread_id: event_thread_root(event).map(str::to_string),
        parent_id: None,
        origin_event_id: None,
    };
    if let Err(err) = adapter.send_message(&channel, &message).await {
        warn!(error = %err, command = ?command, "failed to send Matrix control acknowledgement");
    }
}

fn normalize_homeserver_url(homeserver_url: &str) -> Result<Url> {
    let mut url = Url::parse(homeserver_url.trim())
        .with_context(|| format!("invalid Matrix homeserver_url: {homeserver_url}"))?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https"),
        "Matrix homeserver_url must use http or https"
    );
    anyhow::ensure!(
        url.username().is_empty() && url.password().is_none(),
        "Matrix homeserver_url must not contain credentials"
    );
    url.set_query(None);
    url.set_fragment(None);
    let path = format!("{}/_matrix/client/v3/", url.path().trim_end_matches('/'));
    url.set_path(&path);
    Ok(url)
}

fn url_host_is_loopback(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    })
}

fn matrix_message_content(
    content: &str,
    thread_root: Option<&str>,
    reply_to: Option<&str>,
) -> Value {
    let mut body = json!({ "msgtype": "m.text", "body": content });
    if let Some(thread_root) = thread_root {
        body["m.relates_to"] = json!({
            "rel_type": "m.thread",
            "event_id": thread_root,
            "is_falling_back": true,
            "m.in_reply_to": { "event_id": reply_to.unwrap_or(thread_root) },
        });
    } else if let Some(reply_to) = reply_to {
        body["m.relates_to"] = json!({
            "m.in_reply_to": { "event_id": reply_to },
        });
    }
    body
}

fn matrix_file_content(
    filename: &str,
    media_type: &str,
    size: u64,
    content_uri: &str,
    thread_root: Option<&str>,
) -> Value {
    let msgtype = if media_type.starts_with("image/") {
        "m.image"
    } else if media_type.starts_with("audio/") {
        "m.audio"
    } else if media_type.starts_with("video/") {
        "m.video"
    } else {
        "m.file"
    };
    let mut content = json!({
        "msgtype": msgtype,
        "body": filename,
        "filename": filename,
        "url": content_uri,
        "info": {
            "mimetype": media_type,
            "size": size,
        }
    });
    if let Some(thread_root) = thread_root {
        content["m.relates_to"] = json!({
            "rel_type": "m.thread",
            "event_id": thread_root,
            "is_falling_back": true,
            "m.in_reply_to": { "event_id": thread_root },
        });
    }
    content
}

fn parse_matrix_file_directives(content: &str) -> (Vec<String>, String) {
    const OPEN: &str = "<openab-send-file>";
    const CLOSE: &str = "</openab-send-file>";

    let mut requested = Vec::new();
    let mut remaining = content.trim_start();
    while requested.len() < 5 && remaining.starts_with(OPEN) {
        let value_start = OPEN.len();
        let Some(close_offset) = remaining[value_start..].find(CLOSE) else {
            break;
        };
        let close_start = value_start + close_offset;
        let path = remaining[value_start..close_start].trim();
        if path.is_empty() || path.len() > 4096 || path.chars().any(char::is_control) {
            break;
        }
        requested.push(path.to_string());
        remaining = &remaining[close_start + CLOSE.len()..];
        remaining = remaining.trim_start_matches(['\r', '\n']);
    }
    (requested, remaining.to_string())
}

fn matrix_outbound_media_type(filename: &str) -> &'static str {
    match std::path::Path::new(filename)
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("txt" | "log" | "md" | "csv" | "rs" | "py" | "js" | "ts" | "sh") => {
            "text/plain; charset=utf-8"
        }
        Some("html" | "htm") => "text/html; charset=utf-8",
        Some("json" | "jsonl") => "application/json",
        Some("yaml" | "yml") => "application/yaml",
        Some("toml") => "application/toml",
        Some("xml") => "application/xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("svg") => "image/svg+xml",
        Some("pdf") => "application/pdf",
        Some("zip") => "application/zip",
        Some("gz") => "application/gzip",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        Some("ogg" | "opus") => "audio/ogg",
        Some("m4a") => "audio/mp4",
        Some("mp4" | "m4v") => "video/mp4",
        Some("webm") => "video/webm",
        _ => "application/octet-stream",
    }
}

fn matrix_file_delivery_instruction() -> ContentBlock {
    ContentBlock::Text {
        text: "<openab_matrix_file_delivery>\nTo send an existing local file back to Matrix, begin your final response with one exact tag per file: <openab-send-file>relative/or/absolute/path</openab-send-file>. Paths must be inside the configured outbound file root. After the tags, write any normal user-facing message. Do not claim that Matrix attachments are unsupported when this capability is present.\n</openab_matrix_file_delivery>".to_string(),
    }
}

fn matrix_edit_content(content: &str, event_id: &str, thread_root: Option<&str>) -> Value {
    let mut new_content = json!({
        "msgtype": "m.text",
        "body": content,
    });
    if let Some(thread_root) = thread_root {
        new_content["m.relates_to"] = json!({
            "rel_type": "m.thread",
            "event_id": thread_root,
            "is_falling_back": true,
            "m.in_reply_to": { "event_id": thread_root },
        });
    }
    json!({
        "msgtype": "m.text",
        "body": format!("* {content}"),
        "m.new_content": new_content,
        "m.relates_to": {
            "rel_type": "m.replace",
            "event_id": event_id,
        }
    })
}

fn matrix_direct_inviter(invite: &Value) -> Option<&str> {
    invite
        .get("invite_state")?
        .get("events")?
        .as_array()?
        .iter()
        .find(|event| {
            event.get("type").and_then(Value::as_str) == Some("m.room.member")
                && event
                    .get("content")
                    .and_then(|content| content.get("membership"))
                    .and_then(Value::as_str)
                    == Some("invite")
                && event
                    .get("content")
                    .and_then(|content| content.get("is_direct"))
                    .and_then(Value::as_bool)
                    == Some(true)
        })?
        .get("sender")?
        .as_str()
}

fn matrix_event_body(event: &MatrixEvent) -> Option<&str> {
    match event.content.get("msgtype")?.as_str()? {
        "m.text" | "m.notice" | "m.image" | "m.audio" | "m.video" | "m.file" => {
            event.content.get("body")?.as_str()
        }
        _ => None,
    }
}

fn matrix_text_body(event: &MatrixEvent) -> Option<&str> {
    matches!(
        event.content.get("msgtype")?.as_str()?,
        "m.text" | "m.notice"
    )
    .then(|| event.content.get("body")?.as_str())
    .flatten()
}

fn matrix_event_prompt(event: &MatrixEvent) -> Option<String> {
    let msgtype = event.content.get("msgtype")?.as_str()?;
    let body = matrix_event_body(event)?;
    match msgtype {
        "m.text" | "m.notice" => Some(body.to_string()),
        "m.image" | "m.audio" | "m.video" | "m.file" => {
            if let Some(caption) = event.content["org.matrix.msc1767.caption"].as_str() {
                return Some(caption.to_string());
            }
            let filename = event.content["filename"].as_str();
            if filename.is_some_and(|filename| filename != body) {
                Some(body.to_string())
            } else {
                Some(String::new())
            }
        }
        _ => None,
    }
}

fn matrix_attachment(event: &MatrixEvent) -> Result<Option<MatrixAttachment>> {
    let Some(msgtype) = event.content.get("msgtype").and_then(Value::as_str) else {
        return Ok(None);
    };
    let declared_kind = match msgtype {
        "m.image" => Some(MatrixAttachmentKind::Image),
        "m.audio" => Some(MatrixAttachmentKind::Audio),
        "m.video" => Some(MatrixAttachmentKind::Video),
        "m.file" => Some(MatrixAttachmentKind::File),
        _ => None,
    };
    let Some(mut kind) = declared_kind else {
        return Ok(None);
    };
    anyhow::ensure!(
        !event.content["file"].is_object(),
        "encrypted Matrix attachments are not supported"
    );
    let mxc_uri = event.content["url"]
        .as_str()
        .ok_or_else(|| anyhow!("Matrix attachment is missing an mxc:// URL"))?;
    let raw_filename = event.content["filename"]
        .as_str()
        .or_else(|| event.content["body"].as_str())
        .unwrap_or("file");
    let filename: String = raw_filename
        .chars()
        .filter(|character| !character.is_control())
        .take(200)
        .collect();
    let filename = if filename.is_empty() {
        "file".to_string()
    } else {
        filename.replace('`', "'")
    };
    let mime_type = event.content["info"]["mimetype"]
        .as_str()
        .map(media::strip_mime_params)
        .unwrap_or("")
        .to_string();
    let size = event.content["info"]["size"].as_u64().unwrap_or(0);

    if kind == MatrixAttachmentKind::File {
        kind = if mime_type.starts_with("image/") {
            MatrixAttachmentKind::Image
        } else if mime_type.starts_with("audio/") {
            MatrixAttachmentKind::Audio
        } else if mime_type.starts_with("video/") {
            MatrixAttachmentKind::Video
        } else {
            MatrixAttachmentKind::File
        };
    }

    Ok(Some(MatrixAttachment {
        kind,
        filename,
        mime_type,
        size,
        mxc_uri: mxc_uri.to_string(),
    }))
}

fn is_replacement_event(event: &MatrixEvent) -> bool {
    event.content["m.relates_to"]["rel_type"].as_str() == Some("m.replace")
}

fn event_thread_root(event: &MatrixEvent) -> Option<&str> {
    let relation = &event.content["m.relates_to"];
    (relation["rel_type"].as_str() == Some("m.thread"))
        .then(|| relation["event_id"].as_str())
        .flatten()
        .filter(|value| !value.is_empty())
}

fn event_mentions_user(event: &MatrixEvent, user_id: &str) -> bool {
    event.content["m.mentions"]["user_ids"]
        .as_array()
        .is_some_and(|users| users.iter().any(|value| value.as_str() == Some(user_id)))
        || matrix_event_body(event).is_some_and(|body| body.contains(user_id))
}

fn strip_user_mention(body: &str, user_id: &str) -> String {
    body.replace(user_id, "").trim().to_string()
}

fn parse_control_command(body: &str, user_id: &str) -> Option<MatrixControlCommand> {
    match strip_user_mention(body, user_id).as_str() {
        "/cancel" => Some(MatrixControlCommand::Cancel),
        "/cancel-all" => Some(MatrixControlCommand::CancelAll),
        _ => None,
    }
}

fn matrix_logical_session_id(room_id: &str, event: &MatrixEvent, thread_replies: bool) -> String {
    event_thread_root(event)
        .map(str::to_string)
        .unwrap_or_else(|| {
            if thread_replies {
                event.event_id.clone()
            } else {
                room_id.to_string()
            }
        })
}

fn matrix_thread_channel(room_id: &str, thread_id: &str) -> ChannelRef {
    matrix_response_channel(room_id, thread_id, true)
}

fn matrix_response_channel(room_id: &str, thread_id: &str, threaded: bool) -> ChannelRef {
    ChannelRef {
        platform: "matrix".into(),
        channel_id: room_id.to_string(),
        thread_id: threaded.then(|| thread_id.to_string()),
        parent_id: None,
        origin_event_id: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn should_process_message(
    is_bot: bool,
    is_direct: bool,
    mentions_bot: bool,
    in_thread: bool,
    participated: bool,
    other_bot_present: bool,
    allow_bot_messages: AllowBots,
    allow_user_messages: AllowUsers,
    trusted_bot_ids_configured: bool,
    is_trusted_bot: bool,
) -> bool {
    if is_bot {
        if trusted_bot_ids_configured && !is_trusted_bot {
            return false;
        }
        if is_trusted_bot && mentions_bot {
            return true;
        }
        return match allow_bot_messages {
            AllowBots::Off => false,
            AllowBots::Mentions => is_direct || mentions_bot,
            AllowBots::All => is_direct || mentions_bot || (in_thread && participated),
        };
    }
    if is_direct {
        return true;
    }
    if !in_thread {
        return mentions_bot;
    }
    match allow_user_messages {
        AllowUsers::Mentions => mentions_bot,
        AllowUsers::Involved => mentions_bot || participated,
        AllowUsers::MultibotMentions => mentions_bot || (participated && !other_bot_present),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn message_event(content: Value) -> MatrixEvent {
        MatrixEvent {
            event_type: "m.room.message".into(),
            event_id: "$event:example.com".into(),
            sender: "@alice:example.com".into(),
            origin_server_ts: 1_714_204_397_123,
            content,
        }
    }

    #[test]
    fn normalizes_homeserver_api_path() {
        let url = normalize_homeserver_url("https://matrix.example.com/base/").unwrap();
        assert_eq!(
            url.as_str(),
            "https://matrix.example.com/base/_matrix/client/v3/"
        );
        assert!(normalize_homeserver_url("ftp://matrix.example.com").is_err());
        assert!(normalize_homeserver_url("https://user:pass@matrix.example.com").is_err());
    }

    #[test]
    fn non_loopback_http_requires_explicit_opt_in() {
        let denied: MatrixConfig = toml::from_str(
            "homeserver_url = \"http://matrix.example.com\"\naccess_token = \"token\"\n",
        )
        .unwrap();
        assert!(MatrixAdapter::new(&denied, Duration::from_secs(60)).is_err());

        let allowed: MatrixConfig = toml::from_str(
            "homeserver_url = \"http://matrix.example.com\"\naccess_token = \"token\"\nallow_insecure_http = true\n",
        )
        .unwrap();
        assert!(MatrixAdapter::new(&allowed, Duration::from_secs(60)).is_ok());

        let loopback: MatrixConfig = toml::from_str(
            "homeserver_url = \"http://127.0.0.1:8008\"\naccess_token = \"token\"\n",
        )
        .unwrap();
        assert!(MatrixAdapter::new(&loopback, Duration::from_secs(60)).is_ok());
    }

    #[test]
    fn thread_message_has_fallback_relation() {
        let content = matrix_message_content("hello", Some("$root"), None);
        assert_eq!(content["m.relates_to"]["rel_type"], "m.thread");
        assert_eq!(content["m.relates_to"]["event_id"], "$root");
        assert_eq!(
            content["m.relates_to"]["m.in_reply_to"]["event_id"],
            "$root"
        );
    }

    #[test]
    fn logical_session_id_matches_reply_mode() {
        let top_level = message_event(json!({
            "msgtype": "m.text",
            "body": "hello"
        }));
        assert_eq!(
            matrix_logical_session_id("!room:example.com", &top_level, true),
            "$event:example.com"
        );
        assert_eq!(
            matrix_logical_session_id("!room:example.com", &top_level, false),
            "!room:example.com"
        );

        let threaded = message_event(json!({
            "msgtype": "m.text",
            "body": "follow-up",
            "m.relates_to": { "rel_type": "m.thread", "event_id": "$root" }
        }));
        assert_eq!(
            matrix_logical_session_id("!room:example.com", &threaded, false),
            "$root"
        );
    }

    #[tokio::test]
    async fn create_thread_honors_thread_reply_mode() {
        let trigger = MessageRef {
            channel: matrix_response_channel("!room:example.com", "$root", false),
            message_id: "$trigger:example.com".into(),
        };

        let threaded_cfg: MatrixConfig =
            toml::from_str("homeserver_url = \"http://127.0.0.1:9\"\naccess_token = \"token\"\n")
                .unwrap();
        let threaded = MatrixAdapter::new(&threaded_cfg, Duration::from_secs(60)).unwrap();
        let channel = threaded
            .create_thread(&trigger.channel, &trigger, "test")
            .await
            .unwrap();
        assert_eq!(channel.thread_id.as_deref(), Some("$trigger:example.com"));

        let room_cfg: MatrixConfig = toml::from_str(
            "homeserver_url = \"http://127.0.0.1:9\"\naccess_token = \"token\"\nthread_replies = false\n",
        )
        .unwrap();
        let room = MatrixAdapter::new(&room_cfg, Duration::from_secs(60)).unwrap();
        let channel = room
            .create_thread(&trigger.channel, &trigger, "test")
            .await
            .unwrap();
        assert!(channel.thread_id.is_none());
    }

    #[test]
    fn explicit_reply_is_preserved_inside_thread() {
        let content = matrix_message_content("hello", Some("$root"), Some("$reply"));
        assert_eq!(
            content["m.relates_to"]["m.in_reply_to"]["event_id"],
            "$reply"
        );
    }

    #[test]
    fn edit_payload_uses_replace_relation() {
        let content = matrix_edit_content("updated", "$original", Some("$root"));
        assert_eq!(content["m.relates_to"]["rel_type"], "m.replace");
        assert_eq!(content["m.relates_to"]["event_id"], "$original");
        assert_eq!(content["m.new_content"]["body"], "updated");
        assert_eq!(
            content["m.new_content"]["m.relates_to"]["rel_type"],
            "m.thread"
        );
        assert_eq!(
            content["m.new_content"]["m.relates_to"]["event_id"],
            "$root"
        );
    }

    #[test]
    fn parses_thread_and_structured_mention() {
        let event = message_event(json!({
            "msgtype": "m.text",
            "body": "please help",
            "m.mentions": { "user_ids": ["@openab:example.com"] },
            "m.relates_to": { "rel_type": "m.thread", "event_id": "$root" }
        }));
        assert_eq!(event_thread_root(&event), Some("$root"));
        assert!(event_mentions_user(&event, "@openab:example.com"));
    }

    #[test]
    fn parses_matrix_attachments_and_captions() {
        let image = message_event(json!({
            "msgtype": "m.image",
            "body": "please inspect this",
            "filename": "photo.png",
            "url": "mxc://media.example.com/abc123",
            "info": { "mimetype": "image/png; charset=binary", "size": 42 }
        }));
        assert_eq!(
            matrix_event_prompt(&image).as_deref(),
            Some("please inspect this")
        );
        assert_eq!(
            matrix_attachment(&image).unwrap(),
            Some(MatrixAttachment {
                kind: MatrixAttachmentKind::Image,
                filename: "photo.png".into(),
                mime_type: "image/png".into(),
                size: 42,
                mxc_uri: "mxc://media.example.com/abc123".into(),
            })
        );

        let audio_file = message_event(json!({
            "msgtype": "m.file",
            "body": "voice.ogg",
            "url": "mxc://media.example.com/voice",
            "info": { "mimetype": "audio/ogg", "size": 100 }
        }));
        assert_eq!(
            matrix_attachment(&audio_file).unwrap().unwrap().kind,
            MatrixAttachmentKind::Audio
        );
        assert_eq!(matrix_event_prompt(&audio_file).as_deref(), Some(""));

        let encrypted = message_event(json!({
            "msgtype": "m.file",
            "body": "secret.pdf",
            "file": { "url": "mxc://media.example.com/encrypted" },
            "info": { "mimetype": "application/pdf", "size": 100 }
        }));
        assert!(matrix_attachment(&encrypted)
            .unwrap_err()
            .to_string()
            .contains("encrypted Matrix attachments"));
    }

    #[test]
    fn builds_authenticated_matrix_media_download_url() {
        let cfg: MatrixConfig = toml::from_str(
            "homeserver_url = \"https://matrix.example.com/base\"\naccess_token = \"token\"\n",
        )
        .unwrap();
        let adapter = MatrixAdapter::new(&cfg, Duration::from_secs(60)).unwrap();
        assert_eq!(
            adapter
                .media_download_url("mxc://remote.example.com:8448/media-id")
                .unwrap()
                .as_str(),
            "https://matrix.example.com/base/_matrix/client/v1/media/download/remote.example.com:8448/media-id"
        );
        assert!(adapter
            .media_download_url("https://remote.example.com/media-id")
            .is_err());
        assert!(adapter
            .media_download_url("mxc://remote.example.com/a/b")
            .is_err());
    }

    #[test]
    fn parses_matrix_outbound_file_directives() {
        let (files, body) = parse_matrix_file_directives(
            "<openab-send-file>poem.txt</openab-send-file>\n<openab-send-file>reports/a.pdf</openab-send-file>\nFiles attached.",
        );
        assert_eq!(files, vec!["poem.txt", "reports/a.pdf"]);
        assert_eq!(body, "Files attached.");

        let (files, body) = parse_matrix_file_directives("normal response");
        assert!(files.is_empty());
        assert_eq!(body, "normal response");
    }

    #[tokio::test]
    async fn outbound_file_loader_enforces_configured_root() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("poem.txt"), "hello").unwrap();
        std::fs::write(outside.path().join("secret.txt"), "secret").unwrap();
        let cfg: MatrixConfig = toml::from_str(&format!(
            "homeserver_url = \"http://127.0.0.1:9\"\naccess_token = \"token\"\noutbound_file_root = {:?}\n",
            root.path().to_string_lossy()
        ))
        .unwrap();
        let adapter = MatrixAdapter::new(&cfg, Duration::from_secs(60)).unwrap();
        let (filename, media_type, data) = adapter.load_outbound_file("poem.txt").await.unwrap();
        assert_eq!(filename, "poem.txt");
        assert_eq!(media_type, "text/plain; charset=utf-8");
        assert_eq!(data, b"hello");
        assert!(adapter
            .load_outbound_file(outside.path().join("secret.txt").to_str().unwrap())
            .await
            .unwrap_err()
            .to_string()
            .contains("must stay within"));
    }

    #[tokio::test]
    async fn uploads_agent_file_and_sends_matrix_file_event() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("poem.txt"), "summer night").unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..read]);
                    let Some(header_end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&bytes[..header_end + 4]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    if bytes.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
                let request = String::from_utf8(bytes).unwrap();
                let body = if request.contains("/_matrix/media/v3/upload") {
                    json!({ "content_uri": "mxc://example.com/uploaded" }).to_string()
                } else {
                    json!({ "event_id": "$file:example.com" }).to_string()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                requests.push(request);
            }
            requests
        });

        let cfg: MatrixConfig = toml::from_str(&format!(
            "homeserver_url = \"http://{address}\"\naccess_token = \"upload-token\"\noutbound_file_root = {:?}\n",
            root.path().to_string_lossy()
        ))
        .unwrap();
        let adapter = MatrixAdapter::new(&cfg, Duration::from_secs(60)).unwrap();
        adapter
            .room_security
            .lock()
            .await
            .insert("!room:example.com".into(), false);
        let channel = matrix_thread_channel("!room:example.com", "$root:example.com");
        let sent = adapter
            .send_outbound_file(&channel, "poem.txt")
            .await
            .unwrap();
        assert_eq!(sent.message_id, "$file:example.com");

        let requests = server.await.unwrap();
        assert!(requests[0].contains("POST /_matrix/media/v3/upload?filename=poem.txt"));
        assert!(requests[0].contains("summer night"));
        assert!(requests[0]
            .to_ascii_lowercase()
            .contains("authorization: bearer upload-token"));
        assert!(requests[1].contains("/send/m.room.message/"));
        assert!(requests[1].contains("\"msgtype\":\"m.file\""));
        assert!(requests[1].contains("\"url\":\"mxc://example.com/uploaded\""));
        assert!(requests[1].contains("\"rel_type\":\"m.thread\""));
    }

    #[tokio::test]
    async fn downloads_matrix_text_attachment_with_bearer_auth() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&buffer[..read]);
                if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let body = b"hello from Matrix";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            String::from_utf8(bytes).unwrap()
        });

        let cfg: MatrixConfig = toml::from_str(&format!(
            "homeserver_url = \"http://{address}\"\naccess_token = \"media-token\"\n"
        ))
        .unwrap();
        let adapter = MatrixAdapter::new(&cfg, Duration::from_secs(60)).unwrap();
        let attachment = MatrixAttachment {
            kind: MatrixAttachmentKind::File,
            filename: "notes.txt".into(),
            mime_type: "text/plain".into(),
            size: 17,
            mxc_uri: "mxc://remote.example.com/media-id".into(),
        };
        let outcome = process_matrix_attachment(
            &adapter,
            &attachment,
            &SttConfig::default(),
            #[cfg(feature = "filestore")]
            None,
        )
        .await;
        assert_eq!(outcome.blocks.len(), 1);
        match &outcome.blocks[0] {
            ContentBlock::Text { text } => assert!(text.contains("hello from Matrix")),
            ContentBlock::Image { .. } => panic!("expected text attachment"),
        }

        let request = server.await.unwrap();
        assert!(request.contains("/_matrix/client/v1/media/download/remote.example.com/media-id"));
        assert!(request
            .to_ascii_lowercase()
            .contains("authorization: bearer media-token"));
    }

    #[test]
    fn ignores_replacement_as_inbound_prompt() {
        let event = message_event(matrix_edit_content("updated", "$original", None));
        assert!(is_replacement_event(&event));
    }

    #[test]
    fn control_commands_are_exact_and_allow_optional_mxid() {
        assert_eq!(
            parse_control_command("/cancel", "@openab:example.com"),
            Some(MatrixControlCommand::Cancel)
        );
        assert_eq!(
            parse_control_command("@openab:example.com /cancel-all", "@openab:example.com"),
            Some(MatrixControlCommand::CancelAll)
        );
        assert_eq!(
            parse_control_command("please /cancel", "@openab:example.com"),
            None
        );
    }

    #[test]
    fn untrusted_bot_is_rejected_even_when_mentioned() {
        assert!(!should_process_message(
            true,
            false,
            true,
            true,
            true,
            false,
            AllowBots::All,
            AllowUsers::MultibotMentions,
            true,
            false,
        ));
    }

    #[test]
    fn trusted_bot_mention_overrides_off_mode() {
        assert!(should_process_message(
            true,
            false,
            true,
            true,
            false,
            true,
            AllowBots::Off,
            AllowUsers::MultibotMentions,
            true,
            true,
        ));
    }

    #[test]
    fn direct_human_message_does_not_require_mention() {
        assert!(should_process_message(
            false,
            true,
            false,
            false,
            false,
            false,
            AllowBots::Off,
            AllowUsers::Mentions,
            false,
            false,
        ));
    }

    #[test]
    fn seen_event_cache_deduplicates_and_bounds_memory() {
        let mut seen = SeenEvents::default();
        assert!(seen.insert("$one"));
        assert!(!seen.insert("$one"));
        assert!(!seen.insert(""));
        for index in 0..=SEEN_EVENT_LIMIT {
            seen.insert(&format!("${index}"));
        }
        assert!(seen.order.len() <= SEEN_EVENT_LIMIT);
        assert!(seen.ids.len() <= SEEN_EVENT_LIMIT);
    }

    #[tokio::test]
    async fn initialize_and_send_use_matrix_http_contract() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..read]);
                    let Some(header_end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&bytes[..header_end + 4]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    if bytes.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
                let request = String::from_utf8(bytes).unwrap();
                let body = if request.contains("/account/whoami") {
                    json!({ "user_id": "@openab:example.com" }).to_string()
                } else if request.contains("/_matrix/client/v3/sync") {
                    json!({
                        "next_batch": "s1",
                        "rooms": {
                            "join": {
                                "!room:example.com": {
                                    "state": { "events": [] },
                                    "timeline": { "events": [{
                                        "type": "m.room.message",
                                        "event_id": "$historical:example.com",
                                        "sender": "@alice:example.com",
                                        "origin_server_ts": 1,
                                        "content": { "msgtype": "m.text", "body": "old" }
                                    }] }
                                }
                            }
                        }
                    })
                    .to_string()
                } else {
                    json!({ "event_id": "$sent:example.com" }).to_string()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                requests.push(request);
            }
            requests
        });

        let cfg: MatrixConfig = toml::from_str(&format!(
            "homeserver_url = \"http://{address}\"\naccess_token = \"secret-token\"\nuser_id = \"@openab:example.com\"\nallowed_rooms = [\"!room:example.com\"]\nallowed_users = [\"@alice:example.com\"]\n"
        ))
        .unwrap();
        let adapter = MatrixAdapter::new(&cfg, Duration::from_secs(60)).unwrap();
        let run_config = MatrixRunConfig::from_config(&cfg);
        assert_eq!(adapter.initialize(&run_config).await.unwrap(), "s1");
        assert!(
            !adapter
                .seen_events
                .lock()
                .await
                .insert("$historical:example.com"),
            "initial timeline must be marked seen without dispatching it"
        );
        let sent = adapter
            .send_message(
                &matrix_thread_channel("!room:example.com", "$root:example.com"),
                "hello",
            )
            .await
            .unwrap();
        assert_eq!(sent.message_id, "$sent:example.com");

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|request| request
            .to_ascii_lowercase()
            .contains("authorization: bearer secret-token")));
        let send = &requests[2];
        assert!(send.contains("/rooms/"));
        assert!(send.contains("/send/m.room.message/"));
        assert!(send.contains("\"rel_type\":\"m.thread\""));
        assert!(send.contains("\"event_id\":\"$root:example.com\""));
    }

    #[tokio::test]
    async fn auto_join_accepts_admitted_invites() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..read]);
                    if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(bytes).unwrap();
                let body = if request.contains("/account/whoami") {
                    json!({ "user_id": "@openab:example.com" }).to_string()
                } else if request.contains("/sync") {
                    json!({
                        "next_batch": "s1",
                        "rooms": {
                            "invite": {
                                "!invite:example.com": {
                                    "invite_state": { "events": [] }
                                }
                            }
                        }
                    })
                    .to_string()
                } else {
                    json!({ "room_id": "!invite:example.com" }).to_string()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                requests.push(request);
            }
            requests
        });

        let cfg: MatrixConfig = toml::from_str(&format!(
            "homeserver_url = \"http://{address}\"\naccess_token = \"token\"\nallow_all_rooms = true\nauto_join_invites = true\n"
        ))
        .unwrap();
        let adapter = MatrixAdapter::new(&cfg, Duration::from_secs(60)).unwrap();
        let run_config = MatrixRunConfig::from_config(&cfg);
        assert_eq!(adapter.initialize(&run_config).await.unwrap(), "s1");

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[2].starts_with("POST "));
        assert!(requests[2].contains("/rooms/"));
        assert!(requests[2].contains("/join"));
    }

    #[tokio::test]
    async fn encrypted_and_unknown_rooms_fail_closed_before_http() {
        let cfg: MatrixConfig =
            toml::from_str("homeserver_url = \"http://127.0.0.1:9\"\naccess_token = \"token\"\n")
                .unwrap();
        let adapter = MatrixAdapter::new(&cfg, Duration::from_secs(60)).unwrap();
        let channel = matrix_thread_channel("!encrypted:example.com", "$root");

        let unknown = adapter.send_message(&channel, "hello").await.unwrap_err();
        assert!(unknown.to_string().contains("security state is unknown"));

        adapter
            .room_security
            .lock()
            .await
            .insert("!encrypted:example.com".into(), true);
        let encrypted = adapter.send_message(&channel, "hello").await.unwrap_err();
        assert!(encrypted.to_string().contains("E2EE is not supported"));
    }

    #[test]
    fn direct_invite_metadata_identifies_the_inviter() {
        let invite = json!({
            "invite_state": {
                "events": [{
                    "type": "m.room.member",
                    "sender": "@alice:example.com",
                    "content": { "membership": "invite", "is_direct": true }
                }]
            }
        });
        assert_eq!(matrix_direct_inviter(&invite), Some("@alice:example.com"));

        let group_invite = json!({
            "invite_state": {
                "events": [{
                    "type": "m.room.member",
                    "sender": "@alice:example.com",
                    "content": { "membership": "invite", "is_direct": false }
                }]
            }
        });
        assert_eq!(matrix_direct_inviter(&group_invite), None);
    }

    #[tokio::test]
    async fn remembering_direct_invite_preserves_and_persists_account_data() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for index in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..read]);
                    let Some(header_end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&bytes[..header_end + 4]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    if bytes.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
                requests.push(String::from_utf8(bytes).unwrap());
                let body = if index == 0 {
                    json!({ "@bob:example.com": ["!old:example.com"] }).to_string()
                } else {
                    json!({}).to_string()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });

        let cfg: MatrixConfig = toml::from_str(&format!(
            "homeserver_url = \"http://{address}\"\naccess_token = \"token\"\n"
        ))
        .unwrap();
        let adapter = MatrixAdapter::new(&cfg, Duration::from_secs(60)).unwrap();
        adapter.user_id.set("@openab:example.com".into()).unwrap();
        adapter
            .remember_direct_room("@alice:example.com", "!dm:example.com")
            .await
            .unwrap();
        assert!(adapter.room_is_direct("!dm:example.com").await);

        let requests = server.await.unwrap();
        assert!(requests[0].starts_with("GET "));
        assert!(requests[1].starts_with("PUT "));
        assert!(requests[1].contains("\"@bob:example.com\":[\"!old:example.com\"]"));
        assert!(requests[1].contains("\"@alice:example.com\":[\"!dm:example.com\"]"));
    }

    #[tokio::test]
    async fn direct_room_account_data_replaces_cached_mapping() {
        let cfg: MatrixConfig =
            toml::from_str("homeserver_url = \"http://127.0.0.1:9\"\naccess_token = \"token\"\n")
                .unwrap();
        let adapter = MatrixAdapter::new(&cfg, Duration::from_secs(60)).unwrap();
        let sync: MatrixSyncResponse = serde_json::from_value(json!({
            "next_batch": "s1",
            "account_data": {
                "events": [{
                    "type": "m.direct",
                    "content": { "@alice:example.com": ["!dm:example.com"] }
                }]
            }
        }))
        .unwrap();
        adapter.apply_sync_metadata(&sync).await;
        assert!(adapter.room_is_direct("!dm:example.com").await);
        assert!(!adapter.room_is_direct("!other:example.com").await);
    }

    #[test]
    fn sync_response_parses_room_state_and_timeline() {
        let response: MatrixSyncResponse = serde_json::from_value(json!({
            "next_batch": "s1",
            "rooms": {
                "join": {
                    "!room:example.com": {
                        "state": { "events": [{ "type": "m.room.encryption", "content": {} }] },
                        "timeline": { "events": [{
                            "type": "m.room.message",
                            "event_id": "$event",
                            "sender": "@alice:example.com",
                            "origin_server_ts": 1,
                            "content": { "msgtype": "m.text", "body": "hello" }
                        }] }
                    }
                }
            }
        }))
        .unwrap();
        assert_eq!(response.next_batch, "s1");
        let room = &response.rooms.join["!room:example.com"];
        assert_eq!(room.state.events[0].event_type, "m.room.encryption");
        assert_eq!(matrix_event_body(&room.timeline.events[0]), Some("hello"));
    }
}
