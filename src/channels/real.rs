use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;

use crate::domain::{ChannelKind, OutboxItem};

const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryReceipt {
    pub external_id: String,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ChannelDeliveryError {
    #[error("{0:?} destination is invalid")]
    InvalidDestination(ChannelKind),
    #[error("{0:?} delivery timed out")]
    Timeout(ChannelKind),
    #[error("{0:?} transport failed")]
    Transport(ChannelKind),
    #[error("{kind:?} rate limited delivery for {retry_after_secs} seconds")]
    RateLimited {
        kind: ChannelKind,
        retry_after_secs: u64,
    },
    #[error("{kind:?} destination is unavailable: {detail}")]
    DestinationUnavailable { kind: ChannelKind, detail: String },
    #[error("{kind:?} rejected delivery: {detail}")]
    Rejected { kind: ChannelKind, detail: String },
    #[error("{0:?} returned a malformed or oversized response")]
    Malformed(ChannelKind),
    #[error("{0:?} is not configured")]
    NotConfigured(ChannelKind),
}

impl ChannelDeliveryError {
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout(_) | Self::Transport(_) | Self::RateLimited { .. } | Self::Malformed(_)
        )
    }
}

#[derive(Clone)]
pub struct TelegramClient {
    http: reqwest::Client,
    api_base: String,
    token: String,
    chat_id: i64,
}

impl TelegramClient {
    pub fn new(
        api_base: impl Into<String>,
        token: impl Into<String>,
        chat_id: i64,
        deadline: Duration,
    ) -> Result<Self, ChannelDeliveryError> {
        Ok(Self {
            http: http_client(deadline, ChannelKind::Telegram)?,
            api_base: api_base.into().trim_end_matches('/').to_owned(),
            token: token.into(),
            chat_id,
        })
    }

    pub async fn send(&self, item: &OutboxItem) -> Result<DeliveryReceipt, ChannelDeliveryError> {
        if item.kind != ChannelKind::Telegram {
            return Err(ChannelDeliveryError::InvalidDestination(item.kind));
        }
        let topic_id = item
            .destination
            .parse::<i64>()
            .map_err(|_| ChannelDeliveryError::InvalidDestination(item.kind))?;
        telegram_send(
            &self.http,
            &self.api_base,
            &self.token,
            json!({
                "chat_id": self.chat_id,
                "message_thread_id": topic_id,
                "text": item.body,
                "link_preview_options": {"is_disabled": true}
            }),
            item,
        )
        .await
    }
}

#[derive(Clone)]
pub struct SlackClient {
    http: reqwest::Client,
    api_base: String,
    token: String,
}

impl SlackClient {
    pub fn new(
        api_base: impl Into<String>,
        token: impl Into<String>,
        deadline: Duration,
    ) -> Result<Self, ChannelDeliveryError> {
        Ok(Self {
            http: http_client(deadline, ChannelKind::Slack)?,
            api_base: api_base.into().trim_end_matches('/').to_owned(),
            token: token.into(),
        })
    }

    pub async fn send(&self, item: &OutboxItem) -> Result<DeliveryReceipt, ChannelDeliveryError> {
        if item.kind != ChannelKind::Slack || item.destination.is_empty() {
            return Err(ChannelDeliveryError::InvalidDestination(item.kind));
        }
        let response = self
            .http
            .post(format!("{}/chat.postMessage", self.api_base))
            .bearer_auth(&self.token)
            .header("x-panetone-delivery-id", item.id.to_string())
            .json(&json!({
                "channel": item.destination,
                "text": item.body,
                "client_msg_id": item.id.to_string()
            }))
            .send()
            .await
            .map_err(|error| map_http_error(ChannelKind::Slack, &error))?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            return Err(ChannelDeliveryError::RateLimited {
                kind: ChannelKind::Slack,
                retry_after_secs: retry_after_header(&response),
            });
        }
        let status = response.status();
        let body = response_body(response, ChannelKind::Slack).await?;
        let parsed: SlackResponse = serde_json::from_slice(&body)
            .map_err(|_| ChannelDeliveryError::Malformed(ChannelKind::Slack))?;
        if status.is_success() && parsed.ok {
            return parsed
                .ts
                .filter(|value| !value.is_empty())
                .map(|external_id| DeliveryReceipt { external_id })
                .ok_or(ChannelDeliveryError::Malformed(ChannelKind::Slack));
        }
        let detail = safe_remote_detail(parsed.error.as_deref().unwrap_or("unknown Slack error"));
        if matches!(
            detail.as_str(),
            "channel_not_found" | "is_archived" | "not_in_channel"
        ) {
            Err(ChannelDeliveryError::DestinationUnavailable {
                kind: ChannelKind::Slack,
                detail,
            })
        } else {
            Err(ChannelDeliveryError::Rejected {
                kind: ChannelKind::Slack,
                detail,
            })
        }
    }
}

#[derive(Clone)]
pub struct SignalClient {
    socket: PathBuf,
    account: String,
    deadline: Duration,
}

impl SignalClient {
    pub fn new(socket: impl Into<PathBuf>, account: impl Into<String>, deadline: Duration) -> Self {
        Self {
            socket: socket.into(),
            account: account.into(),
            deadline,
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub async fn send(&self, item: &OutboxItem) -> Result<DeliveryReceipt, ChannelDeliveryError> {
        if item.kind != ChannelKind::Signal || item.destination.is_empty() {
            return Err(ChannelDeliveryError::InvalidDestination(item.kind));
        }
        timeout(self.deadline, self.send_inner(item))
            .await
            .map_err(|_| ChannelDeliveryError::Timeout(ChannelKind::Signal))?
    }

    async fn send_inner(&self, item: &OutboxItem) -> Result<DeliveryReceipt, ChannelDeliveryError> {
        let mut stream = UnixStream::connect(&self.socket)
            .await
            .map_err(|_| ChannelDeliveryError::Transport(ChannelKind::Signal))?;
        let request_id = item.id.to_string();
        let request = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "send",
            "params": {
                "groupId": item.destination,
                "message": item.body,
                "account": self.account
            }
        });
        let mut encoded = serde_json::to_vec(&request)
            .map_err(|_| ChannelDeliveryError::Malformed(ChannelKind::Signal))?;
        encoded.push(b'\n');
        stream
            .write_all(&encoded)
            .await
            .map_err(|_| ChannelDeliveryError::Transport(ChannelKind::Signal))?;
        stream
            .shutdown()
            .await
            .map_err(|_| ChannelDeliveryError::Transport(ChannelKind::Signal))?;

        let mut reader = BufReader::new(stream);
        loop {
            let mut line = Vec::new();
            let read = reader
                .read_until(b'\n', &mut line)
                .await
                .map_err(|_| ChannelDeliveryError::Transport(ChannelKind::Signal))?;
            if read == 0 {
                return Err(ChannelDeliveryError::Transport(ChannelKind::Signal));
            }
            if line.len() > MAX_RESPONSE_BYTES {
                return Err(ChannelDeliveryError::Malformed(ChannelKind::Signal));
            }
            let response: JsonRpcResponse = serde_json::from_slice(&line)
                .map_err(|_| ChannelDeliveryError::Malformed(ChannelKind::Signal))?;
            if response.id.as_deref() != Some(request_id.as_str()) {
                continue;
            }
            if let Some(error) = response.error {
                return Err(ChannelDeliveryError::Rejected {
                    kind: ChannelKind::Signal,
                    detail: safe_remote_detail(&error.to_string()),
                });
            }
            let result = response
                .result
                .ok_or(ChannelDeliveryError::Malformed(ChannelKind::Signal))?;
            let external_id = result
                .get("timestamp")
                .and_then(Value::as_i64)
                .map(|value| value.to_string())
                .or_else(|| {
                    result
                        .get("timestamp")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .ok_or(ChannelDeliveryError::Malformed(ChannelKind::Signal))?;
            return Ok(DeliveryReceipt { external_id });
        }
    }
}

#[derive(Clone, Default)]
pub struct RealChannels {
    pub telegram: Option<TelegramClient>,
    pub signal: Option<SignalClient>,
    pub slack: Option<SlackClient>,
}

impl RealChannels {
    pub async fn send(&self, item: &OutboxItem) -> Result<DeliveryReceipt, ChannelDeliveryError> {
        match item.kind {
            ChannelKind::Telegram => {
                self.telegram
                    .as_ref()
                    .ok_or(ChannelDeliveryError::NotConfigured(item.kind))?
                    .send(item)
                    .await
            }
            ChannelKind::Signal => {
                self.signal
                    .as_ref()
                    .ok_or(ChannelDeliveryError::NotConfigured(item.kind))?
                    .send(item)
                    .await
            }
            ChannelKind::Slack => {
                self.slack
                    .as_ref()
                    .ok_or(ChannelDeliveryError::NotConfigured(item.kind))?
                    .send(item)
                    .await
            }
        }
    }
}

#[derive(Deserialize)]
struct TelegramResponse {
    ok: bool,
    result: Option<TelegramMessage>,
    description: Option<String>,
    error_code: Option<u16>,
    parameters: Option<TelegramParameters>,
}

#[derive(Deserialize)]
struct TelegramMessage {
    message_id: i64,
}

#[derive(Deserialize)]
struct TelegramParameters {
    retry_after: Option<u64>,
}

#[derive(Deserialize)]
struct SlackResponse {
    ok: bool,
    ts: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    id: Option<String>,
    result: Option<Value>,
    error: Option<Value>,
}

async fn telegram_send(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    payload: Value,
    item: &OutboxItem,
) -> Result<DeliveryReceipt, ChannelDeliveryError> {
    let response = client
        .post(format!("{base}/bot{token}/sendMessage"))
        .header("x-panetone-delivery-id", item.id.to_string())
        .json(&payload)
        .send()
        .await
        .map_err(|error| map_http_error(item.kind, &error))?;
    let status = response.status();
    let body = response_body(response, item.kind).await?;
    let parsed: TelegramResponse =
        serde_json::from_slice(&body).map_err(|_| ChannelDeliveryError::Malformed(item.kind))?;
    if status.is_success() && parsed.ok {
        return parsed
            .result
            .map(|message| DeliveryReceipt {
                external_id: message.message_id.to_string(),
            })
            .ok_or(ChannelDeliveryError::Malformed(item.kind));
    }
    if status == StatusCode::TOO_MANY_REQUESTS || parsed.error_code == Some(429) {
        return Err(ChannelDeliveryError::RateLimited {
            kind: item.kind,
            retry_after_secs: parsed
                .parameters
                .and_then(|parameters| parameters.retry_after)
                .unwrap_or(1),
        });
    }
    let detail = safe_remote_detail(
        parsed
            .description
            .as_deref()
            .unwrap_or("unknown Telegram error"),
    );
    let lowered = detail.to_ascii_lowercase();
    if lowered.contains("message thread not found")
        || lowered.contains("chat not found")
        || lowered.contains("topic_closed")
    {
        Err(ChannelDeliveryError::DestinationUnavailable {
            kind: item.kind,
            detail,
        })
    } else {
        Err(ChannelDeliveryError::Rejected {
            kind: item.kind,
            detail,
        })
    }
}

pub(super) fn http_client(
    deadline: Duration,
    kind: ChannelKind,
) -> Result<reqwest::Client, ChannelDeliveryError> {
    reqwest::Client::builder()
        .timeout(deadline)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| ChannelDeliveryError::Transport(kind))
}

fn map_http_error(kind: ChannelKind, error: &reqwest::Error) -> ChannelDeliveryError {
    if error.is_timeout() {
        ChannelDeliveryError::Timeout(kind)
    } else {
        ChannelDeliveryError::Transport(kind)
    }
}

fn retry_after_header(response: &reqwest::Response) -> u64 {
    response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(1)
}

pub(super) async fn response_body(
    mut response: reqwest::Response,
    kind: ChannelKind,
) -> Result<Vec<u8>, ChannelDeliveryError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(ChannelDeliveryError::Malformed(kind));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ChannelDeliveryError::Transport(kind))?
    {
        if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(ChannelDeliveryError::Malformed(kind));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn safe_remote_detail(detail: &str) -> String {
    detail
        .chars()
        .filter(|character| !character.is_control())
        .take(240)
        .collect::<String>()
}
