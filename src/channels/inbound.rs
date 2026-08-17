use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;
use uuid::Uuid;

use crate::domain::{ChannelKind, EffectId};

use super::real::{ChannelDeliveryError, http_client, response_body};

const MAX_INBOUND_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundMessage {
    pub channel: ChannelKind,
    pub external_id: String,
    pub destination: String,
    pub sender_id: Option<String>,
    pub sender: Option<String>,
    pub body: String,
}

impl InboundMessage {
    pub fn effect_id(&self) -> EffectId {
        let namespace = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            b"https://panetone.dev/channels/inbound/v1",
        );
        EffectId::new(Uuid::new_v5(
            &namespace,
            format!("{:?}\0{}", self.channel, self.external_id).as_bytes(),
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundBatch {
    pub messages: Vec<InboundMessage>,
    pub next_offset: i64,
}

#[derive(Clone)]
pub struct TelegramPoller {
    http: reqwest::Client,
    api_base: String,
    token: String,
    kind: ChannelKind,
    chat_id: i64,
}

impl TelegramPoller {
    pub fn telegram(
        api_base: impl Into<String>,
        token: impl Into<String>,
        chat_id: i64,
        deadline: Duration,
    ) -> Result<Self, ChannelDeliveryError> {
        Self::new(api_base, token, ChannelKind::Telegram, chat_id, deadline)
    }

    fn new(
        api_base: impl Into<String>,
        token: impl Into<String>,
        kind: ChannelKind,
        chat_id: i64,
        deadline: Duration,
    ) -> Result<Self, ChannelDeliveryError> {
        Ok(Self {
            http: http_client(deadline, kind)?,
            api_base: api_base.into().trim_end_matches('/').to_owned(),
            token: token.into(),
            kind,
            chat_id,
        })
    }

    pub async fn poll(
        &self,
        offset: i64,
        poll_seconds: u64,
    ) -> Result<InboundBatch, ChannelDeliveryError> {
        let response = self
            .http
            .post(format!("{}/bot{}/getUpdates", self.api_base, self.token))
            .json(&json!({
                "offset": offset,
                "limit": 100,
                "timeout": poll_seconds,
                "allowed_updates": ["message"]
            }))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    ChannelDeliveryError::Timeout(self.kind)
                } else {
                    ChannelDeliveryError::Transport(self.kind)
                }
            })?;
        let body = response_body(response, self.kind).await?;
        let response: TelegramUpdates = serde_json::from_slice(&body)
            .map_err(|_| ChannelDeliveryError::Malformed(self.kind))?;
        if !response.ok {
            return Err(ChannelDeliveryError::Rejected {
                kind: self.kind,
                detail: safe_detail(response.description.as_deref().unwrap_or("poll rejected")),
            });
        }
        let mut next_offset = offset;
        let mut messages = Vec::new();
        for update in response.result {
            next_offset = next_offset.max(update.update_id.saturating_add(1));
            let Some(message) = update.message else {
                continue;
            };
            if message.chat.id != self.chat_id {
                continue;
            }
            let Some(body) = message.text else {
                continue;
            };
            let Some(topic) = message.message_thread_id else {
                continue;
            };
            let destination = topic.to_string();
            messages.push(InboundMessage {
                channel: self.kind,
                external_id: update.update_id.to_string(),
                destination,
                sender_id: message.from.as_ref().map(|user| user.id.to_string()),
                sender: message.from.and_then(TelegramUser::display_name),
                body,
            });
        }
        Ok(InboundBatch {
            messages,
            next_offset,
        })
    }
}

pub struct SignalSubscriber {
    reader: BufReader<UnixStream>,
    deadline: Duration,
}

impl SignalSubscriber {
    pub async fn connect(
        socket: impl Into<PathBuf>,
        account: &str,
        deadline: Duration,
    ) -> Result<Self, ChannelDeliveryError> {
        timeout(
            deadline,
            Self::connect_inner(socket.into(), account, deadline),
        )
        .await
        .map_err(|_| ChannelDeliveryError::Timeout(ChannelKind::Signal))?
    }

    async fn connect_inner(
        socket: PathBuf,
        account: &str,
        deadline: Duration,
    ) -> Result<Self, ChannelDeliveryError> {
        let mut stream = UnixStream::connect(socket)
            .await
            .map_err(|_| ChannelDeliveryError::Transport(ChannelKind::Signal))?;
        let mut request = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": "panetone-subscribe",
            "method": "subscribeReceive",
            "params": {"account": account}
        }))
        .map_err(|_| ChannelDeliveryError::Malformed(ChannelKind::Signal))?;
        request.push(b'\n');
        stream
            .write_all(&request)
            .await
            .map_err(|_| ChannelDeliveryError::Transport(ChannelKind::Signal))?;
        let mut reader = BufReader::new(stream);
        let response = read_bounded_line(&mut reader).await?;
        let response: Value = serde_json::from_slice(&response)
            .map_err(|_| ChannelDeliveryError::Malformed(ChannelKind::Signal))?;
        if response["id"] != "panetone-subscribe" || response.get("error").is_some() {
            return Err(ChannelDeliveryError::Rejected {
                kind: ChannelKind::Signal,
                detail: "Signal subscription was rejected".into(),
            });
        }
        Ok(Self { reader, deadline })
    }

    pub async fn next(&mut self) -> Result<InboundMessage, ChannelDeliveryError> {
        loop {
            let line = timeout(self.deadline, read_bounded_line(&mut self.reader))
                .await
                .map_err(|_| ChannelDeliveryError::Timeout(ChannelKind::Signal))??;
            let notification: Value = serde_json::from_slice(&line)
                .map_err(|_| ChannelDeliveryError::Malformed(ChannelKind::Signal))?;
            if notification.get("method").is_none() {
                continue;
            }
            let envelope = &notification["params"]["result"]["envelope"];
            let data = &envelope["dataMessage"];
            let Some(body) = data["message"].as_str() else {
                continue;
            };
            let destination = data["groupInfo"]["groupId"]
                .as_str()
                .ok_or(ChannelDeliveryError::Malformed(ChannelKind::Signal))?;
            let timestamp = envelope["timestamp"]
                .as_i64()
                .map(|value| value.to_string())
                .or_else(|| envelope["timestamp"].as_str().map(str::to_owned))
                .ok_or(ChannelDeliveryError::Malformed(ChannelKind::Signal))?;
            let source = envelope["sourceNumber"]
                .as_str()
                .or_else(|| envelope["source"].as_str())
                .unwrap_or("unknown");
            return Ok(InboundMessage {
                channel: ChannelKind::Signal,
                external_id: format!("{source}:{timestamp}"),
                destination: destination.trim_end_matches('=').into(),
                sender_id: Some(source.into()),
                sender: envelope["sourceName"]
                    .as_str()
                    .or_else(|| envelope["sourceNumber"].as_str())
                    .map(str::to_owned),
                body: body.into(),
            });
        }
    }
}

#[derive(Deserialize)]
struct TelegramUpdates {
    ok: bool,
    #[serde(default)]
    result: Vec<TelegramUpdate>,
    description: Option<String>,
}

#[derive(Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    message: Option<TelegramMessage>,
}

#[derive(Deserialize)]
struct TelegramMessage {
    chat: TelegramChat,
    message_thread_id: Option<i64>,
    from: Option<TelegramUser>,
    text: Option<String>,
}

#[derive(Deserialize)]
struct TelegramChat {
    id: i64,
}

#[derive(Deserialize)]
struct TelegramUser {
    id: i64,
    first_name: Option<String>,
    last_name: Option<String>,
    username: Option<String>,
}

impl TelegramUser {
    fn display_name(self) -> Option<String> {
        let name = [self.first_name, self.last_name]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" ");
        if name.is_empty() {
            self.username
        } else {
            Some(name)
        }
    }
}

async fn read_bounded_line(
    reader: &mut BufReader<UnixStream>,
) -> Result<Vec<u8>, ChannelDeliveryError> {
    let mut line = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|_| ChannelDeliveryError::Transport(ChannelKind::Signal))?;
        if available.is_empty() {
            return Err(ChannelDeliveryError::Transport(ChannelKind::Signal));
        }
        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if line.len() + consumed > MAX_INBOUND_BYTES {
            return Err(ChannelDeliveryError::Malformed(ChannelKind::Signal));
        }
        let complete = available[consumed - 1] == b'\n';
        line.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if complete {
            return Ok(line);
        }
    }
}

fn safe_detail(detail: &str) -> String {
    detail
        .chars()
        .filter(|character| !character.is_control())
        .take(240)
        .collect()
}
