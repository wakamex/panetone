use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
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
const MAX_TELEGRAM_DOCUMENT_BYTES: u64 = 20 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundMessage {
    pub channel: ChannelKind,
    pub external_id: String,
    pub destination: String,
    pub sender_id: Option<String>,
    pub sender: Option<String>,
    pub reply_to_external_id: Option<String>,
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
    attachment_directory: PathBuf,
}

impl TelegramPoller {
    pub fn telegram(
        api_base: impl Into<String>,
        token: impl Into<String>,
        chat_id: i64,
        attachment_directory: impl Into<PathBuf>,
        deadline: Duration,
    ) -> Result<Self, ChannelDeliveryError> {
        Self::new(
            api_base,
            token,
            ChannelKind::Telegram,
            chat_id,
            attachment_directory,
            deadline,
        )
    }

    fn new(
        api_base: impl Into<String>,
        token: impl Into<String>,
        kind: ChannelKind,
        chat_id: i64,
        attachment_directory: impl Into<PathBuf>,
        deadline: Duration,
    ) -> Result<Self, ChannelDeliveryError> {
        Ok(Self {
            http: http_client(deadline, kind)?,
            api_base: api_base.into().trim_end_matches('/').to_owned(),
            token: token.into(),
            kind,
            chat_id,
            attachment_directory: attachment_directory.into(),
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
        let status = response.status();
        let body = response_body(response, self.kind).await?;
        let response: TelegramUpdates = serde_json::from_slice(&body).map_err(|_| {
            if status.is_server_error() {
                ChannelDeliveryError::Transport(self.kind)
            } else {
                ChannelDeliveryError::Malformed(self.kind)
            }
        })?;
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS || response.error_code == Some(429) {
            return Err(ChannelDeliveryError::RateLimited {
                kind: self.kind,
                retry_after_secs: response
                    .parameters
                    .and_then(|parameters| parameters.retry_after)
                    .unwrap_or(1),
            });
        }
        if status.is_server_error() {
            return Err(ChannelDeliveryError::Transport(self.kind));
        }
        if !status.is_success() || !response.ok {
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
            let Some(topic) = message.message_thread_id else {
                continue;
            };
            let mut body = message.text.or(message.caption).unwrap_or_default();
            if let Some(document) = message.document {
                if !body.is_empty() {
                    body.push('\n');
                }
                body.push_str(&self.document_line(update.update_id, document).await?);
            }
            if body.is_empty() {
                continue;
            }
            let destination = topic.to_string();
            messages.push(InboundMessage {
                channel: self.kind,
                external_id: update.update_id.to_string(),
                destination,
                sender_id: message.from.as_ref().map(|user| user.id.to_string()),
                sender: message.from.and_then(TelegramUser::display_name),
                reply_to_external_id: message
                    .reply_to_message
                    .map(|reply| reply.message_id.to_string()),
                body,
            });
        }
        Ok(InboundBatch {
            messages,
            next_offset,
        })
    }

    async fn document_line(
        &self,
        update_id: i64,
        document: TelegramDocument,
    ) -> Result<String, ChannelDeliveryError> {
        let content_type =
            single_line(document.mime_type.as_deref()).unwrap_or("application/octet-stream");
        if document
            .file_size
            .is_some_and(|size| size > MAX_TELEGRAM_DOCUMENT_BYTES)
        {
            return Ok(format!(
                "[attached {content_type} unavailable: exceeds Telegram's 20 MB bot download limit]"
            ));
        }
        let filename = safe_filename(document.file_name.as_deref().unwrap_or("document"));
        let destination = self
            .attachment_directory
            .join(format!("{update_id}-{filename}"));
        if tokio::fs::metadata(&destination).await.is_err() {
            self.download_document(&document.file_id, update_id, &destination)
                .await?;
        }
        Ok(format!(
            "[attached {content_type}: {}]",
            destination.display()
        ))
    }

    async fn download_document(
        &self,
        file_id: &str,
        update_id: i64,
        destination: &Path,
    ) -> Result<(), ChannelDeliveryError> {
        let response = self
            .http
            .post(format!("{}/bot{}/getFile", self.api_base, self.token))
            .json(&json!({"file_id": file_id}))
            .send()
            .await
            .map_err(|error| map_telegram_http_error(&error))?;
        let status = response.status();
        let body = response_body(response, self.kind).await?;
        let response: TelegramFileResponse = serde_json::from_slice(&body)
            .map_err(|_| ChannelDeliveryError::Malformed(self.kind))?;
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS || response.error_code == Some(429) {
            return Err(ChannelDeliveryError::RateLimited {
                kind: self.kind,
                retry_after_secs: response
                    .parameters
                    .and_then(|parameters| parameters.retry_after)
                    .unwrap_or(1),
            });
        }
        if status.is_server_error() {
            return Err(ChannelDeliveryError::Transport(self.kind));
        }
        let file_path = response
            .ok
            .then_some(response.result)
            .flatten()
            .map(|file| file.file_path)
            .filter(|path| !path.is_empty())
            .ok_or_else(|| ChannelDeliveryError::Rejected {
                kind: self.kind,
                detail: safe_detail(
                    response
                        .description
                        .as_deref()
                        .unwrap_or("getFile rejected"),
                ),
            })?;

        let response = self
            .http
            .get(format!(
                "{}/file/bot{}/{}",
                self.api_base,
                self.token,
                file_path.trim_start_matches('/')
            ))
            .send()
            .await
            .map_err(|error| map_telegram_http_error(&error))?;
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(ChannelDeliveryError::RateLimited {
                kind: self.kind,
                retry_after_secs: 1,
            });
        }
        if response.status().is_server_error() {
            return Err(ChannelDeliveryError::Transport(self.kind));
        }
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|size| size > MAX_TELEGRAM_DOCUMENT_BYTES)
        {
            return Err(ChannelDeliveryError::Rejected {
                kind: self.kind,
                detail: "Telegram document download was rejected".into(),
            });
        }

        let parent = destination
            .parent()
            .ok_or(ChannelDeliveryError::Transport(self.kind))?;
        std::fs::create_dir_all(parent).map_err(|_| ChannelDeliveryError::Transport(self.kind))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                .map_err(|_| ChannelDeliveryError::Transport(self.kind))?;
            let temporary = parent.join(format!(".{update_id}.part"));
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .mode(0o600)
                .open(&temporary)
                .map_err(|_| ChannelDeliveryError::Transport(self.kind))?;
            let mut file = tokio::fs::File::from_std(file);
            let mut response = response;
            let mut written = 0_u64;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| ChannelDeliveryError::Transport(self.kind))?
            {
                written = written.saturating_add(chunk.len() as u64);
                if written > MAX_TELEGRAM_DOCUMENT_BYTES {
                    drop(file);
                    let _ = tokio::fs::remove_file(&temporary).await;
                    return Err(ChannelDeliveryError::Rejected {
                        kind: self.kind,
                        detail: "Telegram document exceeds the download limit".into(),
                    });
                }
                file.write_all(&chunk)
                    .await
                    .map_err(|_| ChannelDeliveryError::Transport(self.kind))?;
            }
            file.sync_all()
                .await
                .map_err(|_| ChannelDeliveryError::Transport(self.kind))?;
            drop(file);
            tokio::fs::rename(temporary, destination)
                .await
                .map_err(|_| ChannelDeliveryError::Transport(self.kind))?;
        }
        #[cfg(not(unix))]
        return Err(ChannelDeliveryError::Transport(self.kind));
        Ok(())
    }
}

pub struct SignalSubscriber {
    reader: BufReader<UnixStream>,
    deadline: Duration,
    attachment_directory: PathBuf,
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
        Ok(Self {
            reader,
            deadline,
            attachment_directory: signal_attachment_directory(),
        })
    }

    pub async fn next(&mut self) -> Result<InboundMessage, ChannelDeliveryError> {
        loop {
            let line = match timeout(self.deadline, read_bounded_line(&mut self.reader)).await {
                Ok(line) => line?,
                Err(_) => continue,
            };
            let notification: Value = serde_json::from_slice(&line)
                .map_err(|_| ChannelDeliveryError::Malformed(ChannelKind::Signal))?;
            if notification.get("method").is_none() {
                continue;
            }
            let envelope = &notification["params"]["result"]["envelope"];
            let data = &envelope["dataMessage"];
            let Some(body) = signal_body(data, &self.attachment_directory) else {
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
                reply_to_external_id: scalar_id(&data["quote"]["id"]),
                body,
            });
        }
    }
}

fn signal_attachment_directory() -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .unwrap_or_else(|| PathBuf::from(".local/share"))
        .join("signal-cli/attachments")
}

fn signal_body(data: &Value, attachment_directory: &Path) -> Option<String> {
    let mut body = data["message"].as_str().unwrap_or_default().to_owned();
    for attachment in data["attachments"].as_array().into_iter().flatten() {
        let Some(id) = attachment["id"]
            .as_str()
            .filter(|id| Path::new(id).file_name() == Some(OsStr::new(id)))
        else {
            continue;
        };
        let content_type = attachment["contentType"]
            .as_str()
            .filter(|value| !value.contains(['\r', '\n']))
            .unwrap_or("application/octet-stream");
        if !body.is_empty() {
            body.push('\n');
        }
        body.push_str(&format!(
            "[attached {content_type}: {}]",
            attachment_directory.join(id).display()
        ));
    }
    (!body.is_empty()).then_some(body)
}

#[derive(Deserialize)]
struct TelegramUpdates {
    ok: bool,
    #[serde(default)]
    result: Vec<TelegramUpdate>,
    description: Option<String>,
    error_code: Option<u16>,
    parameters: Option<TelegramUpdateParameters>,
}

#[derive(Deserialize)]
struct TelegramUpdateParameters {
    retry_after: Option<u64>,
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
    reply_to_message: Option<TelegramReply>,
    text: Option<String>,
    caption: Option<String>,
    document: Option<TelegramDocument>,
}

#[derive(Deserialize)]
struct TelegramDocument {
    file_id: String,
    file_name: Option<String>,
    mime_type: Option<String>,
    file_size: Option<u64>,
}

#[derive(Deserialize)]
struct TelegramFileResponse {
    ok: bool,
    result: Option<TelegramFile>,
    description: Option<String>,
    error_code: Option<u16>,
    parameters: Option<TelegramUpdateParameters>,
}

#[derive(Deserialize)]
struct TelegramFile {
    file_path: String,
}

#[derive(Deserialize)]
struct TelegramReply {
    message_id: i64,
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

fn scalar_id(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|value| value.to_string()))
        .or_else(|| value.as_u64().map(|value| value.to_string()))
}

fn safe_filename(filename: &str) -> String {
    let filename = Path::new(filename)
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or("document");
    let sanitized = filename
        .chars()
        .take(160)
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => character,
            _ => '_',
        })
        .collect::<String>();
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        "document".into()
    } else {
        sanitized
    }
}

fn single_line(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty() && !value.contains(['\r', '\n']))
}

fn map_telegram_http_error(error: &reqwest::Error) -> ChannelDeliveryError {
    if error.is_timeout() {
        ChannelDeliveryError::Timeout(ChannelKind::Telegram)
    } else {
        ChannelDeliveryError::Transport(ChannelKind::Telegram)
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
