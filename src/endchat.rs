//! Routes served by an endchat profile instead of a Wakterm agent.
//!
//! An endchat profile directory is owned by `endchat serve PROFILE`. Panetone
//! delivers a message with the `say` request on the profile's control socket,
//! and reads the profile's replies from its append-only log,
//! `PROFILE/main/YYYY-MM-DD.jsonl`, one JSON record per line.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;

use crate::domain::AgentBinding;

/// The harness name Panetone gives an endchat profile's agent.
pub const HARNESS: &str = "endchat";
const AGENT_PREFIX: &str = "endchat:";
/// How long `say` waits for the server's first answer before taking an open
/// connection as accepted; a turn that is already running answers at once.
const FIRST_ANSWER: Duration = Duration::from_secs(5);

/// The agent Panetone delivers to for an endchat profile.
pub fn binding(profile: &str) -> AgentBinding {
    AgentBinding {
        agent_id: format!("{AGENT_PREFIX}{profile}"),
        incarnation_id: HARNESS.into(),
        harness: HARNESS.into(),
        pane_id: None,
    }
}

/// The profile directory of an endchat agent binding.
pub fn profile(binding: &AgentBinding) -> Option<&str> {
    (binding.harness == HARNESS)
        .then(|| binding.agent_id.strip_prefix(AGENT_PREFIX))
        .flatten()
}

/// Why a message did not reach an endchat profile.
#[derive(Debug)]
pub enum SayError {
    /// Nothing was sent, so the message can be offered again.
    NotSent(String),
    /// The request was written but not confirmed.
    Uncertain(String),
}

/// Hands one message to the profile's server. It is logged and answered in a
/// new turn, or delivered into the turn that is running.
pub async fn say(profile: &str, text: &str) -> Result<(), SayError> {
    let socket = Path::new(profile).join("control.sock");
    let stream = UnixStream::connect(&socket)
        .await
        .map_err(|error| SayError::NotSent(format!("endchat is not serving {profile}: {error}")))?;
    let (reader, mut writer) = stream.into_split();
    let mut request = serde_json::to_vec(&serde_json::json!({"cmd": "say", "text": text}))
        .map_err(|error| SayError::NotSent(error.to_string()))?;
    request.push(b'\n');
    writer.write_all(&request).await.map_err(|error| {
        SayError::Uncertain(format!("could not write to {}: {error}", socket.display()))
    })?;
    let mut line = String::new();
    match timeout(FIRST_ANSWER, BufReader::new(reader).read_line(&mut line)).await {
        // Still working on it: the server read the request and started a turn.
        Err(_) => Ok(()),
        Ok(Ok(0)) => Err(SayError::Uncertain(format!(
            "{} closed without answering",
            socket.display()
        ))),
        Ok(Ok(_)) => {
            let answer: serde_json::Value = serde_json::from_str(&line)
                .map_err(|error| SayError::Uncertain(error.to_string()))?;
            // A rejected request is answered before anything is logged.
            if answer.get("ok").and_then(serde_json::Value::as_bool) == Some(false) {
                Err(SayError::NotSent(format!(
                    "endchat refused the message: {}",
                    answer
                        .get("text")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                )))
            } else {
                Ok(())
            }
        }
        Ok(Err(error)) => Err(SayError::Uncertain(format!(
            "could not read from {}: {error}",
            socket.display()
        ))),
    }
}

/// Where the reader stopped in a profile's log.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogCursor {
    /// The day file being read, such as `2026-10-19.jsonl`.
    pub file: String,
    /// Bytes of that file already read up to the end of a complete message.
    pub offset: u64,
}

#[derive(Deserialize)]
struct Record {
    i: u64,
    kind: String,
    text: String,
    date: String,
    #[serde(default)]
    run: Option<String>,
    #[serde(default)]
    more: bool,
}

/// One complete agent message from the log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentMessage {
    /// The index of the message's last record.
    pub i: u64,
    pub text: String,
    pub date: String,
    pub run: Option<String>,
}

fn day_files(profile: &str) -> Vec<String> {
    let mut files = std::fs::read_dir(Path::new(profile).join("main"))
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter_map(|entry| entry.file_name().into_string().ok())
                .filter(|name| name.ends_with(".jsonl"))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    files.sort();
    files
}

/// A cursor at the end of the profile's log, so reading starts with what is
/// written next.
pub fn end_of_log(profile: &str) -> LogCursor {
    let Some(file) = day_files(profile).pop() else {
        return LogCursor::default();
    };
    let offset = std::fs::metadata(Path::new(profile).join("main").join(&file))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    LogCursor { file, offset }
}

/// Reads the complete agent messages written after `cursor`, and the cursor
/// after them. A message whose pages are not all written yet is left for the
/// next read; other kinds of record are skipped.
pub fn read_agent_messages(
    profile: &str,
    cursor: &LogCursor,
) -> std::io::Result<(Vec<AgentMessage>, LogCursor)> {
    let directory = PathBuf::from(profile).join("main");
    let mut messages = Vec::new();
    let mut next = cursor.clone();
    for file in day_files(profile)
        .into_iter()
        .filter(|file| *file >= cursor.file)
    {
        let path = directory.join(&file);
        // Nothing new in the file the reader stopped in.
        if file == cursor.file && std::fs::metadata(&path)?.len() == cursor.offset {
            continue;
        }
        let bytes = std::fs::read(&path)?;
        let mut position = if file == cursor.file {
            cursor.offset as usize
        } else {
            0
        };
        let mut pages = String::new();
        let mut complete_at = position;
        while let Some(end) = bytes[position.min(bytes.len())..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|length| position + length)
        {
            let line = &bytes[position..end];
            position = end + 1;
            let Ok(record) = serde_json::from_slice::<Record>(line) else {
                complete_at = position;
                continue;
            };
            if record.kind == "agent" {
                pages.push_str(&record.text);
            }
            if record.more {
                continue;
            }
            if record.kind == "agent" {
                messages.push(AgentMessage {
                    i: record.i,
                    text: std::mem::take(&mut pages),
                    date: record.date,
                    run: record.run,
                });
            }
            pages.clear();
            complete_at = position;
        }
        next = LogCursor {
            file,
            offset: complete_at as u64,
        };
        if complete_at < bytes.len() {
            // A message is still being written; read it whole next time.
            break;
        }
    }
    Ok((messages, next))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_agent_messages_are_read_once_with_their_pages_joined() {
        let directory = tempfile::tempdir().unwrap();
        let profile = directory.path().to_str().unwrap();
        std::fs::create_dir_all(directory.path().join("main")).unwrap();
        let day = directory.path().join("main/2026-10-19.jsonl");
        let record = |i: u64, kind: &str, text: &str, more: bool| {
            format!(
                "{}\n",
                serde_json::json!({"i": i, "kind": kind, "text": text, "size": 1,
                    "date": "2026-10-19T12:00:00Z", "run": "r1", "more": more})
            )
        };
        std::fs::write(
            &day,
            [
                record(0, "user", "hi", false),
                record(1, "agent", "first half, ", true),
                record(2, "agent", "second half", false),
                record(3, "tool", "ls", false),
                record(4, "agent", "partial", true),
            ]
            .concat(),
        )
        .unwrap();

        let (messages, cursor) = read_agent_messages(profile, &LogCursor::default()).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].text, "first half, second half");
        assert_eq!(messages[0].i, 2);
        // The unfinished message stays unread until its last page arrives.
        let (again, same) = read_agent_messages(profile, &cursor).unwrap();
        assert!(again.is_empty());
        assert_eq!(same, cursor);
        let mut log = std::fs::read_to_string(&day).unwrap();
        log.push_str(&record(5, "agent", " and the rest", false));
        std::fs::write(&day, log).unwrap();
        let (rest, end) = read_agent_messages(profile, &cursor).unwrap();
        assert_eq!(rest[0].text, "partial and the rest");
        assert_eq!(end, end_of_log(profile));
    }

    #[test]
    fn a_binding_names_its_profile() {
        let agent = binding("/home/user/.endchat/debate");
        assert_eq!(profile(&agent), Some("/home/user/.endchat/debate"));
        assert_eq!(
            profile(&AgentBinding {
                harness: "claude".into(),
                ..agent
            }),
            None
        );
    }
}
