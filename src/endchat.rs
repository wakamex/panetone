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

/// Where the reader stopped in a profile's log and turn list.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct LogCursor {
    /// The day file being read, such as `2026-10-19.jsonl`.
    pub file: String,
    /// Bytes of that file already read up to the end of a complete message.
    pub offset: u64,
    /// The index of the next log record to read.
    #[serde(default)]
    pub next_i: u64,
    /// Bytes of `turns.jsonl` already read.
    #[serde(default)]
    pub turns_offset: u64,
}

#[derive(Deserialize)]
struct Record {
    i: u64,
    kind: String,
    text: String,
    date: String,
    #[serde(default)]
    more: bool,
}

#[derive(Deserialize)]
struct TurnRecord {
    end: u64,
    #[serde(default)]
    ok: bool,
    date: String,
}

/// What happened in an endchat chat, in log order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Entry {
    /// A message reached the chat. A turn starts with it, or it joins the
    /// running turn. `i` is the index of its first record.
    User { i: u64, text: String, date: String },
    /// A complete agent message; `i` is the index of its last record.
    Agent { i: u64, text: String, date: String },
    /// A turn finished once the log held `end` records.
    TurnEnd { end: u64, ok: bool, date: String },
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

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

/// A cursor at the end of the profile's log and turn list, so reading starts
/// with what is written next.
pub fn end_of_log(profile: &str) -> LogCursor {
    let turns_offset = file_len(&Path::new(profile).join("turns.jsonl"));
    let Some(file) = day_files(profile).pop() else {
        return LogCursor {
            turns_offset,
            ..LogCursor::default()
        };
    };
    let path = Path::new(profile).join("main").join(&file);
    let next_i = std::fs::read_to_string(&path)
        .unwrap_or_default()
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<Record>(line).ok())
        .map(|record| record.i + 1)
        .unwrap_or(0);
    LogCursor {
        offset: file_len(&path),
        file,
        next_i,
        turns_offset,
    }
}

/// Complete lines of `bytes` from `position`, with the offset just past each.
fn lines_from(bytes: &[u8], position: usize) -> Vec<(&[u8], usize)> {
    let mut lines = Vec::new();
    let mut start = position.min(bytes.len());
    while let Some(length) = bytes[start..].iter().position(|byte| *byte == b'\n') {
        lines.push((&bytes[start..start + length], start + length + 1));
        start += length + 1;
    }
    lines
}

/// Reads what was written after `cursor`: user and agent messages with their
/// pages joined, and turn ends placed after the records their turn covered.
/// A message whose pages are not all written yet, and a turn end beyond the
/// records read, are left for the next read.
pub fn read(profile: &str, cursor: &LogCursor) -> std::io::Result<(Vec<Entry>, LogCursor)> {
    let directory = PathBuf::from(profile).join("main");
    let mut messages = Vec::new();
    let mut next = cursor.clone();
    for file in day_files(profile)
        .into_iter()
        .filter(|file| *file >= cursor.file)
    {
        let path = directory.join(&file);
        let start = if file == cursor.file {
            cursor.offset
        } else {
            0
        };
        if file_len(&path) == start {
            next.file = file;
            next.offset = start;
            continue;
        }
        let bytes = std::fs::read(&path)?;
        let mut pages = String::new();
        let mut first = None;
        let mut complete_at = start as usize;
        for (line, after) in lines_from(&bytes, start as usize) {
            let Ok(record) = serde_json::from_slice::<Record>(line) else {
                complete_at = after;
                continue;
            };
            first.get_or_insert(record.i);
            pages.push_str(&record.text);
            if record.more {
                continue;
            }
            let i = first.take().unwrap_or(record.i);
            let text = std::mem::take(&mut pages);
            match record.kind.as_str() {
                "user" => messages.push(Entry::User {
                    i,
                    text,
                    date: record.date,
                }),
                "agent" => messages.push(Entry::Agent {
                    i: record.i,
                    text,
                    date: record.date,
                }),
                _ => {}
            }
            next.next_i = record.i + 1;
            complete_at = after;
        }
        next.file = file;
        next.offset = complete_at as u64;
        if complete_at < bytes.len() {
            // A message is still being written; read it whole next time.
            break;
        }
    }

    let turns = std::fs::read(Path::new(profile).join("turns.jsonl")).unwrap_or_default();
    let mut ends = Vec::new();
    for (line, after) in lines_from(&turns, cursor.turns_offset as usize) {
        if let Ok(turn) = serde_json::from_slice::<TurnRecord>(line) {
            if turn.end > next.next_i {
                break;
            }
            ends.push(Entry::TurnEnd {
                end: turn.end,
                ok: turn.ok,
                date: turn.date,
            });
        }
        next.turns_offset = after as u64;
    }

    // A turn end belongs after the last record its turn covered.
    let first_record = |entry: &Entry| match entry {
        Entry::User { i, .. } | Entry::Agent { i, .. } => *i,
        Entry::TurnEnd { .. } => u64::MAX,
    };
    let mut entries = Vec::with_capacity(messages.len() + ends.len());
    let mut ends = ends.into_iter().peekable();
    for message in messages {
        while let Some(Entry::TurnEnd { end, .. }) = ends.peek() {
            if *end > first_record(&message) {
                break;
            }
            entries.extend(ends.next());
        }
        entries.push(message);
    }
    entries.extend(ends);
    Ok((entries, next))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(i: u64, kind: &str, text: &str, more: bool) -> String {
        format!(
            "{}\n",
            serde_json::json!({"i": i, "kind": kind, "text": text, "size": 1,
                "date": "2026-10-19T12:00:00Z", "run": "r1", "more": more})
        )
    }

    fn append(path: &Path, text: &str) {
        let mut existing = std::fs::read_to_string(path).unwrap_or_default();
        existing.push_str(text);
        std::fs::write(path, existing).unwrap();
    }

    #[test]
    fn messages_and_turn_ends_are_read_once_in_log_order() {
        let directory = tempfile::tempdir().unwrap();
        let profile = directory.path().to_str().unwrap();
        std::fs::create_dir_all(directory.path().join("main")).unwrap();
        let day = directory.path().join("main/2026-10-19.jsonl");
        let turns = directory.path().join("turns.jsonl");
        append(
            &day,
            &[
                record(0, "user", "hi", false),
                record(1, "agent", "first half, ", true),
                record(2, "agent", "second half", false),
                record(3, "tool", "ls", false),
                record(4, "user", "next", false),
                record(5, "agent", "partial", true),
            ]
            .concat(),
        );
        // The first turn ended after 4 records; the second is unfinished.
        append(
            &turns,
            "{\"end\": 4, \"ok\": true, \"date\": \"2026-10-19T12:00:00Z\"}\n",
        );

        let (entries, cursor) = read(profile, &LogCursor::default()).unwrap();
        let date = "2026-10-19T12:00:00Z".to_owned();
        assert_eq!(
            entries,
            [
                Entry::User {
                    i: 0,
                    text: "hi".into(),
                    date: date.clone()
                },
                Entry::Agent {
                    i: 2,
                    text: "first half, second half".into(),
                    date: date.clone()
                },
                Entry::TurnEnd {
                    end: 4,
                    ok: true,
                    date: date.clone()
                },
                Entry::User {
                    i: 4,
                    text: "next".into(),
                    date: date.clone()
                },
            ]
        );
        let (again, same) = read(profile, &cursor).unwrap();
        assert!(again.is_empty());
        assert_eq!(same, cursor);

        append(&day, &record(6, "agent", " and the rest", false));
        append(
            &turns,
            "{\"end\": 7, \"ok\": true, \"date\": \"2026-10-19T12:00:00Z\"}\n",
        );
        let (rest, end) = read(profile, &cursor).unwrap();
        assert_eq!(
            rest,
            [
                Entry::Agent {
                    i: 6,
                    text: "partial and the rest".into(),
                    date: date.clone()
                },
                Entry::TurnEnd {
                    end: 7,
                    ok: true,
                    date
                },
            ]
        );
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
