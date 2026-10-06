//! Appends a route's Signal messages to a chat archive in real time.
//!
//! An archive is a pair of files sharing a path stem: `STEM.jsonl` with one
//! JSON object per message and `STEM.txt` with one grep-friendly line per
//! message, in the formats of the debate archive's `build_archive.py`.
//! Appending never blocks delivery: a failure is logged and dropped.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use chrono::{Local, TimeZone};
use nix::fcntl::{Flock, FlockArg};

/// The archives by lowercased route title.
#[derive(Clone, Debug, Default)]
pub struct ChatArchives {
    stems: HashMap<String, PathBuf>,
}

/// One message in an archive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveEntry {
    pub sent_ms: i64,
    pub sender: String,
    pub text: String,
    pub attachments: Vec<String>,
    /// The Signal group ID in standard base64, as Panetone stores it.
    pub group: String,
}

impl ChatArchives {
    /// Parses `title=/path/stem` pairs separated by commas.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut stems = HashMap::new();
        for pair in spec
            .split(',')
            .map(str::trim)
            .filter(|pair| !pair.is_empty())
        {
            let (title, stem) = pair
                .split_once('=')
                .ok_or_else(|| format!("chat archive {pair:?} is not TITLE=PATH_STEM"))?;
            stems.insert(title.trim().to_lowercase(), PathBuf::from(stem.trim()));
        }
        Ok(Self { stems })
    }

    pub fn is_empty(&self) -> bool {
        self.stems.is_empty()
    }

    /// Appends `entry` to the route's archive if it has one.
    pub fn append(&self, route_title: &str, entry: &ArchiveEntry) {
        let Some(stem) = self.stems.get(&route_title.to_lowercase()) else {
            return;
        };
        let lines = [
            (stem.with_extension("jsonl"), json_line(entry)),
            (stem.with_extension("txt"), text_line(entry)),
        ];
        for (path, line) in lines {
            if let Err(error) = append_line(&path, &line) {
                tracing::warn!(path = %path.display(), error = %error, "chat archive append failed");
            }
        }
    }
}

/// Writes one whole line under an exclusive lock, so concurrent appends
/// never interleave.
fn append_line(path: &Path, line: &str) -> std::io::Result<()> {
    let file: File = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(path)?;
    let mut locked = Flock::lock(file, FlockArg::LockExclusive)
        .map_err(|(_, errno)| std::io::Error::from(errno))?;
    locked.write_all(format!("{line}\n").as_bytes())
}

fn local_time(sent_ms: i64) -> chrono::DateTime<Local> {
    Local
        .timestamp_millis_opt(sent_ms)
        .single()
        .unwrap_or_else(Local::now)
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("a string serializes")
}

/// The `.jsonl` line, matching Python's `json.dumps(..., ensure_ascii=False)`.
fn json_line(entry: &ArchiveEntry) -> String {
    let attachments = entry
        .attachments
        .iter()
        .map(|name| json_string(name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "{{\"sent\": {}, \"sender\": {}, \"text\": {}, \"attachments\": [{attachments}], \"reactions\": [], \"edited\": false, \"group\": {}}}",
        json_string(
            &local_time(entry.sent_ms)
                .format("%Y-%m-%dT%H:%M:%S%:z")
                .to_string()
        ),
        json_string(&entry.sender),
        json_string(&entry.text),
        json_string(&archive_group(&entry.group)),
    )
}

/// The `.txt` line: `YYYY-MM-DD HH:MM Sender: text [attachment: name]`.
fn text_line(entry: &ArchiveEntry) -> String {
    let mut parts = vec![entry.text.replace('\n', " ⏎ ")];
    parts.extend(
        entry
            .attachments
            .iter()
            .map(|name| format!("[attachment: {name}]")),
    );
    format!(
        "{} {}: {}",
        local_time(entry.sent_ms).format("%Y-%m-%d %H:%M"),
        entry.sender,
        parts
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
    )
}

/// The archive writes Signal group IDs in unpadded URL-safe base64.
fn archive_group(group: &str) -> String {
    group
        .trim_end_matches('=')
        .replace('+', "-")
        .replace('/', "_")
}

/// Splits a Panetone Signal inbound body into its text and the file names of
/// the `[attached TYPE: PATH]` lines Panetone appends for attachments.
pub fn split_signal_attachments(body: &str) -> (String, Vec<String>) {
    let mut text = Vec::new();
    let mut attachments = Vec::new();
    for line in body.split('\n') {
        let attached = line
            .strip_prefix("[attached ")
            .and_then(|rest| rest.strip_suffix(']'))
            .and_then(|rest| rest.split_once(": "))
            .and_then(|(_, path)| Path::new(path).file_name())
            .map(|name| name.to_string_lossy().into_owned());
        match attached {
            Some(name) => attachments.push(name),
            None => text.push(line),
        }
    }
    (text.join("\n"), attachments)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> ArchiveEntry {
        ArchiveEntry {
            // 2026-10-06T13:30:36Z
            sent_ms: 1_791_300_636_000,
            sender: "Andrew RM".into(),
            text: "Occums razor\nsecond line \"quoted\" ⏎ é".into(),
            attachments: vec!["photo.jpg".into()],
            group: "GRTI+oGi/1wgJAbKEUMGNR3o1tP8o8igPibVb/xAo8k".into(),
        }
    }

    #[test]
    fn lines_match_the_build_archive_formats() {
        let local = local_time(entry().sent_ms);
        assert_eq!(
            json_line(&entry()),
            format!(
                "{{\"sent\": \"{}\", \"sender\": \"Andrew RM\", \"text\": \"Occums razor\\nsecond line \\\"quoted\\\" ⏎ é\", \"attachments\": [\"photo.jpg\"], \"reactions\": [], \"edited\": false, \"group\": \"GRTI-oGi_1wgJAbKEUMGNR3o1tP8o8igPibVb_xAo8k\"}}",
                local.format("%Y-%m-%dT%H:%M:%S%:z")
            )
        );
        assert_eq!(
            text_line(&entry()),
            format!(
                "{} Andrew RM: Occums razor ⏎ second line \"quoted\" ⏎ é [attachment: photo.jpg]",
                local.format("%Y-%m-%d %H:%M")
            )
        );
        let empty = ArchiveEntry {
            text: String::new(),
            ..entry()
        };
        assert!(text_line(&empty).ends_with("Andrew RM: [attachment: photo.jpg]"));
    }

    #[test]
    fn appends_go_to_the_configured_route_only() {
        let directory = tempfile::tempdir().unwrap();
        let stem = directory.path().join("debate");
        let archives = ChatArchives::parse(&format!("Debate={}", stem.display())).unwrap();
        archives.append("debate", &entry());
        archives.append("debate", &entry());
        archives.append("other", &entry());
        let jsonl = std::fs::read_to_string(stem.with_extension("jsonl")).unwrap();
        assert_eq!(jsonl.lines().count(), 2);
        let parsed: serde_json::Value =
            serde_json::from_str(jsonl.lines().next().unwrap()).unwrap();
        assert_eq!(parsed["sender"], "Andrew RM");
        assert_eq!(
            std::fs::read_to_string(stem.with_extension("txt"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        assert!(ChatArchives::parse("no-equals").is_err());
    }

    #[test]
    fn signal_attachment_lines_become_file_names() {
        assert_eq!(
            split_signal_attachments(
                "look at this\n[attached image/png: /home/x/.local/share/signal-cli/attachments/0X57sub2.png]"
            ),
            ("look at this".into(), vec!["0X57sub2.png".into()])
        );
        assert_eq!(split_signal_attachments("plain"), ("plain".into(), vec![]));
    }
}
