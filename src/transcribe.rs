//! Transcription of audio a person sends to a route that asks for it, so its
//! agent can answer a voice note without first transcribing it itself.
//!
//! The transcriber is the command in `PANETONE_TRANSCRIBE_COMMAND`, a shell
//! command with `{input}` and `{output}` placeholders: it reads the audio file
//! at `{input}` and writes the transcript as plain text to `{output}`.

use std::path::Path;
use std::time::Duration;

pub const COMMAND_ENV: &str = "PANETONE_TRANSCRIBE_COMMAND";

/// How long one transcription may take before the message goes on without it.
const TIMEOUT: Duration = Duration::from_secs(60);

/// The audio file an attachment line names, if it is one to transcribe:
/// audio of any kind, or a Telegram video note, which is spoken video.
fn audio_path(line: &str) -> Option<&str> {
    let (content_type, path) = line
        .strip_prefix("[attached ")?
        .strip_suffix(']')?
        .split_once(": ")?;
    (content_type.starts_with("audio/") || path.ends_with("-video-note.mp4")).then_some(path)
}

/// Whether a message has audio to transcribe.
pub fn has_audio(body: &str) -> bool {
    body.lines().any(|line| audio_path(line).is_some())
}

/// The message with a transcript line after each audio attachment line, or a
/// line saying why that attachment was not transcribed.
pub async fn transcribe(template: &str, body: &str) -> String {
    let mut lines = Vec::new();
    for line in body.lines() {
        lines.push(line.to_owned());
        if let Some(path) = audio_path(line) {
            lines.push(match run(template, path).await {
                Ok(text) => format!("[transcript: {text}]"),
                Err(reason) => format!("[transcription failed: {reason}]"),
            });
        }
    }
    lines.join("\n")
}

async fn run(template: &str, input: &str) -> Result<String, String> {
    let output = format!("{input}.transcript.txt");
    let command = template
        .replace("{input}", &shell_quote(input))
        .replace("{output}", &shell_quote(&output));
    let child = tokio::process::Command::new("/bin/sh")
        .args(["-c", &command])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .output();
    let result = tokio::time::timeout(TIMEOUT, child)
        .await
        .map_err(|_| format!("it took longer than {} seconds", TIMEOUT.as_secs()))?
        .map_err(|error| format!("the transcriber could not start: {error}"))?;
    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        let last = stderr.lines().rev().find(|line| !line.trim().is_empty());
        return Err(match last {
            Some(line) => format!(
                "the transcriber exited with {}: {}",
                result.status,
                line.trim()
            ),
            None => format!("the transcriber exited with {}", result.status),
        });
    }
    let text = tokio::fs::read_to_string(Path::new(&output))
        .await
        .map_err(|error| format!("no transcript was written: {error}"))?;
    let text = text.trim();
    if text.is_empty() {
        return Err("the transcript is empty".into());
    }
    Ok(text.to_owned())
}

fn shell_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_audio_and_video_notes_are_transcribed() {
        assert_eq!(
            audio_path("[attached audio/ogg: /a/101-voice.ogg]"),
            Some("/a/101-voice.ogg")
        );
        assert_eq!(
            audio_path("[attached video/mp4: /a/102-video-note.mp4]"),
            Some("/a/102-video-note.mp4")
        );
        assert_eq!(audio_path("[attached video/mp4: /a/103-video.mp4]"), None);
        assert_eq!(audio_path("[attached image/jpeg: /a/104-photo.jpg]"), None);
        assert_eq!(audio_path("say [attached audio/ogg: /a/x.ogg]"), None);
    }

    #[tokio::test]
    async fn a_transcript_or_the_reason_follows_each_audio_line() {
        let directory = tempfile::tempdir().unwrap();
        let audio = directory.path().join("101-voice's.ogg");
        std::fs::write(&audio, b"OggS").unwrap();
        let body = format!("listen\n[attached audio/ogg: {}]", audio.display());
        // A stand-in transcriber that writes a fixed transcript.
        let writes = "test -f {input} && printf ' hello there \\n' > {output}";
        assert_eq!(
            transcribe(writes, &body).await,
            format!("{body}\n[transcript: hello there]")
        );
        let fails = "echo 'model not loaded' >&2; exit 3";
        let failed = transcribe(fails, &body).await;
        assert!(
            failed.ends_with("[transcription failed: the transcriber exited with exit status: 3: model not loaded]"),
            "{failed}"
        );
        assert_eq!(transcribe(writes, "no audio here").await, "no audio here");
    }
}
