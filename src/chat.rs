//! `panetone chat ROOM`: a terminal chat client for a room, where the user and
//! the room's agents all see and answer each other.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rustyline::ExternalPrinter;
use rustyline::error::ReadlineError;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::control::{CONTROL_SCHEMA, ControlRequest, request};

const POLL_WAIT_MS: u64 = 20_000;

/// The icons that mark who is speaking, as small PNGs.
const ICON_YOU: &[u8] = include_bytes!("../assets/icons/you.png");
const ICON_CLAUDE: &[u8] = include_bytes!("../assets/icons/claude.png");
const ICON_CODEX: &[u8] = include_bytes!("../assets/icons/codex.png");
const ICON_GEMINI: &[u8] = include_bytes!("../assets/icons/gemini.png");

const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

struct Style {
    /// The terminal draws inline images (the iTerm2 protocol Wakterm speaks).
    images: bool,
    me: String,
}

impl Style {
    fn detect(me: String) -> Self {
        let program = std::env::var("TERM_PROGRAM").unwrap_or_default();
        let images = std::io::stdout().is_terminal()
            && std::env::var_os("PANETONE_CHAT_PLAIN").is_none()
            && matches!(program.as_str(), "wakterm" | "WezTerm" | "iTerm.app");
        Self { images, me }
    }

    fn colour(harness: &str) -> &'static str {
        match harness {
            "claude" => "\x1b[38;5;208m",
            "codex" => "\x1b[38;5;41m",
            "agy" | "gemini" => "\x1b[38;5;33m",
            "" => "\x1b[38;5;180m",
            _ => "\x1b[38;5;177m",
        }
    }

    /// The speaker's mark: an inline icon where the terminal draws images, a
    /// coloured dot elsewhere.
    fn mark(&self, harness: &str) -> String {
        let icon = match harness {
            "claude" => Some(ICON_CLAUDE),
            "codex" => Some(ICON_CODEX),
            "agy" | "gemini" => Some(ICON_GEMINI),
            "" => Some(ICON_YOU),
            _ => None,
        };
        match icon {
            Some(png) if self.images => format!(
                "\x1b]1337;File=inline=1;width=2;height=1;preserveAspectRatio=1:{}\x07",
                BASE64.encode(png)
            ),
            _ => format!("{}●{RESET} ", Self::colour(harness)),
        }
    }

    fn message(&self, message: &Value) -> String {
        let name = message["name"].as_str().unwrap_or("?");
        let text = message["text"].as_str().unwrap_or("");
        let harness = message["harness"].as_str().unwrap_or("");
        let time = message["at_ms"]
            .as_i64()
            .and_then(|ms| {
                use chrono::TimeZone;
                chrono::Local.timestamp_millis_opt(ms).single()
            })
            .map(|time| time.format("%H:%M").to_string())
            .unwrap_or_default();
        // Your own lines read like a prompt, as in Codex; everyone else's carry
        // their mark, name and time above an indented body.
        if message["kind"] == "user" && name == self.me {
            let mut out = String::new();
            for (index, line) in text.lines().enumerate() {
                let lead = if index == 0 { "›" } else { " " };
                out.push_str(&format!("{BOLD}{lead}{RESET} {line}\n"));
            }
            return out.trim_end().to_owned();
        }
        let mut out = format!(
            "{}{}{name}{RESET} {DIM}· {time}{RESET}",
            self.mark(harness),
            Self::colour(harness)
        );
        for line in text.lines() {
            out.push_str("\n  ");
            out.push_str(line);
        }
        out
    }

    fn members(&self, members: &[Value]) -> String {
        members
            .iter()
            .map(|member| {
                let harness = member["harness"].as_str().unwrap_or("");
                let working = if member["working"] == true {
                    format!(" {DIM}working{RESET}")
                } else {
                    String::new()
                };
                format!(
                    "{}{}{}{RESET}{working}",
                    self.mark(harness),
                    Self::colour(harness),
                    member["name"].as_str().unwrap_or("?")
                )
            })
            .collect::<Vec<_>>()
            .join("  ")
    }
}

async fn call(socket: &Path, method: &str, params: Value) -> anyhow::Result<Value> {
    let response = request(
        socket,
        &ControlRequest {
            schema: CONTROL_SCHEMA.into(),
            id: Uuid::new_v4(),
            method: method.into(),
            params,
        },
    )
    .await?;
    if !response.ok {
        let detail = response
            .error
            .map(|error| error.message)
            .unwrap_or_else(|| "the daemon refused the request".into());
        anyhow::bail!(detail);
    }
    Ok(response.result.unwrap_or(Value::Null))
}

const HELP: &str = "/who  the agents here and who is working
/pause  stop passing agents' messages to each other (yours still go to everyone)
/resume  pass them again
/off  turn the room back into an ordinary route and leave
/quit  leave; the room stays open";

/// Runs the chat client until the user leaves.
pub async fn run(socket: PathBuf, title: String, me: String) -> anyhow::Result<()> {
    let style = Style::detect(me.clone());
    let opened = call(&socket, "room.open", json!({"title": title})).await?;
    let room = opened["room"].as_str().unwrap_or(&title).to_owned();
    let members = opened["members"].as_array().cloned().unwrap_or_default();
    println!(
        "{DIM}room{RESET} {BOLD}{room}{RESET}  {}",
        style.members(&members)
    );
    println!("{DIM}Everyone here sees every message. /help lists commands.{RESET}\n");
    let mut last = -1;
    for message in opened["messages"].as_array().into_iter().flatten() {
        println!("{}\n", style.message(message));
        last = message["id"].as_i64().unwrap_or(last);
    }
    if last < 0 {
        last = 0;
    }

    let mut editor = rustyline::DefaultEditor::new()?;
    let mut printer = editor.create_external_printer()?;
    let poll_socket = socket.clone();
    let poll_room = room.clone();
    let poll_me = me.clone();
    let poller = tokio::spawn(async move {
        let style = Style::detect(poll_me.clone());
        loop {
            match call(
                &poll_socket,
                "room.poll",
                json!({"title": poll_room, "after": last, "wait_ms": POLL_WAIT_MS}),
            )
            .await
            {
                Ok(result) => {
                    for message in result["messages"].as_array().into_iter().flatten() {
                        last = message["id"].as_i64().unwrap_or(last);
                        // Your own lines are already on screen as you typed them.
                        if message["kind"] == "user" && message["name"] == poll_me.as_str() {
                            continue;
                        }
                        let _ = printer.print(format!("{}\n", style.message(message)));
                    }
                }
                Err(error) => {
                    let _ =
                        printer.print(format!("{DIM}connection lost: {error}; retrying{RESET}\n"));
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
    });

    let prompt = format!("{BOLD}›{RESET} ");
    loop {
        let readline = {
            let prompt = prompt.clone();
            let mut moved = editor;
            let (result, back) = tokio::task::spawn_blocking(move || {
                let result = moved.readline(&prompt);
                (result, moved)
            })
            .await?;
            editor = back;
            result
        };
        let line = match readline {
            Ok(line) => line,
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(error) => return Err(error.into()),
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let _ = editor.add_history_entry(line);
        let outcome = match line {
            "/quit" | "/exit" => break,
            "/help" => {
                println!("{DIM}{HELP}{RESET}");
                continue;
            }
            "/who" => call(&socket, "room.open", json!({"title": room}))
                .await
                .map(|result| {
                    let members = result["members"].as_array().cloned().unwrap_or_default();
                    println!("{}", style.members(&members));
                }),
            "/pause" | "/resume" => call(
                &socket,
                "room.set",
                json!({"title": room, "paused": line == "/pause"}),
            )
            .await
            .map(|_| {
                println!(
                    "{DIM}{}{RESET}",
                    if line == "/pause" {
                        "Agents' messages are no longer passed to each other."
                    } else {
                        "Agents' messages are passed to each other again."
                    }
                )
            }),
            "/off" => {
                let result = call(&socket, "room.set", json!({"title": room, "off": true})).await;
                if result.is_ok() {
                    println!("{DIM}{room} is an ordinary route again.{RESET}");
                    break;
                }
                result.map(|_| ())
            }
            text => call(
                &socket,
                "room.say",
                json!({"title": room, "name": me, "text": text}),
            )
            .await
            .map(|_| ()),
        };
        if let Err(error) = outcome {
            println!("{DIM}not sent: {error}{RESET}");
        }
    }
    poller.abort();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn your_lines_read_as_prompts_and_others_carry_a_mark_and_name() {
        let style = Style {
            images: false,
            me: "Mihai".into(),
        };
        let mine = style.message(&json!({"kind": "user", "name": "Mihai", "text": "hi\nthere"}));
        assert_eq!(mine, format!("{BOLD}›{RESET} hi\n{BOLD} {RESET} there"));
        let theirs = style.message(&json!({
            "kind": "agent", "name": "game_claude", "harness": "claude", "text": "one\ntwo"
        }));
        assert!(theirs.contains("●"));
        assert!(theirs.contains("game_claude"));
        assert!(theirs.ends_with("\n  one\n  two"));
        let images = Style {
            images: true,
            me: "Mihai".into(),
        };
        assert!(images.mark("codex").starts_with("\x1b]1337;File=inline=1;"));
        assert!(images.mark("opencode").contains("●"));
    }
}
