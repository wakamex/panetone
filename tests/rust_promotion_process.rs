use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use panetone::domain::{ChannelBinding, Route, RouteId, RouteStatus};
use panetone::store::StoreHandle;
use serde_json::Value;
use tempfile::tempdir;
use uuid::Uuid;

struct HttpCapture {
    base: String,
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    stop: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
}

impl HttpCapture {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let message_id = Arc::new(AtomicU64::new(400));
        let task = std::thread::spawn(move || {
            while !stopped.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        handle_http(stream, &captured, &message_id);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("HTTP capture failed: {error}"),
                }
            }
        });
        Self {
            base: format!("http://{address}"),
            requests,
            stop,
            task: Some(task),
        }
    }

    fn sent_messages(&self) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.ends_with("/sendMessage"))
            .map(|(_, body)| body.clone())
            .collect()
    }
}

impl Drop for HttpCapture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            task.join().unwrap();
        }
    }
}

fn handle_http(
    mut stream: TcpStream,
    requests: &Mutex<Vec<(String, Value)>>,
    message_id: &AtomicU64,
) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 2048];
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(position) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let header = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let content_length = header
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length: ")
                .and_then(|value| value.parse::<usize>().ok())
        })
        .unwrap_or(0);
    while bytes.len() - header_end < content_length {
        let mut chunk = [0_u8; 2048];
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
    }
    let path = header
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .to_owned();
    let body: Value = serde_json::from_slice(&bytes[header_end..header_end + content_length])
        .unwrap_or(Value::Null);
    requests.lock().unwrap().push((path.clone(), body));
    let response = if path.ends_with("/getUpdates") {
        r#"{"ok":true,"result":[]}"#.to_owned()
    } else {
        let id = message_id.fetch_add(1, Ordering::Relaxed);
        format!(r#"{{"ok":true,"result":{{"message_id":{id}}}}}"#)
    };
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        response.len(),
        response
    )
    .unwrap();
}

struct Daemon {
    child: Child,
    socket: PathBuf,
}

impl Daemon {
    fn start(directory: &Path, database: &Path, wakterm: &Path, telegram_base: &str) -> Self {
        let socket = directory.join("control.sock");
        let child = Command::new(env!("CARGO_BIN_EXE_panetone"))
            .args([
                "daemon",
                "--socket",
                socket.to_str().unwrap(),
                "--database",
                database.to_str().unwrap(),
                "--wakterm-bin",
                wakterm.to_str().unwrap(),
                "--wakterm-socket",
                directory.join("mux.sock").to_str().unwrap(),
                "--telegram-api-base",
                telegram_base,
                "--telegram-chat",
                "-1001",
                "--telegram-claude-token",
                "test-token",
                "--worker-poll-ms",
                "100",
            ])
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !socket.exists() {
            assert!(
                Instant::now() < deadline,
                "daemon did not create its socket"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        Self { child, socket }
    }

    fn stop(mut self) {
        assert!(
            Command::new("/bin/kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        assert!(self.child.wait().unwrap().success());
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn unavailable_route(value: u128, title: &str, topic_id: i64) -> Route {
    Route {
        id: RouteId::new(Uuid::from_u128(value)),
        title: title.into(),
        channels: vec![ChannelBinding::Telegram { topic_id }],
        agent: None,
        status: RouteStatus::Unavailable,
    }
}

fn cli(socket: &Path, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_panetone"));
    if args.first() == Some(&"operator") {
        command
            .arg("operator")
            .arg("--socket")
            .arg(socket)
            .args(&args[1..]);
    } else {
        command.args(args).arg("--socket").arg(socket);
    }
    command.output().unwrap()
}

fn operator(socket: &Path, args: &[&str]) -> Value {
    let output = cli(socket, &[&["operator"], args].concat());
    assert!(
        output.status.success(),
        "operator failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn write_wakterm_fake(directory: &Path) -> (PathBuf, PathBuf) {
    let path = directory.join("wakterm-fake");
    let admissions = directory.join("admissions.log");
    fs::write(
        &path,
        format!(
            r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"--version"* ]]; then
  echo 'wakterm promotion-test'
elif [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":500,"agents":[{{"agent_id":"agent-source","incarnation_id":"inc-source","pane_id":1,"name":"not-the-route-title","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}},{{"agent_id":"agent-target","incarnation_id":"inc-target","pane_id":2,"name":"also-renamed","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}}]}}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{{"pane_id":1,"tab_title":"source"}},{{"pane_id":2,"tab_title":"target"}}]'
elif [[ "$operation" == *"agent events"* ]]; then
  echo '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":500,"oldest_available_sequence":1,"latest_sequence":500,"next_after_sequence":500,"events":[]}}'
elif [[ "$operation" == *"agent request watch"* ]]; then
  exit 0
elif [[ "$operation" == *"agent admit"* ]]; then
  target=""
  request_id=""
  incarnation=""
  previous=""
  for argument in "$@"; do
    if [[ "$previous" == "admit" ]]; then target="$argument"; fi
    if [[ "$previous" == "--request-id" ]]; then request_id="$argument"; fi
    if [[ "$previous" == "--incarnation" ]]; then incarnation="$argument"; fi
    previous="$argument"
  done
  cat >/dev/null
  printf '%s %s\n' "$request_id" "$target" >> '{}'
  printf '{{"schema":"wakterm.agent-api.v1","request_id":"%s","status":"accepted","definitive":true,"prompt_written":true,"agent_id":"%s","incarnation_id":"%s","detail":null}}\n' "$request_id" "$target" "$incarnation"
else
  echo "unexpected fake invocation: $operation" >&2
  exit 92
fi
"#,
            admissions.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    (path, admissions)
}

#[tokio::test]
async fn local_promotion_rehearsal_covers_canary_send_report_back_replay_and_restart() {
    let directory = tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&database).unwrap();
    store
        .save_route(unavailable_route(1, "source", 101), 1)
        .await
        .unwrap();
    store
        .save_route(unavailable_route(2, "target", 102), 1)
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let telegram = HttpCapture::start();
    let (wakterm, admissions) = write_wakterm_fake(directory.path());
    let daemon = Daemon::start(directory.path(), &database, &wakterm, &telegram.base);

    for route in ["source", "target"] {
        operator(&daemon.socket, &["reconcile-route", route]);
        operator(&daemon.socket, &["enable-route", route]);
    }
    operator(&daemon.socket, &["init-event-cursor", "500"]);
    operator(&daemon.socket, &["init-telegram-cursor", "0"]);
    operator(&daemon.socket, &["release"]);

    let request_id = "11111111-1111-4111-8111-111111111111";
    let send = cli(
        &daemon.socket,
        &[
            "send",
            "--from",
            "source",
            "--to",
            "target",
            "--id",
            request_id,
            "do the work",
        ],
    );
    assert!(
        send.status.success(),
        "{}",
        String::from_utf8_lossy(&send.stderr)
    );
    let ack: Value = serde_json::from_slice(&send.stdout).unwrap();
    assert_eq!(ack["result"]["delivery_state"], "submitted");
    assert_eq!(ack["result"]["reply_pending"], false);

    let duplicate = cli(
        &daemon.socket,
        &[
            "send",
            "--from",
            "source",
            "--to",
            "target",
            "--id",
            request_id,
            "do the work",
        ],
    );
    assert!(duplicate.status.success());
    let conflict = cli(
        &daemon.socket,
        &[
            "send",
            "--from",
            "source",
            "--to",
            "target",
            "--id",
            request_id,
            "different work",
        ],
    );
    assert!(!conflict.status.success());
    let conflict: Value = serde_json::from_slice(&conflict.stdout).unwrap();
    assert_eq!(conflict["error"]["code"], "idempotency_conflict");

    let report_id = "22222222-2222-4222-8222-222222222222";
    let report = cli(
        &daemon.socket,
        &[
            "send",
            "--from",
            "target",
            "--to",
            "source",
            "--id",
            report_id,
            "received and complete",
        ],
    );
    assert!(report.status.success());
    assert_eq!(fs::read_to_string(&admissions).unwrap().lines().count(), 2);
    let messages = telegram.sent_messages();
    assert_eq!(messages.len(), 4);
    assert!(messages.iter().any(|message| {
        message["message_thread_id"] == 102
            && message["text"].as_str().unwrap().contains("do the work")
    }));
    assert!(messages.iter().any(|message| {
        message["message_thread_id"] == 101
            && message["text"]
                .as_str()
                .unwrap()
                .contains("received and complete")
    }));
    daemon.stop();

    let restarted = Daemon::start(directory.path(), &database, &wakterm, &telegram.base);
    let duplicate_after_restart = cli(
        &restarted.socket,
        &[
            "send",
            "--from",
            "source",
            "--to",
            "target",
            "--id",
            request_id,
            "do the work",
        ],
    );
    assert!(duplicate_after_restart.status.success());
    assert_eq!(fs::read_to_string(&admissions).unwrap().lines().count(), 2);
    assert_eq!(telegram.sent_messages().len(), 4);
    restarted.stop();
}
