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

use panetone::domain::{ChannelBinding, Route, RouteId};
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

    fn created_topics(&self) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.ends_with("/createForumTopic"))
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
    let topic_name = body["name"].as_str().unwrap_or("topic").to_owned();
    requests.lock().unwrap().push((path.clone(), body));
    let response = if path.ends_with("/getUpdates") {
        r#"{"ok":true,"result":[]}"#.to_owned()
    } else if path.ends_with("/createForumTopic") {
        format!(
            r#"{{"ok":true,"result":{{"message_thread_id":777,"name":{topic_name:?},"icon_color":7322096}}}}"#
        )
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

fn write_launcher_wakterm_fake(directory: &Path) -> (PathBuf, PathBuf) {
    let path = directory.join("wakterm-launcher-fake");
    let ready = directory.join("bootstrap-ready");
    fs::write(
        &path,
        format!(
            r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"--version"* ]]; then
  echo 'wakterm launcher-test'
elif [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":500,"agents":[{{"agent_id":"agent-infobase","incarnation_id":"inc-infobase","pane_id":14,"name":"infobase","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-23T00:00:00Z"}},{{"agent_id":"agent-other","incarnation_id":"inc-other","pane_id":15,"name":"other","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-23T00:00:00Z"}}]}}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{{"pane_id":14,"tab_id":14,"window_id":1,"effective_title":"infobase"}},{{"pane_id":15,"tab_id":15,"window_id":1,"effective_title":"other"}}]'
elif [[ "$operation" == *"agent events"* ]]; then
  after=500
  previous=""
  for argument in "$@"; do
    if [[ "$previous" == "--after" ]]; then after="$argument"; fi
    previous="$argument"
  done
  if [[ -e '{}' && "$after" -eq 500 ]]; then
    echo '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":500,"oldest_available_sequence":1,"latest_sequence":502,"next_after_sequence":502,"events":[{{"sequence":501,"event_id":"bootstrap-ready","kind":"assistant_message","agent_id":"agent-infobase","incarnation_id":"inc-infobase","turn_id":"turn-bootstrap","text":"READY"}},{{"sequence":502,"event_id":"other-output","kind":"assistant_message","agent_id":"agent-other","incarnation_id":"inc-other","turn_id":"turn-other","text":"OTHER"}}]}}'
  elif [[ -e '{}' ]]; then
    printf '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":%s,"oldest_available_sequence":1,"latest_sequence":502,"next_after_sequence":%s,"events":[]}}\n' "$after" "$after"
  else
    printf '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":%s,"oldest_available_sequence":1,"latest_sequence":500,"next_after_sequence":%s,"events":[]}}\n' "$after" "$after"
  fi
elif [[ "$operation" == *"agent request watch"* ]]; then
  exit 0
else
  echo "unexpected fake invocation: $operation" >&2
  exit 93
fi
"#,
            ready.display(),
            ready.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    (path, ready)
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
                "--telegram-owner",
                "42",
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
    }
}

fn cli(socket: &Path, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_panetone"));
    command.args(args).arg("--socket").arg(socket);
    command.output().unwrap()
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
  echo 'wakterm production-workflow-test'
elif [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":500,"agents":[{{"agent_id":"agent-source","incarnation_id":"inc-source","pane_id":1,"name":"not-the-route-title","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}},{{"agent_id":"agent-target","incarnation_id":"inc-target","pane_id":2,"name":"also-renamed","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-17T00:00:00Z"}}]}}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{{"pane_id":1,"tab_id":1,"window_id":1,"effective_title":"source"}},{{"pane_id":2,"tab_id":2,"window_id":1,"effective_title":"target"}}]'
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
async fn production_workflow_resolves_live_routes_and_survives_restart() {
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

#[tokio::test]
async fn launcher_can_ensure_a_route_and_wait_for_exact_durable_output() {
    let directory = tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("state.sqlite3");
    let telegram = HttpCapture::start();
    let (wakterm, ready) = write_launcher_wakterm_fake(directory.path());
    let daemon = Daemon::start(directory.path(), &database, &wakterm, &telegram.base);

    let ensured = cli(&daemon.socket, &["route", "ensure", "infobase"]);
    assert!(
        ensured.status.success(),
        "{}",
        String::from_utf8_lossy(&ensured.stderr)
    );
    let ensured: Value = serde_json::from_slice(&ensured.stdout).unwrap();
    assert_eq!(ensured["result"]["created"], true);
    assert_eq!(ensured["result"]["binding_created"], true);
    assert_eq!(ensured["result"]["event_cursor"], 500);
    assert_eq!(ensured["result"]["route"]["title"], "infobase");
    assert_eq!(ensured["result"]["route"]["channels"][0]["topic_id"], 777);
    assert_eq!(ensured["result"]["live"]["status"], "available");
    assert_eq!(
        ensured["result"]["live"]["agents"][0]["agent_id"],
        "agent-infobase"
    );

    let inspected = cli(&daemon.socket, &["route", "inspect", "INFOBASE"]);
    assert!(inspected.status.success());
    let inspected: Value = serde_json::from_slice(&inspected.stdout).unwrap();
    assert_eq!(
        inspected["result"]["route"]["id"],
        ensured["result"]["route"]["id"]
    );
    assert_eq!(inspected["result"]["event_cursor"], 500);
    let ensured_again = cli(&daemon.socket, &["route", "ensure", "Infobase"]);
    assert!(ensured_again.status.success());
    let ensured_again: Value = serde_json::from_slice(&ensured_again.stdout).unwrap();
    assert_eq!(ensured_again["result"]["created"], false);
    assert_eq!(ensured_again["result"]["binding_created"], false);
    assert_eq!(telegram.created_topics().len(), 1);

    fs::write(&ready, b"ready").unwrap();
    let projected = cli(
        &daemon.socket,
        &[
            "output",
            "wait",
            "--route",
            "infobase",
            "--agent-id",
            "agent-infobase",
            "--incarnation-id",
            "inc-infobase",
            "--after",
            "500",
            "--expect-text",
            "READY",
            "--timeout-ms",
            "5000",
            "--poll-ms",
            "20",
        ],
    );
    assert!(
        projected.status.success(),
        "{}",
        String::from_utf8_lossy(&projected.stderr)
    );
    let projected: Value = serde_json::from_slice(&projected.stdout).unwrap();
    assert_eq!(projected["result"]["disposition"], "projected");
    assert_eq!(projected["result"]["event"]["sequence"], 501);
    assert_eq!(projected["result"]["event"]["event_id"], "bootstrap-ready");
    assert_eq!(projected["result"]["event"]["text"], "READY");
    assert_eq!(projected["result"]["actual_route"]["title"], "infobase");

    let unrouted = cli(
        &daemon.socket,
        &[
            "output",
            "wait",
            "--route",
            "infobase",
            "--agent-id",
            "agent-other",
            "--incarnation-id",
            "inc-other",
            "--after",
            "500",
            "--expect-text",
            "OTHER",
            "--timeout-ms",
            "1000",
            "--poll-ms",
            "20",
        ],
    );
    assert!(!unrouted.status.success());
    let unrouted: Value = serde_json::from_slice(&unrouted.stdout).unwrap();
    assert_eq!(unrouted["result"]["disposition"], "unrouted");
    assert_eq!(unrouted["result"]["event"]["event_id"], "other-output");

    daemon.stop();
}
