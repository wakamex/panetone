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
use serde_json::{Value, json};
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
        let read = match stream.read(&mut chunk) {
            Ok(read) if read > 0 => read,
            _ => return,
        };
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
        let read = match stream.read(&mut chunk) {
            Ok(read) if read > 0 => read,
            _ => return,
        };
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
    // The daemon may stop mid-request at the end of a test.
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        response.len(),
        response
    );
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
  if [[ -e '{}' ]]; then
    echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":500,"agents":[{{"agent_id":"agent-infobase","incarnation_id":"inc-infobase","pane_id":14,"name":"infobase","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-23T00:00:00Z"}},{{"agent_id":"agent-other","incarnation_id":"inc-other","pane_id":15,"name":"other","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-23T00:00:00Z"}}]}}'
  else
    echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":500,"agents":[{{"agent_id":"agent-infobase","incarnation_id":"inc-infobase","pane_id":14,"name":"infobase","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-08-23T00:00:00Z"}}]}}'
  fi
elif [[ "$operation" == *"list --format json"* ]]; then
  if [[ -e '{}' ]]; then
    echo '[{{"pane_id":14,"tab_id":14,"window_id":1,"effective_title":"infobase"}},{{"pane_id":15,"tab_id":15,"window_id":1,"effective_title":"other"}}]'
  else
    echo '[{{"pane_id":14,"tab_id":14,"window_id":1,"effective_title":"infobase"}}]'
  fi
elif [[ "$operation" == *"agent events"* ]]; then
  after=500
  previous=""
  for argument in "$@"; do
    if [[ "$previous" == "--after" ]]; then after="$argument"; fi
    previous="$argument"
  done
  if [[ -e '{}' && "$after" -eq 500 ]]; then
    echo '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":500,"oldest_available_sequence":1,"latest_sequence":503,"next_after_sequence":503,"events":[{{"sequence":501,"event_id":"bootstrap-ready","kind":"assistant_message","agent_id":"agent-infobase","incarnation_id":"inc-infobase","turn_id":"turn-bootstrap","text":"READY"}},{{"sequence":502,"event_id":"other-available","kind":"agent_lifecycle","agent_id":"agent-other","incarnation_id":"inc-other","lifecycle":"available"}},{{"sequence":503,"event_id":"other-output","kind":"assistant_message","agent_id":"agent-other","incarnation_id":"inc-other","turn_id":"turn-other","text":"OTHER"}}]}}'
  elif [[ -e '{}' ]]; then
    printf '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":%s,"oldest_available_sequence":1,"latest_sequence":503,"next_after_sequence":%s,"events":[]}}\n' "$after" "$after"
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
            ready.display(),
            ready.display(),
            ready.display()
        ),
    )
    .unwrap();
    panetone::test_support::seal_executable(&path);
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
    panetone::test_support::seal_executable(&path);
    (path, admissions)
}

fn write_source_pane_wakterm_fake(directory: &Path, request_id: &str) -> (PathBuf, PathBuf) {
    let path = directory.join("wakterm-source-pane-fake");
    let admissions = directory.join("source-pane-admissions.log");
    fs::write(
        &path,
        format!(
            r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"--version"* ]]; then
  echo 'wakterm source-pane-test'
elif [[ "$operation" == *"agent caller"* ]]; then
  case "${{CODEX_THREAD_ID-}}" in
    thread-caller) incarnation=inc-caller ;;
    thread-replaced) incarnation=inc-replaced ;;
    *) echo "caller not identified" >&2; exit 1 ;;
  esac
  echo '{{"schema":"wakterm.agent-api.v1","resolved_by":"codex_thread","agent":{{"agent_id":"agent-caller","incarnation_id":"'"$incarnation"'","pane_id":85,"name":"inq2","harness":"claude","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-09-24T00:00:00Z"}}}}'
elif [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":500,"agents":[{{"agent_id":"agent-main","incarnation_id":"inc-main","pane_id":1,"name":"main","harness":"claude","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-09-24T00:00:00Z"}},{{"agent_id":"agent-caller","incarnation_id":"inc-caller","pane_id":85,"name":"inq2","harness":"claude","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-09-24T00:00:00Z"}},{{"agent_id":"agent-target","incarnation_id":"inc-target","pane_id":2,"name":"target","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-09-24T00:00:00Z"}}]}}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{{"pane_id":1,"tab_id":1,"window_id":1,"effective_title":"inquisition"}},{{"pane_id":85,"tab_id":2,"window_id":1,"effective_title":"inq2"}},{{"pane_id":2,"tab_id":3,"window_id":1,"effective_title":"target"}}]'
elif [[ "$operation" == *"agent events"* ]]; then
  echo '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":500,"oldest_available_sequence":1,"latest_sequence":500,"next_after_sequence":500,"events":[]}}'
elif [[ "$operation" == *"agent request watch"* ]]; then
  echo '{{"request_id":"{request_id}","target_agent_id":"agent-target","state":"completed","final_message":"finished","detail":null,"terminal_event_sequence":1}}'
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
            admissions.display(),
            request_id = request_id
        ),
    )
    .unwrap();
    panetone::test_support::seal_executable(&path);
    (path, admissions)
}

#[tokio::test]
async fn wakterm_caller_derives_the_route_and_callback_without_from() {
    let directory = tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&database).unwrap();
    store
        .save_route(unavailable_route(41, "inquisition", 401), 1)
        .await
        .unwrap();
    store
        .save_route(unavailable_route(42, "inq2", 402), 1)
        .await
        .unwrap();
    store
        .save_route(unavailable_route(43, "target", 403), 1)
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let request_id = "44444444-4444-4444-8444-444444444444";
    let telegram = HttpCapture::start();
    let (wakterm, admissions) = write_source_pane_wakterm_fake(directory.path(), request_id);
    let daemon = Daemon::start(directory.path(), &database, &wakterm, &telegram.base);

    let output = Command::new(env!("CARGO_BIN_EXE_panetone"))
        .args([
            "send",
            "--socket",
            daemon.socket.to_str().unwrap(),
            "--to",
            "target",
            "--id",
            request_id,
            "--return-final",
            "do the work",
        ])
        .env("WAKTERM_BIN", &wakterm)
        .env("CODEX_THREAD_ID", "thread-caller")
        .env_remove("WAKTERM_PANE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let log = fs::read_to_string(&admissions).unwrap_or_default();
        if log.contains("agent-target") && log.contains("agent-caller") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "callback was not returned to agent-caller: {log}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    daemon.stop();

    let store = StoreHandle::open(&database).unwrap();
    let workflow_id = panetone::domain::WorkflowId::new(Uuid::parse_str(request_id).unwrap());
    let workflow = store.get_workflow(workflow_id).await.unwrap().unwrap();
    assert_eq!(workflow.command.source, "inq2");
    assert_eq!(workflow.workflow.observed_source.agent_id, "agent-caller");
    assert_eq!(workflow.workflow.observed_source.pane_id, Some(85));
    let returned = store.get_return(workflow_id).await.unwrap().unwrap();
    assert_eq!(returned.agent.source.agent_id, "agent-caller");
    assert_eq!(returned.agent.source.pane_id, Some(85));
    store.shutdown().await.unwrap();
}

fn write_shared_route_wakterm_fake(directory: &Path) -> (PathBuf, PathBuf) {
    let path = directory.join("wakterm-shared-route-fake");
    let admissions = directory.join("shared-route-admissions.log");
    fs::write(
        &path,
        format!(
            r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"--version"* ]]; then
  echo 'wakterm shared-route-test'
elif [[ "$operation" == *"agent caller"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","resolved_by":"wakterm_pane","agent":{{"agent_id":"agent-source","incarnation_id":"inc-source","pane_id":1,"name":"source_codex","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-10-04T00:00:00Z"}}}}'
elif [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":500,"agents":[{{"agent_id":"agent-source","incarnation_id":"inc-source","pane_id":1,"name":"source_codex","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-10-04T00:00:00Z"}},{{"agent_id":"agent-claude","incarnation_id":"inc-claude","pane_id":2,"name":"shared_claude","harness":"claude","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-10-04T00:00:00Z"}},{{"agent_id":"agent-codex","incarnation_id":"inc-codex","pane_id":3,"name":"shared_codex","harness":"codex","status":"busy","turn_state":"waiting_on_agent","alive":true,"observed_at":"2026-10-04T00:00:00Z"}}]}}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{{"pane_id":1,"tab_id":1,"window_id":1,"effective_title":"source"}},{{"pane_id":2,"tab_id":2,"window_id":1,"effective_title":"shared"}},{{"pane_id":3,"tab_id":2,"window_id":1,"effective_title":"shared"}}]'
elif [[ "$operation" == *"agent events"* ]]; then
  echo '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":500,"oldest_available_sequence":1,"latest_sequence":500,"next_after_sequence":500,"events":[]}}'
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
  printf '%s\n' "$target" >> '{}'
  # The Codex is busy for its first admission only.
  if [[ "$target" == "agent-codex" && ! -e '{}.busy-once' ]]; then
    touch '{}.busy-once'
    printf '{{"schema":"wakterm.agent-api.v1","request_id":"%s","status":"busy","definitive":true,"prompt_written":false,"agent_id":"%s","incarnation_id":"%s","detail":"target is busy"}}\n' "$request_id" "$target" "$incarnation"
    exit 0
  fi
  printf '{{"schema":"wakterm.agent-api.v1","request_id":"%s","status":"accepted","definitive":true,"prompt_written":true,"agent_id":"%s","incarnation_id":"%s","detail":null}}\n' "$request_id" "$target" "$incarnation"
else
  echo "unexpected fake invocation: $operation" >&2
  exit 92
fi
"#,
            admissions.display(),
            admissions.display(),
            admissions.display(),
        ),
    )
    .unwrap();
    panetone::test_support::seal_executable(&path);
    (path, admissions)
}

#[tokio::test]
async fn a_route_with_several_agents_is_addressed_by_agent_name() {
    let directory = tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&database).unwrap();
    store
        .save_route(unavailable_route(51, "source", 501), 1)
        .await
        .unwrap();
    store
        .save_route(unavailable_route(52, "shared", 502), 1)
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let telegram = HttpCapture::start();
    let (wakterm, admissions) = write_shared_route_wakterm_fake(directory.path());
    let daemon = Daemon::start(directory.path(), &database, &wakterm, &telegram.base);
    let send = |target: &str| {
        Command::new(env!("CARGO_BIN_EXE_panetone"))
            .args([
                "send",
                "--socket",
                daemon.socket.to_str().unwrap(),
                "--to",
                target,
                "hello",
            ])
            .env("WAKTERM_BIN", &wakterm)
            .env_remove("WAKTERM_PANE")
            .output()
            .unwrap()
    };

    let ambiguous = send("shared");
    assert!(!ambiguous.status.success());
    let response: Value = serde_json::from_slice(&ambiguous.stdout).unwrap();
    assert_eq!(response["error"]["code"], "target_route_has_several_agents");
    assert_eq!(
        response["error"]["details"]["agents"],
        json!(["shared_claude", "shared_codex"])
    );

    let named = send("SHARED_CLAUDE");
    assert!(
        named.status.success(),
        "{}",
        String::from_utf8_lossy(&named.stdout)
    );

    let listed = Command::new(env!("CARGO_BIN_EXE_panetone"))
        .args(["route", "list", "--socket", daemon.socket.to_str().unwrap()])
        .output()
        .unwrap();
    let listed: Value = serde_json::from_slice(&listed.stdout).unwrap();
    let shared = listed["result"]["routes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|route| route["title"] == "shared")
        .unwrap()
        .clone();
    assert_eq!(shared["agents"][0]["name"], "shared_claude");

    // A queued message is retried to the agent it was sent to, never to
    // another agent in the same route.
    let queued = send("shared_codex");
    let response: Value = serde_json::from_slice(&queued.stdout).unwrap();
    assert_eq!(response["result"]["delivery_state"], "queued");
    let deadline = Instant::now() + Duration::from_secs(10);
    while fs::read_to_string(&admissions).unwrap().lines().count() < 3 {
        assert!(
            Instant::now() < deadline,
            "the queued message was not retried"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    daemon.stop();

    assert_eq!(
        fs::read_to_string(&admissions).unwrap(),
        "agent-claude\nagent-codex\nagent-codex\n"
    );
}

#[tokio::test]
async fn send_rejects_a_caller_incarnation_that_is_no_longer_live() {
    let directory = tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&database).unwrap();
    store
        .save_route(unavailable_route(42, "inq2", 402), 1)
        .await
        .unwrap();
    store
        .save_route(unavailable_route(43, "target", 403), 1)
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let telegram = HttpCapture::start();
    let (wakterm, admissions) =
        write_source_pane_wakterm_fake(directory.path(), "55555555-5555-4555-8555-555555555555");
    let daemon = Daemon::start(directory.path(), &database, &wakterm, &telegram.base);

    let output = Command::new(env!("CARGO_BIN_EXE_panetone"))
        .args([
            "send",
            "--socket",
            daemon.socket.to_str().unwrap(),
            "--to",
            "target",
            "do the work",
        ])
        .env("WAKTERM_BIN", &wakterm)
        .env("CODEX_THREAD_ID", "thread-replaced")
        .env_remove("WAKTERM_PANE")
        .output()
        .unwrap();
    daemon.stop();
    assert!(!output.status.success());
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["error"]["code"], "source_agent_unavailable");
    assert_eq!(
        response["error"]["details"]["incarnation_id"],
        "inc-replaced"
    );
    assert!(
        fs::read_to_string(&admissions)
            .unwrap_or_default()
            .is_empty()
    );
}

fn write_busy_steering_wakterm_fake(directory: &Path) -> (PathBuf, PathBuf, PathBuf) {
    let path = directory.join("wakterm-steering-fake");
    let admitted_prompt = directory.join("admitted-prompt.txt");
    let steered_prompt = directory.join("steered-prompt.txt");
    fs::write(
        &path,
        format!(
            r#"#!/bin/bash
set -euo pipefail
operation="$*"
if [[ "$operation" == *"--version"* ]]; then
  echo 'wakterm steering-test'
elif [[ "$operation" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$operation" == *"agent catalog"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":500,"agents":[{{"agent_id":"agent-source","incarnation_id":"inc-source","pane_id":1,"name":"source","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-09-18T00:00:00Z"}},{{"agent_id":"agent-target","incarnation_id":"inc-target","pane_id":2,"name":"target","harness":"codex","status":"busy","turn_state":"waiting_on_agent","alive":true,"observed_at":"2026-09-18T00:00:00Z"}}]}}'
elif [[ "$operation" == *"list --format json"* ]]; then
  echo '[{{"pane_id":1,"tab_id":1,"window_id":1,"effective_title":"source"}},{{"pane_id":2,"tab_id":2,"window_id":1,"effective_title":"target"}}]'
elif [[ "$operation" == *"agent events"* ]]; then
  echo '{{"schema":"wakterm.agent-events.v1","status":"ok","requested_after_sequence":500,"oldest_available_sequence":1,"latest_sequence":500,"next_after_sequence":500,"events":[]}}'
elif [[ "$operation" == *"agent request watch"* ]]; then
  exit 0
elif [[ "$operation" == *"agent admit"* ]]; then
  request_id=""
  incarnation=""
  previous=""
  for argument in "$@"; do
    if [[ "$previous" == "--request-id" ]]; then request_id="$argument"; fi
    if [[ "$previous" == "--incarnation" ]]; then incarnation="$argument"; fi
    previous="$argument"
  done
  cat > '{}'
  printf '{{"schema":"wakterm.agent-api.v1","request_id":"%s","status":"busy","definitive":true,"prompt_written":false,"agent_id":"agent-target","incarnation_id":"%s","detail":"target is busy"}}\n' "$request_id" "$incarnation"
elif [[ "$operation" == *"agent send agent-target"* && -e '{}' ]]; then
  cat >/dev/null
  echo '{{"agent_id":"agent-target","agent_name":"target","pane_id":2,"transport":"ObservedPty","submitted":false,"acknowledgement":{{"kind":"not_requested","acknowledged":false,"latency_ms":null,"session_path":null,"detail":null}},"refusal":{{"reason":"input_blocked","detail":"the target is waiting for dialog open"}}}}'
elif [[ "$operation" == *"agent send agent-target"* ]]; then
  cat > '{}'
  echo '{{"agent_id":"agent-target","agent_name":"target","pane_id":2,"transport":"CodexAppServerTui","submitted":true,"acknowledgement":{{"kind":"app_server","acknowledged":true,"latency_ms":1,"session_path":null,"detail":"[app-server-tui running] Codex commandExecution"}}}}'
else
  echo "unexpected fake invocation: $operation" >&2
  exit 92
fi
"#,
            admitted_prompt.display(),
            directory.join("blocked").display(),
            steered_prompt.display()
        ),
    )
    .unwrap();
    panetone::test_support::seal_executable(&path);
    (path, admitted_prompt, steered_prompt)
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
async fn cli_steer_uses_the_busy_turn_path_and_deduplicates_retries() {
    let directory = tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&database).unwrap();
    store
        .save_route(unavailable_route(31, "source", 301), 1)
        .await
        .unwrap();
    store
        .save_route(unavailable_route(32, "target", 302), 1)
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let telegram = HttpCapture::start();
    let (wakterm, admitted_prompt, steered_prompt) =
        write_busy_steering_wakterm_fake(directory.path());
    let daemon = Daemon::start(directory.path(), &database, &wakterm, &telegram.base);
    let request_id = "33333333-3333-4333-8333-333333333333";
    let args = [
        "send",
        "--from",
        "source",
        "--to",
        "target",
        "--id",
        request_id,
        "--steer",
        "correct the active turn",
    ];

    let first = cli(&daemon.socket, &args);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let ack: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(ack["result"]["delivery_state"], "steered");
    assert_eq!(ack["result"]["submitted"], true);
    assert_eq!(ack["result"]["steering_acknowledged"], true);
    assert_eq!(
        fs::read_to_string(&admitted_prompt).unwrap(),
        fs::read_to_string(&steered_prompt).unwrap()
    );
    assert!(
        fs::read_to_string(&steered_prompt)
            .unwrap()
            .contains("correct the active turn")
    );

    let duplicate = cli(&daemon.socket, &args);
    assert!(duplicate.status.success());
    let duplicate_ack: Value = serde_json::from_slice(&duplicate.stdout).unwrap();
    assert_eq!(duplicate_ack["result"], ack["result"]);
    assert_eq!(telegram.sent_messages().len(), 2);
    daemon.stop();
}

#[tokio::test]
async fn cli_steer_queues_when_the_target_cannot_take_input() {
    let directory = tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&database).unwrap();
    store
        .save_route(unavailable_route(31, "source", 301), 1)
        .await
        .unwrap();
    store
        .save_route(unavailable_route(32, "target", 302), 1)
        .await
        .unwrap();
    store.shutdown().await.unwrap();
    let telegram = HttpCapture::start();
    let (wakterm, _, steered_prompt) = write_busy_steering_wakterm_fake(directory.path());
    fs::write(directory.path().join("blocked"), b"").unwrap();
    let daemon = Daemon::start(directory.path(), &database, &wakterm, &telegram.base);

    let sent = cli(
        &daemon.socket,
        &[
            "send",
            "--from",
            "source",
            "--to",
            "target",
            "--steer",
            "correct the active turn",
        ],
    );
    daemon.stop();
    assert!(
        sent.status.success(),
        "{}",
        String::from_utf8_lossy(&sent.stdout)
    );
    let ack: Value = serde_json::from_slice(&sent.stdout).unwrap();
    assert_eq!(ack["result"]["delivery_state"], "queued");
    assert!(!steered_prompt.exists());
}

#[tokio::test]
async fn launcher_can_ensure_a_route_and_wait_for_exact_durable_output() {
    let directory = tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&database).unwrap();
    store
        .save_route(unavailable_route(99, "offline-project", 999), 1)
        .await
        .unwrap();
    store.shutdown().await.unwrap();
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
    assert_eq!(ensured["result"]["created"], false);
    assert_eq!(ensured["result"]["binding_created"], false);
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

    let listed = cli(&daemon.socket, &["route", "list"]);
    assert!(listed.status.success());
    let listed: Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(listed["result"]["routes"][0]["title"], "infobase");
    assert_eq!(listed["result"]["routes"][0]["available"], true);
    assert_eq!(
        listed["result"]["routes"][0]["agents"][0]["agent_id"],
        "agent-infobase"
    );
    assert_eq!(
        listed["result"]["routes"][1],
        serde_json::json!({
            "title": "offline-project",
            "available": false,
            "agents": [],
        })
    );

    let ensured_again = cli(&daemon.socket, &["route", "ensure", "Infobase"]);
    assert!(ensured_again.status.success());
    let ensured_again: Value = serde_json::from_slice(&ensured_again.stdout).unwrap();
    assert_eq!(ensured_again["result"]["created"], false);
    assert_eq!(ensured_again["result"]["binding_created"], false);
    let created_topics = telegram.created_topics();
    assert_eq!(created_topics.len(), 1);
    assert_eq!(created_topics[0]["name"], "infobase");

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
    let created_topics = telegram.created_topics();
    assert_eq!(created_topics.len(), 2);
    assert!(created_topics.iter().any(|topic| topic["name"] == "other"));
    assert!(
        cli(&daemon.socket, &["route", "inspect", "other"])
            .status
            .success()
    );

    let misrouted = cli(
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
    assert!(!misrouted.status.success());
    let misrouted: Value = serde_json::from_slice(&misrouted.stdout).unwrap();
    assert_eq!(misrouted["result"]["disposition"], "misrouted");
    assert_eq!(misrouted["result"]["event"]["event_id"], "other-output");
    assert_eq!(misrouted["result"]["actual_route"]["title"], "other");

    daemon.stop();
}
