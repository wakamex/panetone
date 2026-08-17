use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use panetone::domain::{AgentBinding, ChannelBinding, Route, RouteId, RouteStatus};
use panetone::store::StoreHandle;
use serde_json::Value;
use tempfile::tempdir;
use uuid::Uuid;

struct ProductionDaemon {
    child: Child,
    socket: PathBuf,
}

impl ProductionDaemon {
    fn start(directory: &Path, database: &Path, wakterm: &Path, mux_socket: &Path) -> Self {
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
                mux_socket.to_str().unwrap(),
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

    fn terminate(mut self) {
        assert!(
            Command::new("/bin/kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        assert!(self.child.wait().unwrap().success());
        assert!(!self.socket.exists());
    }
}

impl Drop for ProductionDaemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn route(id: u128, title: &str, pane_id: u64, topic_id: i64) -> Route {
    Route {
        id: RouteId::new(Uuid::from_u128(id)),
        title: title.into(),
        channels: vec![ChannelBinding::Telegram { topic_id }],
        agent: Some(AgentBinding {
            agent_id: format!("agent-{title}"),
            incarnation_id: format!("incarnation-{title}"),
            harness: "codex".into(),
            pane_id: Some(pane_id),
        }),
        status: RouteStatus::Available,
    }
}

#[tokio::test]
async fn held_production_daemon_opens_state_without_polling_or_external_effects() {
    let directory = tempdir().unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let database = directory.path().join("state.sqlite3");
    let store = StoreHandle::open(&database).unwrap();
    store
        .save_route(route(1, "source", 1, 101), 1)
        .await
        .unwrap();
    store
        .save_route(route(2, "target", 2, 102), 1)
        .await
        .unwrap();
    store.shutdown().await.unwrap();

    let log = directory.path().join("wakterm.log");
    let wakterm = directory.path().join("wakterm-fake");
    fs::write(
        &wakterm,
        format!(
            r#"#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> '{}'
if [[ "$*" == *"--version"* ]]; then
  echo 'wakterm held-test'
elif [[ "$*" == *"agent capabilities"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1","event_stream.v1"]}}'
elif [[ "$*" == *"agent catalog"* ]]; then
  echo '{{"schema":"wakterm.agent-api.v1","as_of_event_sequence":500,"agents":[]}}'
else
  echo 'external operation attempted while held' >&2
  exit 91
fi
"#,
            log.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&wakterm, fs::Permissions::from_mode(0o700)).unwrap();
    let daemon = ProductionDaemon::start(
        directory.path(),
        &database,
        &wakterm,
        &directory.path().join("fake-mux.sock"),
    );

    std::thread::sleep(Duration::from_millis(350));
    let status = Command::new(env!("CARGO_BIN_EXE_panetone"))
        .args([
            "status",
            "--socket",
            daemon.socket.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    assert!(status.status.success());
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["result"]["mode"], "production");
    assert_eq!(
        status["result"]["store"]["promotion"]["delivery_hold"],
        true
    );
    assert_eq!(
        status["result"]["store"]["promotion"]["event_cursor"],
        Value::Null
    );
    assert_eq!(status["result"]["store"]["pending_outbox"], 0);

    let send = Command::new(env!("CARGO_BIN_EXE_panetone"))
        .args([
            "send",
            "--from",
            "source",
            "--to",
            "target",
            "--socket",
            daemon.socket.to_str().unwrap(),
            "held test",
        ])
        .output()
        .unwrap();
    assert!(!send.status.success());
    let response: Value = serde_json::from_slice(&send.stdout).unwrap();
    assert_eq!(response["error"]["code"], "delivery_held");

    let calls = fs::read_to_string(&log).unwrap();
    assert_eq!(calls.lines().count(), 4);
    assert!(!calls.contains("agent events"));
    assert!(!calls.contains("agent request"));
    assert!(!calls.contains("agent admit"));
    daemon.terminate();
}
