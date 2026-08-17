use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::tempdir;

const FIXTURE: &str = "/code/wakterm/docs/agent-api/v1/golden-fixtures.json";

struct Daemon {
    child: Child,
    socket: PathBuf,
}

impl Daemon {
    fn start(directory: &Path, profile: &str) -> Self {
        let socket = directory.join(format!("{profile}.sock"));
        let child = Command::new(env!("CARGO_BIN_EXE_panetone"))
            .args([
                "daemon",
                "--socket",
                socket.to_str().unwrap(),
                "--journal",
                directory
                    .join(format!("{profile}.sqlite3"))
                    .to_str()
                    .unwrap(),
                "--effect-log",
                directory.join(format!("{profile}.jsonl")).to_str().unwrap(),
                "--wakterm-fixture",
                FIXTURE,
                "--profile",
                profile,
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

    fn status(&self) -> Value {
        let output = Command::new(env!("CARGO_BIN_EXE_panetone"))
            .args([
                "status",
                "--socket",
                self.socket.to_str().unwrap(),
                "--json",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "status failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn terminate(mut self) {
        let status = Command::new("/bin/kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
        assert!(self.child.wait().unwrap().success());
        assert!(!self.socket.exists());
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn daemon_status_reports_supervision_backlog_and_capability_gating() {
    let directory = tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();

    let current = Daemon::start(directory.path(), "current");
    let status = current.status();
    assert_eq!(status["ok"], true);
    assert_eq!(status["result"]["mode"], "offline_fake");
    assert_eq!(status["result"]["wakterm"]["profile"], "current");
    assert_eq!(status["result"]["wakterm"]["general_event_consumer"], false);
    assert_eq!(status["result"]["tasks"]["control"]["state"], "running");
    assert_eq!(status["result"]["store"]["workflows"], 0);
    current.terminate();

    let future = Daemon::start(directory.path(), "future-events");
    let status = future.status();
    assert_eq!(status["result"]["wakterm"]["profile"], "future_events");
    assert_eq!(status["result"]["wakterm"]["general_event_consumer"], true);
}

#[test]
fn doctor_validates_both_profiles_and_confirms_production_is_disabled() {
    let directory = tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_panetone"))
        .args([
            "doctor",
            "--socket",
            directory.path().join("control.sock").to_str().unwrap(),
            "--journal",
            directory.path().join("state.sqlite3").to_str().unwrap(),
            "--wakterm-fixture",
            FIXTURE,
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "doctor failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["ok"], true);
    assert_eq!(report["checks"]["wakterm_contract"]["ok"], true);
    assert_eq!(report["checks"]["production_connections"]["ok"], true);
    assert!(
        report["checks"]["production_connections"]["detail"]
            .as_str()
            .unwrap()
            .contains("disabled")
    );
}
