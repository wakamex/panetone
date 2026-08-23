use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use serde_json::Value;
use tempfile::tempdir;

#[test]
fn doctor_preflights_an_explicit_wakterm_binary_socket_and_loaded_capabilities() {
    let directory = tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let binary = directory.path().join("wakterm-fake");
    std::fs::write(
        &binary,
        r#"#!/bin/bash
set -euo pipefail
if [[ "$*" == *"--version"* ]]; then
  echo 'wakterm test-version-1'
elif [[ "$*" == *"agent capabilities"* ]]; then
  echo '{"schema":"wakterm.agent-api.v1","api_major":1,"capabilities":["catalog.v1","prompt_admission.v1","return_request_terminal_stream.v1"]}'
elif [[ "$*" == *"agent catalog"* ]]; then
  echo '{"schema":"wakterm.agent-api.v1","agents":[]}'
elif [[ "$*" == *"list --format json"* ]]; then
  echo '[]'
else
  exit 9
fi
"#,
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = directory.path().join("development-mux.sock");
    let output = Command::new(env!("CARGO_BIN_EXE_panetone"))
        .args([
            "doctor",
            "--socket",
            directory.path().join("control.sock").to_str().unwrap(),
            "--journal",
            directory.path().join("state.sqlite3").to_str().unwrap(),
            "--wakterm-bin",
            binary.to_str().unwrap(),
            "--wakterm-socket",
            socket.to_str().unwrap(),
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
    assert_eq!(report["mode"], "adapter_preflight");
    assert_eq!(
        report["checks"]["wakterm_contract"]["version"],
        "wakterm test-version-1"
    );
    assert_eq!(
        report["checks"]["wakterm_contract"]["general_event_consumer"],
        false
    );
}
