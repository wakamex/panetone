use std::future::Future;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::pin::Pin;
use std::sync::Arc;

use panetone::control::{
    CONTROL_SCHEMA, ControlHandler, ControlRequest, ControlResponse, ControlServer,
    ControlServerError, error_response, request, success_response,
};
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::sync::watch;
use uuid::Uuid;

struct Echo;

#[tokio::test]
async fn send_help_explains_queue_steer_and_asynchronous_return_modes() {
    let output = Command::new(env!("CARGO_BIN_EXE_panetone"))
        .args(["send", "--help"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Request an asynchronous final callback"));
    assert!(stdout.contains("this command exits after admission"));
    assert!(stdout.contains("Steer an active turn immediately"));
    assert!(stdout.contains("starts a normal turn when the target is idle"));

    let incompatible = Command::new(env!("CARGO_BIN_EXE_panetone"))
        .args([
            "send",
            "--from",
            "source",
            "--to",
            "target",
            "--steer",
            "--return-final",
            "message",
        ])
        .output()
        .await
        .unwrap();
    assert!(!incompatible.status.success());
    assert!(
        String::from_utf8(incompatible.stderr)
            .unwrap()
            .contains("cannot be used with")
    );
}

impl ControlHandler for Echo {
    fn handle(
        &self,
        request: ControlRequest,
    ) -> Pin<Box<dyn Future<Output = ControlResponse> + Send + '_>> {
        Box::pin(async move {
            if request.schema == CONTROL_SCHEMA {
                success_response(request.id, request.params)
            } else {
                error_response(request.id, "unsupported_schema", "unsupported", None)
            }
        })
    }
}

fn private_directory() -> tempfile::TempDir {
    let directory = tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

#[tokio::test]
async fn server_and_client_exchange_one_bounded_v1_frame() {
    let directory = private_directory();
    let socket = directory.path().join("control.sock");
    let server = ControlServer::bind(&socket).await.unwrap();
    assert_eq!(
        std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(server.run(Arc::new(Echo), receiver));
    let id = Uuid::new_v4();
    let response = request(
        &socket,
        &ControlRequest {
            schema: CONTROL_SCHEMA.into(),
            id,
            method: "echo".into(),
            params: json!({"hello": "world"}),
        },
    )
    .await
    .unwrap();
    assert_eq!(response.result, Some(json!({"hello": "world"})));
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
    assert!(!socket.exists());
}

#[tokio::test]
async fn client_socket_precedence_is_flag_then_environment_then_runtime_directory() {
    let directory = private_directory();
    let runtime = directory.path().join("runtime");
    let socket_directory = runtime.join("panetone");
    std::fs::create_dir_all(&socket_directory).unwrap();
    std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::set_permissions(&socket_directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = socket_directory.join("control.sock");
    let server = ControlServer::bind(&socket).await.unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(server.run(Arc::new(Echo), receiver));
    let binary = env!("CARGO_BIN_EXE_panetone");

    let defaulted = Command::new(binary)
        .args(["status", "--json"])
        .env("XDG_RUNTIME_DIR", &runtime)
        .env_remove("PANETONE_CONTROL_SOCKET")
        .output()
        .await
        .unwrap();
    assert!(
        defaulted.status.success(),
        "runtime-directory default failed: {}",
        String::from_utf8_lossy(&defaulted.stderr)
    );

    let from_environment = Command::new(binary)
        .args(["status", "--json"])
        .env("XDG_RUNTIME_DIR", directory.path().join("wrong-runtime"))
        .env("PANETONE_CONTROL_SOCKET", &socket)
        .output()
        .await
        .unwrap();
    assert!(
        from_environment.status.success(),
        "environment override failed: {}",
        String::from_utf8_lossy(&from_environment.stderr)
    );

    let from_flag = Command::new(binary)
        .args(["status", "--socket", socket.to_str().unwrap(), "--json"])
        .env("XDG_RUNTIME_DIR", directory.path().join("wrong-runtime"))
        .env(
            "PANETONE_CONTROL_SOCKET",
            directory.path().join("wrong-control.sock"),
        )
        .output()
        .await
        .unwrap();
    assert!(
        from_flag.status.success(),
        "flag override failed: {}",
        String::from_utf8_lossy(&from_flag.stderr)
    );

    let missing_runtime = Command::new(binary)
        .args(["status", "--json"])
        .env_remove("XDG_RUNTIME_DIR")
        .env_remove("PANETONE_CONTROL_SOCKET")
        .output()
        .await
        .unwrap();
    assert!(!missing_runtime.status.success());
    assert!(
        String::from_utf8_lossy(&missing_runtime.stderr).contains("XDG_RUNTIME_DIR is not set")
    );

    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
}

fn write_caller_fake(directory: &std::path::Path) -> std::path::PathBuf {
    let path = directory.join("wakterm-caller-fake");
    std::fs::write(
        &path,
        r#"#!/bin/bash
set -euo pipefail
[[ "$*" == *"agent caller"* ]] || { echo "unexpected: $*" >&2; exit 92; }
if [[ "${CODEX_THREAD_ID-}" == "thread-a" ]]; then
  echo '{"schema":"wakterm.agent-api.v1","resolved_by":"codex_thread","agent":{"agent_id":"agent-a","incarnation_id":"inc-a","pane_id":17,"name":"a","harness":"codex","status":"idle","turn_state":"waiting_on_user","alive":true,"observed_at":"2026-09-30T00:00:00Z"}}'
else
  echo "neither WAKTERM_PANE nor CODEX_THREAD_ID identifies the caller" >&2
  exit 1
fi
"#,
    )
    .unwrap();
    panetone::test_support::seal_executable(&path);
    path
}

#[tokio::test]
async fn send_identifies_the_exact_calling_agent_through_wakterm_without_from() {
    let directory = private_directory();
    let socket = directory.path().join("control.sock");
    let server = ControlServer::bind(&socket).await.unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(server.run(Arc::new(Echo), receiver));
    let wakterm = write_caller_fake(directory.path());
    let send = |thread: Option<&str>, from: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_panetone"));
        command.args([
            "send",
            "--socket",
            socket.to_str().unwrap(),
            "--to",
            "target",
        ]);
        if let Some(from) = from {
            command.args(["--from", from]);
        }
        command
            .arg("do the work")
            .env("WAKTERM_BIN", &wakterm)
            .env_remove("WAKTERM_PANE")
            .env_remove("CODEX_THREAD_ID");
        if let Some(thread) = thread {
            command.env("CODEX_THREAD_ID", thread);
        }
        command.output()
    };

    let output = send(Some("thread-a"), None).await.unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response: ControlResponse = serde_json::from_slice(&output.stdout).unwrap();
    let params = response.result.unwrap();
    assert_eq!(
        params["source_agent"],
        json!({"agent_id": "agent-a", "incarnation_id": "inc-a"})
    );
    assert_eq!(params["source_pane_id"], Value::Null);
    assert_eq!(params["from"], Value::Null);

    let unidentified = send(None, None).await.unwrap();
    assert!(!unidentified.status.success());
    assert!(unidentified.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&unidentified.stderr);
    assert!(stderr.contains("--from ROUTE"), "{stderr}");
    assert!(
        stderr.contains("neither WAKTERM_PANE nor CODEX_THREAD_ID"),
        "{stderr}"
    );

    let routed = send(None, Some("source")).await.unwrap();
    assert!(routed.status.success());
    let response: ControlResponse = serde_json::from_slice(&routed.stdout).unwrap();
    let params = response.result.unwrap();
    assert_eq!(params["from"], "source");
    assert_eq!(params["source_agent"], Value::Null);

    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn malformed_and_oversized_frames_are_rejected_without_panicking() {
    let directory = private_directory();
    let socket = directory.path().join("control.sock");
    let server = ControlServer::bind(&socket).await.unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(server.run(Arc::new(Echo), receiver));

    let mut malformed = UnixStream::connect(&socket).await.unwrap();
    malformed.write_all(b"not json\n").await.unwrap();
    let mut line = String::new();
    BufReader::new(malformed)
        .read_line(&mut line)
        .await
        .unwrap();
    let response: ControlResponse = serde_json::from_str(&line).unwrap();
    assert_eq!(response.error.unwrap().code, "invalid_request");

    let mut oversized = UnixStream::connect(&socket).await.unwrap();
    oversized
        .write_all(&vec![b'x'; 256 * 1024 + 1])
        .await
        .unwrap();
    oversized.write_all(b"\n").await.unwrap();
    let mut line = String::new();
    BufReader::new(oversized)
        .read_line(&mut line)
        .await
        .unwrap();
    let response: ControlResponse = serde_json::from_str(&line).unwrap();
    assert_eq!(response.error.unwrap().code, "request_too_large");

    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn socket_startup_distinguishes_stale_live_and_unsafe_paths() {
    let directory = private_directory();
    let stale = directory.path().join("stale.sock");
    // A socket file nobody accepts stream connections on. A dropped stream
    // listener is not reliably stale here: a process spawned concurrently by
    // another test can inherit it between fork and exec and keep it listening.
    drop(std::os::unix::net::UnixDatagram::bind(&stale).unwrap());
    let server = ControlServer::bind(&stale).await.unwrap();
    drop(server);

    let live = directory.path().join("live.sock");
    let listener = std::os::unix::net::UnixListener::bind(&live).unwrap();
    assert!(matches!(
        ControlServer::bind(&live).await,
        Err(ControlServerError::Active)
    ));
    drop(listener);

    let regular = directory.path().join("regular.sock");
    std::fs::write(&regular, b"not a socket").unwrap();
    assert!(matches!(
        ControlServer::bind(&regular).await,
        Err(ControlServerError::UnsafePath)
    ));

    let target = directory.path().join("target");
    std::fs::write(&target, b"target").unwrap();
    let link = directory.path().join("link.sock");
    symlink(&target, &link).unwrap();
    assert!(matches!(
        ControlServer::bind(&link).await,
        Err(ControlServerError::UnsafePath)
    ));
}

#[tokio::test]
async fn shutdown_never_removes_a_replacement_inode() {
    let directory = private_directory();
    let socket = directory.path().join("control.sock");
    let server = ControlServer::bind(&socket).await.unwrap();
    std::fs::remove_file(&socket).unwrap();
    let replacement = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    drop(server);
    assert!(socket.exists());
    drop(replacement);
}

#[tokio::test]
async fn runtime_directory_must_not_be_group_or_world_accessible() {
    let directory = private_directory();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(
        ControlServer::bind(directory.path().join("control.sock")).await,
        Err(ControlServerError::UnsafeDirectory)
    ));
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires the installed Codex sandbox helper"]
async fn real_codex_workspace_sandbox_can_inspect_but_cannot_send() {
    let directory = private_directory();
    let socket = directory.path().join("control.sock");
    let server = ControlServer::bind(&socket).await.unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(server.run(Arc::new(Echo), receiver));
    let binary = env!("CARGO_BIN_EXE_panetone");
    let manifest = env!("CARGO_MANIFEST_DIR");

    let status = Command::new("codex")
        .args([
            "sandbox",
            "-P",
            "workspace-git",
            "-C",
            manifest,
            "--",
            binary,
            "status",
            "--socket",
            socket.to_str().unwrap(),
            "--json",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        status.status.success(),
        "sandboxed status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );

    let send = Command::new("codex")
        .args([
            "sandbox",
            "-P",
            "workspace-git",
            "-C",
            manifest,
            "--",
            binary,
            "send",
            "--socket",
            socket.to_str().unwrap(),
            "--from",
            "source",
            "--to",
            "target",
            "test",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        !send.status.success(),
        "sandboxed send unexpectedly succeeded"
    );
    let response: ControlResponse = serde_json::from_slice(&send.stdout).unwrap();
    assert_eq!(response.error.unwrap().code, "permission_denied");

    let host_send = Command::new(binary)
        .args([
            "send",
            "--socket",
            socket.to_str().unwrap(),
            "--from",
            "source",
            "--to",
            "target",
            "test",
        ])
        .output()
        .await
        .unwrap();
    assert!(host_send.status.success());

    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
}
