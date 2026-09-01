use std::future::Future;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::pin::Pin;
use std::sync::Arc;

use panetone::control::{
    CONTROL_SCHEMA, ControlHandler, ControlRequest, ControlResponse, ControlServer,
    ControlServerError, error_response, request, success_response,
};
use serde_json::json;
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::sync::watch;
use uuid::Uuid;

struct Echo;

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
    let listener = std::os::unix::net::UnixListener::bind(&stale).unwrap();
    drop(listener);
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
