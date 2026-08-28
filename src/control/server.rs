use std::future::Future;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, watch};

use super::{ControlRequest, ControlResponse, error_response};

const MAX_REQUEST_BYTES: u64 = 256 * 1024;
const CONNECTION_LIMIT: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

pub trait ControlHandler: Send + Sync + 'static {
    fn handle(
        &self,
        request: ControlRequest,
    ) -> Pin<Box<dyn Future<Output = ControlResponse> + Send + '_>>;
}

#[derive(Debug, Error)]
pub enum ControlServerError {
    #[error("control socket error: {0}")]
    Io(#[from] std::io::Error),
    #[error("control socket path is a symbolic link or regular file")]
    UnsafePath,
    #[error("another Panetone control server is already listening")]
    Active,
    #[error("control runtime directory is not private")]
    UnsafeDirectory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SocketIdentity {
    pub device: u64,
    pub inode: u64,
}

pub struct ControlServer {
    listener: UnixListener,
    path: PathBuf,
    identity: SocketIdentity,
}

impl ControlServer {
    pub async fn bind(path: impl Into<PathBuf>) -> Result<Self, ControlServerError> {
        let path = path.into();
        prepare_parent(&path)?;
        prepare_socket_path(&path).await?;
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let metadata = std::fs::symlink_metadata(&path)?;
        Ok(Self {
            listener,
            path,
            identity: SocketIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            },
        })
    }

    pub async fn run(
        self,
        handler: Arc<dyn ControlHandler>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ControlServerError> {
        let semaphore = Arc::new(Semaphore::new(CONNECTION_LIMIT));
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted?;
                    let permit = semaphore.clone().acquire_owned().await
                        .expect("control connection semaphore remains open");
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = serve_connection(stream, handler, IO_TIMEOUT).await;
                    });
                }
            }
        }
        self.cleanup();
        Ok(())
    }

    fn cleanup(&self) {
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path)
            && metadata.dev() == self.identity.device
            && metadata.ino() == self.identity.inode
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn prepare_parent(path: &Path) -> Result<(), ControlServerError> {
    let parent = path.parent().ok_or(ControlServerError::UnsafeDirectory)?;
    if !parent.exists() {
        std::fs::create_dir_all(parent)?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    let metadata = std::fs::symlink_metadata(parent)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() || metadata.mode() & 0o077 != 0 {
        return Err(ControlServerError::UnsafeDirectory);
    }
    Ok(())
}

async fn prepare_socket_path(path: &Path) -> Result<(), ControlServerError> {
    let Ok(before) = std::fs::symlink_metadata(path) else {
        return Ok(());
    };
    if !before.file_type().is_socket() {
        return Err(ControlServerError::UnsafePath);
    }
    if UnixStream::connect(path).await.is_ok() {
        return Err(ControlServerError::Active);
    }
    let current = std::fs::symlink_metadata(path)?;
    if current.dev() != before.dev() || current.ino() != before.ino() {
        return Err(ControlServerError::Active);
    }
    std::fs::remove_file(path)?;
    Ok(())
}

async fn serve_connection(
    stream: UnixStream,
    handler: Arc<dyn ControlHandler>,
    io_timeout: Duration,
) -> Result<(), std::io::Error> {
    #[cfg(target_os = "linux")]
    if stream.peer_cred()?.uid() != unsafe_free_uid() {
        return Ok(());
    }
    let (read_half, mut write_half) = stream.into_split();
    let mut bytes = Vec::new();
    let mut reader = BufReader::new(read_half).take(MAX_REQUEST_BYTES + 1);
    tokio::time::timeout(io_timeout, reader.read_until(b'\n', &mut bytes))
        .await
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))??;
    let response = if bytes.len() as u64 > MAX_REQUEST_BYTES {
        error_response(
            uuid::Uuid::nil(),
            "request_too_large",
            "control requests are limited to 256 KiB",
            None,
        )
    } else {
        match serde_json::from_slice::<ControlRequest>(&bytes) {
            Ok(request) => handler.handle(request).await,
            Err(_) => error_response(
                uuid::Uuid::nil(),
                "invalid_request",
                "request must be one newline-terminated control v1 JSON object",
                None,
            ),
        }
    };
    let mut encoded = serde_json::to_vec(&response).map_err(std::io::Error::other)?;
    encoded.push(b'\n');
    tokio::time::timeout(io_timeout, async {
        write_half.write_all(&encoded).await?;
        write_half.shutdown().await
    })
    .await
    .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))?
}

#[cfg(target_os = "linux")]
fn unsafe_free_uid() -> u32 {
    std::fs::metadata("/proc/self")
        .map(|metadata| metadata.uid())
        .unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::{CONTROL_SCHEMA, success_response};
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    struct SlowHandler;

    impl ControlHandler for SlowHandler {
        fn handle(
            &self,
            request: ControlRequest,
        ) -> Pin<Box<dyn Future<Output = ControlResponse> + Send + '_>> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(25)).await;
                success_response(request.id, json!({"handled": true}))
            })
        }
    }

    #[tokio::test]
    async fn handler_runtime_is_not_limited_by_socket_io_timeout() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let task = tokio::spawn(serve_connection(
            server,
            Arc::new(SlowHandler),
            Duration::from_millis(5),
        ));
        let request = ControlRequest {
            schema: CONTROL_SCHEMA.into(),
            id: uuid::Uuid::new_v4(),
            method: "slow".into(),
            params: json!({}),
        };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        client.write_all(&encoded).await.unwrap();

        let mut line = String::new();
        BufReader::new(client).read_line(&mut line).await.unwrap();
        let response: ControlResponse = serde_json::from_str(&line).unwrap();
        assert_eq!(response.result, Some(json!({"handled": true})));
        task.await.unwrap().unwrap();
    }
}
