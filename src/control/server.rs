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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlAuthority {
    Host,
    RestrictedLocal { peer_pid: u32 },
}

impl ControlAuthority {
    fn permits(self, method: &str) -> bool {
        match self {
            Self::Host => true,
            Self::RestrictedLocal { .. } => {
                matches!(method, "status" | "route.inspect" | "output.disposition")
            }
        }
    }

    fn denial(self, request: &ControlRequest) -> ControlResponse {
        let Self::RestrictedLocal { peer_pid } = self else {
            unreachable!("host authority permits every control request")
        };
        error_response(
            request.id,
            "permission_denied",
            format!(
                "local client PID {peer_pid} is confined by a different OS sandbox; this control method is denied"
            ),
            None,
        )
    }
}

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
    let Some(authority) = connection_authority(&stream)? else {
        return Ok(());
    };
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
            Ok(request) if authority.permits(&request.method) => handler.handle(request).await,
            Ok(request) => authority.denial(&request),
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NamespaceIdentity {
    user: (u64, u64),
    mount: (u64, u64),
    pid: (u64, u64),
    ipc: (u64, u64),
}

#[cfg(target_os = "linux")]
fn namespace_identity(pid: &str) -> std::io::Result<NamespaceIdentity> {
    fn identity(path: impl AsRef<Path>) -> std::io::Result<(u64, u64)> {
        let metadata = std::fs::metadata(path)?;
        Ok((metadata.dev(), metadata.ino()))
    }

    let root = format!("/proc/{pid}/ns");
    Ok(NamespaceIdentity {
        user: identity(format!("{root}/user"))?,
        mount: identity(format!("{root}/mnt"))?,
        pid: identity(format!("{root}/pid"))?,
        ipc: identity(format!("{root}/ipc"))?,
    })
}

#[cfg(target_os = "linux")]
fn pidfd_is_alive(pidfd: &std::os::fd::OwnedFd) -> std::io::Result<bool> {
    use std::os::fd::AsFd;

    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};

    let mut pollfds = [PollFd::new(pidfd.as_fd(), PollFlags::POLLIN)];
    poll(&mut pollfds, PollTimeout::ZERO)
        .map(|ready| ready == 0)
        .map_err(|error| std::io::Error::from_raw_os_error(error as i32))
}

#[cfg(target_os = "linux")]
fn classify_linux_peer(
    peer_pid: u32,
    peer_uid: u32,
    host_uid: u32,
    host_namespaces: Option<NamespaceIdentity>,
    peer_namespaces: Option<NamespaceIdentity>,
) -> Option<ControlAuthority> {
    if peer_uid != host_uid {
        None
    } else if host_namespaces == peer_namespaces && host_namespaces.is_some() {
        Some(ControlAuthority::Host)
    } else {
        Some(ControlAuthority::RestrictedLocal { peer_pid })
    }
}

#[cfg(target_os = "linux")]
fn connection_authority(stream: &UnixStream) -> std::io::Result<Option<ControlAuthority>> {
    use nix::sys::socket::{getsockopt, sockopt::PeerPidfd};

    let credentials = stream.peer_cred()?;
    let Some(peer_pid) = credentials.pid().and_then(|pid| u32::try_from(pid).ok()) else {
        return Ok(Some(ControlAuthority::RestrictedLocal { peer_pid: 0 }));
    };
    if credentials.uid() != unsafe_free_uid() {
        return Ok(None);
    }
    let pidfd = match getsockopt(stream, PeerPidfd) {
        Ok(pidfd) => pidfd,
        Err(_) => return Ok(Some(ControlAuthority::RestrictedLocal { peer_pid })),
    };
    if !pidfd_is_alive(&pidfd).unwrap_or(false) {
        return Ok(Some(ControlAuthority::RestrictedLocal { peer_pid }));
    }
    let peer_namespaces = namespace_identity(&peer_pid.to_string()).ok();
    if !pidfd_is_alive(&pidfd).unwrap_or(false) {
        return Ok(Some(ControlAuthority::RestrictedLocal { peer_pid }));
    }
    Ok(classify_linux_peer(
        peer_pid,
        credentials.uid(),
        unsafe_free_uid(),
        namespace_identity("self").ok(),
        peer_namespaces,
    ))
}

#[cfg(not(target_os = "linux"))]
fn connection_authority(_stream: &UnixStream) -> std::io::Result<Option<ControlAuthority>> {
    Ok(Some(ControlAuthority::Host))
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

    #[test]
    fn restricted_local_authority_allows_passive_methods_only() {
        let authority = ControlAuthority::RestrictedLocal { peer_pid: 42 };
        for method in ["status", "route.inspect", "output.disposition"] {
            assert!(authority.permits(method), "{method}");
        }
        for method in ["send", "route.ensure", "future.method"] {
            assert!(!authority.permits(method), "{method}");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_peer_requires_same_uid_and_kernel_namespaces() {
        let host = NamespaceIdentity {
            user: (1, 1),
            mount: (1, 2),
            pid: (1, 3),
            ipc: (1, 4),
        };
        assert_eq!(
            classify_linux_peer(42, 1000, 1000, Some(host), Some(host)),
            Some(ControlAuthority::Host)
        );
        assert_eq!(
            classify_linux_peer(42, 1001, 1000, Some(host), Some(host)),
            None
        );
        assert_eq!(
            classify_linux_peer(
                43,
                1000,
                1000,
                Some(host),
                Some(NamespaceIdentity {
                    mount: (2, 2),
                    ..host
                })
            ),
            Some(ControlAuthority::RestrictedLocal { peer_pid: 43 })
        );
        assert_eq!(
            classify_linux_peer(44, 1000, 1000, Some(host), None),
            Some(ControlAuthority::RestrictedLocal { peer_pid: 44 })
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn current_process_unix_peer_has_host_authority() {
        let (_client, server) = UnixStream::pair().unwrap();
        assert_eq!(
            connection_authority(&server).unwrap(),
            Some(ControlAuthority::Host)
        );
    }
}
