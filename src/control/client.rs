use std::path::Path;

use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::{ControlRequest, ControlResponse};

const MAX_RESPONSE_BYTES: u64 = 256 * 1024;

#[derive(Debug, Error)]
pub enum ControlClientError {
    #[error("control socket error: {0}")]
    Io(#[from] std::io::Error),
    #[error("control protocol error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("control response exceeded 256 KiB")]
    TooLarge,
    #[error("control server closed without a response")]
    Empty,
}

pub async fn request(
    socket: &Path,
    request: &ControlRequest,
) -> Result<ControlResponse, ControlClientError> {
    let mut stream = UnixStream::connect(socket).await?;
    let mut bytes = serde_json::to_vec(request)?;
    bytes.push(b'\n');
    stream.write_all(&bytes).await?;
    stream.shutdown().await?;
    let mut response = Vec::new();
    let mut reader = BufReader::new(stream).take(MAX_RESPONSE_BYTES + 1);
    reader.read_until(b'\n', &mut response).await?;
    if response.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(ControlClientError::TooLarge);
    }
    if response.is_empty() {
        return Err(ControlClientError::Empty);
    }
    Ok(serde_json::from_slice(&response)?)
}
