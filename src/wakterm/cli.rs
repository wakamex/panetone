use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::domain::{AdmissionReceipt, AgentBinding, EffectId};

use super::{AgentCatalog, ContractError, join_catalog_binding};

const AGENT_API_SCHEMA: &str = "wakterm.agent-api.v1";
const MAX_PROMPT_BYTES: usize = 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct AgentApiCapabilities {
    pub schema: String,
    pub api_major: u32,
    pub capabilities: BTreeSet<String>,
}

impl AgentApiCapabilities {
    pub fn validate_current(&self) -> Result<(), WaktermCliError> {
        if self.schema != AGENT_API_SCHEMA || self.api_major != 1 {
            return Err(WaktermCliError::IncompatibleApi {
                schema: self.schema.clone(),
                major: self.api_major,
            });
        }
        for required in [
            "catalog.v1",
            "prompt_admission.v1",
            "return_request_terminal_stream.v1",
        ] {
            if !self.capabilities.contains(required) {
                return Err(WaktermCliError::MissingCapability(required));
            }
        }
        Ok(())
    }

    pub fn general_event_consumer_enabled(&self) -> bool {
        self.capabilities.contains("event_stream.v1")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ReturnTerminal {
    pub request_id: String,
    pub target_agent_id: String,
    pub state: String,
    pub final_message: Option<String>,
    pub detail: Option<String>,
    pub terminal_event_sequence: u64,
}

#[derive(Clone, Debug)]
pub struct WaktermCli {
    binary: PathBuf,
    socket: PathBuf,
    deadline: Duration,
}

#[derive(Debug, Error)]
pub enum WaktermCliError {
    #[error("Wakterm CLI input exceeds the {0}-byte limit")]
    InputTooLarge(usize),
    #[error("Wakterm CLI output exceeds the {0}-byte limit")]
    OutputTooLarge(usize),
    #[error("Wakterm CLI operation exceeded its deadline")]
    Timeout,
    #[error("failed to start or communicate with Wakterm CLI: {0}")]
    Io(#[from] std::io::Error),
    #[error("Wakterm CLI task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("Wakterm CLI rejected the operation: {0}")]
    Rejected(String),
    #[error("Wakterm CLI returned malformed JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("incompatible Wakterm Agent API schema {schema:?} major {major}")]
    IncompatibleApi { schema: String, major: u32 },
    #[error("Wakterm Agent API lacks required capability {0}")]
    MissingCapability(&'static str),
    #[error("Wakterm Agent API response has an unexpected schema")]
    UnexpectedSchema,
    #[error(transparent)]
    Contract(#[from] ContractError),
}

#[derive(Deserialize)]
struct WireReceipt {
    schema: String,
    #[serde(flatten)]
    receipt: AdmissionReceipt,
}

impl WaktermCli {
    pub fn new(binary: impl Into<PathBuf>, socket: impl Into<PathBuf>, deadline: Duration) -> Self {
        Self {
            binary: binary.into(),
            socket: socket.into(),
            deadline,
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub async fn version(&self) -> Result<String, WaktermCliError> {
        let output = self.run_command(&["--version"], None, false).await?;
        let version = String::from_utf8(output)
            .map_err(|_| WaktermCliError::Rejected("version output was not UTF-8".into()))?;
        let version = version.trim();
        if version.is_empty() {
            Err(WaktermCliError::Rejected("version output was empty".into()))
        } else {
            Ok(version.into())
        }
    }

    pub async fn capabilities(&self) -> Result<AgentApiCapabilities, WaktermCliError> {
        let capabilities: AgentApiCapabilities =
            self.run_json(&["agent", "capabilities"], None).await?;
        capabilities.validate_current()?;
        Ok(capabilities)
    }

    pub async fn catalog(&self) -> Result<AgentCatalog, WaktermCliError> {
        let catalog: AgentCatalog = self.run_json(&["agent", "catalog"], None).await?;
        if catalog.schema != AGENT_API_SCHEMA {
            return Err(WaktermCliError::UnexpectedSchema);
        }
        Ok(catalog)
    }

    pub async fn resolve_stable_binding(
        &self,
        pane_id: u64,
        observe_route: impl FnOnce() -> Result<(), WaktermCliError>,
    ) -> Result<AgentBinding, WaktermCliError> {
        let before = self.catalog().await?;
        observe_route()?;
        let after = self.catalog().await?;
        Ok(join_catalog_binding(pane_id, &before, &after)?)
    }

    pub async fn admit(
        &self,
        request_id: EffectId,
        binding: &AgentBinding,
        prompt: &str,
        return_final: bool,
        final_timeout_ms: u64,
    ) -> Result<AdmissionReceipt, WaktermCliError> {
        if prompt.len() > MAX_PROMPT_BYTES {
            return Err(WaktermCliError::InputTooLarge(MAX_PROMPT_BYTES));
        }
        let request_id = request_id.to_string();
        let mut owned = vec![
            "agent".to_owned(),
            "admit".to_owned(),
            binding.agent_id.clone(),
            "--exact-agent-id".to_owned(),
            "--incarnation".to_owned(),
            binding.incarnation_id.clone(),
            "--request-id".to_owned(),
            request_id,
        ];
        if return_final {
            owned.push("--return-final".to_owned());
            owned.push("--final-timeout-ms".to_owned());
            owned.push(final_timeout_ms.to_string());
        }
        let borrowed = owned.iter().map(String::as_str).collect::<Vec<_>>();
        let wire: WireReceipt = self.run_json(&borrowed, Some(prompt.as_bytes())).await?;
        if wire.schema != AGENT_API_SCHEMA {
            return Err(WaktermCliError::UnexpectedSchema);
        }
        Ok(wire.receipt)
    }

    pub async fn terminal_events(
        &self,
        after_sequence: u64,
    ) -> Result<Vec<ReturnTerminal>, WaktermCliError> {
        let after = after_sequence.to_string();
        let output = self
            .run(
                &["agent", "request", "watch", "--after", &after, "--once"],
                None,
            )
            .await?;
        output
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
            .map(serde_json::from_slice)
            .collect::<Result<Vec<_>, _>>()
            .map_err(WaktermCliError::from)
    }

    async fn run_json<T: DeserializeOwned>(
        &self,
        args: &[&str],
        input: Option<&[u8]>,
    ) -> Result<T, WaktermCliError> {
        let output = self.run(args, input).await?;
        Ok(serde_json::from_slice(&output)?)
    }

    async fn run(&self, args: &[&str], input: Option<&[u8]>) -> Result<Vec<u8>, WaktermCliError> {
        self.run_command(args, input, true).await
    }

    async fn run_command(
        &self,
        args: &[&str],
        input: Option<&[u8]>,
        cli_mode: bool,
    ) -> Result<Vec<u8>, WaktermCliError> {
        let mut command = Command::new(&self.binary);
        command.arg("--skip-config");
        if cli_mode {
            command.args(["cli", "--prefer-mux", "--no-auto-start"]);
        }
        command
            .args(args)
            .env("WAKTERM_UNIX_SOCKET", &self.socket)
            .env_remove("WAKTERM_CONFIG_DIR")
            .env_remove("WAKTERM_CONFIG_FILE")
            .env_remove("WAKTERM_PANE")
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let stdout = bounded_read(child.stdout.take().expect("Wakterm stdout was piped"));
        let stderr = bounded_read(child.stderr.take().expect("Wakterm stderr was piped"));
        let stdout_task = tokio::spawn(stdout);
        let stderr_task = tokio::spawn(stderr);

        let operation = async {
            if let Some(input) = input {
                let mut stdin = child.stdin.take().expect("Wakterm stdin was piped");
                stdin.write_all(input).await?;
                stdin.shutdown().await?;
            }
            child.wait().await
        };
        let status = match timeout(self.deadline, operation).await {
            Ok(status) => status?,
            Err(_) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                stdout_task.abort();
                stderr_task.abort();
                let _ = stdout_task.await;
                let _ = stderr_task.await;
                return Err(WaktermCliError::Timeout);
            }
        };
        let stdout = task_output(stdout_task).await?;
        let stderr = task_output(stderr_task).await?;
        if !status.success() {
            return Err(WaktermCliError::Rejected(safe_detail(&stderr)));
        }
        Ok(stdout)
    }
}

async fn bounded_read(
    stream: impl AsyncRead + Unpin + Send + 'static,
) -> Result<Vec<u8>, std::io::Error> {
    let mut output = Vec::new();
    stream
        .take((MAX_OUTPUT_BYTES + 1) as u64)
        .read_to_end(&mut output)
        .await?;
    Ok(output)
}

async fn task_output(
    task: JoinHandle<Result<Vec<u8>, std::io::Error>>,
) -> Result<Vec<u8>, WaktermCliError> {
    let output = task.await??;
    if output.len() > MAX_OUTPUT_BYTES {
        return Err(WaktermCliError::OutputTooLarge(MAX_OUTPUT_BYTES));
    }
    Ok(output)
}

fn safe_detail(stderr: &[u8]) -> String {
    let detail = String::from_utf8_lossy(stderr);
    let detail = detail
        .chars()
        .map(|character| {
            if character.is_control() && !matches!(character, '\n' | '\t') {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    let detail = detail.trim();
    if detail.is_empty() {
        "Wakterm exited unsuccessfully without diagnostic output".into()
    } else {
        detail.into()
    }
}
