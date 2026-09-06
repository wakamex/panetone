use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::{Mutex, OnceCell};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::domain::{AdmissionReceipt, AgentBinding, EffectId};

use super::{AgentCatalog, ContractError, EventRead, EventRecord, join_catalog_binding};

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SteeringReceipt {
    agent_id: String,
    pane_id: u64,
    submitted: bool,
    acknowledgement: SteeringAcknowledgement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct SteeringAcknowledgement {
    acknowledged: bool,
}

impl SteeringReceipt {
    pub fn validate(&self, binding: &AgentBinding) -> Result<(), WaktermCliError> {
        if self.agent_id != binding.agent_id
            || binding.pane_id != Some(self.pane_id)
            || !self.submitted
        {
            return Err(WaktermCliError::InvalidSteeringReceipt);
        }
        Ok(())
    }

    pub fn acknowledged(&self) -> bool {
        self.acknowledgement.acknowledged
    }
}

#[derive(Clone, Debug)]
pub struct WaktermCli {
    binary: PathBuf,
    socket: PathBuf,
    deadline: Duration,
    capabilities: Arc<OnceCell<AgentApiCapabilities>>,
    event_follower: Arc<Mutex<Option<EventFollower>>>,
}

#[derive(Debug)]
struct EventFollower {
    after_sequence: u64,
    limit: u32,
    _child: Child,
    lines: Lines<BufReader<ChildStdout>>,
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
    #[error("Wakterm steering receipt does not match the requested agent pane")]
    InvalidSteeringReceipt,
    #[error("no live Wakterm agent pane matches route title {0:?}")]
    RouteNotFound(String),
    #[error("live Wakterm route {0:?} has no agent pane")]
    RouteUnavailable(String),
    #[error("Wakterm Agent API event page violates the v1 contract: {0}")]
    InvalidEventPage(&'static str),
    #[error(transparent)]
    Contract(#[from] ContractError),
}

#[derive(Deserialize)]
struct WireReceipt {
    schema: String,
    #[serde(flatten)]
    receipt: AdmissionReceipt,
}

#[derive(Deserialize)]
struct LivePane {
    pane_id: u64,
    effective_title: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveRoute {
    pub title: String,
    pub agents: Vec<AgentBinding>,
}

impl LiveRoute {
    pub fn select(&self, preferred: Option<&AgentBinding>) -> Option<AgentBinding> {
        let preferred = preferred.and_then(|preferred| {
            self.agents
                .iter()
                .find(|agent| {
                    agent.agent_id == preferred.agent_id
                        && agent.incarnation_id == preferred.incarnation_id
                })
                .or_else(|| {
                    self.agents
                        .iter()
                        .find(|agent| agent.agent_id == preferred.agent_id)
                })
                .or_else(|| {
                    preferred.pane_id.and_then(|pane_id| {
                        self.agents
                            .iter()
                            .find(|agent| agent.pane_id == Some(pane_id))
                    })
                })
        });
        preferred.or_else(|| self.agents.first()).cloned()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveRouteSnapshot {
    routes: Vec<LiveRoute>,
}

impl LiveRouteSnapshot {
    pub fn routes(&self) -> &[LiveRoute] {
        &self.routes
    }

    pub fn route(&self, title: &str) -> Result<&LiveRoute, WaktermCliError> {
        let route = self
            .routes
            .iter()
            .find(|route| route.title.eq_ignore_ascii_case(title))
            .ok_or_else(|| WaktermCliError::RouteNotFound(title.into()))?;
        if route.agents.is_empty() {
            Err(WaktermCliError::RouteUnavailable(title.into()))
        } else {
            Ok(route)
        }
    }

    pub fn resolve(
        &self,
        title: &str,
        preferred: Option<&AgentBinding>,
    ) -> Result<AgentBinding, WaktermCliError> {
        self.route(title)?
            .select(preferred)
            .ok_or_else(|| WaktermCliError::RouteUnavailable(title.into()))
    }
}

#[derive(Deserialize)]
struct WireEventPage {
    schema: String,
    status: String,
    requested_after_sequence: u64,
    oldest_available_sequence: u64,
    latest_sequence: u64,
    next_after_sequence: Option<u64>,
    #[serde(default)]
    events: Vec<EventRecord>,
    recovery: Option<WireEventRecovery>,
}

#[derive(Deserialize)]
struct WireEventRecovery {
    kind: String,
    catalog_as_of_sequence: u64,
}

impl WaktermCli {
    pub fn new(binary: impl Into<PathBuf>, socket: impl Into<PathBuf>, deadline: Duration) -> Self {
        Self {
            binary: binary.into(),
            socket: socket.into(),
            deadline,
            capabilities: Arc::new(OnceCell::new()),
            event_follower: Arc::new(Mutex::new(None)),
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
        self.capabilities
            .get_or_try_init(|| async {
                let capabilities: AgentApiCapabilities =
                    self.run_json(&["agent", "capabilities"], None).await?;
                capabilities.validate_current()?;
                Ok(capabilities)
            })
            .await
            .cloned()
    }

    pub async fn catalog(&self) -> Result<AgentCatalog, WaktermCliError> {
        self.capabilities().await?;
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

    pub async fn live_routes(&self) -> Result<LiveRouteSnapshot, WaktermCliError> {
        let before = self.catalog().await?;
        let panes: Vec<LivePane> = self.run_json(&["list", "--format", "json"], None).await?;
        let after = self.catalog().await?;
        let mut grouped = BTreeMap::<String, LiveRoute>::new();
        for pane in panes {
            if pane.effective_title.trim().is_empty() {
                continue;
            }
            let route = grouped
                .entry(pane.effective_title.to_ascii_lowercase())
                .or_insert_with(|| LiveRoute {
                    title: pane.effective_title,
                    agents: Vec::new(),
                });
            match join_catalog_binding(pane.pane_id, &before, &after) {
                Ok(binding)
                    if !route.agents.iter().any(|agent| {
                        agent.agent_id == binding.agent_id
                            && agent.incarnation_id == binding.incarnation_id
                    }) =>
                {
                    route.agents.push(binding);
                }
                Ok(_) => {}
                Err(
                    ContractError::MissingPane(_)
                    | ContractError::MissingIncarnation(_)
                    | ContractError::UnstableCatalog,
                ) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let routes = grouped
            .into_values()
            .map(|mut route| {
                route.agents.sort_by_key(|agent| agent.pane_id);
                route
            })
            .collect();
        Ok(LiveRouteSnapshot { routes })
    }

    pub async fn resolve_route_binding(
        &self,
        route_title: &str,
        preferred: Option<&AgentBinding>,
    ) -> Result<AgentBinding, WaktermCliError> {
        self.live_routes().await?.resolve(route_title, preferred)
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
        self.capabilities().await?;
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

    pub async fn steer(
        &self,
        binding: &AgentBinding,
        prompt: &str,
    ) -> Result<SteeringReceipt, WaktermCliError> {
        if prompt.len() > MAX_PROMPT_BYTES {
            return Err(WaktermCliError::InputTooLarge(MAX_PROMPT_BYTES));
        }
        self.capabilities().await?;
        let receipt: SteeringReceipt = self
            .run_json(
                &["agent", "send", binding.agent_id.as_str()],
                Some(prompt.as_bytes()),
            )
            .await?;
        receipt.validate(binding)?;
        Ok(receipt)
    }

    pub async fn terminal_events(
        &self,
        after_sequence: u64,
    ) -> Result<Vec<ReturnTerminal>, WaktermCliError> {
        self.capabilities().await?;
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

    pub async fn event_page(
        &self,
        after_sequence: u64,
        limit: u32,
    ) -> Result<EventRead, WaktermCliError> {
        let capabilities = self.capabilities().await?;
        if !capabilities.general_event_consumer_enabled() {
            return Ok(EventRead::Unsupported);
        }
        let limit = limit.clamp(1, 1000);
        let mut follower = self.event_follower.lock().await;
        for _ in 0..2 {
            let needs_follower = follower
                .as_ref()
                .map(|follower| {
                    follower.after_sequence != after_sequence || follower.limit != limit
                })
                .unwrap_or(true);
            if needs_follower {
                *follower = Some(self.spawn_event_follower(after_sequence, limit)?);
            }

            let line = timeout(
                self.deadline,
                follower
                    .as_mut()
                    .expect("event follower was initialized")
                    .lines
                    .next_line(),
            )
            .await
            .map_err(|_| WaktermCliError::Timeout)??;
            let Some(line) = line else {
                *follower = None;
                continue;
            };
            if line.len() > MAX_OUTPUT_BYTES {
                *follower = None;
                return Err(WaktermCliError::OutputTooLarge(MAX_OUTPUT_BYTES));
            }
            let page: WireEventPage = match serde_json::from_str(&line) {
                Ok(page) => page,
                Err(error) => {
                    *follower = None;
                    return Err(error.into());
                }
            };
            let read = match parse_event_page(page, after_sequence) {
                Ok(read) => read,
                Err(error) => {
                    *follower = None;
                    return Err(error);
                }
            };
            match &read {
                EventRead::Events {
                    next_after_sequence,
                    ..
                } => {
                    follower
                        .as_mut()
                        .expect("event follower remains live")
                        .after_sequence = *next_after_sequence;
                }
                EventRead::CursorTooOld { .. } | EventRead::Unsupported => {
                    *follower = None;
                }
            }
            return Ok(read);
        }
        Err(WaktermCliError::Rejected(
            "Wakterm event follower exited before producing a page".into(),
        ))
    }

    fn spawn_event_follower(
        &self,
        after_sequence: u64,
        limit: u32,
    ) -> Result<EventFollower, WaktermCliError> {
        let wait_ms = (self.deadline.as_millis() / 2).max(1).to_string();
        let mut command = Command::new(&self.binary);
        command
            .arg("--skip-config")
            .args(["cli", "--prefer-mux", "--no-auto-start"])
            .args([
                "agent",
                "events",
                "--after",
                &after_sequence.to_string(),
                "--limit",
                &limit.to_string(),
                "--follow",
                "--wait-ms",
                &wait_ms,
            ])
            .env("WAKTERM_UNIX_SOCKET", &self.socket)
            .env_remove("WAKTERM_CONFIG_DIR")
            .env_remove("WAKTERM_CONFIG_FILE")
            .env_remove("WAKTERM_PANE")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().expect("Wakterm stdout was piped");
        Ok(EventFollower {
            after_sequence,
            limit,
            _child: child,
            lines: BufReader::new(stdout).lines(),
        })
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

fn parse_event_page(
    page: WireEventPage,
    after_sequence: u64,
) -> Result<EventRead, WaktermCliError> {
    if page.schema != "wakterm.agent-events.v1"
        || page.requested_after_sequence != after_sequence
        || page.oldest_available_sequence > page.latest_sequence.saturating_add(1)
    {
        return Err(WaktermCliError::InvalidEventPage(
            "schema, request cursor, or retention bounds are invalid",
        ));
    }
    match page.status.as_str() {
        "cursor_too_old" if page.events.is_empty() && page.next_after_sequence.is_none() => {
            let recovery = page.recovery.ok_or(WaktermCliError::InvalidEventPage(
                "cursor gap has no catalog-snapshot recovery",
            ))?;
            if recovery.kind != "catalog_snapshot" {
                return Err(WaktermCliError::InvalidEventPage(
                    "cursor gap has an unknown recovery kind",
                ));
            }
            Ok(EventRead::CursorTooOld {
                requested_after_sequence: after_sequence,
                oldest_available_sequence: page.oldest_available_sequence,
                latest_sequence: page.latest_sequence,
                catalog_as_of_sequence: recovery.catalog_as_of_sequence,
            })
        }
        "ok" => {
            validate_live_events(after_sequence, page.latest_sequence, &page.events)?;
            let expected_next = page
                .events
                .last()
                .map_or(after_sequence, |event| event.sequence);
            if page.next_after_sequence != Some(expected_next) {
                return Err(WaktermCliError::InvalidEventPage(
                    "next cursor does not equal the last returned event sequence",
                ));
            }
            if expected_next < page.latest_sequence && page.events.is_empty() {
                return Err(WaktermCliError::InvalidEventPage(
                    "an empty page did not reach the advertised stream head",
                ));
            }
            Ok(EventRead::Events {
                events: page.events,
                next_after_sequence: expected_next,
                latest_sequence: page.latest_sequence,
            })
        }
        _ => Err(WaktermCliError::InvalidEventPage(
            "event page status or payload is invalid",
        )),
    }
}

fn validate_live_events(
    after_sequence: u64,
    latest_sequence: u64,
    events: &[EventRecord],
) -> Result<(), WaktermCliError> {
    let mut previous = after_sequence;
    for event in events {
        if event.sequence <= previous
            || event.sequence > latest_sequence
            || event.event_id.is_empty()
            || event.agent_id.is_empty()
            || event.incarnation_id.is_empty()
            || !matches!(
                event.kind.as_str(),
                "agent_lifecycle"
                    | "turn_started"
                    | "turn_state_changed"
                    | "plan"
                    | "assistant_message"
                    | "observer_failure"
                    | "turn_final"
            )
        {
            return Err(WaktermCliError::InvalidEventPage(
                "events are unordered or contain an invalid required field",
            ));
        }
        if matches!(event.kind.as_str(), "plan" | "assistant_message")
            && event
                .fields
                .get("text")
                .and_then(serde_json::Value::as_str)
                .is_none()
        {
            return Err(WaktermCliError::InvalidEventPage(
                "visible output event has no text",
            ));
        }
        previous = event.sequence;
    }
    Ok(())
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
