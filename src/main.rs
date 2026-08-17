use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use panetone::control::{CONTROL_SCHEMA, ControlRequest, ControlServer, SendParams, request};
use panetone::service::ConformanceService;
use panetone::store::StoreHandle;
use panetone::supervisor::{Supervisor, TaskPolicy};
use panetone::wakterm::{ProfileKind, WaktermCli, WaktermContract};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Parser)]
#[command(version, about = "Durable local message router for Wakterm agents")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the offline fake daemon used for Phase 2 validation.
    Daemon(DaemonArgs),
    /// Send one cross-agent message through the running daemon.
    Send(SendArgs),
    /// Inspect durable daemon health and backlogs.
    Status(StatusArgs),
    /// Check local paths and the offline store without sending messages.
    Doctor(DoctorArgs),
    /// Create or upgrade the offline database schema explicitly.
    Migrate(StoreArgs),
    #[command(hide = true)]
    ConformanceBackend(DaemonArgs),
}

#[derive(Clone, Args)]
struct DaemonArgs {
    #[arg(long)]
    socket: PathBuf,
    #[arg(long, alias = "database")]
    journal: PathBuf,
    #[arg(long)]
    effect_log: PathBuf,
    #[arg(
        long,
        default_value = "/code/wakterm/docs/agent-api/v1/golden-fixtures.json"
    )]
    wakterm_fixture: PathBuf,
    #[arg(long, value_enum, default_value_t = ProfileArg::Current)]
    profile: ProfileArg,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProfileArg {
    Current,
    FutureEvents,
}

impl From<ProfileArg> for ProfileKind {
    fn from(value: ProfileArg) -> Self {
        match value {
            ProfileArg::Current => ProfileKind::Current,
            ProfileArg::FutureEvents => ProfileKind::FutureEvents,
        }
    }
}

#[derive(Args)]
struct SendArgs {
    #[arg(long = "from")]
    source: String,
    #[arg(long = "to")]
    target: String,
    #[arg(long)]
    id: Option<Uuid>,
    #[arg(long)]
    return_final: bool,
    #[arg(long, default_value_t = 0)]
    timeout_ms: u64,
    #[arg(long, env = "PANETONE_CONTROL_SOCKET")]
    socket: PathBuf,
    message: String,
}

#[derive(Args)]
struct StatusArgs {
    #[arg(long, env = "PANETONE_CONTROL_SOCKET")]
    socket: PathBuf,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct DoctorArgs {
    #[arg(long)]
    socket: PathBuf,
    #[arg(long, alias = "database")]
    journal: PathBuf,
    #[arg(
        long,
        default_value = "/code/wakterm/docs/agent-api/v1/golden-fixtures.json"
    )]
    wakterm_fixture: PathBuf,
    #[arg(long, requires = "wakterm_socket")]
    wakterm_bin: Option<PathBuf>,
    #[arg(long, requires = "wakterm_bin")]
    wakterm_socket: Option<PathBuf>,
}

#[derive(Args)]
struct StoreArgs {
    #[arg(long, alias = "database")]
    journal: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Daemon(args) | Command::ConformanceBackend(args) => run_daemon(args).await,
        Command::Send(args) => run_send(args).await,
        Command::Status(args) => run_status(args).await,
        Command::Doctor(args) => run_doctor(args).await,
        Command::Migrate(args) => run_migrate(args).await,
    }
}

async fn run_daemon(args: DaemonArgs) -> Result<()> {
    let fixture = std::fs::read_to_string(&args.wakterm_fixture)
        .context("read pinned Wakterm Agent API fixture")?;
    let contract = WaktermContract::from_golden_json(&fixture, args.profile.into())
        .context("validate Wakterm Agent API fake profile")?;
    let store = StoreHandle::open(&args.journal).context("open offline store")?;
    let server = ControlServer::bind(&args.socket)
        .await
        .context("bind Panetone control socket")?;
    let mut supervisor = Supervisor::new();
    let handler = Arc::new(
        ConformanceService::new(store.clone(), args.effect_log).with_runtime_status(
            supervisor.handle(),
            match args.profile {
                ProfileArg::Current => "current",
                ProfileArg::FutureEvents => "future_events",
            },
            contract.capabilities.iter().cloned().collect(),
            args.socket.clone(),
        ),
    );
    let shutdown = supervisor.shutdown_receiver();
    supervisor.spawn("control", TaskPolicy::Critical, async move {
        server
            .run(handler, shutdown)
            .await
            .map_err(|error| error.to_string())
    });
    supervisor
        .run_until(shutdown_signal())
        .await
        .context("supervise offline daemon")?;
    store.shutdown().await.context("stop store owner")?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn run_send(args: SendArgs) -> Result<()> {
    let params = SendParams {
        source: args.source,
        target: args.target,
        message: args.message,
        return_final: args.return_final,
        timeout_ms: args.timeout_ms,
    };
    let response = request(
        &args.socket,
        &ControlRequest {
            schema: CONTROL_SCHEMA.into(),
            id: args.id.unwrap_or_else(Uuid::new_v4),
            method: "send".into(),
            params: serde_json::to_value(params)?,
        },
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    if response.ok {
        Ok(())
    } else {
        bail!("send was rejected")
    }
}

async fn run_status(args: StatusArgs) -> Result<()> {
    let response = request(
        &args.socket,
        &ControlRequest {
            schema: CONTROL_SCHEMA.into(),
            id: Uuid::new_v4(),
            method: "status".into(),
            params: Value::Null,
        },
    )
    .await?;
    if args.json {
        println!("{}", serde_json::to_string(&response)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&response)?);
    }
    if response.ok {
        Ok(())
    } else {
        bail!("status request failed")
    }
}

async fn run_doctor(args: DoctorArgs) -> Result<()> {
    let parent = args
        .socket
        .parent()
        .context("control socket requires a parent directory")?;
    let parent_check = private_directory_check(parent);
    let live_preflight = args.wakterm_bin.is_some();
    let wakterm_check = match (args.wakterm_bin.as_ref(), args.wakterm_socket.as_ref()) {
        (Some(binary), Some(socket)) => {
            let cli = WaktermCli::new(binary, socket, Duration::from_secs(5));
            match tokio::try_join!(cli.version(), cli.capabilities(), cli.catalog()) {
                Ok((version, capabilities, catalog)) => json!({
                    "ok": true,
                    "detail": "explicit Wakterm Agent API preflight succeeded",
                    "version": version,
                    "socket": socket,
                    "capabilities": capabilities.capabilities,
                    "general_event_consumer": capabilities.general_event_consumer_enabled(),
                    "catalog_agents": catalog.agents.len()
                }),
                Err(error) => json!({"ok": false, "detail": error.to_string()}),
            }
        }
        (None, None) => match std::fs::read_to_string(&args.wakterm_fixture) {
            Ok(fixture) => match (
                WaktermContract::from_golden_json(&fixture, ProfileKind::Current),
                WaktermContract::from_golden_json(&fixture, ProfileKind::FutureEvents),
            ) {
                (Ok(current), Ok(future)) => json!({
                    "ok": !current.general_event_consumer_enabled() && future.general_event_consumer_enabled(),
                    "detail": "current and fixture-only future profiles are compatible",
                    "current_capabilities": current.capabilities,
                    "future_capabilities": future.capabilities
                }),
                (Err(error), _) | (_, Err(error)) => {
                    json!({"ok": false, "detail": error.to_string()})
                }
            },
            Err(error) => json!({"ok": false, "detail": error.to_string()}),
        },
        _ => unreachable!("clap requires both explicit Wakterm arguments"),
    };
    let store = StoreHandle::open(&args.journal);
    let (store_check, opened) = match store {
        Ok(store) => match store.status().await {
            Ok(status) => {
                let degraded = status.failed_workflows > 0
                    || status.indeterminate_workflows > 0
                    || status.legacy_indeterminate_requests > 0
                    || status.legacy_unresolved_returns > 0
                    || status.failed_outbox > 0
                    || status.indeterminate_outbox > 0
                    || status.unresolved_returns > 0;
                (
                    json!({
                        "ok": !degraded,
                        "detail": if degraded {
                            "schema is compatible, but durable failures require operator attention"
                        } else {
                            "schema is compatible and no durable failures are recorded"
                        },
                        "status": status
                    }),
                    Some(store),
                )
            }
            Err(error) => (
                json!({"ok": false, "detail": error.to_string()}),
                Some(store),
            ),
        },
        Err(error) => (json!({"ok": false, "detail": error.to_string()}), None),
    };
    let report = json!({
        "ok": parent_check["ok"] == true && store_check["ok"] == true && wakterm_check["ok"] == true,
        "mode": if live_preflight { "adapter_preflight" } else { "offline_fake" },
        "checks": {
            "runtime_directory": parent_check,
            "store": store_check,
            "wakterm_contract": wakterm_check,
            "production_connections": {
                "ok": true,
                "detail": if live_preflight {
                    "only the explicitly selected Wakterm Agent API was read; channel connections and prompt submission were disabled"
                } else {
                    "disabled in offline mode"
                }
            }
        }
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    if let Some(store) = opened {
        store.shutdown().await?;
    }
    if report["ok"] == true {
        Ok(())
    } else {
        bail!("doctor found unsafe or incompatible local state")
    }
}

async fn run_migrate(args: StoreArgs) -> Result<()> {
    let store = StoreHandle::open(&args.journal)?;
    store.shutdown().await?;
    println!("{}", json!({"ok": true, "schema_version": 1}));
    Ok(())
}

fn private_directory_check(path: &Path) -> Value {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        match std::fs::symlink_metadata(path) {
            Ok(metadata)
                if metadata.is_dir()
                    && !metadata.file_type().is_symlink()
                    && metadata.permissions().mode() & 0o077 == 0 =>
            {
                json!({"ok": true, "detail": "directory is private"})
            }
            Ok(_) => {
                json!({"ok": false, "detail": "directory must be a real mode 0700 directory"})
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                json!({"ok": true, "detail": "directory will be created with mode 0700"})
            }
            Err(error) => json!({"ok": false, "detail": error.to_string()}),
        }
    }
    #[cfg(not(unix))]
    json!({"ok": false, "detail": "Unix sockets are required"})
}
