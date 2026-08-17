use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use panetone::control::{CONTROL_SCHEMA, ControlRequest, ControlServer, SendParams, request};
use panetone::service::ConformanceService;
use panetone::store::StoreHandle;
use serde_json::{Value, json};
use tokio::sync::watch;
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
    let store = StoreHandle::open(&args.journal).context("open offline store")?;
    let handler = Arc::new(ConformanceService::new(store.clone(), args.effect_log));
    let server = ControlServer::bind(&args.socket)
        .await
        .context("bind Panetone control socket")?;
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let task = tokio::spawn(server.run(handler, shutdown_receiver));
    tokio::signal::ctrl_c().await.context("wait for shutdown")?;
    let _ = shutdown_sender.send(true);
    task.await.context("join control server")??;
    store.shutdown().await.context("stop store owner")?;
    Ok(())
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
    let store = StoreHandle::open(&args.journal);
    let (store_check, opened) = match store {
        Ok(store) => (
            json!({"ok": true, "detail": "schema is compatible"}),
            Some(store),
        ),
        Err(error) => (json!({"ok": false, "detail": error.to_string()}), None),
    };
    let report = json!({
        "ok": parent_check["ok"] == true && store_check["ok"] == true,
        "mode": "offline_fake",
        "checks": {
            "runtime_directory": parent_check,
            "store": store_check,
            "production_connections": {"ok": true, "detail": "disabled in Phase 2"}
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
