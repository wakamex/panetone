use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use panetone::channels::{
    ChannelDeliveryError, RealChannels, SignalClient, SignalSubscriber, TelegramClient,
    TelegramPoller,
};
use panetone::control::{
    CONTROL_SCHEMA, ControlRequest, ControlServer, OutputDispositionParams, RouteEnsureParams,
    RouteInspectParams, SendParams, SourceAgent, request,
};
use panetone::service::{InboundIngestor, ProductionService};
use panetone::store::StoreHandle;
use panetone::supervisor::{Supervisor, TaskPolicy, run_periodic};
use panetone::wakterm::{EventRead, EventRecord, WaktermCli, resume_cursor};
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
    /// Run the production Panetone daemon.
    Daemon(Box<ProductionArgs>),
    /// Send one cross-agent message through the running daemon.
    Send(SendArgs),
    /// Inspect durable daemon health and backlogs.
    Status(StatusArgs),
    /// Inspect or establish durable workspace routes.
    Route(RouteArgs),
    /// Wait for durable routing of exact Wakterm agent output.
    Output(OutputArgs),
    /// Check local paths and the store without sending messages.
    Doctor(DoctorArgs),
}

#[derive(Clone, Args)]
struct ProductionArgs {
    #[arg(long, env = "PANETONE_CONTROL_SOCKET")]
    socket: PathBuf,
    #[arg(long, alias = "journal", env = "PANETONE_DATABASE")]
    database: PathBuf,
    #[arg(long, env = "WAKTERM_BIN")]
    wakterm_bin: PathBuf,
    #[arg(long, env = "WAKTERM_UNIX_SOCKET")]
    wakterm_socket: PathBuf,
    #[arg(
        long,
        env = "PANETONE_TELEGRAM_API_BASE",
        default_value = "https://api.telegram.org"
    )]
    telegram_api_base: String,
    #[arg(long, env = "WAK_TG_CHAT", allow_hyphen_values = true)]
    telegram_chat: Option<i64>,
    #[arg(long, env = "WAK_TG_TOKEN_CLAUDE")]
    telegram_claude_token: Option<String>,
    #[arg(long, env = "WAK_TG_TOKEN_CODEX")]
    telegram_codex_token: Option<String>,
    #[arg(long, env = "WAK_TG_TOKEN_GEMINI")]
    telegram_gemini_token: Option<String>,
    #[arg(long, env = "WAK_TG_TOKEN_OPENCODE")]
    telegram_opencode_token: Option<String>,
    #[arg(long, env = "WAK_TG_OWNER")]
    telegram_owner: Option<String>,
    #[arg(long, env = "WAK_SIG_SOCKET")]
    signal_socket: Option<PathBuf>,
    #[arg(long, env = "WAK_SIG_ACCOUNT")]
    signal_account: Option<String>,
    #[arg(long, env = "WAK_SIG_OWNER")]
    signal_owner: Option<String>,
    #[arg(long, env = "PANETONE_WORKER_POLL_MS", default_value_t = 1000)]
    worker_poll_ms: u64,
    #[arg(long, env = "PANETONE_REPLAY_OFFLINE_OUTPUT", default_value_t = false)]
    replay_offline_output: bool,
}

#[derive(Args)]
struct ControlSocketArgs {
    #[arg(
        long,
        env = "PANETONE_CONTROL_SOCKET",
        help = "Control socket (default: $XDG_RUNTIME_DIR/panetone/control.sock)"
    )]
    socket: Option<PathBuf>,
}

#[derive(Args)]
struct SendArgs {
    #[arg(long = "from")]
    source: Option<String>,
    #[arg(
        long = "to",
        help = "Target route title, or an agent's Wakterm name when the route has several agents"
    )]
    target: String,
    #[arg(long)]
    id: Option<Uuid>,
    #[arg(
        long,
        help = "Exact calling pane (default: the agent `wakterm agent caller` identifies)"
    )]
    source_pane_id: Option<u64>,
    #[arg(
        long,
        help = "Request an asynchronous final callback; this command exits after admission"
    )]
    return_final: bool,
    #[arg(
        long,
        conflicts_with = "return_final",
        help = "Steer an active turn immediately; starts a normal turn when the target is idle"
    )]
    steer: bool,
    #[command(flatten)]
    control: ControlSocketArgs,
    message: String,
}

#[derive(Args)]
struct StatusArgs {
    #[command(flatten)]
    control: ControlSocketArgs,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct RouteArgs {
    #[command(subcommand)]
    command: RouteCommand,
}

#[derive(Subcommand)]
enum RouteCommand {
    /// List configured routes and their current availability.
    List(RouteListArgs),
    /// Inspect one exact case-insensitive route title.
    Inspect(RouteInspectArgs),
    /// Ensure a live title has durable channel bindings.
    Ensure(RouteEnsureArgs),
}

#[derive(Args)]
struct RouteListArgs {
    #[command(flatten)]
    control: ControlSocketArgs,
}

#[derive(Args)]
struct RouteInspectArgs {
    title: String,
    #[command(flatten)]
    control: ControlSocketArgs,
}

#[derive(Args)]
struct RouteEnsureArgs {
    title: String,
    #[arg(long)]
    telegram_topic_id: Option<i64>,
    #[arg(long)]
    signal_group_id: Option<String>,
    #[arg(long, requires = "signal_group_id")]
    signal_allow_members: bool,
    #[command(flatten)]
    control: ControlSocketArgs,
}

#[derive(Args)]
struct OutputArgs {
    #[command(subcommand)]
    command: OutputCommand,
}

#[derive(Subcommand)]
enum OutputCommand {
    /// Wait for exact assistant output after a durable event sequence.
    Wait(OutputWaitArgs),
}

#[derive(Args)]
struct OutputWaitArgs {
    #[arg(long)]
    route: String,
    #[arg(long)]
    agent_id: String,
    #[arg(long)]
    incarnation_id: String,
    #[arg(long = "after")]
    after_sequence: u64,
    #[arg(long)]
    expect_text: String,
    #[arg(long, default_value_t = 90_000)]
    timeout_ms: u64,
    #[arg(long, default_value_t = 100)]
    poll_ms: u64,
    #[command(flatten)]
    control: ControlSocketArgs,
}

#[derive(Args)]
struct DoctorArgs {
    #[command(flatten)]
    control: ControlSocketArgs,
    #[arg(long, alias = "database")]
    journal: PathBuf,
    #[arg(long)]
    wakterm_bin: PathBuf,
    #[arg(long)]
    wakterm_socket: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Daemon(args) => run_production_daemon(*args).await,
        Command::Send(args) => run_send(args).await,
        Command::Status(args) => run_status(args).await,
        Command::Route(args) => run_route(args).await,
        Command::Output(args) => run_output(args).await,
        Command::Doctor(args) => run_doctor(args).await,
    }
}

#[derive(Clone, Copy)]
enum ProductionWorker {
    Events,
    Outbox,
    BusyTargets,
    ReturnTerminals,
    PendingReturns,
    Inbox,
}

async fn run_production_daemon(args: ProductionArgs) -> Result<()> {
    let deadline = Duration::from_secs(10);
    let wakterm = WaktermCli::new(&args.wakterm_bin, &args.wakterm_socket, deadline);
    let (version, capabilities, catalog) =
        tokio::try_join!(wakterm.version(), wakterm.capabilities(), wakterm.catalog())
            .context("preflight the live Wakterm Agent API")?;
    if !capabilities.general_event_consumer_enabled() {
        bail!("production requires Wakterm capability event_stream.v1");
    }
    let live_routes = wakterm
        .live_routes()
        .await
        .context("preflight Wakterm effective route titles")?;
    tracing::info!(
        wakterm_version = %version,
        capabilities = ?capabilities.capabilities,
        live_routes = live_routes.routes().len(),
        "Wakterm production preflight succeeded"
    );

    let (channels, telegram, signal) = production_channels(&args, deadline)?;
    let store = StoreHandle::open(&args.database).context("open production store")?;
    let event_cursor = if args.replay_offline_output {
        store
            .initialize_event_cursor(catalog.as_of_event_sequence)
            .await
            .context("initialize Wakterm event cursor")?
    } else {
        let head = catalog.as_of_event_sequence;
        let resume = match store
            .status()
            .await
            .context("read event cursor")?
            .event_cursor
        {
            Some(cursor) if cursor < head => resume_cursor(
                &offline_events(&wakterm, cursor, head).await?,
                head,
                wall_now_ms(),
            ),
            _ => head,
        };
        store
            .rebaseline_event_cursor(resume)
            .await
            .context("skip stale offline Wakterm output")?;
        resume
    };
    if let Some((poller, _)) = telegram.as_ref()
        && store
            .get_metadata("telegram_update_offset".into())
            .await
            .context("read Telegram update offset")?
            .is_none()
    {
        let baseline = poller
            .poll(-1, 0)
            .await
            .context("initialize Telegram update offset")?;
        store
            .set_metadata(
                "telegram_update_offset".into(),
                baseline.next_offset.max(0).to_string(),
            )
            .await
            .context("persist Telegram update offset")?;
    }
    tracing::info!(
        event_cursor,
        replay_offline_output = args.replay_offline_output,
        "durable cursors are initialized"
    );
    let server = ControlServer::bind(&args.socket)
        .await
        .context("bind Panetone control socket")?;
    let mut supervisor = Supervisor::new();
    let handler = Arc::new(ProductionService::new(
        store.clone(),
        wakterm,
        channels,
        supervisor.handle(),
        capabilities.capabilities.iter().cloned().collect(),
        args.socket.clone(),
    ));
    let created_routes = handler
        .reconcile_live_routes(&live_routes)
        .await
        .map_err(|error| anyhow::anyhow!("reconcile live Wakterm routes: {error}"))?;
    tracing::info!(created_routes, "live Wakterm routes are reconciled");

    let control_shutdown = supervisor.shutdown_receiver();
    let control_handler = handler.clone();
    supervisor.spawn("control", TaskPolicy::Critical, async move {
        server
            .run(control_handler, control_shutdown)
            .await
            .map_err(|error| error.to_string())
    });
    let worker_period = Duration::from_millis(args.worker_poll_ms.max(100));
    for (name, worker) in [
        ("wakterm-events", ProductionWorker::Events),
        ("outbox", ProductionWorker::Outbox),
        ("busy-targets", ProductionWorker::BusyTargets),
        ("return-terminals", ProductionWorker::ReturnTerminals),
        ("pending-returns", ProductionWorker::PendingReturns),
        ("inbox", ProductionWorker::Inbox),
    ] {
        let service = handler.clone();
        let shutdown = supervisor.shutdown_receiver();
        let health = supervisor.handle();
        supervisor.spawn(name, TaskPolicy::Critical, async move {
            run_periodic(
                name,
                health,
                worker_period,
                Duration::from_secs(1),
                shutdown,
                || run_production_worker(&service, worker),
            )
            .await
        });
    }
    if let Some((poller, owner)) = telegram {
        let ingestor = InboundIngestor::new(store.clone());
        let channel_store = store.clone();
        let channel_service = handler.clone();
        let shutdown = supervisor.shutdown_receiver();
        supervisor.spawn("telegram-inbound", TaskPolicy::Critical, async move {
            telegram_loop(
                poller,
                owner,
                ingestor,
                channel_store,
                channel_service,
                shutdown,
            )
            .await
        });
    }
    if let Some((socket, account, owner)) = signal {
        let ingestor = InboundIngestor::new(store.clone());
        let shutdown = supervisor.shutdown_receiver();
        supervisor.spawn("signal-inbound", TaskPolicy::Critical, async move {
            signal_loop(socket, account, owner, ingestor, shutdown).await
        });
    }
    supervisor
        .run_until(shutdown_signal())
        .await
        .context("supervise production daemon")?;
    store.shutdown().await.context("stop store owner")?;
    Ok(())
}

type TelegramRuntime = (TelegramPoller, String);
type SignalRuntime = (PathBuf, String, String);

type ProductionChannels = (RealChannels, Option<TelegramRuntime>, Option<SignalRuntime>);

fn production_channels(args: &ProductionArgs, deadline: Duration) -> Result<ProductionChannels> {
    let mut channels = RealChannels::default();
    let tokens = [
        ("claude", args.telegram_claude_token.as_ref()),
        ("codex", args.telegram_codex_token.as_ref()),
        ("gemini", args.telegram_gemini_token.as_ref()),
        ("opencode", args.telegram_opencode_token.as_ref()),
    ];
    let telegram = match (
        args.telegram_chat,
        args.telegram_claude_token.as_ref(),
        args.telegram_owner.as_ref(),
    ) {
        (None, None, None) if tokens.iter().all(|(_, token)| token.is_none()) => None,
        (Some(chat), Some(primary), Some(owner)) => {
            for (harness, token) in tokens
                .into_iter()
                .filter_map(|(name, token)| token.map(|token| (name, token)))
            {
                channels.telegram_by_harness.insert(
                    harness.into(),
                    TelegramClient::new(&args.telegram_api_base, token, chat, deadline)?,
                );
            }
            channels.telegram = channels.telegram_by_harness.get("claude").cloned();
            Some((
                TelegramPoller::telegram(
                    &args.telegram_api_base,
                    primary,
                    chat,
                    args.database.with_file_name("attachments").join("telegram"),
                    Duration::from_secs(35),
                )?,
                owner.clone(),
            ))
        }
        _ => bail!(
            "Telegram requires WAK_TG_CHAT, WAK_TG_TOKEN_CLAUDE, and WAK_TG_OWNER together; other harness tokens cannot poll inbound"
        ),
    };

    let signal = match (
        args.signal_socket.as_ref(),
        args.signal_account.as_ref(),
        args.signal_owner.as_ref(),
    ) {
        (None, None, None) => None,
        (Some(socket), Some(account), Some(owner)) => {
            channels.signal = Some(SignalClient::new(socket, account, deadline));
            Some((socket.clone(), account.clone(), owner.clone()))
        }
        _ => bail!("Signal requires WAK_SIG_SOCKET, WAK_SIG_ACCOUNT, and WAK_SIG_OWNER together"),
    };

    Ok((channels, telegram, signal))
}

/// The Wakterm events after `cursor` up to `head`, produced while Panetone was
/// stopped. A cursor older than Wakterm retains yields none.
async fn offline_events(wakterm: &WaktermCli, cursor: u64, head: u64) -> Result<Vec<EventRecord>> {
    let mut events = Vec::new();
    let mut after = cursor;
    while after < head {
        match wakterm
            .event_page(after, 1000)
            .await
            .context("read offline Wakterm events")?
        {
            EventRead::Events {
                events: page,
                next_after_sequence,
                ..
            } if next_after_sequence > after => {
                events.extend(page.into_iter().filter(|event| event.sequence <= head));
                after = next_after_sequence;
            }
            _ => break,
        }
    }
    Ok(events)
}

async fn run_production_worker(
    service: &ProductionService,
    worker: ProductionWorker,
) -> Result<(), String> {
    match worker {
        ProductionWorker::Events => service.event_once().await?,
        ProductionWorker::Outbox => service.outbox_once().await?,
        ProductionWorker::BusyTargets => service.busy_once().await?,
        ProductionWorker::ReturnTerminals => service.terminal_once().await?,
        ProductionWorker::PendingReturns => service.pending_return_once().await?,
        ProductionWorker::Inbox => service.inbox_once().await?,
    };
    Ok(())
}

async fn telegram_loop(
    poller: TelegramPoller,
    owner: String,
    ingestor: InboundIngestor,
    store: StoreHandle,
    service: Arc<ProductionService>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    let mut consecutive_failures = 0_u32;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let cursor = store
            .get_metadata("telegram_update_offset".into())
            .await
            .map_err(|error| error.to_string())?;
        if cursor.is_none() {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = shutdown.changed() => continue,
            }
            continue;
        }
        let offset = cursor
            .as_deref()
            .unwrap_or_default()
            .parse::<i64>()
            .map_err(|_| "stored Telegram update offset is invalid".to_string())?;
        let mut batch = match poller.poll(offset, 1).await {
            Ok(batch) => {
                consecutive_failures = 0;
                batch
            }
            Err(error) => {
                let Some(delay) =
                    telegram_poll_retry_delay(&error, consecutive_failures.saturating_add(1))
                else {
                    return Err(error.to_string());
                };
                consecutive_failures = consecutive_failures.saturating_add(1);
                tracing::warn!(
                    error = %error,
                    retry_after_ms = delay.as_millis(),
                    "Telegram polling failed; retrying"
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                }
                continue;
            }
        };
        let approvals = std::mem::take(&mut batch.approvals);
        for approval in approvals {
            let result = if approval.sender_id == owner {
                service.resolve_telegram_approval(&approval).await
            } else {
                Err("Only the configured owner can answer approvals".into())
            };
            let answer = match result {
                Ok(label) => format!("Applied: {label}"),
                Err(error) => {
                    tracing::warn!(
                        request_id = %approval.request_id,
                        error = %error,
                        "Telegram approval response was rejected"
                    );
                    "This approval is stale or unavailable".to_string()
                }
            };
            poller
                .answer_callback(&approval.query_id, &answer)
                .await
                .map_err(|error| error.to_string())?;
        }
        batch
            .messages
            .retain(|message| message.sender_id.as_deref() == Some(owner.as_str()));
        ingestor
            .persist_telegram_batch("telegram_update_offset", batch, wall_now_ms())
            .await
            .map_err(|error| error.to_string())?;
    }
}

fn telegram_poll_retry_delay(
    error: &ChannelDeliveryError,
    consecutive_failures: u32,
) -> Option<Duration> {
    let exponent = consecutive_failures.saturating_sub(1).min(5);
    let backoff = Duration::from_secs((1_u64 << exponent).min(30));
    match error {
        ChannelDeliveryError::RateLimited {
            retry_after_secs, ..
        } => Some(backoff.max(Duration::from_secs(*retry_after_secs))),
        ChannelDeliveryError::Timeout(_) | ChannelDeliveryError::Transport(_) => Some(backoff),
        _ => None,
    }
}

async fn signal_loop(
    socket: PathBuf,
    account: String,
    owner: String,
    ingestor: InboundIngestor,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    let mut subscriber = None;
    let mut consecutive_failures = 0_u32;
    loop {
        if subscriber.is_none() {
            match SignalSubscriber::connect(&socket, &account, Duration::from_secs(35)).await {
                Ok(connected) => {
                    subscriber = Some(connected);
                    consecutive_failures = 0;
                }
                Err(error) => {
                    let Some(delay) =
                        signal_retry_delay(&error, consecutive_failures.saturating_add(1))
                    else {
                        return Err(error.to_string());
                    };
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    tracing::warn!(
                        error = %error,
                        retry_after_ms = delay.as_millis(),
                        "Signal subscription failed; retrying"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                    }
                    continue;
                }
            }
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            message = subscriber.as_mut().expect("subscriber was connected").next() => {
                match message {
                    Ok(message) => {
                        consecutive_failures = 0;
                        ingestor
                            .persist_signal(message, &owner, wall_now_ms())
                            .await
                            .map_err(|error| error.to_string())?;
                    }
                    Err(error) => {
                        let Some(delay) = signal_retry_delay(
                            &error,
                            consecutive_failures.saturating_add(1),
                        ) else {
                            return Err(error.to_string());
                        };
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        subscriber = None;
                        tracing::warn!(
                            error = %error,
                            retry_after_ms = delay.as_millis(),
                            "Signal subscription disconnected; retrying"
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn signal_retry_delay(error: &ChannelDeliveryError, consecutive_failures: u32) -> Option<Duration> {
    let exponent = consecutive_failures.saturating_sub(1).min(5);
    let backoff = Duration::from_secs((1_u64 << exponent).min(30));
    match error {
        ChannelDeliveryError::Timeout(_) | ChannelDeliveryError::Transport(_) => Some(backoff),
        _ => None,
    }
}

fn wall_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
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
    let socket = control_socket(args.control.socket)?;
    let source_agent = match (args.source_pane_id, &args.source) {
        (Some(_), _) => None,
        (None, None) => Some(wakterm_caller().await.context(
            "cannot identify the calling Wakterm agent; pass --from ROUTE or --source-pane-id PANE",
        )?),
        (None, Some(_)) => wakterm_caller().await.ok(),
    };
    let params = SendParams {
        source: args.source,
        target: args.target,
        message: args.message,
        source_pane_id: args.source_pane_id,
        source_agent,
        return_final: args.return_final,
        steer: args.steer,
        timeout_ms: 0,
    };
    let response = request(
        &socket,
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

/// Asks Wakterm which live agent this command runs for. Wakterm resolves it
/// from the inherited environment, including managed Codex tool commands that
/// run outside the agent's pane, so the environment must be passed unchanged.
async fn wakterm_caller() -> Result<SourceAgent> {
    let binary = std::env::var_os("WAKTERM_BIN").unwrap_or_else(|| "wakterm".into());
    let output = tokio::process::Command::new(&binary)
        .args(["--skip-config", "cli", "--prefer-mux", "--no-auto-start"])
        .args(["agent", "caller"])
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .with_context(|| format!("failed to run {}", Path::new(&binary).display()))?;
    if !output.status.success() {
        bail!(
            "wakterm agent caller failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let caller: Value = serde_json::from_slice(&output.stdout)
        .context("wakterm agent caller returned invalid JSON")?;
    serde_json::from_value(caller["agent"].clone())
        .context("wakterm agent caller returned no agent identity")
}

async fn run_status(args: StatusArgs) -> Result<()> {
    let socket = control_socket(args.control.socket)?;
    let response = request(
        &socket,
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

async fn run_route(args: RouteArgs) -> Result<()> {
    let (socket, method, params) = match args.command {
        RouteCommand::List(args) => (
            control_socket(args.control.socket)?,
            "route.list",
            Value::Null,
        ),
        RouteCommand::Inspect(args) => (
            control_socket(args.control.socket)?,
            "route.inspect",
            serde_json::to_value(RouteInspectParams { title: args.title })?,
        ),
        RouteCommand::Ensure(args) => (
            control_socket(args.control.socket)?,
            "route.ensure",
            serde_json::to_value(RouteEnsureParams {
                title: args.title,
                telegram_topic_id: args.telegram_topic_id,
                signal_group_id: args.signal_group_id,
                signal_allow_members: args.signal_allow_members,
            })?,
        ),
    };
    let response = request(
        &socket,
        &ControlRequest {
            schema: CONTROL_SCHEMA.into(),
            id: Uuid::new_v4(),
            method: method.into(),
            params,
        },
    )
    .await?;
    println!("{}", serde_json::to_string(&response)?);
    if response.ok {
        Ok(())
    } else {
        bail!("route request failed")
    }
}

async fn run_output(args: OutputArgs) -> Result<()> {
    match args.command {
        OutputCommand::Wait(args) => run_output_wait(args).await,
    }
}

async fn run_output_wait(args: OutputWaitArgs) -> Result<()> {
    if args.timeout_ms == 0 || args.poll_ms == 0 {
        bail!("--timeout-ms and --poll-ms must be positive");
    }
    let socket = control_socket(args.control.socket)?;
    let started = Instant::now();
    loop {
        let response = request(
            &socket,
            &ControlRequest {
                schema: CONTROL_SCHEMA.into(),
                id: Uuid::new_v4(),
                method: "output.disposition".into(),
                params: serde_json::to_value(OutputDispositionParams {
                    route: args.route.clone(),
                    agent_id: args.agent_id.clone(),
                    incarnation_id: args.incarnation_id.clone(),
                    after_sequence: args.after_sequence,
                    expected_text: args.expect_text.clone(),
                })?,
            },
        )
        .await?;
        if !response.ok {
            println!("{}", serde_json::to_string(&response)?);
            bail!("output disposition request failed");
        }
        let disposition = response
            .result
            .as_ref()
            .and_then(|result| result.get("disposition"))
            .and_then(Value::as_str)
            .context("output disposition response is malformed")?;
        if disposition != "pending" {
            println!("{}", serde_json::to_string(&response)?);
            return if disposition == "projected" {
                Ok(())
            } else {
                bail!("Wakterm output disposition is {disposition}")
            };
        }
        if started.elapsed() >= Duration::from_millis(args.timeout_ms) {
            println!("{}", serde_json::to_string(&response)?);
            bail!("timed out waiting for Wakterm output disposition");
        }
        tokio::time::sleep(Duration::from_millis(args.poll_ms)).await;
    }
}

async fn run_doctor(args: DoctorArgs) -> Result<()> {
    let socket = control_socket(args.control.socket)?;
    let parent = socket
        .parent()
        .context("control socket requires a parent directory")?;
    let parent_check = private_directory_check(parent);
    let cli = WaktermCli::new(
        &args.wakterm_bin,
        &args.wakterm_socket,
        Duration::from_secs(5),
    );
    let wakterm_check = match tokio::try_join!(
        cli.version(),
        cli.capabilities(),
        cli.catalog(),
        cli.live_routes()
    ) {
        Ok((version, capabilities, catalog, live_routes)) => json!({
            "ok": true,
            "detail": "explicit Wakterm Agent API and effective-title preflight succeeded",
            "version": version,
            "socket": args.wakterm_socket,
            "capabilities": capabilities.capabilities,
            "general_event_consumer": capabilities.general_event_consumer_enabled(),
            "catalog_agents": catalog.agents.len(),
            "live_routes": live_routes.routes().len()
        }),
        Err(error) => json!({"ok": false, "detail": error.to_string()}),
    };
    let store = StoreHandle::open(&args.journal);
    let (store_check, opened) = match store {
        Ok(store) => match store.status().await {
            Ok(status) => {
                let degraded = status.failed_workflows > 0
                    || status.indeterminate_workflows > 0
                    || status.failed_outbox > 0
                    || status.indeterminate_outbox > 0
                    || status.unresolved_returns > 0;
                (
                    json!({
                        "ok": !degraded,
                        "detail": if degraded {
                            "schema is compatible, but durable failures require manual attention"
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
        "mode": "adapter_preflight",
        "checks": {
            "runtime_directory": parent_check,
            "store": store_check,
            "wakterm_contract": wakterm_check,
            "production_connections": {
                "ok": true,
                "detail": "only the explicitly selected Wakterm Agent API was read; channel connections and prompt submission were disabled"
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

fn control_socket(socket: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(socket) = socket {
        return Ok(socket);
    }
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|value| !value.is_empty())
        .context("XDG_RUNTIME_DIR is not set; pass --socket or set PANETONE_CONTROL_SOCKET")?;
    let runtime = PathBuf::from(runtime);
    if !runtime.is_absolute() {
        bail!("XDG_RUNTIME_DIR must be an absolute path");
    }
    Ok(runtime.join("panetone/control.sock"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use panetone::domain::ChannelKind;

    fn production_args() -> ProductionArgs {
        ProductionArgs {
            socket: "/tmp/panetone-test.sock".into(),
            database: "/tmp/panetone-test.sqlite3".into(),
            wakterm_bin: "/bin/false".into(),
            wakterm_socket: "/tmp/wakterm-test.sock".into(),
            telegram_api_base: "http://127.0.0.1:1".into(),
            telegram_chat: None,
            telegram_claude_token: None,
            telegram_codex_token: None,
            telegram_gemini_token: None,
            telegram_opencode_token: None,
            telegram_owner: None,
            signal_socket: None,
            signal_account: None,
            signal_owner: None,
            worker_poll_ms: 1000,
            replay_offline_output: false,
        }
    }

    #[test]
    fn telegram_configuration_fails_closed_without_an_owner() {
        let mut args = production_args();
        args.telegram_chat = Some(-1001);
        args.telegram_claude_token = Some("test-token".into());
        let error = match production_channels(&args, Duration::from_secs(1)) {
            Ok(_) => panic!("Telegram unexpectedly started without an owner"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("WAK_TG_OWNER"));

        args.telegram_owner = Some("42".into());
        assert!(production_channels(&args, Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn telegram_poll_retries_use_bounded_backoff_and_honor_rate_limits() {
        let transport = ChannelDeliveryError::Transport(ChannelKind::Telegram);
        assert_eq!(
            telegram_poll_retry_delay(&transport, 1),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            telegram_poll_retry_delay(&transport, 6),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            telegram_poll_retry_delay(&transport, u32::MAX),
            Some(Duration::from_secs(30))
        );

        let limited = ChannelDeliveryError::RateLimited {
            kind: ChannelKind::Telegram,
            retry_after_secs: 45,
        };
        assert_eq!(
            telegram_poll_retry_delay(&limited, 1),
            Some(Duration::from_secs(45))
        );

        let malformed = ChannelDeliveryError::Malformed(ChannelKind::Telegram);
        assert_eq!(telegram_poll_retry_delay(&malformed, 1), None);
    }

    #[test]
    fn signal_subscription_retries_only_temporary_failures() {
        let transport = ChannelDeliveryError::Transport(ChannelKind::Signal);
        assert_eq!(
            signal_retry_delay(&transport, 1),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            signal_retry_delay(&transport, 6),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            signal_retry_delay(&transport, u32::MAX),
            Some(Duration::from_secs(30))
        );

        let timeout = ChannelDeliveryError::Timeout(ChannelKind::Signal);
        assert_eq!(
            signal_retry_delay(&timeout, 2),
            Some(Duration::from_secs(2))
        );

        let malformed = ChannelDeliveryError::Malformed(ChannelKind::Signal);
        assert_eq!(signal_retry_delay(&malformed, 1), None);
    }
}
