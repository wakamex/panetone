use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use panetone::channels::{
    RealChannels, SignalClient, SignalSubscriber, SlackClient, SlackSocket, TelegramClient,
    TelegramPoller,
};
use panetone::control::{CONTROL_SCHEMA, ControlRequest, ControlServer, SendParams, request};
use panetone::migration::{MigrationOptions, migrate};
use panetone::service::{ConformanceService, InboundIngestor, ProductionService};
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
    /// Run the production Panetone daemon.
    Daemon(Box<ProductionArgs>),
    /// Send one cross-agent message through the running daemon.
    Send(SendArgs),
    /// Inspect durable daemon health and backlogs.
    Status(StatusArgs),
    /// Apply one audited, idempotent promotion operation.
    Operator(OperatorArgs),
    /// Check local paths and the offline store without sending messages.
    Doctor(DoctorArgs),
    /// Create or upgrade the offline database schema explicitly.
    Migrate(MigrationArgs),
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
    #[arg(long, env = "WEZ_TG_CHAT", allow_hyphen_values = true)]
    telegram_chat: Option<i64>,
    #[arg(long, env = "WEZ_TG_TOKEN_CLAUDE")]
    telegram_claude_token: Option<String>,
    #[arg(long, env = "WEZ_TG_TOKEN_CODEX")]
    telegram_codex_token: Option<String>,
    #[arg(long, env = "WEZ_TG_TOKEN_GEMINI")]
    telegram_gemini_token: Option<String>,
    #[arg(long, env = "WEZ_TG_TOKEN_OPENCODE")]
    telegram_opencode_token: Option<String>,
    #[arg(long, env = "WEZ_TG_OWNER")]
    telegram_owner: Option<String>,
    #[arg(long, env = "WEZ_SIG_SOCKET")]
    signal_socket: Option<PathBuf>,
    #[arg(long, env = "WEZ_SIG_ACCOUNT")]
    signal_account: Option<String>,
    #[arg(long, env = "WEZ_SIG_OWNER")]
    signal_owner: Option<String>,
    #[arg(
        long,
        env = "PANETONE_SLACK_API_BASE",
        default_value = "https://slack.com/api"
    )]
    slack_api_base: String,
    #[arg(long, env = "WEZ_SLACK_BOT_TOKEN")]
    slack_bot_token: Option<String>,
    #[arg(long, env = "WEZ_SLACK_APP_TOKEN")]
    slack_app_token: Option<String>,
    #[arg(long, env = "PANETONE_SLACK_SOCKET_URL")]
    slack_socket_url: Option<String>,
    #[arg(long, env = "PANETONE_WORKER_POLL_MS", default_value_t = 1000)]
    worker_poll_ms: u64,
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
struct OperatorArgs {
    #[arg(long, env = "PANETONE_CONTROL_SOCKET")]
    socket: PathBuf,
    #[arg(long)]
    id: Option<Uuid>,
    #[command(subcommand)]
    action: OperatorCommand,
}

#[derive(Subcommand)]
enum OperatorCommand {
    Hold,
    Release,
    EnableRoute {
        route: String,
    },
    DisableRoute {
        route: String,
    },
    InitEventCursor {
        sequence: u64,
    },
    BaselineEvent,
    AcknowledgeEventGap {
        requested_after_sequence: u64,
        #[arg(long)]
        evidence: String,
    },
    InitTelegramCursor {
        offset: u64,
    },
    BaselineTelegram,
    ReconcileRoute {
        route: String,
        #[arg(long)]
        replace_identity: bool,
    },
    DisposeLegacy {
        #[arg(value_enum)]
        record_kind: LegacyKindArg,
        record_id: String,
        #[arg(value_enum)]
        decision: LegacyDecisionArg,
        #[arg(long)]
        evidence: String,
        #[arg(long)]
        route: Option<String>,
        #[arg(long)]
        expected_legacy_destination: Option<String>,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum LegacyKindArg {
    Control,
    Return,
    Debate,
}

#[derive(Clone, Copy, ValueEnum)]
enum LegacyDecisionArg {
    NoReplay,
    ExternallyVerified,
    MapDebateToSignal,
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
struct MigrationArgs {
    #[arg(long)]
    state: PathBuf,
    #[arg(long)]
    pending: PathBuf,
    #[arg(long)]
    control_journal: PathBuf,
    #[arg(long)]
    signal_database: Option<PathBuf>,
    #[arg(long)]
    legacy_control_socket: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Daemon(args) => run_production_daemon(*args).await,
        Command::ConformanceBackend(args) => run_daemon(args).await,
        Command::Send(args) => run_send(args).await,
        Command::Status(args) => run_status(args).await,
        Command::Operator(args) => run_operator(args).await,
        Command::Doctor(args) => run_doctor(args).await,
        Command::Migrate(args) => run_migrate(args).await,
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
    let (version, capabilities, _catalog) =
        tokio::try_join!(wakterm.version(), wakterm.capabilities(), wakterm.catalog())
            .context("preflight the live Wakterm Agent API")?;
    if !capabilities.general_event_consumer_enabled() {
        bail!("production requires Wakterm capability event_stream.v1");
    }
    tracing::info!(
        wakterm_version = %version,
        capabilities = ?capabilities.capabilities,
        "Wakterm production preflight succeeded"
    );

    let (channels, telegram, signal, slack) = production_channels(&args, deadline)?;
    let store = StoreHandle::open(&args.database).context("open production store")?;
    let server = ControlServer::bind(&args.socket)
        .await
        .context("bind Panetone control socket")?;
    let mut supervisor = Supervisor::new();
    let handler = Arc::new(ProductionService::new(
        store.clone(),
        wakterm,
        channels,
        telegram.as_ref().map(|(poller, _)| poller.clone()),
        supervisor.handle(),
        capabilities.capabilities.iter().cloned().collect(),
        args.socket.clone(),
    ));

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
        supervisor.spawn(name, TaskPolicy::Degraded, async move {
            production_worker_loop(service, worker, worker_period, shutdown).await
        });
    }
    if let Some((poller, owner)) = telegram {
        let ingestor = InboundIngestor::new(store.clone());
        let channel_store = store.clone();
        let shutdown = supervisor.shutdown_receiver();
        supervisor.spawn("telegram-inbound", TaskPolicy::Degraded, async move {
            telegram_loop(poller, owner, ingestor, channel_store, shutdown).await
        });
    }
    if let Some((socket, account, owner)) = signal {
        let ingestor = InboundIngestor::new(store.clone());
        let channel_store = store.clone();
        let shutdown = supervisor.shutdown_receiver();
        supervisor.spawn("signal-inbound", TaskPolicy::Degraded, async move {
            signal_loop(socket, account, owner, ingestor, channel_store, shutdown).await
        });
    }
    if let Some(slack) = slack {
        let ingestor = InboundIngestor::new(store.clone());
        let channel_store = store.clone();
        let shutdown = supervisor.shutdown_receiver();
        supervisor.spawn("slack-inbound", TaskPolicy::Degraded, async move {
            slack_loop(slack, ingestor, channel_store, shutdown).await
        });
    }

    supervisor
        .run_until(shutdown_signal())
        .await
        .context("supervise production daemon")?;
    store.shutdown().await.context("stop store owner")?;
    Ok(())
}

type TelegramRuntime = (TelegramPoller, Option<String>);
type SignalRuntime = (PathBuf, String, String);

enum SlackRuntime {
    Url(String),
    AppToken { api_base: String, token: String },
}

type ProductionChannels = (
    RealChannels,
    Option<TelegramRuntime>,
    Option<SignalRuntime>,
    Option<SlackRuntime>,
);

fn production_channels(args: &ProductionArgs, deadline: Duration) -> Result<ProductionChannels> {
    let mut channels = RealChannels::default();
    let tokens = [
        ("claude", args.telegram_claude_token.as_ref()),
        ("codex", args.telegram_codex_token.as_ref()),
        ("gemini", args.telegram_gemini_token.as_ref()),
        ("opencode", args.telegram_opencode_token.as_ref()),
    ];
    let telegram = match (args.telegram_chat, args.telegram_claude_token.as_ref()) {
        (None, None) if tokens.iter().all(|(_, token)| token.is_none()) => None,
        (Some(chat), Some(primary)) => {
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
                    Duration::from_secs(35),
                )?,
                args.telegram_owner.clone(),
            ))
        }
        _ => bail!(
            "Telegram requires WEZ_TG_CHAT and WEZ_TG_TOKEN_CLAUDE together; other harness tokens cannot poll inbound"
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
        _ => bail!("Signal requires WEZ_SIG_SOCKET, WEZ_SIG_ACCOUNT, and WEZ_SIG_OWNER together"),
    };

    if let Some(token) = args.slack_bot_token.as_ref() {
        channels.slack = Some(SlackClient::new(&args.slack_api_base, token, deadline)?);
    }
    let slack = match (
        args.slack_socket_url.as_ref(),
        args.slack_app_token.as_ref(),
    ) {
        (Some(url), None) => Some(SlackRuntime::Url(url.clone())),
        (None, Some(token)) => Some(SlackRuntime::AppToken {
            api_base: args.slack_api_base.clone(),
            token: token.clone(),
        }),
        (None, None) => None,
        (Some(_), Some(_)) => {
            bail!("configure either PANETONE_SLACK_SOCKET_URL or WEZ_SLACK_APP_TOKEN, not both")
        }
    };
    if slack.is_some() && channels.slack.is_none() {
        bail!("Slack inbound requires WEZ_SLACK_BOT_TOKEN for outbound parity");
    }
    Ok((channels, telegram, signal, slack))
}

async fn production_worker_loop(
    service: Arc<ProductionService>,
    worker: ProductionWorker,
    period: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = interval.tick() => {
                match worker {
                    ProductionWorker::Events => service.event_once().await?,
                    ProductionWorker::Outbox => service.outbox_once().await?,
                    ProductionWorker::BusyTargets => service.busy_once().await?,
                    ProductionWorker::ReturnTerminals => service.terminal_once().await?,
                    ProductionWorker::PendingReturns => service.pending_return_once().await?,
                    ProductionWorker::Inbox => service.inbox_once().await?,
                };
            }
        }
    }
}

async fn telegram_loop(
    poller: TelegramPoller,
    owner: Option<String>,
    ingestor: InboundIngestor,
    store: StoreHandle,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let promotion = store
            .promotion_status()
            .await
            .map_err(|error| error.to_string())?;
        let cursor = store
            .get_metadata("telegram_update_offset".into())
            .await
            .map_err(|error| error.to_string())?;
        if promotion.delivery_hold || cursor.is_none() {
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
        let mut batch = poller
            .poll(offset, 1)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(owner) = owner.as_deref() {
            batch
                .messages
                .retain(|message| message.sender_id.as_deref() == Some(owner));
        }
        ingestor
            .persist_telegram_batch("telegram_update_offset", batch, wall_now_ms())
            .await
            .map_err(|error| error.to_string())?;
    }
}

async fn signal_loop(
    socket: PathBuf,
    account: String,
    owner: String,
    ingestor: InboundIngestor,
    store: StoreHandle,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        if store
            .promotion_status()
            .await
            .map_err(|error| error.to_string())?
            .delivery_hold
        {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = shutdown.changed() => continue,
            }
            continue;
        }
        let mut subscriber = SignalSubscriber::connect(&socket, &account, Duration::from_secs(35))
            .await
            .map_err(|error| error.to_string())?;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                message = subscriber.next() => {
                    let message = message.map_err(|error| error.to_string())?;
                    if message.sender_id.as_deref() == Some(owner.as_str()) {
                        ingestor.persist(message, wall_now_ms()).await.map_err(|error| error.to_string())?;
                    }
                }
            }
            if store
                .promotion_status()
                .await
                .map_err(|error| error.to_string())?
                .delivery_hold
            {
                break;
            }
        }
    }
}

async fn slack_loop(
    runtime: SlackRuntime,
    ingestor: InboundIngestor,
    store: StoreHandle,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        if store
            .promotion_status()
            .await
            .map_err(|error| error.to_string())?
            .delivery_hold
        {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = shutdown.changed() => continue,
            }
            continue;
        }
        let mut socket = match &runtime {
            SlackRuntime::Url(url) => SlackSocket::connect(url, Duration::from_secs(35)).await,
            SlackRuntime::AppToken { api_base, token } => {
                SlackSocket::connect_with_app_token(api_base, token, Duration::from_secs(35)).await
            }
        }
        .map_err(|error| error.to_string())?;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                result = ingestor.ingest_slack_once(&mut socket, wall_now_ms()) => {
                    result.map_err(|error| error.to_string())?;
                }
            }
            if store
                .promotion_status()
                .await
                .map_err(|error| error.to_string())?
                .delivery_hold
            {
                break;
            }
        }
    }
}

fn wall_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
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

async fn run_operator(args: OperatorArgs) -> Result<()> {
    let (method, params) = match args.action {
        OperatorCommand::Hold => ("set_delivery_hold", json!({"held": true})),
        OperatorCommand::Release => ("set_delivery_hold", json!({"held": false})),
        OperatorCommand::EnableRoute { route } => (
            "set_route_enabled",
            json!({"route": route, "enabled": true}),
        ),
        OperatorCommand::DisableRoute { route } => (
            "set_route_enabled",
            json!({"route": route, "enabled": false}),
        ),
        OperatorCommand::InitEventCursor { sequence } => {
            ("initialize_event_cursor", json!({"sequence": sequence}))
        }
        OperatorCommand::BaselineEvent => ("baseline_event_cursor", Value::Null),
        OperatorCommand::AcknowledgeEventGap {
            requested_after_sequence,
            evidence,
        } => (
            "acknowledge_event_cursor_gap",
            json!({
                "requested_after_sequence": requested_after_sequence,
                "evidence": evidence,
            }),
        ),
        OperatorCommand::InitTelegramCursor { offset } => (
            "initialize_inbound_cursor",
            json!({"channel": "telegram", "cursor": offset}),
        ),
        OperatorCommand::BaselineTelegram => ("baseline_telegram_cursor", Value::Null),
        OperatorCommand::ReconcileRoute {
            route,
            replace_identity,
        } => (
            "reconcile_route",
            json!({"route": route, "replace_identity": replace_identity}),
        ),
        OperatorCommand::DisposeLegacy {
            record_kind,
            record_id,
            decision,
            evidence,
            route,
            expected_legacy_destination,
        } => (
            "dispose_legacy",
            json!({
                "record_kind": match record_kind {
                    LegacyKindArg::Control => "control",
                    LegacyKindArg::Return => "return",
                    LegacyKindArg::Debate => "debate",
                },
                "record_id": record_id,
                "decision": match decision {
                    LegacyDecisionArg::NoReplay => "no_replay",
                    LegacyDecisionArg::ExternallyVerified => "externally_verified",
                    LegacyDecisionArg::MapDebateToSignal => "map_debate_to_signal",
                },
                "evidence": evidence,
                "route": route,
                "expected_legacy_destination": expected_legacy_destination,
            }),
        ),
    };
    let response = request(
        &args.socket,
        &ControlRequest {
            schema: CONTROL_SCHEMA.into(),
            id: args.id.unwrap_or_else(Uuid::new_v4),
            method: method.into(),
            params,
        },
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    if response.ok {
        Ok(())
    } else {
        bail!("operator action was rejected")
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
                    "ok": true,
                    "detail": "both pinned Wakterm capability snapshots are compatible",
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
                    || status.legacy_debate_outbox > 0
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

async fn run_migrate(args: MigrationArgs) -> Result<()> {
    let output = args.output.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        migrate(&MigrationOptions {
            state: args.state,
            pending: args.pending,
            control_journal: args.control_journal,
            signal_database: args.signal_database,
            legacy_control_socket: args.legacy_control_socket,
            output: args.output,
        })
    })
    .await
    .context("migration worker stopped")??;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "ok": true,
            "reused": outcome.reused,
            "bundle": output,
            "database": output.join("panetone.sqlite3"),
            "manifest": outcome.manifest,
        }))?
    );
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
