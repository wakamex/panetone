use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rusqlite::backup::Backup;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::domain::{
    ChannelBinding, ChannelKind, EffectId, OutboxItem, OutboxState, PYTHON_CONTROL_HASH_KIND,
    Route, RouteId, RouteStatus, normalize_signal_group_id,
};
use crate::store::{InboxItem, SCHEMA_VERSION, migrate_schema};

const MANIFEST_SCHEMA: &str = "panetone.migration-manifest.v1";
const BUNDLE_DATABASE: &str = "panetone.sqlite3";
const BUNDLE_MANIFEST: &str = "migration-manifest.json";
const SNAPSHOT_DIR: &str = "legacy";
const STATE_SNAPSHOT: &str = "state.json";
const PENDING_SNAPSHOT: &str = "pending_sends.json";
const CONTROL_SNAPSHOT: &str = "control-journal.sqlite3";
const SIGNAL_SNAPSHOT: &str = "signal.sqlite3";

#[derive(Clone, Debug)]
pub struct MigrationOptions {
    pub state: PathBuf,
    pub pending: PathBuf,
    pub control_journal: PathBuf,
    pub signal_database: Option<PathBuf>,
    pub legacy_control_socket: PathBuf,
    pub output: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MigrationManifest {
    pub schema: String,
    pub target_schema_version: i64,
    pub source_hashes: BTreeMap<String, String>,
    pub snapshot_hashes: BTreeMap<String, String>,
    pub counts: BTreeMap<String, u64>,
    pub target_logical_sha256: String,
    pub warnings: Vec<String>,
    pub expired_uuid_boundary: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationOutcome {
    pub manifest: MigrationManifest,
    pub reused: bool,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("legacy Panetone is still listening on {0}")]
    LegacyServiceRunning(PathBuf),
    #[error("legacy control socket path is not a Unix socket: {0}")]
    UnsafeControlSocket(PathBuf),
    #[error("migration source must be a real regular file: {0}")]
    UnsafeSource(PathBuf),
    #[error("migration output must have a private real parent directory: {0}")]
    UnsafeOutputParent(PathBuf),
    #[error("migration output exists but is incomplete: {0}")]
    IncompleteOutput(PathBuf),
    #[error("migration output does not match the current source snapshot: {0}")]
    SourceChanged(String),
    #[error("legacy state is malformed: {0}")]
    Malformed(String),
    #[error(
        "legacy pending item {0:?} uses removed Slack delivery; archive or dispose it before migration"
    )]
    RemovedSlackPending(String),
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type MigrationResult<T> = Result<T, MigrationError>;

#[derive(Clone, Debug)]
struct SourceMaterial {
    state_bytes: Vec<u8>,
    pending_bytes: Vec<u8>,
    source_hashes: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
struct RouteBuilder {
    title: String,
    channels: Vec<ChannelBinding>,
}

pub fn migrate(options: &MigrationOptions) -> MigrationResult<MigrationOutcome> {
    require_legacy_service_stopped(&options.legacy_control_socket)?;
    validate_source(&options.state)?;
    validate_source(&options.pending)?;
    validate_source(&options.control_journal)?;
    if let Some(path) = &options.signal_database {
        validate_source(path)?;
    }
    validate_output_parent(&options.output)?;

    let material = read_source_material(options)?;
    if options.output.exists() {
        return verify_existing_bundle(options, &material);
    }

    let parent = options
        .output
        .parent()
        .ok_or_else(|| MigrationError::UnsafeOutputParent(options.output.clone()))?;
    let name = options
        .output
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| MigrationError::UnsafeOutputParent(options.output.clone()))?;
    let partial = parent.join(format!(".{name}.partial-{}", Uuid::new_v4()));
    fs::create_dir(&partial)?;
    fs::set_permissions(&partial, fs::Permissions::from_mode(0o700))?;

    let result = build_bundle(options, &material, &partial);
    match result {
        Ok(manifest) => {
            sync_directory(&partial)?;
            fs::rename(&partial, &options.output)?;
            sync_directory(parent)?;
            Ok(MigrationOutcome {
                manifest,
                reused: false,
            })
        }
        Err(error) => {
            let _ = fs::remove_dir_all(&partial);
            Err(error)
        }
    }
}

fn build_bundle(
    options: &MigrationOptions,
    material: &SourceMaterial,
    bundle: &Path,
) -> MigrationResult<MigrationManifest> {
    let snapshot = bundle.join(SNAPSHOT_DIR);
    fs::create_dir(&snapshot)?;
    fs::set_permissions(&snapshot, fs::Permissions::from_mode(0o700))?;
    write_private(&snapshot.join(STATE_SNAPSHOT), &material.state_bytes)?;
    write_private(&snapshot.join(PENDING_SNAPSHOT), &material.pending_bytes)?;
    backup_sqlite(&options.control_journal, &snapshot.join(CONTROL_SNAPSHOT))?;
    if let Some(source) = &options.signal_database {
        backup_sqlite(source, &snapshot.join(SIGNAL_SNAPSHOT))?;
    }
    require_legacy_service_stopped(&options.legacy_control_socket)?;
    let after_snapshot = read_source_material(options)?;
    if after_snapshot.source_hashes != material.source_hashes
        || after_snapshot.state_bytes != material.state_bytes
        || after_snapshot.pending_bytes != material.pending_bytes
    {
        return Err(MigrationError::SourceChanged(
            "legacy state changed while the offline snapshot was being created".into(),
        ));
    }

    let snapshot_hashes = snapshot_hashes(&snapshot, options.signal_database.is_some())?;
    let database_path = bundle.join(BUNDLE_DATABASE);
    let mut target = Connection::open(&database_path)?;
    target.pragma_update(None, "journal_mode", "DELETE")?;
    target.pragma_update(None, "synchronous", "FULL")?;
    target.pragma_update(None, "foreign_keys", "ON")?;
    migrate_schema(&mut target).map_err(|error| MigrationError::Malformed(error.to_string()))?;

    let state: Value = serde_json::from_slice(&material.state_bytes)?;
    let pending: Value = serde_json::from_slice(&material.pending_bytes)?;
    let mut warnings = BTreeSet::new();
    let mut counts = BTreeMap::new();
    {
        let transaction = target.transaction()?;
        let routes = import_routes(&transaction, &state, &mut warnings)?;
        counts.insert("routes".into(), routes.len() as u64);
        let pending_counts = import_pending(&transaction, &pending, &routes, &mut warnings)?;
        counts.insert("pending_outbox".into(), pending_counts.outbox);
        counts.insert(
            "legacy_debate_outbox_held".into(),
            pending_counts.debate_held,
        );
        let signal = import_signal(
            &transaction,
            &snapshot,
            &state,
            options.signal_database.is_some(),
        )?;
        counts.insert("signal_messages".into(), signal.messages);
        counts.insert("pending_signal_inbox".into(), signal.pending_inbox);
        let control = import_control(&transaction, &snapshot.join(CONTROL_SNAPSHOT))?;
        counts.insert("legacy_control_requests".into(), control.requests);
        counts.insert(
            "legacy_indeterminate_requests".into(),
            control.indeterminate,
        );
        counts.insert("legacy_return_deliveries".into(), control.returns);
        counts.insert(
            "legacy_unresolved_returns".into(),
            control.unresolved_returns,
        );
        transaction.commit()?;
    }
    let target_logical_sha256 = logical_database_hash(&target)?;
    let manifest = MigrationManifest {
        schema: MANIFEST_SCHEMA.into(),
        target_schema_version: SCHEMA_VERSION,
        source_hashes: material.source_hashes.clone(),
        snapshot_hashes,
        counts,
        target_logical_sha256,
        warnings: warnings.into_iter().collect(),
        expired_uuid_boundary: "UUIDs already expired from the Python control journal before this snapshot are unknowable; every UUID present in the snapshot remains permanently reserved".into(),
    };
    let encoded = canonical_json(&manifest)?;
    let manifest_hash = sha256(&encoded);
    target.execute(
        "INSERT INTO metadata(key, value) VALUES ('migration_manifest_json', ?1)",
        params![String::from_utf8(encoded.clone()).expect("manifest JSON is UTF-8")],
    )?;
    target.execute(
        "INSERT INTO metadata(key, value) VALUES ('migration_manifest_sha256', ?1)",
        params![manifest_hash],
    )?;
    target.pragma_update(None, "wal_checkpoint", "TRUNCATE")?;
    drop(target);
    fs::set_permissions(&database_path, fs::Permissions::from_mode(0o600))?;
    write_private(&bundle.join(BUNDLE_MANIFEST), &encoded)?;
    sync_file(&database_path)?;
    sync_directory(&snapshot)?;
    Ok(manifest)
}

fn verify_existing_bundle(
    options: &MigrationOptions,
    material: &SourceMaterial,
) -> MigrationResult<MigrationOutcome> {
    let manifest_path = options.output.join(BUNDLE_MANIFEST);
    let database_path = options.output.join(BUNDLE_DATABASE);
    let snapshot = options.output.join(SNAPSHOT_DIR);
    if !manifest_path.is_file() || !database_path.is_file() || !snapshot.is_dir() {
        return Err(MigrationError::IncompleteOutput(options.output.clone()));
    }
    let manifest: MigrationManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if manifest.schema != MANIFEST_SCHEMA || manifest.target_schema_version != SCHEMA_VERSION {
        return Err(MigrationError::IncompleteOutput(options.output.clone()));
    }
    if manifest.source_hashes != material.source_hashes {
        return Err(MigrationError::SourceChanged(
            "legacy source hashes differ from the completed migration".into(),
        ));
    }
    let actual_snapshots = snapshot_hashes(&snapshot, options.signal_database.is_some())?;
    if actual_snapshots != manifest.snapshot_hashes {
        return Err(MigrationError::SourceChanged(
            "the rollback snapshot no longer matches its manifest".into(),
        ));
    }
    let target = Connection::open_with_flags(&database_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let digest = logical_database_hash(&target)?;
    if digest != manifest.target_logical_sha256 {
        return Err(MigrationError::SourceChanged(
            "the migrated database no longer matches its manifest".into(),
        ));
    }
    Ok(MigrationOutcome {
        manifest,
        reused: true,
    })
}

fn require_legacy_service_stopped(path: &Path) -> MigrationResult<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_socket() {
        return Err(MigrationError::UnsafeControlSocket(path.to_path_buf()));
    }
    if UnixStream::connect(path).is_ok() {
        return Err(MigrationError::LegacyServiceRunning(path.to_path_buf()));
    }
    Ok(())
}

fn validate_source(path: &Path) -> MigrationResult<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| MigrationError::UnsafeSource(path.to_path_buf()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(MigrationError::UnsafeSource(path.to_path_buf()));
    }
    Ok(())
}

fn validate_output_parent(output: &Path) -> MigrationResult<()> {
    let parent = output
        .parent()
        .ok_or_else(|| MigrationError::UnsafeOutputParent(output.to_path_buf()))?;
    let metadata = fs::symlink_metadata(parent)
        .map_err(|_| MigrationError::UnsafeOutputParent(parent.to_path_buf()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(MigrationError::UnsafeOutputParent(parent.to_path_buf()));
    }
    Ok(())
}

fn read_source_material(options: &MigrationOptions) -> MigrationResult<SourceMaterial> {
    let state_bytes = fs::read(&options.state)?;
    let pending_bytes = fs::read(&options.pending)?;
    serde_json::from_slice::<Value>(&state_bytes)?;
    serde_json::from_slice::<Value>(&pending_bytes)?;
    let mut source_hashes = BTreeMap::new();
    source_hashes.insert("state_json".into(), sha256(&state_bytes));
    source_hashes.insert("pending_json".into(), sha256(&pending_bytes));
    source_hashes.insert(
        "control_journal_logical".into(),
        logical_source_hash(&options.control_journal)?,
    );
    if let Some(path) = &options.signal_database {
        source_hashes.insert("signal_database_logical".into(), logical_source_hash(path)?);
    }
    Ok(SourceMaterial {
        state_bytes,
        pending_bytes,
        source_hashes,
    })
}

fn backup_sqlite(source_path: &Path, destination_path: &Path) -> MigrationResult<()> {
    let source = Connection::open_with_flags(source_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let integrity: String = source.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(MigrationError::Malformed(format!(
            "SQLite integrity check failed for {}: {integrity}",
            source_path.display()
        )));
    }
    let mut destination = Connection::open(destination_path)?;
    {
        let backup = Backup::new(&source, &mut destination)?;
        backup.run_to_completion(64, Duration::from_millis(1), None)?;
    }
    drop(destination);
    fs::set_permissions(destination_path, fs::Permissions::from_mode(0o600))?;
    sync_file(destination_path)?;
    Ok(())
}

fn snapshot_hashes(snapshot: &Path, signal: bool) -> MigrationResult<BTreeMap<String, String>> {
    let mut hashes = BTreeMap::new();
    for name in [STATE_SNAPSHOT, PENDING_SNAPSHOT, CONTROL_SNAPSHOT] {
        hashes.insert(name.into(), sha256(&fs::read(snapshot.join(name))?));
    }
    if signal {
        hashes.insert(
            SIGNAL_SNAPSHOT.into(),
            sha256(&fs::read(snapshot.join(SIGNAL_SNAPSHOT))?),
        );
    }
    Ok(hashes)
}

fn import_routes(
    transaction: &Transaction<'_>,
    state: &Value,
    warnings: &mut BTreeSet<String>,
) -> MigrationResult<BTreeMap<String, Route>> {
    let state = object(state, "state.json")?;
    let mut builders = BTreeMap::<String, RouteBuilder>::new();
    if let Some(topics) = optional_object(state, "telegram_topics")? {
        for (title, topic) in topics {
            let topic = topic.as_i64().ok_or_else(|| {
                MigrationError::Malformed(format!("telegram topic for {title:?} is not an integer"))
            })?;
            if topic <= 0 {
                return Err(MigrationError::Malformed(format!(
                    "telegram topic for {title:?} is not positive"
                )));
            }
            add_route_channel(
                &mut builders,
                title,
                ChannelBinding::Telegram { topic_id: topic },
            )?;
        }
    }

    let legacy_names = optional_object(state, "signal_group_names")?;
    if let Some(names) = legacy_names {
        for (tab_id, title) in names {
            if tab_id.parse::<i64>().is_err()
                || title.as_str().is_none_or(|value| value.trim().is_empty())
            {
                return Err(MigrationError::Malformed(
                    "signal_group_names must map numeric tab IDs to titles".into(),
                ));
            }
        }
    }
    if let Some(groups) = optional_object(state, "signal_groups")? {
        for (key, group) in groups {
            let group = group.as_str().ok_or_else(|| {
                MigrationError::Malformed(format!("Signal group for {key:?} is not a string"))
            })?;
            let group = normalize_signal_group_id(Some(group));
            if group.is_empty() {
                return Err(MigrationError::Malformed(format!(
                    "Signal group for {key:?} is empty"
                )));
            }
            let title = if key.parse::<i64>().is_ok() {
                legacy_names
                    .and_then(|names| names.get(key))
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
            } else {
                Some(key.as_str())
            };
            let Some(title) = title else {
                warnings.insert(format!(
                    "legacy Signal tab {key} has no stable title and remains rollback-only"
                ));
                continue;
            };
            add_route_channel(
                &mut builders,
                title,
                ChannelBinding::Signal { group_id: group },
            )?;
        }
    }

    let mut routes = BTreeMap::new();
    for (key, builder) in builders {
        let route = Route {
            id: stable_route_id(&key),
            title: builder.title,
            channels: builder.channels,
            agent: None,
            status: RouteStatus::Unavailable,
        };
        transaction.execute(
            "INSERT INTO routes(route_id, route_json, updated_at_ms)
             VALUES (?1, ?2, 0)",
            params![route.id.to_string(), serde_json::to_string(&route)?],
        )?;
        routes.insert(key, route);
    }

    let mut preferences = BTreeMap::new();
    let mut held_debate_preferences = BTreeMap::new();
    let mut removed_slack_preferences = BTreeMap::new();
    if let Some(sources) = optional_object(state, "last_sources")? {
        for (title, source) in sources {
            let source = source.as_str().ok_or_else(|| {
                MigrationError::Malformed(format!("last source for {title:?} is not a string"))
            })?;
            if !matches!(source, "tg" | "sig" | "slack" | "debate") {
                return Err(MigrationError::Malformed(format!(
                    "last source for {title:?} is unsupported"
                )));
            }
            let key = normalize_title(title)?;
            if source == "slack" {
                removed_slack_preferences.insert(key, source.to_owned());
                warnings.insert(format!(
                    "legacy Slack preference for {title:?} was preserved as deprecated metadata and is not deliverable"
                ));
                continue;
            }
            if let Some(route) = routes.get(&key) {
                if source == "debate" {
                    if route
                        .channels
                        .iter()
                        .any(|binding| matches!(binding, ChannelBinding::Signal { .. }))
                    {
                        preferences.insert(route.id.to_string(), "sig".to_owned());
                        warnings.insert(format!(
                            "legacy Debate preference for {title:?} was normalized to its exact Signal route"
                        ));
                    } else {
                        held_debate_preferences.insert(route.id.to_string(), source.to_owned());
                        warnings.insert(format!(
                            "legacy Debate preference for {title:?} has no exact Signal binding and remains held"
                        ));
                    }
                } else {
                    preferences.insert(route.id.to_string(), source.to_owned());
                }
            } else {
                warnings.insert(format!(
                    "last-source route {title:?} has no stable channel binding"
                ));
            }
        }
    }
    insert_metadata(
        transaction,
        "migrated_route_preferences_v1",
        &serde_json::to_string(&preferences)?,
    )?;
    insert_metadata(
        transaction,
        "legacy_debate_preferences_held",
        &serde_json::to_string(&held_debate_preferences)?,
    )?;
    insert_metadata(
        transaction,
        "legacy_removed_slack_preferences_v1",
        &serde_json::to_string(&removed_slack_preferences)?,
    )?;

    let muted_groups = optional_array(state, "clod_off_groups")?;
    let mut muted_routes = BTreeSet::new();
    for value in muted_groups.into_iter().flatten() {
        let group = value.as_str().ok_or_else(|| {
            MigrationError::Malformed("clod_off_groups must contain strings".into())
        })?;
        let group = normalize_signal_group_id(Some(group));
        let matched = routes.values().find(|route| {
            route.channels.iter().any(|channel| {
                matches!(channel, ChannelBinding::Signal { group_id } if group_id == &group)
            })
        });
        if let Some(route) = matched {
            muted_routes.insert(route.id.to_string());
        } else {
            warnings.insert(format!(
                "muted Signal group {group:?} has no stable route binding"
            ));
        }
    }
    insert_metadata(
        transaction,
        "migrated_muted_routes_v1",
        &serde_json::to_string(&muted_routes)?,
    )?;

    let collab = optional_object(state, "collab")?;
    if let Some(collab) = collab {
        for (tab_id, rounds) in collab {
            if tab_id.parse::<i64>().is_err() || rounds.as_u64().is_none() {
                return Err(MigrationError::Malformed(
                    "collab must map numeric tab IDs to non-negative rounds".into(),
                ));
            }
        }
    }
    let clod_off = optional_array(state, "clod_off")?;
    if clod_off.is_some_and(|items| {
        items.iter().any(|item| {
            item.as_str()
                .is_none_or(|tab_id| tab_id.parse::<i64>().is_err())
        })
    }) {
        return Err(MigrationError::Malformed(
            "clod_off must contain numeric tab IDs encoded as strings".into(),
        ));
    }
    let unresolved = json!({
        "collab": collab.cloned().unwrap_or_default(),
        "clod_off": clod_off.cloned().unwrap_or_default(),
        "signal_group_names": state.get("signal_group_names").cloned().unwrap_or_else(|| json!({})),
    });
    insert_metadata(
        transaction,
        "legacy_ephemeral_state_rollback_only",
        &serde_json::to_string(&unresolved)?,
    )?;
    if unresolved["collab"]
        .as_object()
        .is_some_and(|value| !value.is_empty())
    {
        warnings
            .insert("collaboration tab IDs remain rollback-only because they are ephemeral".into());
    }
    if unresolved["clod_off"]
        .as_array()
        .is_some_and(|value| !value.is_empty())
    {
        warnings
            .insert("legacy muted tab IDs remain rollback-only because they are ephemeral".into());
    }
    Ok(routes)
}

fn add_route_channel(
    routes: &mut BTreeMap<String, RouteBuilder>,
    title: &str,
    channel: ChannelBinding,
) -> MigrationResult<()> {
    let key = normalize_title(title)?;
    let entry = routes.entry(key).or_insert_with(|| RouteBuilder {
        title: title.trim().to_owned(),
        channels: Vec::new(),
    });
    if entry
        .channels
        .iter()
        .any(|existing| same_channel_kind(existing, &channel))
    {
        if !entry.channels.contains(&channel) {
            return Err(MigrationError::Malformed(format!(
                "route {:?} has conflicting bindings for one channel",
                entry.title
            )));
        }
        return Ok(());
    }
    entry.channels.push(channel);
    entry.channels.sort_by_key(channel_order);
    Ok(())
}

fn same_channel_kind(left: &ChannelBinding, right: &ChannelBinding) -> bool {
    matches!(
        (left, right),
        (
            ChannelBinding::Telegram { .. },
            ChannelBinding::Telegram { .. }
        ) | (ChannelBinding::Signal { .. }, ChannelBinding::Signal { .. })
    )
}

fn channel_order(channel: &ChannelBinding) -> u8 {
    match channel {
        ChannelBinding::Telegram { .. } => 0,
        ChannelBinding::Signal { .. } => 1,
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PendingCounts {
    outbox: u64,
    debate_held: u64,
}

fn import_pending(
    transaction: &Transaction<'_>,
    pending: &Value,
    routes: &BTreeMap<String, Route>,
    warnings: &mut BTreeSet<String>,
) -> MigrationResult<PendingCounts> {
    let pending = object(pending, "pending_sends.json")?;
    if pending.get("schema").and_then(Value::as_str) != Some("panetone.delivery-state.v2") {
        return Err(MigrationError::Malformed(
            "pending_sends.json has an unsupported schema".into(),
        ));
    }
    let cursors = pending
        .get("cursors")
        .ok_or_else(|| MigrationError::Malformed("pending cursor map is missing".into()))?;
    let cursor_map = object(cursors, "pending cursors")?;
    if cursor_map.values().any(|cursor| !cursor.is_object()) {
        return Err(MigrationError::Malformed(
            "pending cursor values must be objects".into(),
        ));
    }
    insert_metadata(
        transaction,
        "legacy_provider_cursors_rollback_only",
        &serde_json::to_string(cursors)?,
    )?;

    let saved_at = pending
        .get("saved_at")
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)
        .ok_or_else(|| MigrationError::Malformed("pending saved_at is invalid".into()))?;
    let items = pending
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| MigrationError::Malformed("pending item list is missing".into()))?;
    let mut identities = BTreeSet::new();
    let mut counts = PendingCounts::default();
    for item in items {
        let item = object(item, "pending item")?;
        let legacy_id = required_string(item, "id", "pending item")?;
        if !identities.insert(legacy_id.to_owned()) {
            return Err(MigrationError::Malformed(format!(
                "pending item ID {legacy_id:?} is duplicated"
            )));
        }
        let legacy_kind = required_string(item, "kind", "pending item")?;
        let kind = match legacy_kind {
            "tg" => Some(ChannelKind::Telegram),
            "sig" => Some(ChannelKind::Signal),
            "slack" => return Err(MigrationError::RemovedSlackPending(legacy_id.to_owned())),
            "debate" => None,
            other => {
                return Err(MigrationError::Malformed(format!(
                    "pending item {legacy_id:?} has unknown kind {other:?}"
                )));
            }
        };
        let destination = destination_string(
            item.get("target")
                .ok_or_else(|| MigrationError::Malformed("pending target is missing".into()))?,
        )?;
        let body = item
            .get("chunk")
            .and_then(Value::as_str)
            .ok_or_else(|| MigrationError::Malformed("pending chunk is not a string".into()))?;
        let route_title = item
            .get("route_title")
            .and_then(Value::as_str)
            .unwrap_or_default();
        item.get("pane_id")
            .and_then(Value::as_u64)
            .ok_or_else(|| MigrationError::Malformed("pending pane_id is invalid".into()))?;
        let sender_harness = required_string(item, "harness", "pending item")?.to_owned();
        let effect_id = stable_effect_id("python-pending-output", legacy_id);
        let Some(kind) = kind else {
            transaction.execute(
                "INSERT INTO legacy_debate_outbox(
                     effect_id, destination, record_json, resolution_state,
                     resolution_json, created_at_ms, updated_at_ms
                 ) VALUES (?1, ?2, ?3, 'held', NULL, ?4, ?4)",
                params![
                    effect_id.to_string(),
                    destination,
                    serde_json::to_string(item)?,
                    saved_at,
                ],
            )?;
            counts.debate_held += 1;
            warnings.insert(format!(
                "legacy Debate pending item {legacy_id:?} remains held until its Signal group is matched exactly"
            ));
            continue;
        };
        let route_id = resolve_pending_route(kind, &destination, route_title, routes)?;
        if route_id.is_none() && matches!(kind, ChannelKind::Telegram | ChannelKind::Signal) {
            warnings.insert(format!(
                "pending {kind:?} item {legacy_id:?} has no unique stable route binding"
            ));
        }
        let record = OutboxItem {
            id: effect_id,
            route_id,
            sender_harness: Some(sender_harness),
            kind,
            destination,
            body: body.to_owned(),
            state: OutboxState::Pending,
            attempts: 0,
            last_error: None,
            external_receipt: None,
        };
        transaction.execute(
            "INSERT INTO outbox(
                 effect_id, request_id, channel, destination, state, record_json,
                 created_at_ms, updated_at_ms
             ) VALUES (?1, NULL, ?2, ?3, 'pending', ?4, ?5, ?5)",
            params![
                effect_id.to_string(),
                channel_name(kind),
                record.destination,
                serde_json::to_string(&record)?,
                saved_at,
            ],
        )?;
        counts.outbox += 1;
    }
    Ok(counts)
}

fn resolve_pending_route(
    kind: ChannelKind,
    destination: &str,
    title: &str,
    routes: &BTreeMap<String, Route>,
) -> MigrationResult<Option<RouteId>> {
    if !title.trim().is_empty() {
        let key = normalize_title(title)?;
        return Ok(routes.get(&key).map(|route| route.id));
    }
    let normalized_signal = normalize_signal_group_id(Some(destination));
    let matches = routes
        .values()
        .filter(|route| {
            route.channels.iter().any(|channel| match (kind, channel) {
                (ChannelKind::Telegram, ChannelBinding::Telegram { topic_id }) => {
                    topic_id.to_string() == destination
                }
                (ChannelKind::Signal, ChannelBinding::Signal { group_id }) => {
                    group_id == &normalized_signal
                }
                _ => false,
            })
        })
        .map(|route| route.id)
        .collect::<Vec<_>>();
    Ok(match matches.as_slice() {
        [route_id] => Some(*route_id),
        _ => None,
    })
}

#[derive(Clone, Copy, Debug, Default)]
struct SignalCounts {
    messages: u64,
    pending_inbox: u64,
}

fn import_signal(
    transaction: &Transaction<'_>,
    snapshot: &Path,
    state: &Value,
    has_signal_database: bool,
) -> MigrationResult<SignalCounts> {
    let mut counts = SignalCounts::default();
    if has_signal_database {
        let source = Connection::open_with_flags(
            snapshot.join(SIGNAL_SNAPSHOT),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        require_columns(
            &source,
            "signal_messages",
            &[
                "id",
                "group_id",
                "envelope_timestamp",
                "received_at",
                "sender_id",
                "sender_number",
                "sender_name",
                "text",
                "formatted_text",
                "data_json",
                "direction",
                "accepted",
                "is_command",
                "is_mention",
                "delivered_at",
            ],
        )?;
        let mut statement = source.prepare(
            "SELECT id, group_id, envelope_timestamp, received_at, sender_id,
                    sender_number, sender_name, text, formatted_text, data_json,
                    direction, accepted, is_command, is_mention, delivered_at
             FROM signal_messages ORDER BY id",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let group_id: String = row.get(1)?;
            let group_id = normalize_signal_group_id(Some(&group_id));
            if group_id.is_empty() {
                return Err(MigrationError::Malformed(format!(
                    "Signal row {id} has an empty group ID"
                )));
            }
            let timestamp: Option<i64> = row.get(2)?;
            let received_at: i64 = row.get(3)?;
            let sender_id: String = row.get(4)?;
            let sender_number: String = row.get(5)?;
            let sender_name: String = row.get(6)?;
            let text: String = row.get(7)?;
            let formatted_text: String = row.get(8)?;
            let data_json: String = row.get(9)?;
            serde_json::from_str::<Value>(&data_json).map_err(|error| {
                MigrationError::Malformed(format!("Signal row {id} has invalid data_json: {error}"))
            })?;
            let direction: String = row.get(10)?;
            let accepted: i64 = row.get(11)?;
            let is_command: i64 = row.get(12)?;
            let is_mention: i64 = row.get(13)?;
            let delivered_at: Option<i64> = row.get(14)?;
            if !matches!(direction.as_str(), "incoming" | "outgoing")
                || !matches!(accepted, 0 | 1)
                || !matches!(is_command, 0 | 1)
                || !matches!(is_mention, 0 | 1)
            {
                return Err(MigrationError::Malformed(format!(
                    "Signal row {id} has invalid flags"
                )));
            }
            let pending = direction == "incoming"
                && accepted == 1
                && is_command == 0
                && !formatted_text.is_empty()
                && delivered_at.is_none();
            let raw = json!({
                "id": id,
                "group_id": group_id,
                "envelope_timestamp": timestamp,
                "received_at": received_at,
                "sender_id": sender_id,
                "sender_number": sender_number,
                "sender_name": sender_name,
                "text": text,
                "formatted_text": formatted_text,
                "data_json": data_json,
                "direction": direction,
                "accepted": accepted,
                "is_command": is_command,
                "is_mention": is_mention,
                "delivered_at": delivered_at,
            });
            transaction.execute(
                "INSERT INTO signal_messages(
                     legacy_id, group_id, state, record_json, received_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    format!("sqlite:{id}"),
                    group_id,
                    if pending { "pending" } else { "archived" },
                    serde_json::to_string(&raw)?,
                    received_at,
                ],
            )?;
            counts.messages += 1;
            if pending {
                let external_id = format!(
                    "{}:{}:{}:{}",
                    group_id,
                    sender_id,
                    timestamp.unwrap_or(received_at),
                    id
                );
                let inbox = InboxItem {
                    id: stable_effect_id("python-signal-inbox", &external_id),
                    channel: ChannelKind::Signal,
                    external_id,
                    destination: group_id,
                    sender_id: Some(sender_number.clone()),
                    sender: Some(if sender_id.is_empty() {
                        sender_number
                    } else {
                        sender_id
                    }),
                    body: formatted_text,
                    state: "pending".into(),
                    created_at_ms: received_at,
                };
                transaction.execute(
                    "INSERT INTO inbox(
                         effect_id, channel, external_id, state, record_json, created_at_ms
                     ) VALUES (?1, 'signal', ?2, 'pending', ?3, ?4)",
                    params![
                        inbox.id.to_string(),
                        inbox.external_id,
                        serde_json::to_string(&inbox)?,
                        received_at,
                    ],
                )?;
                counts.pending_inbox += 1;
            }
        }
    }

    let state = object(state, "state.json")?;
    if let Some(history) = optional_object(state, "clod_history")? {
        for (group, messages) in history {
            let messages = messages.as_array().ok_or_else(|| {
                MigrationError::Malformed(format!(
                    "legacy Signal history for {group:?} is not an array"
                ))
            })?;
            let group_id = normalize_signal_group_id(Some(group));
            for (index, message) in messages.iter().enumerate() {
                let message = message.as_str().ok_or_else(|| {
                    MigrationError::Malformed("legacy Signal history is not text".into())
                })?;
                let legacy_id = format!("json:{group_id}:{index}");
                let raw = json!({
                    "legacy_source": "clod_history",
                    "group_id": group_id,
                    "formatted_text": message,
                });
                transaction.execute(
                    "INSERT INTO signal_messages(
                         legacy_id, group_id, state, record_json, received_at_ms
                     ) VALUES (?1, ?2, 'pending', ?3, 0)",
                    params![legacy_id, group_id, serde_json::to_string(&raw)?],
                )?;
                counts.messages += 1;
                let external_id = format!("legacy-json:{group_id}:{index}");
                let inbox = InboxItem {
                    id: stable_effect_id("python-signal-inbox", &external_id),
                    channel: ChannelKind::Signal,
                    external_id,
                    destination: group_id.clone(),
                    sender_id: None,
                    sender: None,
                    body: message.to_owned(),
                    state: "pending".into(),
                    created_at_ms: 0,
                };
                transaction.execute(
                    "INSERT INTO inbox(
                         effect_id, channel, external_id, state, record_json, created_at_ms
                     ) VALUES (?1, 'signal', ?2, 'pending', ?3, 0)",
                    params![
                        inbox.id.to_string(),
                        inbox.external_id,
                        serde_json::to_string(&inbox)?,
                    ],
                )?;
                counts.pending_inbox += 1;
            }
        }
    }
    Ok(counts)
}

#[derive(Clone, Copy, Debug, Default)]
struct ControlCounts {
    requests: u64,
    indeterminate: u64,
    returns: u64,
    unresolved_returns: u64,
}

fn import_control(
    transaction: &Transaction<'_>,
    source_path: &Path,
) -> MigrationResult<ControlCounts> {
    let source = Connection::open_with_flags(source_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    require_columns(
        &source,
        "control_request",
        &[
            "request_id",
            "request_hash",
            "source",
            "target",
            "state",
            "progress_json",
            "response_json",
            "created_at",
            "updated_at",
        ],
    )?;
    require_columns(
        &source,
        "return_delivery",
        &[
            "request_id",
            "source_json",
            "target_json",
            "state",
            "result_json",
            "agent_state",
            "telegram_state",
            "last_error",
            "created_at",
            "updated_at",
        ],
    )?;
    require_columns(&source, "control_meta", &["key", "value"])?;

    let mut counts = ControlCounts::default();
    let mut statement = source.prepare(
        "SELECT request_id, request_hash, source, target, state, progress_json,
                response_json, created_at, updated_at
         FROM control_request ORDER BY request_id",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let request_id: String = row.get(0)?;
        let parsed_request_id = Uuid::parse_str(&request_id).map_err(|_| {
            MigrationError::Malformed(format!("control request ID {request_id:?} is not a UUID"))
        })?;
        if parsed_request_id.to_string() != request_id {
            return Err(MigrationError::Malformed(format!(
                "control request ID {request_id:?} is not canonical"
            )));
        }
        let request_hash: String = row.get(1)?;
        if request_hash.len() != 64
            || !request_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(MigrationError::Malformed(format!(
                "control request {request_id} has an invalid hash"
            )));
        }
        let source_name: String = row.get(2)?;
        let target_name: String = row.get(3)?;
        if source_name.is_empty() || target_name.is_empty() {
            return Err(MigrationError::Malformed(format!(
                "control request {request_id} has an empty route"
            )));
        }
        let legacy_state: String = row.get(4)?;
        if !matches!(
            legacy_state.as_str(),
            "in_progress"
                | "audit_posted"
                | "delivering"
                | "succeeded"
                | "failed"
                | "indeterminate"
        ) {
            return Err(MigrationError::Malformed(format!(
                "control request {request_id} has unknown state {legacy_state:?}"
            )));
        }
        let progress_json: Option<String> = row.get(5)?;
        let response_json: Option<String> = row.get(6)?;
        validate_optional_json(
            &progress_json,
            &format!("control request {request_id} progress"),
        )?;
        validate_optional_json(
            &response_json,
            &format!("control request {request_id} response"),
        )?;
        let created_at = seconds_to_millis(row.get::<_, f64>(7)?)?;
        let updated_at = seconds_to_millis(row.get::<_, f64>(8)?)?;
        if updated_at < created_at {
            return Err(MigrationError::Malformed(format!(
                "control request {request_id} was updated before it was created"
            )));
        }
        if matches!(
            legacy_state.as_str(),
            "succeeded" | "failed" | "indeterminate"
        ) && response_json.is_none()
        {
            return Err(MigrationError::Malformed(format!(
                "terminal control request {request_id} has no response"
            )));
        }
        let state = if matches!(
            legacy_state.as_str(),
            "in_progress" | "audit_posted" | "delivering"
        ) {
            "indeterminate"
        } else {
            legacy_state.as_str()
        };
        let record = json!({
            "request_id": request_id,
            "request_hash": request_hash,
            "hash_kind": PYTHON_CONTROL_HASH_KIND,
            "source": source_name,
            "target": target_name,
            "legacy_state": legacy_state,
            "migrated_state": state,
            "progress_json": progress_json,
            "response_json": response_json,
            "created_at_ms": created_at,
            "updated_at_ms": updated_at,
        });
        transaction.execute(
            "INSERT INTO idempotency_tombstones(
                 request_id, semantic_hash, hash_kind, terminal_state,
                 created_at_ms, completed_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                request_id,
                request_hash,
                PYTHON_CONTROL_HASH_KIND,
                state,
                created_at,
                updated_at,
            ],
        )?;
        transaction.execute(
            "INSERT INTO legacy_control_requests(
                 request_id, state, source, target, record_json,
                 created_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                request_id,
                state,
                source_name,
                target_name,
                serde_json::to_string(&record)?,
                created_at,
                updated_at,
            ],
        )?;
        counts.requests += 1;
        counts.indeterminate += u64::from(state == "indeterminate");
    }

    let mut statement = source.prepare(
        "SELECT request_id, source_json, target_json, state, result_json,
                agent_state, telegram_state, last_error, created_at, updated_at
         FROM return_delivery ORDER BY request_id",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let request_id: String = row.get(0)?;
        let exists = transaction
            .query_row(
                "SELECT 1 FROM idempotency_tombstones WHERE request_id = ?1",
                params![request_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            return Err(MigrationError::Malformed(format!(
                "return delivery {request_id} has no control request"
            )));
        }
        let source_json: String = row.get(1)?;
        let target_json: String = row.get(2)?;
        if !serde_json::from_str::<Value>(&source_json)?.is_object()
            || !serde_json::from_str::<Value>(&target_json)?.is_object()
        {
            return Err(MigrationError::Malformed(format!(
                "return delivery {request_id} routes are not objects"
            )));
        }
        let legacy_state: String = row.get(3)?;
        if !matches!(legacy_state.as_str(), "pending" | "terminal") {
            return Err(MigrationError::Malformed(format!(
                "return delivery {request_id} has invalid state"
            )));
        }
        let result_json: Option<String> = row.get(4)?;
        validate_optional_json(
            &result_json,
            &format!("return delivery {request_id} result"),
        )?;
        if (legacy_state == "terminal") != result_json.is_some() {
            return Err(MigrationError::Malformed(format!(
                "return delivery {request_id} state and result disagree"
            )));
        }
        let legacy_agent_state: String = row.get(5)?;
        let legacy_mirror_state: String = row.get(6)?;
        let agent_state = migrated_delivery_state(&legacy_agent_state, &request_id)?;
        let mirror_state = migrated_delivery_state(&legacy_mirror_state, &request_id)?;
        let last_error: Option<String> = row.get(7)?;
        let created_at = seconds_to_millis(row.get::<_, f64>(8)?)?;
        let updated_at = seconds_to_millis(row.get::<_, f64>(9)?)?;
        if updated_at < created_at {
            return Err(MigrationError::Malformed(format!(
                "return delivery {request_id} was updated before it was created"
            )));
        }
        let record = json!({
            "request_id": request_id,
            "source_json": source_json,
            "target_json": target_json,
            "legacy_state": legacy_state,
            "result_json": result_json,
            "agent_state": agent_state,
            "mirror_state": mirror_state,
            "shared_last_error": last_error,
            "identity_state": "reconciliation_required",
            "created_at_ms": created_at,
            "updated_at_ms": updated_at,
        });
        transaction.execute(
            "INSERT INTO legacy_return_deliveries(
                 request_id, state, agent_state, mirror_state, record_json,
                 created_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                request_id,
                legacy_state,
                agent_state,
                mirror_state,
                serde_json::to_string(&record)?,
                created_at,
                updated_at,
            ],
        )?;
        counts.returns += 1;
        counts.unresolved_returns +=
            u64::from(agent_state != "delivered" || mirror_state != "delivered");
    }

    let mut metadata = BTreeMap::new();
    let mut statement = source.prepare("SELECT key, value FROM control_meta ORDER BY key")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        metadata.insert(row.get::<_, String>(0)?, row.get::<_, String>(1)?);
    }
    insert_metadata(
        transaction,
        "legacy_control_meta_rollback_only",
        &serde_json::to_string(&metadata)?,
    )?;
    Ok(counts)
}

fn migrated_delivery_state<'a>(state: &'a str, request_id: &str) -> MigrationResult<&'a str> {
    match state {
        "pending" | "delivered" | "failed" | "indeterminate" => Ok(state),
        "delivering" => Ok("indeterminate"),
        _ => Err(MigrationError::Malformed(format!(
            "return delivery {request_id} has invalid destination state {state:?}"
        ))),
    }
}

fn validate_optional_json(value: &Option<String>, label: &str) -> MigrationResult<()> {
    if let Some(value) = value {
        serde_json::from_str::<Value>(value)
            .map_err(|error| MigrationError::Malformed(format!("{label} is invalid: {error}")))?;
    }
    Ok(())
}

fn seconds_to_millis(value: f64) -> MigrationResult<i64> {
    if !value.is_finite() || value < 0.0 || value > i64::MAX as f64 / 1_000.0 {
        return Err(MigrationError::Malformed(
            "legacy timestamp is outside the supported range".into(),
        ));
    }
    Ok((value * 1_000.0).round() as i64)
}

fn logical_source_hash(path: &Path) -> MigrationResult<String> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(MigrationError::Malformed(format!(
            "SQLite integrity check failed for {}: {integrity}",
            path.display()
        )));
    }
    logical_database_hash(&connection)
}

fn logical_database_hash(connection: &Connection) -> MigrationResult<String> {
    let mut digest = Sha256::new();
    let mut statement = connection.prepare(
        "SELECT name, sql FROM sqlite_schema
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )?;
    let tables = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    for (table, sql) in tables {
        hash_field(&mut digest, table.as_bytes());
        hash_field(&mut digest, sql.as_bytes());
        let columns = table_columns(connection, &table)?;
        for column in &columns {
            hash_field(&mut digest, column.as_bytes());
        }
        let quoted_table = quote_identifier(&table);
        let quoted_columns = columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>();
        let mut query = format!("SELECT {} FROM {quoted_table}", quoted_columns.join(", "));
        if table == "metadata" {
            query.push_str(
                " WHERE key NOT IN ('migration_manifest_json', 'migration_manifest_sha256')",
            );
        }
        query.push_str(&format!(" ORDER BY {}", quoted_columns.join(", ")));
        let mut rows = connection.prepare(&query)?;
        let mut rows = rows.query([])?;
        while let Some(row) = rows.next()? {
            digest.update([0xff]);
            for index in 0..columns.len() {
                match row.get_ref(index)? {
                    ValueRef::Null => hash_field(&mut digest, b"n"),
                    ValueRef::Integer(value) => {
                        hash_field(&mut digest, format!("i{value}").as_bytes())
                    }
                    ValueRef::Real(value) => {
                        hash_field(&mut digest, format!("r{:016x}", value.to_bits()).as_bytes())
                    }
                    ValueRef::Text(value) => {
                        digest.update(b"t");
                        hash_field(&mut digest, value);
                    }
                    ValueRef::Blob(value) => {
                        digest.update(b"b");
                        hash_field(&mut digest, value);
                    }
                }
            }
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn table_columns(connection: &Connection, table: &str) -> MigrationResult<Vec<String>> {
    let query = format!("PRAGMA table_info({})", quote_identifier(table));
    let mut statement = connection.prepare(&query)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err(MigrationError::Malformed(format!(
            "SQLite table {table:?} has no columns"
        )));
    }
    Ok(columns)
}

fn require_columns(connection: &Connection, table: &str, required: &[&str]) -> MigrationResult<()> {
    let actual = table_columns(connection, table)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let missing = required
        .iter()
        .filter(|column| !actual.contains(**column))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(MigrationError::Malformed(format!(
            "SQLite table {table:?} is missing columns: {}",
            missing.join(", ")
        )));
    }
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn hash_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(value.len().to_le_bytes());
    digest.update(value);
}

fn object<'a>(value: &'a Value, label: &str) -> MigrationResult<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| MigrationError::Malformed(format!("{label} is not a JSON object")))
}

fn optional_object<'a>(
    value: &'a Map<String, Value>,
    key: &str,
) -> MigrationResult<Option<&'a Map<String, Value>>> {
    value
        .get(key)
        .map(|value| {
            value.as_object().ok_or_else(|| {
                MigrationError::Malformed(format!("state field {key:?} is not an object"))
            })
        })
        .transpose()
}

fn optional_array<'a>(
    value: &'a Map<String, Value>,
    key: &str,
) -> MigrationResult<Option<&'a Vec<Value>>> {
    value
        .get(key)
        .map(|value| {
            value.as_array().ok_or_else(|| {
                MigrationError::Malformed(format!("state field {key:?} is not an array"))
            })
        })
        .transpose()
}

fn required_string<'a>(
    value: &'a Map<String, Value>,
    key: &str,
    label: &str,
) -> MigrationResult<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| MigrationError::Malformed(format!("{label} field {key:?} is invalid")))
}

fn destination_string(value: &Value) -> MigrationResult<String> {
    match value {
        Value::String(value) if !value.is_empty() => Ok(value.clone()),
        Value::Number(value) if value.is_i64() || value.is_u64() => Ok(value.to_string()),
        _ => Err(MigrationError::Malformed(
            "pending destination must be a string or integer".into(),
        )),
    }
}

fn normalize_title(value: &str) -> MigrationResult<String> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 128
        || value
            .chars()
            .any(|character| character.is_control() || (0x7f..=0x9f).contains(&(character as u32)))
    {
        return Err(MigrationError::Malformed(format!(
            "route title {value:?} is invalid"
        )));
    }
    Ok(value.to_lowercase())
}

fn stable_route_id(title: &str) -> RouteId {
    let namespace = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        b"https://panetone.dev/migration/python-route",
    );
    RouteId::new(Uuid::new_v5(&namespace, title.as_bytes()))
}

fn stable_effect_id(kind: &str, legacy_id: &str) -> EffectId {
    let namespace = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        b"https://panetone.dev/migration/python-effect",
    );
    EffectId::new(Uuid::new_v5(
        &namespace,
        format!("{kind}:{legacy_id}").as_bytes(),
    ))
}

fn channel_name(kind: ChannelKind) -> &'static str {
    match kind {
        ChannelKind::Telegram => "telegram",
        ChannelKind::Signal => "signal",
    }
}

fn insert_metadata(transaction: &Transaction<'_>, key: &str, value: &str) -> MigrationResult<()> {
    transaction.execute(
        "INSERT INTO metadata(key, value) VALUES (?1, ?2)",
        params![key, value],
    )?;
    Ok(())
}

fn canonical_json<T: Serialize>(value: &T) -> MigrationResult<Vec<u8>> {
    let mut encoded = serde_json::to_vec_pretty(value)?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn sha256(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}

fn write_private(path: &Path, value: &[u8]) -> MigrationResult<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    use std::io::Write;
    file.write_all(value)?;
    file.sync_all()?;
    Ok(())
}

fn sync_file(path: &Path) -> MigrationResult<()> {
    OpenOptions::new().read(true).open(path)?.sync_all()?;
    Ok(())
}

fn sync_directory(path: &Path) -> MigrationResult<()> {
    OpenOptions::new().read(true).open(path)?.sync_all()?;
    Ok(())
}
