//! Rooms: routes where everyone sees everything. Each message, from the user
//! or from one of the room's agents, is kept in the room's transcript and
//! delivered to every other agent in the room. A room's agents are those of
//! its own route and of its member routes, which keep their own channels.

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

use super::{StoreResult, get_metadata, set_metadata};
use crate::domain::RouteId;

pub(super) const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS room_messages (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         route_id TEXT NOT NULL,
         record_json TEXT NOT NULL,
         created_at_ms INTEGER NOT NULL
     );
     CREATE INDEX IF NOT EXISTS room_messages_route ON room_messages(route_id, id);
     CREATE TABLE IF NOT EXISTS room_deliveries (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         route_id TEXT NOT NULL,
         agent_id TEXT NOT NULL,
         state TEXT NOT NULL,
         record_json TEXT NOT NULL,
         created_at_ms INTEGER NOT NULL
     );
     CREATE INDEX IF NOT EXISTS room_deliveries_state ON room_deliveries(state, id);
     CREATE INDEX IF NOT EXISTS room_deliveries_agent ON room_deliveries(agent_id);";

/// Display names of room speakers by agent ID, remembered so an agent that
/// has gone keeps its name in the transcript.
pub const ROOM_NAMES_KEY: &str = "room_names_v1";

/// The line an agent writes to post a message to its room from any turn.
pub const ROOM_MARKER: &str = "[panetone:room]";

const ROOMS_KEY: &str = "rooms_v1";

/// A room's settings.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoomSettings {
    /// Agents' messages are not passed to the other agents while paused; the
    /// user's still are.
    #[serde(default)]
    pub paused: bool,
    /// Other routes whose agents are in the room. Each keeps its own channels,
    /// and only the turns a room message started, or messages marked with
    /// [`ROOM_MARKER`], join the room.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<RouteId>,
    /// Single agents in the room, by Wakterm agent ID, each without the
    /// other agents of its tab; they are members as a member route's agents
    /// are.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agents: Vec<String>,
}

/// The room an agent of a route is in: the route itself when it is a room,
/// or the room that lists the route or the agent as a member.
pub fn room_of<'a>(
    rooms: &'a std::collections::BTreeMap<String, RoomSettings>,
    route_id: RouteId,
    agent_id: &str,
) -> Option<(RouteId, &'a RoomSettings)> {
    if let Some(settings) = rooms.get(&route_id.to_string()) {
        return Some((route_id, settings));
    }
    rooms.iter().find_map(|(key, settings)| {
        (settings.members.contains(&route_id)
            || settings.agents.iter().any(|agent| agent == agent_id))
        .then(|| uuid::Uuid::parse_str(key).ok())
        .flatten()
        .map(|room| (RouteId::from(room), settings))
    })
}

/// A message without its standalone [`ROOM_MARKER`] lines outside code, or
/// `None` when it has none.
pub fn take_marker(body: &str) -> Option<String> {
    let mut fence: Option<&str> = None;
    let mut found = false;
    let mut kept = Vec::new();
    for line in body.split('\n') {
        let trimmed = line.trim();
        if let Some(marker) = fence {
            if trimmed.starts_with(marker) {
                fence = None;
            }
        } else if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            fence = Some(&trimmed[..3]);
        } else if trimmed == ROOM_MARKER && !line.starts_with("    ") && !line.starts_with('\t') {
            found = true;
            continue;
        }
        kept.push(line);
    }
    let mut clean = kept.join("\n").trim().to_owned();
    while clean.contains("\n\n\n") {
        clean = clean.replace("\n\n\n", "\n\n");
    }
    found.then_some(clean)
}

/// Whether the turn of an agent's event was started by a room message.
pub(super) fn is_room_turn(
    connection: &Connection,
    event: &crate::wakterm::EventRecord,
) -> StoreResult<bool> {
    let input = super::turn_input(connection, event)?;
    if input.request_id.is_none() && input.input_sha256.is_none() {
        return Ok(false);
    }
    Ok(connection
        .query_row(
            "SELECT 1 FROM room_deliveries
             WHERE agent_id = ?1
               AND (json_extract(record_json, '$.request_id') = ?3
                    OR ?2 IN (
                        json_extract(record_json, '$.input_sha256'),
                        json_extract(record_json, '$.tab_expanded_sha256')
                    ))
             LIMIT 1",
            params![event.agent_id, input.input_sha256, input.request_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

/// One message in a room's transcript.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoomMessage {
    #[serde(default)]
    pub id: i64,
    /// `user` for the owner and other people, `agent` for an agent.
    pub kind: String,
    /// The person's name, or the agent's ID.
    pub speaker: String,
    /// The agent's harness, such as `claude`, for an agent message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    pub text: String,
    pub created_at_ms: i64,
}

/// A room message on its way to one agent.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoomDelivery {
    #[serde(default)]
    pub id: i64,
    pub route_id: RouteId,
    pub agent_id: String,
    /// The speaker's agent ID, or the person's name, resolved to a display
    /// name when the message is delivered.
    pub speaker: String,
    pub speaker_is_agent: bool,
    pub text: String,
    #[serde(default)]
    pub attempts: u32,
    /// Hashes of the prompt as delivered, as Wakterm reports its input, so
    /// the turn that received it can be recognized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tab_expanded_sha256: Option<String>,
    /// The Wakterm request that typed the prompt, which Wakterm names on the
    /// input's record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

pub(super) fn rooms(
    connection: &Connection,
) -> StoreResult<std::collections::BTreeMap<String, RoomSettings>> {
    Ok(get_metadata(connection, ROOMS_KEY)?
        .map(|json| serde_json::from_str(&json))
        .transpose()?
        .unwrap_or_default())
}

pub(super) fn set_room(
    connection: &mut Connection,
    route_id: RouteId,
    settings: Option<RoomSettings>,
) -> StoreResult<()> {
    let mut all = rooms(connection)?;
    match settings {
        Some(settings) => all.insert(route_id.to_string(), settings),
        None => all.remove(&route_id.to_string()),
    };
    set_metadata(connection, ROOMS_KEY, &serde_json::to_string(&all)?)
}

/// Adds a message to a room's transcript and queues it for each of
/// `recipients`. Returns the message with its ID.
pub(super) fn post(
    transaction: &Transaction<'_>,
    route_id: RouteId,
    mut message: RoomMessage,
    recipients: &[String],
) -> StoreResult<RoomMessage> {
    transaction.execute(
        "INSERT INTO room_messages(route_id, record_json, created_at_ms) VALUES (?1, ?2, ?3)",
        params![
            route_id.to_string(),
            serde_json::to_string(&message)?,
            message.created_at_ms
        ],
    )?;
    message.id = transaction.last_insert_rowid();
    for agent_id in recipients {
        let delivery = RoomDelivery {
            id: 0,
            route_id,
            agent_id: agent_id.clone(),
            speaker: message.speaker.clone(),
            speaker_is_agent: message.kind == "agent",
            text: message.text.clone(),
            attempts: 0,
            input_sha256: None,
            tab_expanded_sha256: None,
            request_id: None,
        };
        transaction.execute(
            "INSERT INTO room_deliveries(route_id, agent_id, state, record_json, created_at_ms)
             VALUES (?1, ?2, 'pending', ?3, ?4)",
            params![
                route_id.to_string(),
                agent_id,
                serde_json::to_string(&delivery)?,
                message.created_at_ms
            ],
        )?;
    }
    Ok(message)
}

pub(super) fn messages_after(
    connection: &Connection,
    route_id: RouteId,
    after: i64,
    limit: usize,
) -> StoreResult<Vec<RoomMessage>> {
    // With no starting point, the last `limit` messages.
    let mut statement = connection.prepare(if after < 0 {
        "SELECT id, record_json FROM (
             SELECT id, record_json FROM room_messages WHERE route_id = ?1
             ORDER BY id DESC LIMIT ?3
         ) ORDER BY id"
    } else {
        "SELECT id, record_json FROM room_messages
         WHERE route_id = ?1 AND id > ?2 ORDER BY id LIMIT ?3"
    })?;
    let rows = statement
        .query_map(params![route_id.to_string(), after, limit as i64], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(id, json)| {
            let mut message: RoomMessage = serde_json::from_str(&json)?;
            message.id = id;
            Ok(message)
        })
        .collect()
}

pub(super) fn pending_deliveries(connection: &Connection) -> StoreResult<Vec<RoomDelivery>> {
    let mut statement = connection.prepare(
        "SELECT id, record_json FROM room_deliveries WHERE state = 'pending' ORDER BY id",
    )?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(id, json)| {
            let mut delivery: RoomDelivery = serde_json::from_str(&json)?;
            delivery.id = id;
            Ok(delivery)
        })
        .collect()
}

pub(super) fn set_delivery(
    connection: &mut Connection,
    delivery: &RoomDelivery,
    state: &str,
) -> StoreResult<()> {
    connection.execute(
        "UPDATE room_deliveries SET state = ?2, record_json = ?3 WHERE id = ?1",
        params![delivery.id, state, serde_json::to_string(delivery)?],
    )?;
    Ok(())
}

/// How many different people have spoken in a room.
pub(super) fn people(connection: &Connection, route_id: RouteId) -> StoreResult<usize> {
    Ok(connection.query_row(
        "SELECT COUNT(DISTINCT json_extract(record_json, '$.speaker')) FROM room_messages
         WHERE route_id = ?1 AND json_extract(record_json, '$.kind') = 'user'",
        params![route_id.to_string()],
        |row| row.get::<_, i64>(0),
    )? as usize)
}

pub(super) fn latest_message_id(connection: &Connection, route_id: RouteId) -> StoreResult<i64> {
    Ok(connection
        .query_row(
            "SELECT MAX(id) FROM room_messages WHERE route_id = ?1",
            params![route_id.to_string()],
            |row| row.get::<_, Option<i64>>(0),
        )
        .optional()?
        .flatten()
        .unwrap_or(0))
}
