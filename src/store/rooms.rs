//! Rooms: routes where everyone sees everything. Each message, from the user
//! or from one of the route's agents, is kept in the room's transcript and
//! delivered to every other agent in the room.

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
     CREATE INDEX IF NOT EXISTS room_deliveries_state ON room_deliveries(state, id);";

const ROOMS_KEY: &str = "rooms_v1";

/// A room's settings.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoomSettings {
    /// Agents' messages are not passed to the other agents while paused; the
    /// user's still are.
    #[serde(default)]
    pub paused: bool,
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
