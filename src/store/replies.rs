//! Replies owed between agents: a request that asks for a reply opens a debt,
//! a message the target tags `[panetone:reply]` settles it, and reminders
//! follow the target's turns until then (ADR 0006).

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

use super::{StoreResult, event_cursor};
use crate::domain::{AgentBinding, EffectId, RouteId, WorkflowId};

pub(super) const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS reply_debts (
         request_id TEXT PRIMARY KEY,
         target_agent_id TEXT NOT NULL,
         state TEXT NOT NULL,
         record_json TEXT NOT NULL,
         created_at_ms INTEGER NOT NULL,
         updated_at_ms INTEGER NOT NULL
     );
     CREATE INDEX IF NOT EXISTS reply_debts_state ON reply_debts(state, target_agent_id);
     CREATE TABLE IF NOT EXISTS reply_messages (
         effect_id TEXT PRIMARY KEY,
         agent_id TEXT NOT NULL,
         kind TEXT NOT NULL,
         state TEXT NOT NULL,
         record_json TEXT NOT NULL,
         created_at_ms INTEGER NOT NULL
     );
     CREATE INDEX IF NOT EXISTS reply_messages_state ON reply_messages(state, kind);
     CREATE TABLE IF NOT EXISTS reply_reminders (
         effect_id TEXT PRIMARY KEY,
         agent_id TEXT NOT NULL,
         turn_id TEXT NOT NULL,
         input_sha256 TEXT NOT NULL,
         created_at_ms INTEGER NOT NULL
     );
     CREATE INDEX IF NOT EXISTS reply_reminders_turn ON reply_reminders(agent_id, turn_id);";

/// A reply the target owes the sender of a request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplyDebt {
    pub request_id: WorkflowId,
    /// The sender as it named itself in the request, which a reply is sent to.
    pub source: String,
    pub source_route_id: RouteId,
    pub source_agent: AgentBinding,
    pub target: String,
    pub target_route_id: RouteId,
    pub target_agent_id: String,
    /// The first line of the request, quoted in replies and reminders.
    pub summary: String,
    /// Turns that end after this event sequence can be followed by a reminder.
    #[serde(default)]
    pub after_sequence: u64,
    pub created_at_ms: i64,
    /// `open`, `replied` or `cancelled`.
    pub state: String,
}

/// A tagged message on its way to the sender, or a tag that matched no debt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplyMessage {
    pub id: EffectId,
    pub request_id: Option<WorkflowId>,
    /// `final`, `progress` or `invalid`.
    pub kind: String,
    pub agent_id: String,
    pub route_id: RouteId,
    /// The message without its tag, or for an invalid tag why it matched nothing.
    pub body: String,
    pub created_at_ms: i64,
    /// `pending`, then `sent` or `failed`; an invalid tag becomes `reported`.
    pub state: String,
}

/// How the last turn of an agent ended, for deciding on a reminder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TurnEnd {
    pub turn_id: String,
    pub ended_at_ms: i64,
    /// Another turn of the agent started after this one ended.
    pub superseded: bool,
    /// A reminder started this turn.
    pub started_by_reminder: bool,
    /// A reminder already followed this turn.
    pub reminded: bool,
}

pub(super) fn open_debt(connection: &mut Connection, mut debt: ReplyDebt) -> StoreResult<()> {
    // An endchat agent's events are numbered apart from Wakterm's.
    debt.after_sequence = if debt.target_agent_id.starts_with("endchat:") {
        super::ENDCHAT_SEQUENCE_BASE
            + super::get_metadata(connection, "endchat_event_count")?
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0)
    } else {
        event_cursor(connection)?.unwrap_or(0)
    };
    debt.state = "open".into();
    connection.execute(
        "INSERT OR IGNORE INTO reply_debts(
             request_id, target_agent_id, state, record_json, created_at_ms, updated_at_ms
         ) VALUES (?1, ?2, 'open', ?3, ?4, ?4)",
        params![
            debt.request_id.to_string(),
            debt.target_agent_id,
            serde_json::to_string(&debt)?,
            debt.created_at_ms,
        ],
    )?;
    Ok(())
}

pub(super) fn debt(
    connection: &Connection,
    request_id: WorkflowId,
) -> StoreResult<Option<ReplyDebt>> {
    connection
        .query_row(
            "SELECT record_json FROM reply_debts WHERE request_id = ?1",
            params![request_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(Into::into))
        .transpose()
}

pub(super) fn open_debts(connection: &Connection) -> StoreResult<Vec<ReplyDebt>> {
    let mut statement = connection.prepare(
        "SELECT record_json FROM reply_debts WHERE state = 'open' ORDER BY created_at_ms",
    )?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    rows.iter()
        .map(|json| serde_json::from_str(json).map_err(Into::into))
        .collect()
}

fn set_debt_state(
    connection: &Connection,
    request_id: &str,
    state: &str,
    now_ms: i64,
) -> StoreResult<Option<String>> {
    let Some(json) = connection
        .query_row(
            "SELECT record_json FROM reply_debts WHERE request_id = ?1",
            params![request_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    else {
        return Ok(None);
    };
    let mut debt: ReplyDebt = serde_json::from_str(&json)?;
    let previous = debt.state.clone();
    if previous == "open" {
        debt.state = state.into();
        connection.execute(
            "UPDATE reply_debts SET state = ?2, record_json = ?3, updated_at_ms = ?4
             WHERE request_id = ?1",
            params![request_id, state, serde_json::to_string(&debt)?, now_ms],
        )?;
    }
    Ok(Some(previous))
}

/// Withdraws an open debt. Returns its state before, or `None` when no
/// request with that ID asked for a reply.
pub(super) fn cancel_debt(
    connection: &mut Connection,
    request_id: WorkflowId,
    now_ms: i64,
) -> StoreResult<Option<String>> {
    set_debt_state(connection, &request_id.to_string(), "cancelled", now_ms)
}

/// The kind of tag on an agent's message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TagKind {
    Final,
    Progress,
}

/// The reply and progress tags in one message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Tagged {
    /// Each tag with the request ID or prefix it names and the text it
    /// answers. One tag answers with the whole message. With several, each
    /// takes the text since the previous tag, and the last also takes any
    /// text after it.
    pub sections: Vec<(TagKind, Option<String>, String)>,
    /// The message without its tag lines, as shown in the agent's chat.
    pub clean: String,
}

/// Finds the standalone reply and progress tags outside code in a message.
pub(super) fn find_tags(body: &str) -> Option<Tagged> {
    let mut tags = Vec::new();
    let mut retained = Vec::new();
    // The retained lines before each tag, so each section can be cut out.
    let mut boundaries = Vec::new();
    let mut fence: Option<&str> = None;
    for line in body.split('\n') {
        let trimmed = line.trim();
        if let Some(marker) = fence {
            if trimmed.starts_with(marker) {
                fence = None;
            }
            retained.push(line);
            continue;
        }
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            fence = Some(&trimmed[..3]);
            retained.push(line);
            continue;
        }
        let indented = line.starts_with("    ") || line.starts_with('\t');
        let tag = trimmed
            .strip_prefix("[panetone:")
            .and_then(|rest| rest.strip_suffix(']'))
            .and_then(|inner| {
                let (name, id) = inner.split_once(' ').unwrap_or((inner, ""));
                let kind = match name {
                    "reply" => TagKind::Final,
                    "progress" => TagKind::Progress,
                    _ => return None,
                };
                let id = id.trim();
                Some((kind, (!id.is_empty()).then(|| id.to_owned())))
            });
        match tag {
            Some(tag) if !indented => {
                tags.push(tag);
                boundaries.push(retained.len());
            }
            _ => retained.push(line),
        }
    }
    if tags.is_empty() {
        return None;
    }
    let text = |lines: &[&str]| lines.join("\n").trim().to_owned();
    // A removed tag line leaves no extra blank lines behind.
    let mut clean = text(&retained);
    while clean.contains("\n\n\n") {
        clean = clean.replace("\n\n\n", "\n\n");
    }
    let sections = if tags.len() == 1 {
        tags.into_iter()
            .map(|(kind, id)| (kind, id, clean.clone()))
            .collect()
    } else {
        let count = tags.len();
        tags.into_iter()
            .enumerate()
            .map(|(index, (kind, id))| {
                let start = if index == 0 { 0 } else { boundaries[index - 1] };
                let end = if index + 1 == count {
                    retained.len()
                } else {
                    boundaries[index]
                };
                (kind, id, text(&retained[start..end]))
            })
            .collect()
    };
    Some(Tagged { sections, clean })
}

/// Settles or reports a tag an agent wrote, in the event ingest transaction.
/// Returns whether the tag matched an open debt.
#[allow(clippy::too_many_arguments)]
pub(super) fn record_tag(
    transaction: &Transaction<'_>,
    id: EffectId,
    agent_id: &str,
    route_id: RouteId,
    kind: TagKind,
    named: Option<&str>,
    body: &str,
    now_ms: i64,
) -> StoreResult<bool> {
    let owed = open_debts(transaction)?
        .into_iter()
        .filter(|debt| debt.target_agent_id == agent_id || debt.target_route_id == route_id)
        .collect::<Vec<_>>();
    // An agent's own debts come before others owed by its route.
    let own = owed
        .iter()
        .filter(|debt| debt.target_agent_id == agent_id)
        .collect::<Vec<_>>();
    let candidates = if own.is_empty() {
        owed.iter().collect::<Vec<_>>()
    } else {
        own
    };
    let matching = candidates
        .into_iter()
        .filter(|debt| named.is_none_or(|named| debt.request_id.to_string().starts_with(named)))
        .collect::<Vec<_>>();
    let (debt, error) = match (matching.as_slice(), named) {
        ([debt], _) => (Some(*debt), None),
        ([], Some(named)) => (
            None,
            Some(format!(
                "[panetone:reply {named}] matched no request you owe a reply to"
            )),
        ),
        ([], None) => (
            None,
            Some("a reply tag was written while no reply is owed".to_owned()),
        ),
        _ => (
            None,
            Some(
                "a reply tag without a request ID was written while several replies are owed; name one"
                    .to_owned(),
            ),
        ),
    };
    let message = ReplyMessage {
        id,
        request_id: debt.map(|debt| debt.request_id),
        kind: match (debt, kind) {
            (None, _) => "invalid",
            (Some(_), TagKind::Final) => "final",
            (Some(_), TagKind::Progress) => "progress",
        }
        .into(),
        agent_id: agent_id.into(),
        route_id,
        body: error.unwrap_or_else(|| body.to_owned()),
        created_at_ms: now_ms,
        state: "pending".into(),
    };
    transaction.execute(
        "INSERT OR IGNORE INTO reply_messages(
             effect_id, agent_id, kind, state, record_json, created_at_ms
         ) VALUES (?1, ?2, ?3, 'pending', ?4, ?5)",
        params![
            message.id.to_string(),
            message.agent_id,
            message.kind,
            serde_json::to_string(&message)?,
            now_ms,
        ],
    )?;
    if let (Some(debt), TagKind::Final) = (debt, kind) {
        set_debt_state(transaction, &debt.request_id.to_string(), "replied", now_ms)?;
    }
    Ok(debt.is_some())
}

/// Tagged messages waiting to be sent to their senders.
pub(super) fn pending_messages(connection: &Connection) -> StoreResult<Vec<ReplyMessage>> {
    messages(
        connection,
        "kind IN ('final', 'progress') AND state = 'pending'",
        &[],
    )
}

/// Invalid tags of an agent not yet reported to it.
pub(super) fn unreported_tags(
    connection: &Connection,
    agent_id: &str,
) -> StoreResult<Vec<ReplyMessage>> {
    messages(
        connection,
        "kind = 'invalid' AND state = 'pending' AND agent_id = ?1",
        &[agent_id],
    )
}

fn messages(
    connection: &Connection,
    condition: &str,
    values: &[&str],
) -> StoreResult<Vec<ReplyMessage>> {
    let mut statement = connection.prepare(&format!(
        "SELECT record_json FROM reply_messages WHERE {condition} ORDER BY created_at_ms"
    ))?;
    let rows = statement
        .query_map(rusqlite::params_from_iter(values), |row| {
            row.get::<_, String>(0)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.iter()
        .map(|json| serde_json::from_str(json).map_err(Into::into))
        .collect()
}

pub(super) fn set_message_state(
    connection: &mut Connection,
    id: EffectId,
    state: &str,
) -> StoreResult<()> {
    let Some(json) = connection
        .query_row(
            "SELECT record_json FROM reply_messages WHERE effect_id = ?1",
            params![id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    else {
        return Ok(());
    };
    let mut message: ReplyMessage = serde_json::from_str(&json)?;
    message.state = state.into();
    connection.execute(
        "UPDATE reply_messages SET state = ?2, record_json = ?3 WHERE effect_id = ?1",
        params![id.to_string(), state, serde_json::to_string(&message)?],
    )?;
    Ok(())
}

/// The most recent turn of an agent that ended after an event sequence.
pub(super) fn last_turn_end(
    connection: &Connection,
    agent_id: &str,
    after_sequence: u64,
) -> StoreResult<Option<TurnEnd>> {
    let Some((sequence, turn_id, ended_at_ms)) = connection
        .query_row(
            "SELECT sequence, json_extract(record_json, '$.turn_id'), created_at_ms
             FROM agent_events
             WHERE agent_id = ?1 AND kind = 'turn_final' AND sequence > ?2
             ORDER BY sequence DESC LIMIT 1",
            params![agent_id, after_sequence],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?
    else {
        return Ok(None);
    };
    let Some(turn_id) = turn_id else {
        return Ok(None);
    };
    let superseded = connection
        .query_row(
            "SELECT 1 FROM agent_events
             WHERE agent_id = ?1 AND kind = 'turn_started' AND sequence > ?2 LIMIT 1",
            params![agent_id, sequence],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    let started_by_reminder = connection
        .query_row(
            "SELECT 1 FROM agent_events AS started
             JOIN reply_reminders AS reminder
               ON reminder.agent_id = started.agent_id
              AND reminder.input_sha256 = json_extract(started.record_json, '$.input_sha256')
             WHERE started.agent_id = ?1 AND started.kind = 'turn_started'
               AND json_extract(started.record_json, '$.turn_id') = ?2
             LIMIT 1",
            params![agent_id, turn_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    let reminded = connection
        .query_row(
            "SELECT 1 FROM reply_reminders WHERE agent_id = ?1 AND turn_id = ?2",
            params![agent_id, turn_id],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    Ok(Some(TurnEnd {
        turn_id,
        ended_at_ms,
        superseded,
        started_by_reminder,
        reminded,
    }))
}

/// Records a reminder sent after a turn, and marks the invalid tags it
/// reported.
pub(super) fn record_reminder(
    connection: &mut Connection,
    id: EffectId,
    agent_id: &str,
    turn_id: &str,
    input_sha256: &str,
    reported: &[EffectId],
    now_ms: i64,
) -> StoreResult<()> {
    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT OR IGNORE INTO reply_reminders(
             effect_id, agent_id, turn_id, input_sha256, created_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id.to_string(), agent_id, turn_id, input_sha256, now_ms],
    )?;
    transaction.commit()?;
    for id in reported {
        set_message_state(connection, *id, "reported")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TagKind, find_tags};

    fn find_tag(body: &str) -> Option<(TagKind, Option<String>, String)> {
        find_tags(body).map(|tagged| tagged.sections[0].clone())
    }

    #[test]
    fn a_standalone_tag_outside_code_is_found_and_removed() {
        assert_eq!(
            find_tag("All 23 trials passed.\n[panetone:reply]\n"),
            Some((TagKind::Final, None, "All 23 trials passed.".into()))
        );
        assert_eq!(
            find_tag("[panetone:progress 1a2b3c4d]\nStill running."),
            Some((
                TagKind::Progress,
                Some("1a2b3c4d".into()),
                "Still running.".into()
            ))
        );
        // Quoted in code, inline, or indented, a tag is text.
        for text in [
            "```\n[panetone:reply]\n```",
            "Write [panetone:reply] when done.",
            "    [panetone:reply]",
            "[panetone:attach /tmp/x]",
        ] {
            assert_eq!(find_tag(text), None, "{text:?}");
        }
    }

    #[test]
    fn several_tags_each_answer_the_text_before_them() {
        let tagged = find_tags(
            "It shipped as gs_update.\n\n[panetone:reply a85a1456]\n\nThe same answer covers your follow-up.\n\n[panetone:reply d2e9a870]",
        )
        .unwrap();
        assert_eq!(
            tagged.sections,
            [
                (
                    TagKind::Final,
                    Some("a85a1456".into()),
                    "It shipped as gs_update.".into()
                ),
                (
                    TagKind::Final,
                    Some("d2e9a870".into()),
                    "The same answer covers your follow-up.".into()
                ),
            ]
        );
        assert_eq!(
            tagged.clean,
            "It shipped as gs_update.\n\nThe same answer covers your follow-up."
        );
    }
}
