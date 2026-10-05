use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::domain::AgentBinding;

const AGENT_API_SCHEMA: &str = "wakterm.agent-api.v1";
const EVENT_SCHEMA: &str = "wakterm.agent-events.v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileKind {
    Current,
    FutureEvents,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogAgent {
    pub agent_id: String,
    pub incarnation_id: Option<String>,
    pub pane_id: u64,
    pub name: String,
    pub harness: String,
    pub status: String,
    pub turn_state: String,
    pub alive: bool,
    pub observed_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentCatalog {
    pub schema: String,
    #[serde(default)]
    pub as_of_event_sequence: u64,
    pub agents: Vec<CatalogAgent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EventRecord {
    pub sequence: u64,
    pub event_id: String,
    pub kind: String,
    pub agent_id: String,
    pub incarnation_id: String,
    #[serde(flatten)]
    pub fields: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApprovalChoice {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApprovalRequest {
    pub schema: String,
    #[serde(default = "command_approval_kind")]
    pub kind: String,
    pub request_id: String,
    pub agent_id: String,
    pub incarnation_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub observed_at: String,
    #[serde(default)]
    pub prompt: Option<String>,
    pub reason: Option<String>,
    pub command: Option<String>,
    pub cwd: Option<String>,
    pub choices: Vec<ApprovalChoice>,
}

fn command_approval_kind() -> String {
    "command_approval".to_string()
}

impl EventRecord {
    pub fn approval(&self) -> Result<Option<ApprovalRequest>, &'static str> {
        if self.kind != "approval_requested" {
            return Ok(None);
        }
        let value = self
            .fields
            .get("approval")
            .cloned()
            .ok_or("approval event has no request")?;
        let approval: ApprovalRequest =
            serde_json::from_value(value).map_err(|_| "approval event has an invalid request")?;
        if approval.schema != "wakterm.agent-approval.v1"
            || approval.agent_id != self.agent_id
            || approval.incarnation_id != self.incarnation_id
            // A question form lists its questions in the prompt and is
            // answered in the agent's pane, so it has no choices.
            || approval.choices.is_empty() != (approval.kind == "user_question_form")
        {
            return Err("approval event identity or choices are invalid");
        }
        Ok(Some(approval))
    }

    pub fn visible_output_body(&self) -> Result<Option<String>, &'static str> {
        match self.kind.as_str() {
            "plan" => self
                .text()
                .map(|text| Some(format!("Plan:\n{text}")))
                .ok_or("visible output event has no text"),
            "assistant_message" => self
                .text()
                .map(|text| Some(text.to_owned()))
                .ok_or("visible output event has no text"),
            // A turn that ends without a reply is reported to the sender through
            // the unconfirmed-delivery notice, so it is not shown again.
            "turn_final"
                if self.fields.get("outcome").and_then(Value::as_str) == Some("aborted")
                    && self.fields.get("reason").and_then(Value::as_str) == Some("no_reply") =>
            {
                Ok(None)
            }
            "turn_final"
                if self.fields.get("outcome").and_then(Value::as_str) == Some("aborted") =>
            {
                Ok(self
                    .fields
                    .get("detail")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|detail| !detail.is_empty())
                    .map(|detail| format!("Turn failed: {detail}")))
            }
            _ => Ok(None),
        }
    }

    fn text(&self) -> Option<&str> {
        self.fields.get("text").and_then(Value::as_str)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum EventRead {
    Events {
        events: Vec<EventRecord>,
        next_after_sequence: u64,
        latest_sequence: u64,
    },
    CursorTooOld {
        requested_after_sequence: u64,
        oldest_available_sequence: u64,
        latest_sequence: u64,
        catalog_as_of_sequence: u64,
    },
    Unsupported,
}

#[derive(Clone, Debug)]
pub struct WaktermContract {
    pub profile: ProfileKind,
    pub capabilities: BTreeSet<String>,
    pub catalog: AgentCatalog,
    events: Vec<EventRecord>,
    oldest_available_sequence: u64,
    latest_sequence: u64,
}

#[derive(Debug, Error)]
pub enum ContractError {
    #[error("invalid Wakterm fixture JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Wakterm API major version {0} is incompatible with v1")]
    IncompatibleMajor(u64),
    #[error("Wakterm fixture violates the v1 contract: {0}")]
    Invalid(&'static str),
    #[error("no live catalog agent has pane id {0}")]
    MissingPane(u64),
    #[error("pane id {0} is ambiguous in the catalog")]
    AmbiguousPane(u64),
    #[error("live catalog agent on pane {0} has no process incarnation")]
    MissingIncarnation(u64),
    #[error("the catalog changed agent identity while resolving the route")]
    UnstableCatalog,
}

impl WaktermContract {
    pub fn from_golden_json(json: &str, profile: ProfileKind) -> Result<Self, ContractError> {
        let root: Value = serde_json::from_str(json)?;
        if root["fixture_schema"] != "wakterm.agent-api-golden.v1" {
            return Err(ContractError::Invalid("unexpected fixture schema"));
        }
        let capability_key = match profile {
            ProfileKind::Current => "current_capabilities",
            ProfileKind::FutureEvents => "event_stream_capabilities",
        };
        let capabilities = &root[capability_key];
        if capabilities["schema"] != AGENT_API_SCHEMA {
            return Err(ContractError::Invalid("unexpected Agent API schema"));
        }
        let major = capabilities["api_major"]
            .as_u64()
            .ok_or(ContractError::Invalid("missing API major"))?;
        if major != 1 {
            return Err(ContractError::IncompatibleMajor(major));
        }
        let capabilities = capabilities["capabilities"]
            .as_array()
            .ok_or(ContractError::Invalid("missing capabilities"))?
            .iter()
            .map(|capability| {
                capability
                    .as_str()
                    .map(str::to_owned)
                    .ok_or(ContractError::Invalid("capability must be a string"))
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        for required in [
            "catalog.v1",
            "prompt_admission.v1",
            "return_request_terminal_stream.v1",
        ] {
            if !capabilities.contains(required) {
                return Err(ContractError::Invalid("required capability is absent"));
            }
        }
        if profile == ProfileKind::FutureEvents && !capabilities.contains("event_stream.v1") {
            return Err(ContractError::Invalid(
                "event fixture profile must enable general events",
            ));
        }
        let catalog: AgentCatalog = serde_json::from_value(root["catalog"].clone())?;
        if catalog.schema != AGENT_API_SCHEMA {
            return Err(ContractError::Invalid("catalog schema mismatch"));
        }
        let mut panes = BTreeSet::new();
        if catalog
            .agents
            .iter()
            .any(|agent| !panes.insert(agent.pane_id))
        {
            return Err(ContractError::Invalid("catalog pane ids are not unique"));
        }

        let (events, oldest_available_sequence, latest_sequence) =
            if capabilities.contains("event_stream.v1") {
                let mut events = parse_page(&root["event_page"])?;
                events.extend(parse_page(&root["lifecycle_page"])?);
                events.sort_by_key(|event| event.sequence);
                let retention = &root["retention"];
                let oldest = retention["oldest_available_sequence"]
                    .as_u64()
                    .ok_or(ContractError::Invalid("missing oldest sequence"))?;
                let latest = retention["latest_sequence"]
                    .as_u64()
                    .ok_or(ContractError::Invalid("missing latest sequence"))?;
                validate_events(&events)?;
                (events, oldest, latest)
            } else {
                (Vec::new(), 0, 0)
            };
        Ok(Self {
            profile,
            capabilities,
            catalog,
            events,
            oldest_available_sequence,
            latest_sequence,
        })
    }

    pub fn general_event_consumer_enabled(&self) -> bool {
        self.capabilities.contains("event_stream.v1")
    }

    pub fn read_events(&self, after_sequence: u64) -> Result<EventRead, ContractError> {
        if !self.general_event_consumer_enabled() {
            return Ok(EventRead::Unsupported);
        }
        if after_sequence.saturating_add(1) < self.oldest_available_sequence {
            return Ok(EventRead::CursorTooOld {
                requested_after_sequence: after_sequence,
                oldest_available_sequence: self.oldest_available_sequence,
                latest_sequence: self.latest_sequence,
                catalog_as_of_sequence: self.catalog.as_of_event_sequence,
            });
        }
        let events = self
            .events
            .iter()
            .filter(|event| event.sequence > after_sequence)
            .cloned()
            .collect::<Vec<_>>();
        let next = events.last().map_or(after_sequence, |event| event.sequence);
        Ok(EventRead::Events {
            events,
            next_after_sequence: next,
            latest_sequence: self.latest_sequence,
        })
    }
}

pub fn join_catalog_binding(
    pane_id: u64,
    before: &AgentCatalog,
    after: &AgentCatalog,
) -> Result<AgentBinding, ContractError> {
    let first = live_agent_for_pane(pane_id, before)?;
    let second = live_agent_for_pane(pane_id, after)?;
    if first.agent_id != second.agent_id || first.incarnation_id != second.incarnation_id {
        return Err(ContractError::UnstableCatalog);
    }
    let incarnation_id = second
        .incarnation_id
        .clone()
        .ok_or(ContractError::MissingIncarnation(pane_id))?;
    Ok(AgentBinding {
        agent_id: second.agent_id.clone(),
        incarnation_id,
        harness: second.harness.clone(),
        pane_id: Some(pane_id),
    })
}

fn live_agent_for_pane(
    pane_id: u64,
    catalog: &AgentCatalog,
) -> Result<&CatalogAgent, ContractError> {
    let matches = catalog
        .agents
        .iter()
        .filter(|agent| agent.alive && agent.pane_id == pane_id)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err(ContractError::MissingPane(pane_id)),
        [agent] => Ok(agent),
        _ => Err(ContractError::AmbiguousPane(pane_id)),
    }
}

fn parse_page(value: &Value) -> Result<Vec<EventRecord>, ContractError> {
    if value["schema"] != EVENT_SCHEMA || value["status"] != "ok" {
        return Err(ContractError::Invalid(
            "event page schema or status mismatch",
        ));
    }
    serde_json::from_value(value["events"].clone()).map_err(ContractError::from)
}

fn validate_events(events: &[EventRecord]) -> Result<(), ContractError> {
    let mut previous = None;
    for event in events {
        if previous.is_some_and(|sequence| sequence >= event.sequence) {
            return Err(ContractError::Invalid("event sequences are not increasing"));
        }
        if !matches!(
            event.kind.as_str(),
            "agent_lifecycle"
                | "approval_requested"
                | "turn_started"
                | "turn_state_changed"
                | "plan"
                | "assistant_message"
                | "observer_failure"
                | "turn_final"
        ) {
            return Err(ContractError::Invalid("unknown event kind"));
        }
        previous = Some(event.sequence);
    }
    Ok(())
}

/// Output observed this recently before a restart is still delivered.
pub const RECENT_OUTPUT_MS: i64 = 10 * 60 * 1000;

/// The event cursor to resume from after a restart, given the events produced
/// while Panetone was stopped. Older output is skipped so a long outage does not
/// flood the channels, but recent output and questions an agent is still waiting
/// on are delivered: resumption starts just before the first such event, or at
/// `head` when there is none.
pub fn resume_cursor(events: &[EventRecord], head: u64, now_ms: i64) -> u64 {
    events
        .iter()
        .enumerate()
        .find(|(index, event)| {
            let recent = event
                .fields
                .get("observed_at")
                .and_then(Value::as_str)
                .and_then(utc_millis)
                .is_none_or(|observed| observed >= now_ms - RECENT_OUTPUT_MS);
            recent
                || (event.kind == "approval_requested" && unanswered(event, &events[index + 1..]))
        })
        .map_or(head, |(_, event)| event.sequence - 1)
}

/// A question is unanswered while its agent incarnation has made no progress
/// after asking it.
fn unanswered(question: &EventRecord, later: &[EventRecord]) -> bool {
    !later.iter().any(|event| {
        event.agent_id == question.agent_id
            && event.incarnation_id == question.incarnation_id
            && matches!(
                event.kind.as_str(),
                "turn_started" | "plan" | "assistant_message" | "turn_final" | "agent_lifecycle"
            )
    })
}

/// Milliseconds since the Unix epoch for an RFC 3339 timestamp such as
/// `2026-10-05T03:48:29.668Z`.
fn utc_millis(timestamp: &str) -> Option<i64> {
    let parsed =
        time::OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339)
            .ok()?;
    i64::try_from(parsed.unix_timestamp_nanos() / 1_000_000).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_791_172_109_668; // 2026-10-05T03:48:29.668Z

    fn event(sequence: u64, agent: &str, kind: &str, observed_at: &str) -> EventRecord {
        EventRecord {
            sequence,
            event_id: format!("event-{sequence}"),
            kind: kind.into(),
            agent_id: agent.into(),
            incarnation_id: format!("{agent}-1"),
            fields: serde_json::Map::from_iter([("observed_at".into(), observed_at.into())]),
        }
    }

    const OLD: &str = "2026-10-05T03:30:00Z";
    const RECENT: &str = "2026-10-05T03:45:00.5Z";

    #[test]
    fn utc_timestamps_convert_to_epoch_milliseconds() {
        assert_eq!(utc_millis("2026-10-05T03:48:29.668Z"), Some(NOW));
        assert_eq!(
            utc_millis("2024-02-29T23:59:59.005123Z"),
            Some(1_709_251_199_005)
        );
        assert_eq!(utc_millis("2026-10-05 03:48:29"), None);
    }

    #[test]
    fn stale_output_is_skipped_and_recent_output_is_replayed() {
        let events = [
            event(11, "a", "assistant_message", OLD),
            event(12, "a", "assistant_message", RECENT),
        ];
        assert_eq!(resume_cursor(&events[..1], 20, NOW), 20);
        assert_eq!(resume_cursor(&events, 20, NOW), 11);
    }

    #[test]
    fn an_unanswered_question_is_replayed_however_old() {
        let events = [
            event(11, "a", "assistant_message", OLD),
            event(12, "a", "approval_requested", OLD),
            event(13, "b", "assistant_message", OLD),
            event(14, "a", "turn_state_changed", OLD),
        ];
        assert_eq!(resume_cursor(&events, 20, NOW), 11);

        let answered = [
            event(12, "a", "approval_requested", OLD),
            event(13, "a", "assistant_message", OLD),
        ];
        assert_eq!(resume_cursor(&answered, 20, NOW), 20);
    }
}
