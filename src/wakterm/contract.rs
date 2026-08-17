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

#[derive(Clone, Debug, PartialEq)]
pub enum EventRead {
    Events {
        events: Vec<EventRecord>,
        next_after_sequence: u64,
    },
    CursorTooOld {
        requested_after_sequence: u64,
        oldest_available_sequence: u64,
        latest_sequence: u64,
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

        let (events, oldest_available_sequence, latest_sequence) = match profile {
            ProfileKind::Current => {
                if capabilities.contains("event_stream.v1") {
                    return Err(ContractError::Invalid(
                        "current profile must not enable general events",
                    ));
                }
                (Vec::new(), 0, 0)
            }
            ProfileKind::FutureEvents => {
                if !capabilities.contains("event_stream.v1") {
                    return Err(ContractError::Invalid(
                        "future profile must enable general events",
                    ));
                }
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
            }
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
