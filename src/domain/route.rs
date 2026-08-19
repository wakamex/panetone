use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{ChannelBinding, RouteId};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentBinding {
    pub agent_id: String,
    pub incarnation_id: String,
    pub harness: String,
    pub pane_id: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteStatus {
    Available,
    Unavailable,
    ReconciliationRequired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Route {
    pub id: RouteId,
    pub title: String,
    pub channels: Vec<ChannelBinding>,
    pub agent: Option<AgentBinding>,
    pub status: RouteStatus,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LiveRoute {
    pub title: String,
    pub tab_id: u64,
    pub pane_id: Option<u64>,
    pub harness: Option<String>,
    pub topic_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileDecision {
    Unchanged,
    Rebound,
    Unavailable,
    ReconciliationRequired,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RouteError {
    #[error("no route matches the requested title")]
    NotFound,
    #[error("more than one route matches the requested title")]
    Ambiguous,
    #[error("the route has no live agent or channel binding")]
    Unavailable,
}

pub fn resolve_live_route<'a>(
    locator: &str,
    routes: &'a [LiveRoute],
) -> Result<&'a LiveRoute, RouteError> {
    let locator = locator.to_lowercase();
    let matches = routes
        .iter()
        .filter(|route| route.title.to_lowercase() == locator)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err(RouteError::NotFound),
        [route]
            if route.pane_id.is_none() || route.harness.is_none() || route.topic_id.is_none() =>
        {
            Err(RouteError::Unavailable)
        }
        [route] => Ok(route),
        _ => Err(RouteError::Ambiguous),
    }
}

impl Route {
    pub fn reconcile(&mut self, observed: Option<AgentBinding>) -> ReconcileDecision {
        let Some(observed) = observed else {
            self.status = RouteStatus::Unavailable;
            return ReconcileDecision::Unavailable;
        };
        match self.agent.as_ref() {
            None => {
                self.agent = Some(observed);
                self.status = RouteStatus::Available;
                ReconcileDecision::Rebound
            }
            Some(current)
                if current.agent_id == observed.agent_id
                    && current.incarnation_id == observed.incarnation_id =>
            {
                let changed = current.pane_id != observed.pane_id;
                self.agent = Some(observed);
                self.status = RouteStatus::Available;
                if changed {
                    ReconcileDecision::Rebound
                } else {
                    ReconcileDecision::Unchanged
                }
            }
            Some(_) => {
                self.status = RouteStatus::ReconciliationRequired;
                ReconcileDecision::ReconciliationRequired
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn binding(incarnation: &str, pane_id: u64) -> AgentBinding {
        AgentBinding {
            agent_id: "agent-alpha".into(),
            incarnation_id: incarnation.into(),
            harness: "codex".into(),
            pane_id: Some(pane_id),
        }
    }

    fn route() -> Route {
        Route {
            id: RouteId::new(Uuid::nil()),
            title: "Alpha".into(),
            channels: vec![ChannelBinding::Telegram { topic_id: 101 }],
            agent: Some(binding("process-1", 11)),
            status: RouteStatus::Available,
        }
    }

    #[test]
    fn disappearance_preserves_channel_binding() {
        let mut route = route();
        let binding = route.agent.clone();
        assert_eq!(route.reconcile(None), ReconcileDecision::Unavailable);
        assert_eq!(route.channels.len(), 1);
        assert_eq!(route.agent, binding);
        assert_eq!(route.status, RouteStatus::Unavailable);
    }

    #[test]
    fn incarnation_change_requires_reconciliation() {
        let mut route = route();
        assert_eq!(
            route.reconcile(Some(binding("process-2", 11))),
            ReconcileDecision::ReconciliationRequired
        );
        assert_eq!(route.agent.unwrap().incarnation_id, "process-1");
    }

    #[test]
    fn same_incarnation_can_rebind_ephemeral_pane() {
        let mut route = route();
        assert_eq!(
            route.reconcile(Some(binding("process-1", 44))),
            ReconcileDecision::Rebound
        );
        assert_eq!(route.agent.unwrap().pane_id, Some(44));
    }
}
