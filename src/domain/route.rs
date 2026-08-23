use serde::{Deserialize, Serialize};

use super::{ChannelBinding, RouteId};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentBinding {
    pub agent_id: String,
    pub incarnation_id: String,
    pub harness: String,
    pub pane_id: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Route {
    pub id: RouteId,
    pub title: String,
    pub channels: Vec<ChannelBinding>,
    #[serde(skip)]
    pub agent: Option<AgentBinding>,
}

impl Route {
    pub fn with_agent(&self, agent: AgentBinding) -> Self {
        let mut route = self.clone();
        route.agent = Some(agent);
        route
    }
}
