use serde::{Deserialize, Serialize};

use super::{ChannelBinding, ChannelKind, RouteId, SIGNAL_DIRECT_PREFIX};

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

    /// The route's topic in the owner's Telegram supergroup, which only the
    /// owner reads.
    pub fn telegram_topic(&self) -> Option<i64> {
        self.channels.iter().find_map(|binding| match binding {
            ChannelBinding::Telegram { topic_id } => Some(*topic_id),
            _ => None,
        })
    }

    /// Whether a destination is one of the route's channels that people
    /// other than the owner read.
    pub fn is_shared(&self, kind: ChannelKind, destination: &str) -> bool {
        self.channels
            .iter()
            .any(|binding| binding.admits_members() && binding.serves(kind, destination))
    }

    /// Where the route's agent output goes. A preference is the channel last
    /// used, `tg` or `sig` with the destination after a colon, such as
    /// `tg:195219` or `sig:direct:+15550100`; without a destination it names
    /// the topic or the Signal group. With no preference, or one the route no
    /// longer has, output goes to the route's group, else its topic.
    pub fn output_destination(&self, preference: Option<&str>) -> Option<(ChannelKind, String)> {
        let (kind, destination) =
            match preference.map(|value| value.split_once(':').unwrap_or((value, ""))) {
                Some(("tg", destination)) => (Some(ChannelKind::Telegram), destination),
                Some(("sig", destination)) => (Some(ChannelKind::Signal), destination),
                _ => (None, ""),
            };
        if let Some(kind) = kind
            && !destination.is_empty()
        {
            let known = if destination.starts_with(SIGNAL_DIRECT_PREFIX) {
                kind == ChannelKind::Signal
                    && self.channels.iter().any(|binding| {
                        matches!(binding, ChannelBinding::Signal { direct: true, .. })
                    })
            } else {
                self.channels
                    .iter()
                    .any(|binding| binding.serves(kind, destination))
            };
            if known {
                return Some((kind, destination.to_owned()));
            }
        }
        let topic = || {
            self.telegram_topic()
                .map(|topic_id| (ChannelKind::Telegram, topic_id.to_string()))
        };
        let signal_group = || {
            self.channels.iter().find_map(|binding| {
                matches!(binding, ChannelBinding::Signal { .. }).then(|| binding.destination())
            })
        };
        match kind {
            Some(ChannelKind::Telegram) => topic(),
            Some(ChannelKind::Signal) => signal_group(),
            None => signal_group()
                .or_else(|| {
                    self.channels.iter().find_map(|binding| {
                        matches!(binding, ChannelBinding::TelegramGroup { .. })
                            .then(|| binding.destination())
                    })
                })
                .or_else(topic),
        }
    }
}
