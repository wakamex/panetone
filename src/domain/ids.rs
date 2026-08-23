use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            pub const fn new(value: Uuid) -> Self {
                Self(value)
            }

            pub fn random() -> Self {
                Self(Uuid::new_v4())
            }

            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }
    };
}

id_type!(RouteId);
id_type!(WorkflowId);
id_type!(EffectId);

impl EffectId {
    pub fn named(workflow_id: WorkflowId, purpose: &str) -> Self {
        let namespace = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            b"https://panetone.dev/control/v1/effect",
        );
        let name = format!("{}:{purpose}", workflow_id.0);
        Self(Uuid::new_v5(&namespace, name.as_bytes()))
    }

    pub fn target_admission(workflow_id: WorkflowId) -> Self {
        Self(workflow_id.0)
    }

    pub fn callback_admission(workflow_id: WorkflowId) -> Self {
        Self::named(workflow_id, "return-agent-callback")
    }

    pub fn chunk(parent: Self, index: usize) -> Self {
        let namespace = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            b"https://panetone.dev/outbox/chunk/v1",
        );
        Self(Uuid::new_v5(
            &namespace,
            format!("{}:{index}", parent.0).as_bytes(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effect_ids_are_stable_and_separate() {
        let workflow =
            WorkflowId::new(Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap());
        assert_eq!(
            EffectId::target_admission(workflow),
            EffectId::target_admission(workflow)
        );
        assert_eq!(
            EffectId::callback_admission(workflow),
            EffectId::callback_admission(workflow)
        );
        assert_ne!(
            EffectId::target_admission(workflow),
            EffectId::callback_admission(workflow)
        );
        assert_ne!(
            EffectId::named(workflow, "audit"),
            EffectId::named(workflow, "return-mirror")
        );
        let parent = EffectId::named(workflow, "output");
        assert_eq!(EffectId::chunk(parent, 0), EffectId::chunk(parent, 0));
        assert_ne!(EffectId::chunk(parent, 0), EffectId::chunk(parent, 1));
    }
}
