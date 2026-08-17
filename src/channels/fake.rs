use std::collections::HashSet;
use std::sync::Mutex;

use thiserror::Error;

use crate::domain::{ChannelKind, EffectId, OutboxItem};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelCall {
    pub effect_id: EffectId,
    pub kind: ChannelKind,
    pub destination: String,
    pub body: String,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ChannelError {
    #[error("fake {0:?} delivery failed")]
    Rejected(ChannelKind),
}

#[derive(Default)]
pub struct RecordingChannels {
    calls: Mutex<Vec<ChannelCall>>,
    failed: Mutex<HashSet<ChannelKind>>,
}

impl RecordingChannels {
    pub fn fail(&self, kind: ChannelKind) {
        self.failed
            .lock()
            .expect("fake failure lock is healthy")
            .insert(kind);
    }

    pub fn restore(&self, kind: ChannelKind) {
        self.failed
            .lock()
            .expect("fake failure lock is healthy")
            .remove(&kind);
    }

    pub fn send(&self, item: &OutboxItem) -> Result<String, ChannelError> {
        if self
            .failed
            .lock()
            .expect("fake failure lock is healthy")
            .contains(&item.kind)
        {
            return Err(ChannelError::Rejected(item.kind));
        }
        let mut calls = self
            .calls
            .lock()
            .expect("fake channel call lock is healthy");
        calls.push(ChannelCall {
            effect_id: item.id,
            kind: item.kind,
            destination: item.destination.clone(),
            body: item.body.clone(),
        });
        Ok(format!("fake-{}-{}", item.id, calls.len()))
    }

    pub fn calls(&self) -> Vec<ChannelCall> {
        self.calls
            .lock()
            .expect("fake channel call lock is healthy")
            .clone()
    }
}
