use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinSet;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPolicy {
    Critical,
    Degraded,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskHealth {
    pub policy: TaskPolicy,
    pub state: String,
    pub last_error: Option<String>,
}

#[derive(Clone, Default)]
pub struct SupervisorHandle {
    tasks: Arc<Mutex<BTreeMap<String, TaskHealth>>>,
}

impl SupervisorHandle {
    pub fn snapshot(&self) -> BTreeMap<String, TaskHealth> {
        self.tasks
            .lock()
            .expect("supervisor health lock is healthy")
            .clone()
    }

    pub fn degraded(&self) -> bool {
        self.tasks
            .lock()
            .expect("supervisor health lock is healthy")
            .values()
            .any(|health| health.state == "failed" || health.state == "retrying")
    }

    /// Records a running task's state, such as `retrying`, without ending it.
    pub fn report(&self, name: &str, state: &str, last_error: Option<String>) {
        if let Some(health) = self
            .tasks
            .lock()
            .expect("supervisor health lock is healthy")
            .get_mut(name)
        {
            health.state = state.into();
            health.last_error = last_error;
        }
    }
}

/// Runs `step` every `period` until shutdown. A failed step is reported as
/// `retrying` and retried after a backoff that starts at `first_backoff` and
/// doubles up to `MAX_RETRY_BACKOFF`, so one failing dependency cannot stop
/// the daemon's other tasks.
pub async fn run_periodic<F, Fut>(
    name: &str,
    health: SupervisorHandle,
    period: Duration,
    first_backoff: Duration,
    mut shutdown: watch::Receiver<bool>,
    mut step: F,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut backoff = first_backoff;
    let mut failing = false;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            _ = interval.tick() => {}
        }
        let result = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            result = step() => result,
        };
        match result {
            Ok(()) => {
                if failing {
                    tracing::info!(task = name, "task recovered");
                    health.report(name, "running", None);
                    failing = false;
                    backoff = first_backoff;
                }
            }
            Err(error) => {
                tracing::warn!(task = name, %error, ?backoff, "task failed; retrying");
                health.report(name, "retrying", Some(error));
                failing = true;
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            return Ok(());
                        }
                    }
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
            }
        }
    }
}

const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

struct TaskExit {
    name: String,
    policy: TaskPolicy,
    result: Result<(), String>,
}

pub struct Supervisor {
    handle: SupervisorHandle,
    shutdown_sender: watch::Sender<bool>,
    shutdown_receiver: watch::Receiver<bool>,
    tasks: JoinSet<TaskExit>,
}

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("critical task {task} failed: {detail}")]
    Critical { task: String, detail: String },
    #[error("supervised task panicked: {0}")]
    Join(String),
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl Supervisor {
    pub fn new() -> Self {
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        Self {
            handle: SupervisorHandle::default(),
            shutdown_sender,
            shutdown_receiver,
            tasks: JoinSet::new(),
        }
    }

    pub fn handle(&self) -> SupervisorHandle {
        self.handle.clone()
    }

    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_receiver.clone()
    }

    pub fn spawn<F>(&mut self, name: impl Into<String>, policy: TaskPolicy, future: F)
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        let name = name.into();
        self.handle
            .tasks
            .lock()
            .expect("supervisor health lock is healthy")
            .insert(
                name.clone(),
                TaskHealth {
                    policy,
                    state: "running".into(),
                    last_error: None,
                },
            );
        self.tasks.spawn(async move {
            TaskExit {
                name,
                policy,
                result: future.await,
            }
        });
    }

    pub async fn run_until<F>(mut self, shutdown: F) -> Result<(), SupervisorError>
    where
        F: Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        let mut critical = None;
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                exit = self.tasks.join_next() => {
                    let Some(exit) = exit else {
                        break;
                    };
                    if let Some(failure) = self.record_exit(exit)? {
                        critical = Some(failure);
                        break;
                    }
                }
            }
        }
        let _ = self.shutdown_sender.send(true);
        while let Some(exit) = self.tasks.join_next().await {
            let reported = self.record_exit(exit)?;
            if critical.is_none() {
                critical = reported;
            }
        }
        if let Some((task, detail)) = critical {
            Err(SupervisorError::Critical { task, detail })
        } else {
            Ok(())
        }
    }

    fn record_exit(
        &self,
        exit: Result<TaskExit, tokio::task::JoinError>,
    ) -> Result<Option<(String, String)>, SupervisorError> {
        let exit = exit.map_err(|error| SupervisorError::Join(error.to_string()))?;
        let shutting_down = *self.shutdown_receiver.borrow();
        let detail = match exit.result {
            Ok(()) if shutting_down => None,
            Ok(()) => Some("task exited before shutdown".into()),
            Err(error) => Some(error),
        };
        self.handle
            .tasks
            .lock()
            .expect("supervisor health lock is healthy")
            .insert(
                exit.name.clone(),
                TaskHealth {
                    policy: exit.policy,
                    state: if detail.is_some() {
                        "failed"
                    } else {
                        "stopped"
                    }
                    .into(),
                    last_error: detail.clone(),
                },
            );
        Ok(match (exit.policy, detail) {
            (TaskPolicy::Critical, Some(detail)) => Some((exit.name, detail)),
            _ => None,
        })
    }
}
