use panetone::supervisor::{Supervisor, SupervisorError, TaskPolicy};

#[tokio::test]
async fn critical_failure_cancels_owned_tasks_and_remains_in_health() {
    let mut supervisor = Supervisor::new();
    let health = supervisor.handle();
    let mut shutdown = supervisor.shutdown_receiver();
    supervisor.spawn("follower", TaskPolicy::Degraded, async move {
        shutdown
            .changed()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    });
    supervisor.spawn("critical", TaskPolicy::Critical, async {
        Err("listener failed".into())
    });
    let result = supervisor.run_until(std::future::pending()).await;
    assert!(matches!(
        result,
        Err(SupervisorError::Critical { ref task, ref detail })
            if task == "critical" && detail == "listener failed"
    ));
    let snapshot = health.snapshot();
    assert_eq!(snapshot["critical"].state, "failed");
    assert_eq!(snapshot["follower"].state, "stopped");
    assert!(health.degraded());
}

#[tokio::test]
async fn every_unexpected_production_style_failure_stops_the_service() {
    let mut supervisor = Supervisor::new();
    let health = supervisor.handle();
    let mut shutdown = supervisor.shutdown_receiver();
    supervisor.spawn("adapter", TaskPolicy::Critical, async {
        Err("capability unavailable".into())
    });
    supervisor.spawn("control", TaskPolicy::Critical, async move {
        shutdown
            .changed()
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    });
    let result = supervisor.run_until(std::future::pending()).await;
    assert!(matches!(
        result,
        Err(SupervisorError::Critical { ref task, ref detail })
            if task == "adapter" && detail == "capability unavailable"
    ));
    let snapshot = health.snapshot();
    assert_eq!(snapshot["adapter"].state, "failed");
    assert_eq!(snapshot["control"].state, "stopped");
    assert!(health.degraded());
}

#[tokio::test]
async fn a_failing_periodic_task_retries_without_stopping_the_daemon() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use panetone::supervisor::run_periodic;

    let mut supervisor = Supervisor::new();
    let health = supervisor.handle();
    let attempts = Arc::new(AtomicUsize::new(0));
    let (states_sender, mut states) = tokio::sync::mpsc::unbounded_channel();
    let task_health = health.clone();
    let task_attempts = attempts.clone();
    let shutdown = supervisor.shutdown_receiver();
    supervisor.spawn("events", TaskPolicy::Critical, async move {
        run_periodic(
            "events",
            task_health.clone(),
            Duration::from_millis(1),
            Duration::from_millis(1),
            shutdown,
            || {
                let attempt = task_attempts.fetch_add(1, Ordering::SeqCst);
                let state = task_health.snapshot()["events"].state.clone();
                let _ = states_sender.send(state);
                async move {
                    if attempt < 2 {
                        Err(format!("stream unavailable {attempt}"))
                    } else {
                        Ok(())
                    }
                }
            },
        )
        .await
    });
    let observed = tokio::spawn(async move {
        let mut seen = Vec::new();
        while seen.len() < 4 {
            seen.push(states.recv().await.unwrap());
        }
        seen
    });

    let result = supervisor
        .run_until(async {
            while attempts.load(Ordering::SeqCst) < 4 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
    assert!(result.is_ok());
    // Each step sees the state left by the previous one: two failures, then
    // recovery once a step succeeds.
    assert_eq!(
        observed.await.unwrap(),
        ["running", "retrying", "retrying", "running"]
    );
}
