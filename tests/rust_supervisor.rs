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
