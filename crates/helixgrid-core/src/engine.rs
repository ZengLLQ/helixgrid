use crate::snapshot::{EngineHealth, EngineSnapshot, RecoverySummary};
use helixgrid_config::{AppConfig, AppPaths, ConfigError, RegisteredExecutableEntry};
use helixgrid_domain::{
    ArtifactId, ArtifactKind, DomainError, EventSeverity, ExecutionArtifact, ExecutionId,
    ExecutionOutcome, ExecutionRecord, Priority, ProgressState, ResourceRequirements, RetryPolicy,
    RetrySafety, SystemEvent, SystemEventKind, WorkerId, WorkerInfo, WorkerStatus,
    WorkflowDefinition, WorkflowId, WorkflowStatus, WorkflowTaskDefinition, WorkflowTaskInput,
    WorkflowTaskState, Workload, WorkloadId, WorkloadSpec, WorkloadStatus,
};
use helixgrid_executor::{ExecutorError, ExecutorEvent, WorkerPoolSupervisor};
use helixgrid_observability::{
    DiagnosticBundle, EventRingBuffer, ObservabilityError, export_diagnostic_bundle, now_utc_millis,
};
use helixgrid_platform_windows::{is_system_reduced_motion_enabled, windows_platform_summary};
use helixgrid_protocol::{ExecutionRequestMsg, IpcMessage};
use helixgrid_resource::ResourceManager;
use helixgrid_scheduler::{
    SchedulerEngine, SchedulerError, WorkflowScheduler, collect_upstream_artifact_paths,
};
use helixgrid_security::{SecurityError, validate_registered_executable, verify_safe_retry};
use helixgrid_storage::{DatabaseStatusReport, StorageEngine, migrations::StorageError};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, info, warn};

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("domain error: {0}")]
    Domain(#[from] DomainError),

    #[error("security policy rejected operation: {0}")]
    Security(#[from] SecurityError),

    #[error("scheduler error: {0}")]
    Scheduler(#[from] SchedulerError),

    #[error("executor error: {0}")]
    Executor(#[from] ExecutorError),

    #[error("observability error: {0}")]
    Observability(#[from] ObservabilityError),

    #[error("coordinator channel closed")]
    CoordinatorStopped,

    #[error("entity not found: {0}")]
    NotFound(String),
}

#[derive(Debug, Clone)]
pub struct SubmitWorkloadRequest {
    pub name: String,
    pub description: String,
    pub priority: Priority,
    pub spec: WorkloadSpec,
    pub resources: ResourceRequirements,
    pub timeout_ms: Option<u64>,
    pub retry_policy: Option<RetryPolicy>,
}

pub enum EngineCommand {
    SubmitWorkload {
        req: SubmitWorkloadRequest,
        reply: oneshot::Sender<Result<WorkloadId, CoreError>>,
    },
    CancelWorkload {
        workload_id: WorkloadId,
        reply: oneshot::Sender<Result<(), CoreError>>,
    },
    SafeRetryWorkload {
        workload_id: WorkloadId,
        reply: oneshot::Sender<Result<WorkloadId, CoreError>>,
    },
    SubmitAndStartWorkflow {
        name: String,
        description: String,
        tasks: Vec<WorkflowTaskDefinition>,
        reply: oneshot::Sender<Result<WorkflowId, CoreError>>,
    },
    CancelWorkflow {
        workflow_id: WorkflowId,
        reply: oneshot::Sender<Result<(), CoreError>>,
    },
    SpawnWorker {
        reply: oneshot::Sender<Result<WorkerInfo, CoreError>>,
    },
    ShutdownWorker {
        worker_id: WorkerId,
        reply: oneshot::Sender<Result<(), CoreError>>,
    },
    RestartWorker {
        worker_id: WorkerId,
        reply: oneshot::Sender<Result<WorkerInfo, CoreError>>,
    },
    ForceKillWorker {
        worker_id: WorkerId,
        reply: oneshot::Sender<Result<(), CoreError>>,
    },
    UpdateConfig {
        config: AppConfig,
        reply: oneshot::Sender<Result<(), CoreError>>,
    },
    RegisterExecutable {
        entry: RegisteredExecutableEntry,
        reply: oneshot::Sender<Result<(), CoreError>>,
    },
    RemoveRegisteredExecutable {
        id: String,
        reply: oneshot::Sender<Result<(), CoreError>>,
    },
    BackupDatabase {
        dest_path: PathBuf,
        reply: oneshot::Sender<Result<PathBuf, CoreError>>,
    },
    ExportDiagnostics {
        dest_dir: Option<PathBuf>,
        reply: oneshot::Sender<Result<PathBuf, CoreError>>,
    },
    ForceRefreshSnapshot {
        reply: oneshot::Sender<Result<(), CoreError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Public asynchronous handle to the HelixGrid local control plane.
#[derive(Clone)]
pub struct HelixGridEngine {
    cmd_tx: mpsc::Sender<EngineCommand>,
    snapshot_rx: watch::Receiver<Arc<EngineSnapshot>>,
    paths: AppPaths,
}

impl HelixGridEngine {
    /// Starts the local compute engine: loads config, opens SQLite, reconciles prior crashes,
    /// spawns initial isolated worker processes, and runs the coordinator supervision loop.
    pub async fn start(
        paths: AppPaths,
        worker_exe_override: Option<PathBuf>,
        initial_workers_override: Option<u32>,
    ) -> Result<Self, CoreError> {
        paths.ensure_directories()?;
        let config = AppConfig::load_or_create(&paths.config_file)?;
        let storage = StorageEngine::open(&paths.database_file)?;

        let worker_exe = match worker_exe_override {
            Some(p) => p,
            None => WorkerPoolSupervisor::resolve_worker_executable()?,
        };

        let (exec_tx, exec_rx) = mpsc::channel::<ExecutorEvent>(512);
        let (cmd_tx, cmd_rx) = mpsc::channel::<EngineCommand>(256);

        let supervisor = WorkerPoolSupervisor::new(worker_exe, paths.clone(), exec_tx);
        let resources = ResourceManager::new_windows();
        let scheduler = SchedulerEngine::new();
        let event_ring = EventRingBuffer::new(500);

        let mut state = CoordinatorState::initialize(
            paths.clone(),
            config,
            storage,
            supervisor,
            resources,
            scheduler,
            event_ring,
            initial_workers_override,
        )?;

        let initial_snapshot = Arc::new(state.build_snapshot());
        let (snapshot_tx, snapshot_rx) = watch::channel(initial_snapshot);

        tokio::spawn(async move {
            state.run_loop(cmd_rx, exec_rx, snapshot_tx).await;
        });

        Ok(Self { cmd_tx, snapshot_rx, paths })
    }

    #[must_use]
    pub fn paths(&self) -> &AppPaths {
        &self.paths
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<EngineSnapshot> {
        self.snapshot_rx.borrow().clone()
    }

    #[must_use]
    pub fn subscribe_snapshots(&self) -> watch::Receiver<Arc<EngineSnapshot>> {
        self.snapshot_rx.clone()
    }

    async fn call<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, CoreError>>) -> EngineCommand,
    ) -> Result<T, CoreError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx.send(build(tx)).await.map_err(|_| CoreError::CoordinatorStopped)?;
        rx.await.map_err(|_| CoreError::CoordinatorStopped)?
    }

    pub async fn submit_workload(
        &self,
        req: SubmitWorkloadRequest,
    ) -> Result<WorkloadId, CoreError> {
        self.call(|reply| EngineCommand::SubmitWorkload { req, reply }).await
    }

    pub async fn cancel_workload(&self, workload_id: WorkloadId) -> Result<(), CoreError> {
        self.call(|reply| EngineCommand::CancelWorkload { workload_id, reply }).await
    }

    pub async fn safe_retry_workload(
        &self,
        workload_id: WorkloadId,
    ) -> Result<WorkloadId, CoreError> {
        self.call(|reply| EngineCommand::SafeRetryWorkload { workload_id, reply }).await
    }

    pub async fn submit_and_start_workflow(
        &self,
        name: String,
        description: String,
        tasks: Vec<WorkflowTaskDefinition>,
    ) -> Result<WorkflowId, CoreError> {
        self.call(|reply| EngineCommand::SubmitAndStartWorkflow { name, description, tasks, reply })
            .await
    }

    pub async fn cancel_workflow(&self, workflow_id: WorkflowId) -> Result<(), CoreError> {
        self.call(|reply| EngineCommand::CancelWorkflow { workflow_id, reply }).await
    }

    pub async fn spawn_worker(&self) -> Result<WorkerInfo, CoreError> {
        self.call(|reply| EngineCommand::SpawnWorker { reply }).await
    }

    pub async fn shutdown_worker(&self, worker_id: WorkerId) -> Result<(), CoreError> {
        self.call(|reply| EngineCommand::ShutdownWorker { worker_id, reply }).await
    }

    pub async fn restart_worker(&self, worker_id: WorkerId) -> Result<WorkerInfo, CoreError> {
        self.call(|reply| EngineCommand::RestartWorker { worker_id, reply }).await
    }

    pub async fn force_kill_worker(&self, worker_id: WorkerId) -> Result<(), CoreError> {
        self.call(|reply| EngineCommand::ForceKillWorker { worker_id, reply }).await
    }

    pub async fn update_config(&self, config: AppConfig) -> Result<(), CoreError> {
        self.call(|reply| EngineCommand::UpdateConfig { config, reply }).await
    }

    pub async fn register_executable(
        &self,
        entry: RegisteredExecutableEntry,
    ) -> Result<(), CoreError> {
        self.call(|reply| EngineCommand::RegisterExecutable { entry, reply }).await
    }

    pub async fn remove_registered_executable(&self, id: String) -> Result<(), CoreError> {
        self.call(|reply| EngineCommand::RemoveRegisteredExecutable { id, reply }).await
    }

    pub async fn backup_database(&self, dest_path: PathBuf) -> Result<PathBuf, CoreError> {
        self.call(|reply| EngineCommand::BackupDatabase { dest_path, reply }).await
    }

    pub async fn export_diagnostics(
        &self,
        dest_dir: Option<PathBuf>,
    ) -> Result<PathBuf, CoreError> {
        self.call(|reply| EngineCommand::ExportDiagnostics { dest_dir, reply }).await
    }

    pub async fn refresh_snapshot(&self) -> Result<(), CoreError> {
        self.call(|reply| EngineCommand::ForceRefreshSnapshot { reply }).await
    }

    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.cmd_tx.send(EngineCommand::Shutdown { reply: tx }).await.is_ok() {
            let _ = rx.await;
        }
    }
}

struct ActiveExecutionClock {
    workload_id: WorkloadId,
    worker_id: WorkerId,
    started_instant: Instant,
    timeout_ms: u64,
    cancel_requested_at: Option<Instant>,
    timed_out: bool,
}

struct CoordinatorState {
    paths: AppPaths,
    config: AppConfig,
    storage: StorageEngine,
    supervisor: WorkerPoolSupervisor,
    resources: ResourceManager,
    scheduler: SchedulerEngine,
    event_ring: EventRingBuffer,
    workloads: HashMap<WorkloadId, Workload>,
    workflows: HashMap<WorkflowId, WorkflowDefinition>,
    executions_by_workload: HashMap<WorkloadId, Vec<ExecutionRecord>>,
    artifacts_by_workload: HashMap<WorkloadId, Vec<ExecutionArtifact>>,
    active_clocks: HashMap<ExecutionId, ActiveExecutionClock>,
    historical_snapshots: Vec<helixgrid_domain::SystemMetricsSnapshot>,
    cached_db_status: DatabaseStatusReport,
    recovery_summary: RecoverySummary,
    health: EngineHealth,
    health_reason: String,
    reduced_motion_os: bool,
    last_metrics_tick: Instant,
    last_db_status_tick: Instant,
}

impl CoordinatorState {
    #[allow(clippy::too_many_arguments)]
    fn initialize(
        paths: AppPaths,
        config: AppConfig,
        storage: StorageEngine,
        mut supervisor: WorkerPoolSupervisor,
        resources: ResourceManager,
        mut scheduler: SchedulerEngine,
        event_ring: EventRingBuffer,
        initial_workers_override: Option<u32>,
    ) -> Result<Self, CoreError> {
        let now_utc = now_utc_millis();

        // 1. Reconcile incomplete executions & workloads from any prior crash/restart.
        let interrupted_execs = storage.mark_open_executions_interrupted(now_utc)?;
        let mut recovery_summary = RecoverySummary {
            interrupted_executions_found: interrupted_execs,
            ..Default::default()
        };

        let mut stored_workloads = storage.list_workloads(1_000)?;
        for wl in &mut stored_workloads {
            let status = wl.status;
            match status {
                WorkloadStatus::Scheduled | WorkloadStatus::Starting | WorkloadStatus::Running => {
                    recovery_summary.interrupted_workloads_found += 1;
                    wl.assigned_worker_id = None;
                    let _ = wl.transition_status(WorkloadStatus::Interrupted, now_utc);
                    let can_auto_retry = config.auto_recover_idempotent_interrupted
                        && wl.retry_policy.retry_safety == RetrySafety::SafeIdempotent
                        && wl.attempt_count <= wl.retry_policy.max_retries
                        && wl.spec.default_retry_safety() == RetrySafety::SafeIdempotent;

                    if can_auto_retry {
                        if wl.transition_status(WorkloadStatus::Retrying, now_utc).is_ok()
                            && scheduler.enqueue(wl, &config, now_utc).is_ok()
                        {
                            recovery_summary.auto_recovered_workloads += 1;
                            let mut ev = SystemEvent::new(
                                SystemEventKind::RecoveryReconciled,
                                EventSeverity::Warning,
                                format!(
                                    "Recovered interrupted idempotent workload '{}' into retry queue",
                                    wl.name
                                ),
                                now_utc,
                            );
                            ev.workload_id = Some(wl.id);
                            let _ = storage.insert_event(&ev, config.event_retention_max_rows);
                            event_ring.push(ev);
                        }
                    } else {
                        recovery_summary.manual_attention_workloads += 1;
                        wl.error_message = Some(
                            "Interrupted by coordinator exit; requires explicit inspection or safe retry"
                                .to_string(),
                        );
                        let mut ev = SystemEvent::new(
                            SystemEventKind::WorkloadInterrupted,
                            EventSeverity::Warning,
                            format!(
                                "Marked interrupted workload '{}' (non-idempotent or retry budget exhausted)",
                                wl.name
                            ),
                            now_utc,
                        );
                        ev.workload_id = Some(wl.id);
                        let _ = storage.insert_event(&ev, config.event_retention_max_rows);
                        event_ring.push(ev);
                    }
                    let _ = storage.update_workload(wl);
                }
                WorkloadStatus::Submitted
                | WorkloadStatus::Validating
                | WorkloadStatus::Queued
                | WorkloadStatus::Retrying
                    if scheduler.enqueue(wl, &config, now_utc).is_ok() =>
                {
                    recovery_summary.requeued_workloads += 1;
                    let _ = storage.update_workload(wl);
                }
                _ => {}
            }
        }

        let workloads: HashMap<WorkloadId, Workload> =
            stored_workloads.into_iter().map(|w| (w.id, w)).collect();

        let mut executions_by_workload = HashMap::new();
        let mut artifacts_by_workload = HashMap::new();
        for wl_id in workloads.keys().take(200) {
            if let Ok(execs) = storage.list_executions_for_workload(*wl_id)
                && !execs.is_empty()
            {
                executions_by_workload.insert(*wl_id, execs);
            }
            if let Ok(arts) = storage.list_artifacts_for_workload(*wl_id)
                && !arts.is_empty()
            {
                artifacts_by_workload.insert(*wl_id, arts);
            }
        }
        let cached_db_status = storage.status_report()?;

        let stored_workflows = storage.list_workflows(200)?;
        let mut workflows = HashMap::new();
        for mut wf in stored_workflows {
            if wf.status == WorkflowStatus::Running {
                let _ = WorkflowScheduler::evaluate_workflow(&mut wf, &workloads, now_utc);
                let _ = storage.update_workflow_state(&wf);
            }
            workflows.insert(wf.id, wf);
        }

        // Load recent events & historical snapshots from DB so UI shows real history immediately.
        let recent_events = storage
            .query_events(&helixgrid_storage::EventFilter { limit: 250, ..Default::default() })?;
        for ev in recent_events.into_iter().rev() {
            event_ring.push(ev);
        }
        let historical_snapshots = storage.list_recent_snapshots(120)?;

        // Spawn initial isolated worker pool.
        let target_initial = initial_workers_override
            .unwrap_or_else(|| config.max_workers.min(2))
            .min(config.max_workers);
        let logical_cpus = resources.logical_processors();
        let per_worker_mem = Self::compute_per_worker_mem_mb(&resources, &config);

        let mut spawn_errors = Vec::new();
        for _ in 0..target_initial {
            match supervisor.spawn_worker(&config, logical_cpus, per_worker_mem) {
                Ok(info) => {
                    let _ = storage.upsert_worker(&info, now_utc);
                    let mut ev = SystemEvent::new(
                        SystemEventKind::WorkerSpawned,
                        EventSeverity::Info,
                        format!("Spawned {} (PID {:?})", info.name, info.pid),
                        now_utc,
                    );
                    ev.worker_id = Some(info.id);
                    let _ = storage.insert_event(&ev, config.event_retention_max_rows);
                    event_ring.push(ev);
                }
                Err(err) => {
                    error!("failed to spawn initial worker: {err}");
                    spawn_errors.push(err.to_string());
                }
            }
        }

        let (health, health_reason) = if spawn_errors.is_empty() {
            (
                EngineHealth::Healthy,
                "Coordinator, SQLite storage, and worker pool online".to_string(),
            )
        } else {
            (EngineHealth::Degraded, format!("Worker spawn warning: {}", spawn_errors.join("; ")))
        };

        let start_ev = SystemEvent::new(
            SystemEventKind::EngineStarted,
            EventSeverity::Info,
            format!(
                "HelixGrid engine started ({} logical CPUs, {} workers spawned)",
                logical_cpus,
                supervisor.worker_infos().len()
            ),
            now_utc,
        );
        let _ = storage.insert_event(&start_ev, config.event_retention_max_rows);
        event_ring.push(start_ev);

        Ok(Self {
            paths,
            config,
            storage,
            supervisor,
            resources,
            scheduler,
            event_ring,
            workloads,
            workflows,
            executions_by_workload,
            artifacts_by_workload,
            active_clocks: HashMap::new(),
            historical_snapshots,
            cached_db_status,
            recovery_summary,
            health,
            health_reason,
            reduced_motion_os: is_system_reduced_motion_enabled(),
            last_metrics_tick: Instant::now(),
            last_db_status_tick: Instant::now(),
        })
    }

    fn compute_per_worker_mem_mb(resources: &ResourceManager, config: &AppConfig) -> u64 {
        let total_mb = resources
            .latest_snapshot()
            .total_physical_memory_bytes
            .value()
            .map(|b| *b / (1024 * 1024))
            .unwrap_or(8192);
        let ceiling_mb =
            ((total_mb as f64) * (config.memory_allocation_ceiling_pct / 100.0)) as u64;
        ceiling_mb.saturating_sub(config.reserved_system_memory_mb).max(1024)
    }

    fn record_event(&mut self, mut ev: SystemEvent) {
        if ev.timestamp_utc == 0 {
            ev.timestamp_utc = now_utc_millis();
        }
        if let Err(err) = self.storage.insert_event(&ev, self.config.event_retention_max_rows) {
            warn!("failed to persist event: {err}");
        }
        self.event_ring.push(ev);
    }

    async fn run_loop(
        &mut self,
        mut cmd_rx: mpsc::Receiver<EngineCommand>,
        mut exec_rx: mpsc::Receiver<ExecutorEvent>,
        snapshot_tx: watch::Sender<Arc<EngineSnapshot>>,
    ) {
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        EngineCommand::Shutdown { reply } => {
                            info!("Coordinator shutting down gracefully");
                            self.supervisor.shutdown_all().await;
                            let now = now_utc_millis();
                            self.record_event(SystemEvent::new(
                                SystemEventKind::EngineShutdown,
                                EventSeverity::Info,
                                "HelixGrid coordinator shut down cleanly",
                                now,
                            ));
                            self.health = EngineHealth::Stopped;
                            self.health_reason = "Engine stopped".to_string();
                            let _ = snapshot_tx.send(Arc::new(self.build_snapshot()));
                            let _ = reply.send(());
                            break;
                        }
                        other => {
                            self.handle_command(other).await;
                            while let Ok(more) = cmd_rx.try_recv() {
                                if let EngineCommand::Shutdown { reply } = more {
                                    self.supervisor.shutdown_all().await;
                                    let _ = snapshot_tx.send(Arc::new(self.build_snapshot()));
                                    let _ = reply.send(());
                                    return;
                                }
                                self.handle_command(more).await;
                            }
                            self.run_scheduling_and_workflows().await;
                            let _ = snapshot_tx.send(Arc::new(self.build_snapshot()));
                        }
                    }
                }
                Some(ev) = exec_rx.recv() => {
                    self.handle_executor_event(ev).await;
                    while let Ok(more_ev) = exec_rx.try_recv() {
                        self.handle_executor_event(more_ev).await;
                    }
                    self.run_scheduling_and_workflows().await;
                    let _ = snapshot_tx.send(Arc::new(self.build_snapshot()));
                }
                _ = tick.tick() => {
                    self.handle_periodic_supervision().await;
                    self.run_scheduling_and_workflows().await;

                    if self.last_metrics_tick.elapsed().as_millis() as u64 >= self.config.metrics_sample_interval_ms {
                        self.sample_and_persist_metrics();
                        self.last_metrics_tick = Instant::now();
                    }
                    if self.last_db_status_tick.elapsed() >= Duration::from_secs(10) {
                        if let Ok(rep) = self.storage.status_report() {
                            self.cached_db_status = rep;
                        }
                        self.last_db_status_tick = Instant::now();
                    }
                    let _ = snapshot_tx.send(Arc::new(self.build_snapshot()));
                }
            }
        }
    }

    async fn handle_command(&mut self, cmd: EngineCommand) {
        let now_utc = now_utc_millis();
        match cmd {
            EngineCommand::SubmitWorkload { req, reply } => {
                let res = self.submit_workload_internal(req, now_utc);
                let _ = reply.send(res);
            }
            EngineCommand::CancelWorkload { workload_id, reply } => {
                let res = self.cancel_workload_internal(workload_id, now_utc).await;
                let _ = reply.send(res);
            }
            EngineCommand::SafeRetryWorkload { workload_id, reply } => {
                let res = self.safe_retry_internal(workload_id, now_utc);
                let _ = reply.send(res);
            }
            EngineCommand::SubmitAndStartWorkflow { name, description, tasks, reply } => {
                let res = self.submit_workflow_internal(name, description, tasks, now_utc);
                let _ = reply.send(res);
            }
            EngineCommand::CancelWorkflow { workflow_id, reply } => {
                let res = self.cancel_workflow_internal(workflow_id, now_utc).await;
                let _ = reply.send(res);
            }
            EngineCommand::SpawnWorker { reply } => {
                let res = self.spawn_worker_internal(now_utc);
                let _ = reply.send(res);
            }
            EngineCommand::ShutdownWorker { worker_id, reply } => {
                let res = self.shutdown_worker_internal(worker_id, now_utc).await;
                let _ = reply.send(res);
            }
            EngineCommand::RestartWorker { worker_id, reply } => {
                let _ = self.shutdown_worker_internal(worker_id, now_utc).await;
                let res = self.spawn_worker_internal(now_utc);
                let _ = reply.send(res);
            }
            EngineCommand::ForceKillWorker { worker_id, reply } => {
                let res =
                    self.supervisor.force_terminate_worker(worker_id).map_err(CoreError::from);
                if res.is_ok() {
                    self.handle_worker_failure(
                        worker_id,
                        "Terminated forcefully via operator/fault-injection",
                        now_utc,
                    );
                }
                let _ = reply.send(res);
            }
            EngineCommand::UpdateConfig { config, reply } => {
                let res = (|| -> Result<(), CoreError> {
                    config.validate()?;
                    config.save(&self.paths.config_file)?;
                    self.config = config;
                    Ok(())
                })();
                let _ = reply.send(res);
            }
            EngineCommand::RegisterExecutable { entry, reply } => {
                let res = (|| -> Result<(), CoreError> {
                    let mut updated = self.config.clone();
                    updated.registered_executables.retain(|e| e.id != entry.id);
                    updated.registered_executables.push(entry);
                    updated.validate()?;
                    updated.save(&self.paths.config_file)?;
                    self.config = updated;
                    Ok(())
                })();
                let _ = reply.send(res);
            }
            EngineCommand::RemoveRegisteredExecutable { id, reply } => {
                let res = (|| -> Result<(), CoreError> {
                    let mut updated = self.config.clone();
                    updated.registered_executables.retain(|e| e.id != id);
                    updated.save(&self.paths.config_file)?;
                    self.config = updated;
                    Ok(())
                })();
                let _ = reply.send(res);
            }
            EngineCommand::BackupDatabase { dest_path, reply } => {
                let res = (|| -> Result<PathBuf, CoreError> {
                    self.storage.backup_to(&dest_path)?;
                    self.record_event(SystemEvent::new(
                        SystemEventKind::DatabaseMaintenance,
                        EventSeverity::Info,
                        format!("Database backed up to {}", dest_path.display()),
                        now_utc,
                    ));
                    Ok(dest_path)
                })();
                let _ = reply.send(res);
            }
            EngineCommand::ExportDiagnostics { dest_dir, reply } => {
                let res = self.export_diagnostics_internal(dest_dir);
                let _ = reply.send(res);
            }
            EngineCommand::ForceRefreshSnapshot { reply } => {
                self.sample_and_persist_metrics();
                if let Ok(rep) = self.storage.status_report() {
                    self.cached_db_status = rep;
                }
                let _ = reply.send(Ok(()));
            }
            EngineCommand::Shutdown { .. } => {}
        }
    }

    fn submit_workload_internal(
        &mut self,
        req: SubmitWorkloadRequest,
        now_utc: i64,
    ) -> Result<WorkloadId, CoreError> {
        let retry_policy = req.retry_policy.unwrap_or_else(|| RetryPolicy {
            max_retries: 0,
            backoff_ms: 500,
            retry_safety: req.spec.default_retry_safety(),
            retry_on_timeout: false,
        });

        verify_safe_retry(&req.spec, &retry_policy)?;
        if let WorkloadSpec::RegisteredExecutable(ref exe_input) = req.spec {
            validate_registered_executable(&self.config, exe_input)?;
        }

        let timeout_ms = req.timeout_ms.unwrap_or(self.config.default_timeout_ms);
        let mut wl = Workload::new(
            req.name,
            req.description,
            req.priority,
            req.spec,
            req.resources,
            timeout_ms,
            retry_policy,
            now_utc,
        )?;

        self.storage.insert_workload(&wl)?;
        if let Err(err) = self.scheduler.enqueue(&mut wl, &self.config, now_utc) {
            let _ = wl.transition_status(WorkloadStatus::Failed, now_utc);
            wl.error_message = Some(err.to_string());
            let _ = self.storage.update_workload(&wl);
            self.workloads.insert(wl.id, wl);
            if matches!(err, SchedulerError::QueueFull { .. }) {
                self.record_event(SystemEvent::new(
                    SystemEventKind::SchedulerBackpressure,
                    EventSeverity::Warning,
                    err.to_string(),
                    now_utc,
                ));
            }
            return Err(CoreError::Scheduler(err));
        }

        self.storage.update_workload(&wl)?;
        let id = wl.id;
        let name = wl.name.clone();
        self.workloads.insert(id, wl);

        let mut ev = SystemEvent::new(
            SystemEventKind::WorkloadQueued,
            EventSeverity::Info,
            format!("Queued workload '{name}'"),
            now_utc,
        );
        ev.workload_id = Some(id);
        self.record_event(ev);
        Ok(id)
    }

    async fn cancel_workload_internal(
        &mut self,
        workload_id: WorkloadId,
        now_utc: i64,
    ) -> Result<(), CoreError> {
        let Some(wl) = self.workloads.get_mut(&workload_id) else {
            return Err(CoreError::NotFound(format!("workload {workload_id}")));
        };
        if wl.status.is_terminal() {
            return Err(CoreError::Domain(DomainError::IllegalStateTransition {
                from: format!("{:?}", wl.status),
                to: "Cancelled".to_string(),
            }));
        }

        if self.scheduler.remove_from_queue(workload_id)
            || matches!(
                wl.status,
                WorkloadStatus::Submitted
                    | WorkloadStatus::Validating
                    | WorkloadStatus::Queued
                    | WorkloadStatus::Interrupted
                    | WorkloadStatus::Retrying
            )
        {
            wl.transition_status(WorkloadStatus::Cancelled, now_utc)?;
            wl.error_message = Some("Cancelled before execution".to_string());
            self.storage.update_workload(wl)?;
            let mut ev = SystemEvent::new(
                SystemEventKind::WorkloadCancelled,
                EventSeverity::Info,
                format!("Cancelled workload '{}'", wl.name),
                now_utc,
            );
            ev.workload_id = Some(workload_id);
            self.record_event(ev);
            return Ok(());
        }

        // Active on worker -> send CancelExecutionCommand
        if let (Some(worker_id), Some(exec_id)) = (wl.assigned_worker_id, wl.latest_execution_id) {
            if let Some(clock) = self.active_clocks.get_mut(&exec_id) {
                clock.cancel_requested_at = Some(Instant::now());
            }
            self.supervisor
                .cancel_execution(worker_id, workload_id, exec_id, "Cancelled by user")
                .await?;
        }
        Ok(())
    }

    fn safe_retry_internal(
        &mut self,
        workload_id: WorkloadId,
        now_utc: i64,
    ) -> Result<WorkloadId, CoreError> {
        let existing = self
            .workloads
            .get(&workload_id)
            .cloned()
            .ok_or_else(|| CoreError::NotFound(format!("workload {workload_id}")))?;

        if existing.status == WorkloadStatus::Completed {
            return Err(CoreError::Domain(DomainError::IllegalStateTransition {
                from: "Completed".to_string(),
                to: "Retrying".to_string(),
            }));
        }

        // Reject unsafe automatic retry unless explicitly confirmed or safe
        if existing.retry_policy.retry_safety != RetrySafety::SafeIdempotent {
            return Err(CoreError::Domain(DomainError::UnsafeRetryRejected(existing.name.clone())));
        }

        let mut retry_wl = Workload::new(
            format!("{} (Retry)", existing.name),
            existing.description,
            existing.priority,
            existing.spec,
            existing.resources,
            existing.timeout_ms,
            existing.retry_policy,
            now_utc,
        )?;
        retry_wl.parent_workload_id = Some(existing.id);

        self.storage.insert_workload(&retry_wl)?;
        self.scheduler.enqueue(&mut retry_wl, &self.config, now_utc)?;
        self.storage.update_workload(&retry_wl)?;

        let new_id = retry_wl.id;
        self.workloads.insert(new_id, retry_wl);

        let mut ev = SystemEvent::new(
            SystemEventKind::WorkloadRetrying,
            EventSeverity::Info,
            format!("Scheduled safe retry for '{}'", existing.name),
            now_utc,
        );
        ev.workload_id = Some(new_id);
        self.record_event(ev);
        Ok(new_id)
    }

    fn submit_workflow_internal(
        &mut self,
        name: String,
        description: String,
        tasks: Vec<WorkflowTaskDefinition>,
        now_utc: i64,
    ) -> Result<WorkflowId, CoreError> {
        let mut wf = WorkflowDefinition::new(name, description, tasks, now_utc)?;
        for task in &wf.tasks {
            verify_safe_retry(&task.spec, &task.retry_policy)?;
            if let WorkloadSpec::RegisteredExecutable(ref exe) = task.spec {
                validate_registered_executable(&self.config, exe)?;
            }
        }
        wf.status = WorkflowStatus::Running;
        wf.updated_at_utc = now_utc;
        self.storage.insert_workflow(&wf)?;

        let wf_id = wf.id;
        let wf_name = wf.name.clone();
        self.workflows.insert(wf_id, wf);

        let mut ev = SystemEvent::new(
            SystemEventKind::WorkflowStarted,
            EventSeverity::Info,
            format!("Started workflow '{wf_name}'"),
            now_utc,
        );
        ev.workflow_id = Some(wf_id);
        self.record_event(ev);
        Ok(wf_id)
    }

    async fn cancel_workflow_internal(
        &mut self,
        workflow_id: WorkflowId,
        now_utc: i64,
    ) -> Result<(), CoreError> {
        let to_cancel: Vec<WorkloadId> = {
            let wf = self
                .workflows
                .get_mut(&workflow_id)
                .ok_or_else(|| CoreError::NotFound(format!("workflow {workflow_id}")))?;
            wf.status = WorkflowStatus::Cancelled;
            wf.updated_at_utc = now_utc;
            let mut ids = Vec::new();
            for t in &mut wf.tasks {
                if t.state == WorkflowTaskState::Pending {
                    t.state = WorkflowTaskState::Cancelled;
                } else if t.state == WorkflowTaskState::Dispatched
                    && let Some(wl_id) = t.workload_id
                {
                    ids.push(wl_id);
                }
            }
            let _ = self.storage.update_workflow_state(wf);
            ids
        };
        for wl_id in to_cancel {
            let _ = self.cancel_workload_internal(wl_id, now_utc).await;
        }
        Ok(())
    }

    fn spawn_worker_internal(&mut self, now_utc: i64) -> Result<WorkerInfo, CoreError> {
        let current_alive =
            self.supervisor.worker_infos().into_iter().filter(|w| w.status.is_alive()).count()
                as u32;
        if current_alive >= self.config.max_workers {
            return Err(CoreError::Scheduler(SchedulerError::QueueFull {
                capacity: self.config.max_workers as usize,
            }));
        }
        let logical = self.resources.logical_processors();
        let mem_mb = Self::compute_per_worker_mem_mb(&self.resources, &self.config);
        let info = self.supervisor.spawn_worker(&self.config, logical, mem_mb)?;
        self.storage.upsert_worker(&info, now_utc)?;
        let mut ev = SystemEvent::new(
            SystemEventKind::WorkerSpawned,
            EventSeverity::Info,
            format!("Spawned {} (PID {:?})", info.name, info.pid),
            now_utc,
        );
        ev.worker_id = Some(info.id);
        self.record_event(ev);
        Ok(info)
    }

    async fn shutdown_worker_internal(
        &mut self,
        worker_id: WorkerId,
        now_utc: i64,
    ) -> Result<(), CoreError> {
        self.supervisor
            .graceful_shutdown_worker(worker_id, "Operator requested worker shutdown")
            .await?;
        let mut worker_infos = self.supervisor.worker_infos();
        let target = worker_infos.iter_mut().find(|w| w.id == worker_id);
        let released = self.resources.release_all_for_worker(worker_id, target);
        self.supervisor.update_worker_infos_snapshot(&worker_infos);

        for res in released {
            self.mark_workload_interrupted(
                res.workload_id,
                res.execution_id,
                Some(worker_id),
                "Worker shut down while execution was active",
                now_utc,
            );
        }

        if let Some(info) = self.supervisor.worker_infos().into_iter().find(|w| w.id == worker_id) {
            let _ = self.storage.upsert_worker(&info, now_utc);
        }
        let mut ev = SystemEvent::new(
            SystemEventKind::WorkerStopped,
            EventSeverity::Info,
            format!("Worker {worker_id} stopped cleanly"),
            now_utc,
        );
        ev.worker_id = Some(worker_id);
        self.record_event(ev);
        Ok(())
    }

    async fn handle_executor_event(&mut self, ev: ExecutorEvent) {
        let now_utc = now_utc_millis();
        match ev {
            ExecutorEvent::WorkerConnected { worker_id, pid } => {
                if let Some(w) = self.supervisor.worker_info_mut(worker_id) {
                    w.pid = Some(pid);
                    w.last_heartbeat_utc = Some(now_utc);
                    let copy = w.clone();
                    let _ = self.storage.upsert_worker(&copy, now_utc);
                }
                let mut sys_ev = SystemEvent::new(
                    SystemEventKind::WorkerConnected,
                    EventSeverity::Info,
                    format!("Worker {worker_id} authenticated IPC pipe (PID {pid})"),
                    now_utc,
                );
                sys_ev.worker_id = Some(worker_id);
                self.record_event(sys_ev);
            }
            ExecutorEvent::WorkerReady { worker_id, logical_processors } => {
                if let Some(w) = self.supervisor.worker_info_mut(worker_id) {
                    w.status = WorkerStatus::Idle;
                    w.max_cpu_cores = logical_processors.max(1);
                    w.last_heartbeat_utc = Some(now_utc);
                    let copy = w.clone();
                    let _ = self.storage.upsert_worker(&copy, now_utc);
                }
            }
            ExecutorEvent::WorkerHeartbeat {
                worker_id, cpu_usage_pct, memory_usage_bytes, ..
            } => {
                if let Some(w) = self.supervisor.worker_info_mut(worker_id) {
                    w.last_heartbeat_utc = Some(now_utc);
                    if let Some(c) = cpu_usage_pct {
                        w.cpu_usage_pct = helixgrid_domain::MeasurementValue::Current(c);
                    }
                    if let Some(m) = memory_usage_bytes {
                        w.memory_usage_bytes = helixgrid_domain::MeasurementValue::Current(m);
                    }
                }
            }
            ExecutorEvent::WorkerIpcAuthRejected { worker_id, reason } => {
                let _ = self.supervisor.force_terminate_worker(worker_id);
                let mut sys_ev = SystemEvent::new(
                    SystemEventKind::SecurityViolation,
                    EventSeverity::Critical,
                    format!("Rejected worker {worker_id} IPC authentication: {reason}"),
                    now_utc,
                );
                sys_ev.worker_id = Some(worker_id);
                self.record_event(sys_ev);
            }
            ExecutorEvent::WorkerDisconnected { worker_id, reason } => {
                let was_expected = self
                    .supervisor
                    .worker_infos()
                    .into_iter()
                    .find(|w| w.id == worker_id)
                    .is_some_and(|w| {
                        matches!(w.status, WorkerStatus::Draining | WorkerStatus::Stopped)
                    });
                if !was_expected {
                    self.handle_worker_failure(
                        worker_id,
                        &format!("IPC disconnected: {reason}"),
                        now_utc,
                    );
                }
            }
            ExecutorEvent::IpcMessageReceived { worker_id, message } => {
                self.handle_worker_ipc_message(worker_id, message, now_utc);
            }
        }
    }

    fn handle_worker_ipc_message(&mut self, worker_id: WorkerId, msg: IpcMessage, now_utc: i64) {
        match msg {
            IpcMessage::ExecutionAccepted(m) => {
                if let Some(wl) = self.workloads.get_mut(&m.workload_id)
                    && wl.status == WorkloadStatus::Scheduled
                {
                    let _ = wl.transition_status(WorkloadStatus::Starting, now_utc);
                    let _ = self.storage.update_workload(wl);
                }
            }
            IpcMessage::ExecutionStarted(m) => {
                if let Some(wl) = self.workloads.get_mut(&m.workload_id) {
                    if wl.status == WorkloadStatus::Scheduled {
                        let _ = wl.transition_status(WorkloadStatus::Starting, now_utc);
                    }
                    let _ = wl.transition_status(WorkloadStatus::Running, now_utc);
                    let _ = self.storage.update_workload(wl);
                    let mut ev = SystemEvent::new(
                        SystemEventKind::WorkloadStarted,
                        EventSeverity::Info,
                        format!("Started '{}' on worker {}", wl.name, worker_id),
                        now_utc,
                    );
                    ev.workload_id = Some(wl.id);
                    ev.worker_id = Some(worker_id);
                    self.record_event(ev);
                }
            }
            IpcMessage::ExecutionProgress(m) => {
                if let Some(wl) = self.workloads.get_mut(&m.workload_id)
                    && !wl.status.is_terminal()
                {
                    wl.progress = m.progress;
                    wl.updated_at_utc = now_utc;
                }
            }
            IpcMessage::ExecutionCompleted(m) => {
                self.active_clocks.remove(&m.execution_id);
                let mut workers = self.supervisor.worker_infos();
                self.resources.release_for_workload(m.workload_id, &mut workers);
                if let Some(w) = workers.iter_mut().find(|w| w.id == worker_id) {
                    if w.reserved_cpu_cores == 0 && w.status == WorkerStatus::Busy {
                        w.status = WorkerStatus::Idle;
                    }
                    w.completed_executions = w.completed_executions.saturating_add(1);
                }
                self.supervisor.update_worker_infos_snapshot(&workers);

                if let Some(wl) = self.workloads.get_mut(&m.workload_id) {
                    if wl.status == WorkloadStatus::Starting {
                        let _ = wl.transition_status(WorkloadStatus::Running, now_utc);
                    }
                    let _ = wl.transition_status(WorkloadStatus::Completed, now_utc);
                    wl.progress = ProgressState::determinate(100, 100, Some("Completed".into()));
                    wl.result_metadata = Some(m.result.clone());
                    wl.assigned_worker_id = Some(worker_id);
                    let _ = self.storage.update_workload(wl);

                    let exec = ExecutionRecord {
                        id: m.execution_id,
                        workload_id: m.workload_id,
                        worker_id,
                        attempt_number: wl.attempt_count,
                        status: WorkloadStatus::Completed,
                        outcome: Some(ExecutionOutcome::Completed),
                        started_at_utc: now_utc.saturating_sub(m.result.duration_ms as i64),
                        finished_at_utc: Some(now_utc),
                        duration_ms: Some(m.result.duration_ms),
                        exit_code: m.result.exit_code,
                        error_message: None,
                        result_json: Some(m.result.metrics_json.clone()),
                        log_file_path: None,
                    };
                    let _ = self.storage.update_execution(&exec);
                    Self::upsert_cached_execution(&mut self.executions_by_workload, exec);

                    for rep in m.artifacts {
                        let art = ExecutionArtifact {
                            id: ArtifactId::new(),
                            execution_id: m.execution_id,
                            workload_id: m.workload_id,
                            name: rep.name,
                            kind: parse_artifact_kind(&rep.kind),
                            file_path: rep.file_path,
                            size_bytes: rep.size_bytes,
                            sha256_hex: rep.sha256_hex,
                            created_at_utc: now_utc,
                        };
                        let _ = self.storage.insert_artifact(&art);
                        self.artifacts_by_workload.entry(m.workload_id).or_default().insert(0, art);
                    }

                    let mut ev = SystemEvent::new(
                        SystemEventKind::WorkloadCompleted,
                        EventSeverity::Info,
                        format!("Completed '{}': {}", wl.name, m.result.summary),
                        now_utc,
                    );
                    ev.workload_id = Some(wl.id);
                    ev.worker_id = Some(worker_id);
                    self.record_event(ev);
                }
            }
            IpcMessage::ExecutionFailed(m) => {
                self.active_clocks.remove(&m.execution_id);
                let mut workers = self.supervisor.worker_infos();
                self.resources.release_for_workload(m.workload_id, &mut workers);
                if let Some(w) = workers.iter_mut().find(|w| w.id == worker_id) {
                    if w.reserved_cpu_cores == 0 && w.status == WorkerStatus::Busy {
                        w.status = WorkerStatus::Idle;
                    }
                    w.failed_executions = w.failed_executions.saturating_add(1);
                }
                self.supervisor.update_worker_infos_snapshot(&workers);

                if let Some(wl) = self.workloads.get_mut(&m.workload_id) {
                    if wl.status == WorkloadStatus::Starting {
                        let _ = wl.transition_status(WorkloadStatus::Running, now_utc);
                    }
                    let is_timeout = m.reason.contains("timed out");
                    let target_status =
                        if is_timeout { WorkloadStatus::TimedOut } else { WorkloadStatus::Failed };
                    let _ = wl.transition_status(target_status, now_utc);
                    wl.error_message = Some(m.reason.clone());
                    let _ = self.storage.update_workload(wl);

                    let exec = ExecutionRecord {
                        id: m.execution_id,
                        workload_id: m.workload_id,
                        worker_id,
                        attempt_number: wl.attempt_count,
                        status: target_status,
                        outcome: Some(m.outcome),
                        started_at_utc: now_utc.saturating_sub(m.duration_ms as i64),
                        finished_at_utc: Some(now_utc),
                        duration_ms: Some(m.duration_ms),
                        exit_code: m.exit_code,
                        error_message: Some(m.reason.clone()),
                        result_json: None,
                        log_file_path: None,
                    };
                    let _ = self.storage.update_execution(&exec);
                    Self::upsert_cached_execution(&mut self.executions_by_workload, exec);

                    let kind = if is_timeout {
                        SystemEventKind::WorkloadTimedOut
                    } else {
                        SystemEventKind::WorkloadFailed
                    };
                    let mut ev = SystemEvent::new(
                        kind,
                        EventSeverity::Error,
                        format!("Workload '{}' failed: {}", wl.name, m.reason),
                        now_utc,
                    );
                    ev.workload_id = Some(wl.id);
                    ev.worker_id = Some(worker_id);
                    self.record_event(ev);
                }
            }
            IpcMessage::ExecutionCancelled(m) => {
                let was_timeout =
                    self.active_clocks.remove(&m.execution_id).is_some_and(|c| c.timed_out);
                let mut workers = self.supervisor.worker_infos();
                self.resources.release_for_workload(m.workload_id, &mut workers);
                if let Some(w) = workers.iter_mut().find(|w| w.id == worker_id)
                    && w.reserved_cpu_cores == 0
                    && w.status == WorkerStatus::Busy
                {
                    w.status = WorkerStatus::Idle;
                }
                self.supervisor.update_worker_infos_snapshot(&workers);

                if let Some(wl) = self.workloads.get_mut(&m.workload_id) {
                    let target_status = if was_timeout {
                        WorkloadStatus::TimedOut
                    } else {
                        WorkloadStatus::Cancelled
                    };
                    let reason = if was_timeout {
                        format!("Timed out after {} ms", wl.timeout_ms)
                    } else {
                        m.reason.clone()
                    };
                    let _ = wl.transition_status(target_status, now_utc);
                    wl.error_message = Some(reason.clone());
                    let _ = self.storage.update_workload(wl);

                    let exec = ExecutionRecord {
                        id: m.execution_id,
                        workload_id: m.workload_id,
                        worker_id,
                        attempt_number: wl.attempt_count,
                        status: target_status,
                        outcome: Some(ExecutionOutcome::Failed),
                        started_at_utc: now_utc.saturating_sub(m.duration_ms as i64),
                        finished_at_utc: Some(now_utc),
                        duration_ms: Some(m.duration_ms),
                        exit_code: None,
                        error_message: Some(reason.clone()),
                        result_json: None,
                        log_file_path: None,
                    };
                    let _ = self.storage.update_execution(&exec);
                    Self::upsert_cached_execution(&mut self.executions_by_workload, exec);

                    let (ev_kind, ev_sev, ev_msg) = if was_timeout {
                        (
                            SystemEventKind::WorkloadTimedOut,
                            EventSeverity::Error,
                            format!("Workload '{}' timed out: {}", wl.name, reason),
                        )
                    } else {
                        (
                            SystemEventKind::WorkloadCancelled,
                            EventSeverity::Info,
                            format!("Cancelled '{}': {}", wl.name, reason),
                        )
                    };
                    let mut ev = SystemEvent::new(ev_kind, ev_sev, ev_msg, now_utc);
                    ev.workload_id = Some(wl.id);
                    ev.worker_id = Some(worker_id);
                    self.record_event(ev);
                }
            }
            IpcMessage::DiagnosticEvent(d) => {
                let mut ev = SystemEvent::new(
                    SystemEventKind::EngineStarted,
                    d.severity,
                    d.message,
                    now_utc,
                );
                ev.worker_id = Some(d.worker_id);
                ev.workload_id = d.workload_id;
                self.record_event(ev);
            }
            _ => {}
        }
    }

    async fn handle_periodic_supervision(&mut self) {
        let now_utc = now_utc_millis();

        // 1. OS process exit check
        let exited = self.supervisor.poll_exited_workers();
        for (worker_id, exit_code) in exited {
            self.handle_worker_failure(
                worker_id,
                &format!("Worker process terminated unexpectedly with exit code {exit_code}"),
                now_utc,
            );
        }

        // 2. Heartbeat timeout check
        let timed_out_workers: Vec<WorkerId> = self
            .supervisor
            .worker_infos()
            .into_iter()
            .filter(|w| w.status.is_alive())
            .filter(|w| {
                let ref_ts = w.last_heartbeat_utc.unwrap_or(w.started_at_utc);
                now_utc.saturating_sub(ref_ts) as u64 > self.config.heartbeat_timeout_ms
            })
            .map(|w| w.id)
            .collect();

        for wid in timed_out_workers {
            let _ = self.supervisor.force_terminate_worker(wid);
            self.handle_worker_failure(wid, "Worker heartbeat timeout exceeded", now_utc);
        }

        // 3. Execution timeout supervision
        let mut timed_out_execs = Vec::new();
        for (exec_id, clock) in &mut self.active_clocks {
            let elapsed_ms = clock.started_instant.elapsed().as_millis() as u64;
            if elapsed_ms > clock.timeout_ms {
                if clock.cancel_requested_at.is_none() {
                    clock.cancel_requested_at = Some(Instant::now());
                    clock.timed_out = true;
                    let _ = self
                        .supervisor
                        .cancel_execution(
                            clock.worker_id,
                            clock.workload_id,
                            *exec_id,
                            "Execution timeout exceeded",
                        )
                        .await;
                } else if let Some(req_at) = clock.cancel_requested_at
                    && req_at.elapsed() > Duration::from_millis(1200)
                {
                    timed_out_execs.push((
                        *exec_id,
                        clock.workload_id,
                        clock.worker_id,
                        elapsed_ms,
                    ));
                }
            }
        }

        for (exec_id, wl_id, worker_id, elapsed_ms) in timed_out_execs {
            self.active_clocks.remove(&exec_id);
            let _ = self.supervisor.force_terminate_worker(worker_id);
            let mut workers = self.supervisor.worker_infos();
            let w_mut = workers.iter_mut().find(|w| w.id == worker_id);
            let _ = self.resources.release_all_for_worker(worker_id, w_mut);
            self.supervisor.update_worker_infos_snapshot(&workers);

            let mut attempt_number = 1;
            if let Some(wl) = self.workloads.get_mut(&wl_id) {
                attempt_number = wl.attempt_count;
                let _ = wl.transition_status(WorkloadStatus::TimedOut, now_utc);
                wl.error_message =
                    Some(format!("Timed out after {elapsed_ms} ms (worker terminated)"));
                let _ = self.storage.update_workload(wl);
            }
            let exec = ExecutionRecord {
                id: exec_id,
                workload_id: wl_id,
                worker_id,
                attempt_number,
                status: WorkloadStatus::TimedOut,
                outcome: Some(ExecutionOutcome::Failed),
                started_at_utc: now_utc.saturating_sub(elapsed_ms as i64),
                finished_at_utc: Some(now_utc),
                duration_ms: Some(elapsed_ms),
                exit_code: None,
                error_message: Some("Timed out and worker terminated".to_string()),
                result_json: None,
                log_file_path: None,
            };
            let _ = self.storage.update_execution(&exec);
            Self::upsert_cached_execution(&mut self.executions_by_workload, exec);
            let mut ev = SystemEvent::new(
                SystemEventKind::WorkloadTimedOut,
                EventSeverity::Error,
                format!("Workload {wl_id} exceeded timeout and worker {worker_id} was terminated"),
                now_utc,
            );
            ev.workload_id = Some(wl_id);
            ev.worker_id = Some(worker_id);
            self.record_event(ev);
        }
    }

    fn handle_worker_failure(&mut self, worker_id: WorkerId, reason: &str, now_utc: i64) {
        let mut workers = self.supervisor.worker_infos();
        if let Some(w) = workers.iter_mut().find(|w| w.id == worker_id) {
            if w.status == WorkerStatus::Crashed && w.reserved_cpu_cores == 0 {
                return;
            }
            w.status = WorkerStatus::Crashed;
            w.failed_executions = w.failed_executions.saturating_add(1);
        }
        let target = workers.iter_mut().find(|w| w.id == worker_id);
        let released = self.resources.release_all_for_worker(worker_id, target);
        self.supervisor.update_worker_infos_snapshot(&workers);

        if let Some(info) = workers.iter().find(|w| w.id == worker_id) {
            let _ = self.storage.upsert_worker(info, now_utc);
        }

        let mut ev = SystemEvent::new(
            SystemEventKind::WorkerCrashed,
            EventSeverity::Critical,
            format!("Worker {worker_id} failed: {reason}"),
            now_utc,
        );
        ev.worker_id = Some(worker_id);
        self.record_event(ev);

        for res in released {
            self.active_clocks.remove(&res.execution_id);
            self.mark_workload_interrupted(
                res.workload_id,
                res.execution_id,
                Some(worker_id),
                reason,
                now_utc,
            );
        }
    }

    fn mark_workload_interrupted(
        &mut self,
        workload_id: WorkloadId,
        execution_id: ExecutionId,
        worker_id_opt: Option<WorkerId>,
        reason: &str,
        now_utc: i64,
    ) {
        let Some(wl) = self.workloads.get_mut(&workload_id) else {
            return;
        };
        if wl.status.is_terminal() {
            return;
        }
        let prev_worker = worker_id_opt.or(wl.assigned_worker_id);
        wl.assigned_worker_id = None;
        let _ = wl.transition_status(WorkloadStatus::Interrupted, now_utc);
        wl.error_message = Some(reason.to_string());

        if let Some(wid) = prev_worker {
            let exec = ExecutionRecord {
                id: execution_id,
                workload_id,
                worker_id: wid,
                attempt_number: wl.attempt_count,
                status: WorkloadStatus::Interrupted,
                outcome: Some(ExecutionOutcome::Interrupted),
                started_at_utc: now_utc,
                finished_at_utc: Some(now_utc),
                duration_ms: None,
                exit_code: None,
                error_message: Some(reason.to_string()),
                result_json: None,
                log_file_path: None,
            };
            let _ = self.storage.update_execution(&exec);
            Self::upsert_cached_execution(&mut self.executions_by_workload, exec);
        }

        let can_retry = self.config.auto_recover_idempotent_interrupted
            && wl.retry_policy.retry_safety == RetrySafety::SafeIdempotent
            && wl.attempt_count <= wl.retry_policy.max_retries
            && wl.spec.default_retry_safety() == RetrySafety::SafeIdempotent;

        if can_retry
            && wl.transition_status(WorkloadStatus::Retrying, now_utc).is_ok()
            && self.scheduler.enqueue(wl, &self.config, now_utc).is_ok()
        {
            self.recovery_summary.auto_recovered_workloads += 1;
            let mut ev = SystemEvent::new(
                SystemEventKind::RecoveryReconciled,
                EventSeverity::Warning,
                format!("Auto-retrying interrupted idempotent workload '{}'", wl.name),
                now_utc,
            );
            ev.workload_id = Some(workload_id);
            let _ = self.storage.update_workload(wl);
            self.record_event(ev);
        } else {
            let _ = self.storage.update_workload(wl);
            let mut ev = SystemEvent::new(
                SystemEventKind::WorkloadInterrupted,
                EventSeverity::Error,
                format!("Workload '{}' interrupted: {}", wl.name, reason),
                now_utc,
            );
            ev.workload_id = Some(workload_id);
            self.record_event(ev);
        }
    }

    async fn run_scheduling_and_workflows(&mut self) {
        let now_utc = now_utc_millis();

        // 1. Advance active DAG workflows and enqueue newly ready tasks
        if self.workflows.values().any(|w| w.status == WorkflowStatus::Running) {
            let artifacts_map: HashMap<WorkloadId, Vec<String>> = self
                .artifacts_by_workload
                .iter()
                .map(|(k, v)| (*k, v.iter().map(|a| a.file_path.clone()).collect()))
                .collect();

            let wf_ids: Vec<WorkflowId> = self.workflows.keys().copied().collect();
            for wf_id in wf_ids {
                let mut tasks_to_dispatch = Vec::new();
                let mut wf_finished_ev: Option<SystemEvent> = None;

                if let Some(wf) = self.workflows.get_mut(&wf_id) {
                    if wf.status != WorkflowStatus::Running {
                        continue;
                    }
                    let ready_ids =
                        WorkflowScheduler::evaluate_workflow(wf, &self.workloads, now_utc);
                    for tid in ready_ids {
                        if let Some(idx) = wf.tasks.iter().position(|t| t.id == tid) {
                            let upstream = collect_upstream_artifact_paths(
                                &wf.tasks[idx],
                                &wf.tasks,
                                &artifacts_map,
                            );
                            tasks_to_dispatch.push((idx, wf.tasks[idx].clone(), upstream));
                        }
                    }
                    if wf.status == WorkflowStatus::Completed {
                        let mut ev = SystemEvent::new(
                            SystemEventKind::WorkflowCompleted,
                            EventSeverity::Info,
                            format!("Workflow '{}' completed successfully", wf.name),
                            now_utc,
                        );
                        ev.workflow_id = Some(wf_id);
                        wf_finished_ev = Some(ev);
                    } else if wf.status == WorkflowStatus::Failed {
                        let mut ev = SystemEvent::new(
                            SystemEventKind::WorkflowFailed,
                            EventSeverity::Error,
                            format!("Workflow '{}' failed due to stage failure", wf.name),
                            now_utc,
                        );
                        ev.workflow_id = Some(wf_id);
                        wf_finished_ev = Some(ev);
                    }
                    let _ = self.storage.update_workflow_state(wf);
                }

                if let Some(ev) = wf_finished_ev {
                    self.record_event(ev);
                }

                for (task_idx, task_def, upstream) in tasks_to_dispatch {
                    let wf_name =
                        self.workflows.get(&wf_id).map(|w| w.name.clone()).unwrap_or_default();
                    let req = SubmitWorkloadRequest {
                        name: format!("{} · {}", wf_name, task_def.name),
                        description: format!("Workflow stage '{}'", task_def.name),
                        priority: task_def.priority,
                        spec: WorkloadSpec::WorkflowTask(WorkflowTaskInput {
                            workflow_id: wf_id,
                            task_id: task_def.id,
                            stage_name: task_def.name.clone(),
                            inner: Box::new(task_def.spec.clone()),
                            upstream_artifacts: upstream,
                        }),
                        resources: task_def.resources.clone(),
                        timeout_ms: Some(task_def.timeout_ms),
                        retry_policy: Some(task_def.retry_policy.clone()),
                    };
                    match self.submit_workload_internal(req, now_utc) {
                        Ok(wl_id) => {
                            if let Some(wf) = self.workflows.get_mut(&wf_id) {
                                wf.tasks[task_idx].state = WorkflowTaskState::Dispatched;
                                wf.tasks[task_idx].workload_id = Some(wl_id);
                                let _ = self.storage.update_workflow_state(wf);
                            }
                        }
                        Err(err) => {
                            if let Some(wf) = self.workflows.get_mut(&wf_id) {
                                wf.tasks[task_idx].state = WorkflowTaskState::Failed;
                                wf.status = WorkflowStatus::Failed;
                                let _ = self.storage.update_workflow_state(wf);
                            }
                            warn!("failed to dispatch workflow stage: {err}");
                        }
                    }
                }
            }
        }

        // 2. Run deterministic scheduler pass
        let mut workers = self.supervisor.worker_infos();
        let dispatches = self.scheduler.schedule_ready(
            &mut self.workloads,
            &mut workers,
            &mut self.resources,
            &self.config,
            now_utc,
        );
        self.supervisor.update_worker_infos_snapshot(&workers);

        for dispatch in dispatches {
            let Some(wl) = self.workloads.get_mut(&dispatch.workload_id) else {
                continue;
            };
            let artifact_dir = self
                .paths
                .artifacts_dir
                .join(dispatch.workload_id.to_string())
                .join(dispatch.execution_id.to_string());
            let exec_rec = ExecutionRecord {
                id: dispatch.execution_id,
                workload_id: dispatch.workload_id,
                worker_id: dispatch.worker_id,
                attempt_number: wl.attempt_count,
                status: WorkloadStatus::Scheduled,
                outcome: None,
                started_at_utc: now_utc,
                finished_at_utc: None,
                duration_ms: None,
                exit_code: None,
                error_message: None,
                result_json: None,
                log_file_path: Some(artifact_dir.to_string_lossy().into_owned()),
            };
            let _ = self.storage.update_workload(wl);
            let _ = self.storage.insert_execution(&exec_rec);
            self.executions_by_workload
                .entry(dispatch.workload_id)
                .or_default()
                .insert(0, exec_rec);

            if let Some(w) = self.supervisor.worker_info_mut(dispatch.worker_id) {
                w.status = WorkerStatus::Busy;
            }

            let req_msg = ExecutionRequestMsg {
                workload_id: dispatch.workload_id,
                execution_id: dispatch.execution_id,
                attempt_number: wl.attempt_count,
                spec: wl.spec.clone(),
                resources: wl.resources.clone(),
                timeout_ms: wl.timeout_ms,
                artifact_dir: artifact_dir.to_string_lossy().into_owned(),
            };

            if let Err(err) = self.supervisor.dispatch_execution(dispatch.worker_id, req_msg).await
            {
                let mut workers_after = self.supervisor.worker_infos();
                self.resources.release_for_workload(dispatch.workload_id, &mut workers_after);
                if let Some(w) = workers_after.iter_mut().find(|w| w.id == dispatch.worker_id) {
                    w.status = WorkerStatus::Unhealthy;
                }
                self.supervisor.update_worker_infos_snapshot(&workers_after);
                if let Some(wl_mut) = self.workloads.get_mut(&dispatch.workload_id) {
                    let _ = wl_mut.transition_status(WorkloadStatus::Interrupted, now_utc);
                    wl_mut.error_message = Some(format!("IPC dispatch failed: {err}"));
                    let _ = self.storage.update_workload(wl_mut);
                }
            } else {
                self.active_clocks.insert(
                    dispatch.execution_id,
                    ActiveExecutionClock {
                        workload_id: dispatch.workload_id,
                        worker_id: dispatch.worker_id,
                        started_instant: Instant::now(),
                        timeout_ms: wl.timeout_ms,
                        cancel_requested_at: None,
                        timed_out: false,
                    },
                );
                let mut ev = SystemEvent::new(
                    SystemEventKind::WorkloadScheduled,
                    EventSeverity::Info,
                    format!(
                        "Scheduled '{}' on worker {} (queue wait {} ms)",
                        wl.name, dispatch.worker_id, dispatch.queue_latency_ms
                    ),
                    now_utc,
                );
                ev.workload_id = Some(dispatch.workload_id);
                ev.worker_id = Some(dispatch.worker_id);
                self.record_event(ev);
            }
        }
    }

    fn sample_and_persist_metrics(&mut self) {
        let mut workers = self.supervisor.worker_infos();
        let active_execs = self.active_clocks.len() as u32;
        let queue_depth = self.scheduler.queue_depth() as u32;
        let snap = self.resources.refresh_snapshot(&mut workers, active_execs, queue_depth);
        self.supervisor.update_worker_infos_snapshot(&workers);

        if let Err(err) =
            self.storage.insert_resource_snapshot(&snap, self.config.snapshot_retention_max_rows)
        {
            warn!("failed to persist resource snapshot: {err}");
        }
        self.historical_snapshots.push(snap);
        if self.historical_snapshots.len() > 120 {
            let overflow = self.historical_snapshots.len() - 120;
            self.historical_snapshots.drain(0..overflow);
        }
    }

    fn export_diagnostics_internal(&self, dest_dir: Option<PathBuf>) -> Result<PathBuf, CoreError> {
        let dir = dest_dir.unwrap_or_else(|| self.paths.exports_dir.clone());
        let bundle = DiagnosticBundle {
            generated_at_utc: now_utc_millis(),
            application_version: env!("CARGO_PKG_VERSION").to_string(),
            os_description: windows_platform_summary(),
            database_status: format!("{:?}", self.storage.status_report()),
            ipc_status: format!(
                "Local Windows Named Pipes ({} active workers)",
                self.supervisor.worker_infos().iter().filter(|w| w.status.is_alive()).count()
            ),
            latest_metrics: Some(self.resources.latest_snapshot().clone()),
            active_worker_count: self
                .supervisor
                .worker_infos()
                .iter()
                .filter(|w| w.status.is_alive())
                .count(),
            recent_errors: self.event_ring.recent_errors(),
            recent_events: self.event_ring.snapshot(),
        };
        Ok(export_diagnostic_bundle(&dir, &bundle)?)
    }

    fn build_snapshot(&self) -> EngineSnapshot {
        let mut workloads_vec: Vec<Workload> = self.workloads.values().cloned().collect();
        workloads_vec.sort_by_key(|a| std::cmp::Reverse(a.created_at_utc));

        let mut workflows_vec: Vec<WorkflowDefinition> = self.workflows.values().cloned().collect();
        workflows_vec.sort_by_key(|a| std::cmp::Reverse(a.created_at_utc));

        let mut workers = self.supervisor.worker_infos();
        workers.sort_by(|a, b| a.name.cmp(&b.name));

        let executions_by_workload = self.executions_by_workload.clone();
        let artifacts_by_workload = self.artifacts_by_workload.clone();
        let db_status = self.cached_db_status.clone();

        let alive_workers = workers.iter().filter(|w| w.status.is_alive()).count();
        let (health, health_reason) = if !db_status.integrity_ok {
            (EngineHealth::Degraded, "Database integrity warning detected".to_string())
        } else if alive_workers == 0 {
            (EngineHealth::Degraded, "No live worker processes running".to_string())
        } else {
            (self.health, self.health_reason.clone())
        };

        EngineSnapshot {
            app_version: env!("CARGO_PKG_VERSION").to_string(),
            health,
            health_reason,
            config: self.config.clone(),
            latest_metrics: self.resources.latest_snapshot().clone(),
            historical_metrics: self.historical_snapshots.clone(),
            workers,
            workloads: workloads_vec,
            workflows: workflows_vec,
            executions_by_workload,
            artifacts_by_workload,
            events: self.event_ring.snapshot(),
            scheduler_diagnostics: self.scheduler.diagnostics().clone(),
            database_status: db_status,
            recovery_summary: self.recovery_summary.clone(),
            ipc_status: format!("Local Windows Named Pipes (v1, {alive_workers} live channels)"),
            reduced_motion_os: self.reduced_motion_os,
        }
    }

    fn upsert_cached_execution(
        map: &mut HashMap<WorkloadId, Vec<ExecutionRecord>>,
        exec: ExecutionRecord,
    ) {
        let list = map.entry(exec.workload_id).or_default();
        if let Some(existing) = list.iter_mut().find(|e| e.id == exec.id) {
            *existing = exec;
        } else {
            list.insert(0, exec);
        }
    }
}

fn parse_artifact_kind(s: &str) -> ArtifactKind {
    match s {
        "StdoutLog" => ArtifactKind::StdoutLog,
        "StderrLog" => ArtifactKind::StderrLog,
        "BatchOutput" => ArtifactKind::BatchOutput,
        "DiagnosticReport" => ArtifactKind::DiagnosticReport,
        _ => ArtifactKind::ResultJson,
    }
}
