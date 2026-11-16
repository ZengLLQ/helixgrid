use helixgrid_config::{AppConfig, SchedulingPolicy};
use helixgrid_domain::{
    DomainError, ExecutionId, Priority, WorkerId, WorkerInfo, WorkflowDefinition, WorkflowStatus,
    WorkflowTaskDefinition, WorkflowTaskId, WorkflowTaskState, Workload, WorkloadId,
    WorkloadStatus,
};
use helixgrid_resource::{ResourceError, ResourceManager, ResourceReservation};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("scheduler queue is full (capacity {capacity}): backpressure active")]
    QueueFull { capacity: usize },

    #[error("workload {0} is already queued or scheduled")]
    DuplicateWorkload(WorkloadId),

    #[error("workload {0} is not in a schedulable state ({1:?})")]
    NotSchedulable(WorkloadId, WorkloadStatus),

    #[error("domain error: {0}")]
    Domain(#[from] DomainError),

    #[error("resource error: {0}")]
    Resource(#[from] ResourceError),
}

#[derive(Debug, Clone)]
pub struct QueuedEntry {
    pub workload_id: WorkloadId,
    pub priority: Priority,
    pub sequence: u64,
    pub enqueued_at_utc: i64,
}

#[derive(Debug, Clone)]
pub struct ScheduledDispatch {
    pub workload_id: WorkloadId,
    pub execution_id: ExecutionId,
    pub worker_id: WorkerId,
    pub reservation: ResourceReservation,
    pub queue_latency_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchedulerDiagnostics {
    pub queued_count: usize,
    pub total_dispatched: u64,
    pub total_backpressure_rejections: u64,
    pub last_scheduling_pass_ns: u64,
    pub average_queue_latency_ms: f64,
}

/// Pure deterministic, Windows-independent resource-aware scheduler.
pub struct SchedulerEngine {
    queue: Vec<QueuedEntry>,
    queued_ids: HashSet<WorkloadId>,
    next_sequence: u64,
    diagnostics: SchedulerDiagnostics,
    total_queue_latency_ms: u64,
}

impl Default for SchedulerEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulerEngine {
    #[must_use]
    pub fn new() -> Self {
        Self {
            queue: Vec::new(),
            queued_ids: HashSet::new(),
            next_sequence: 1,
            diagnostics: SchedulerDiagnostics::default(),
            total_queue_latency_ms: 0,
        }
    }

    pub fn enqueue(
        &mut self,
        workload: &mut Workload,
        config: &AppConfig,
        now_utc: i64,
    ) -> Result<(), SchedulerError> {
        if self.queued_ids.contains(&workload.id) {
            return Err(SchedulerError::DuplicateWorkload(workload.id));
        }
        if self.queue.len() >= config.max_queue_depth as usize {
            self.diagnostics.total_backpressure_rejections += 1;
            return Err(SchedulerError::QueueFull { capacity: config.max_queue_depth as usize });
        }
        if workload.status == WorkloadStatus::Submitted {
            workload.transition_status(WorkloadStatus::Validating, now_utc)?;
            workload.spec.validate()?;
            workload.resources.validate()?;
            workload.transition_status(WorkloadStatus::Queued, now_utc)?;
        } else if workload.status == WorkloadStatus::Retrying {
            workload.transition_status(WorkloadStatus::Queued, now_utc)?;
        } else if workload.status != WorkloadStatus::Queued {
            return Err(SchedulerError::NotSchedulable(workload.id, workload.status));
        }

        let seq = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.queued_ids.insert(workload.id);
        self.queue.push(QueuedEntry {
            workload_id: workload.id,
            priority: workload.priority,
            sequence: seq,
            enqueued_at_utc: now_utc,
        });
        self.diagnostics.queued_count = self.queue.len();
        Ok(())
    }

    pub fn remove_from_queue(&mut self, workload_id: WorkloadId) -> bool {
        if !self.queued_ids.remove(&workload_id) {
            return false;
        }
        self.queue.retain(|e| e.workload_id != workload_id);
        self.diagnostics.queued_count = self.queue.len();
        true
    }

    #[must_use]
    pub fn queue_depth(&self) -> usize {
        self.queue.len()
    }

    #[must_use]
    pub fn queued_ids(&self) -> Vec<WorkloadId> {
        self.queue.iter().map(|e| e.workload_id).collect()
    }

    #[must_use]
    pub fn diagnostics(&self) -> &SchedulerDiagnostics {
        &self.diagnostics
    }

    /// Runs one deterministic scheduling pass, selecting eligible queued workloads
    /// and matching them against available workers while obeying capacity & concurrency ceilings.
    pub fn schedule_ready(
        &mut self,
        workloads: &mut HashMap<WorkloadId, Workload>,
        workers: &mut [WorkerInfo],
        resources: &mut ResourceManager,
        config: &AppConfig,
        now_utc: i64,
    ) -> Vec<ScheduledDispatch> {
        let pass_start = Instant::now();
        let mut dispatches = Vec::new();

        // Sort queue deterministically according to configured scheduling policy.
        match config.scheduling_policy {
            SchedulingPolicy::PriorityResourceAware => {
                self.queue.sort_by(|a, b| {
                    b.priority.cmp(&a.priority).then_with(|| a.sequence.cmp(&b.sequence))
                });
            }
            SchedulingPolicy::FifoResourceAware => {
                self.queue.sort_by_key(|a| a.sequence);
            }
        }

        let mut active_count = workloads.values().filter(|w| w.status.is_active()).count() as u32;

        let mut remaining = Vec::with_capacity(self.queue.len());
        let pending_entries = std::mem::take(&mut self.queue);

        for entry in pending_entries {
            if active_count >= config.max_concurrent_workloads {
                remaining.push(entry);
                continue;
            }

            let Some(wl) = workloads.get_mut(&entry.workload_id) else {
                self.queued_ids.remove(&entry.workload_id);
                continue;
            };
            if wl.status != WorkloadStatus::Queued {
                self.queued_ids.remove(&entry.workload_id);
                continue;
            }

            if resources.check_host_headroom(config, &wl.resources).is_err() {
                remaining.push(entry);
                continue;
            }

            // Pick best eligible idle worker (most available headroom, deterministic tie-breaker).
            let best_worker_idx = select_best_worker(workers, wl);
            let Some(worker_idx) = best_worker_idx else {
                remaining.push(entry);
                continue;
            };

            let exec_id = ExecutionId::new();
            let worker = &mut workers[worker_idx];
            match resources.reserve(config, worker, wl.id, exec_id, &wl.resources, now_utc) {
                Ok(reservation) => {
                    if wl.transition_status(WorkloadStatus::Scheduled, now_utc).is_ok() {
                        wl.assigned_worker_id = Some(worker.id);
                        wl.latest_execution_id = Some(exec_id);
                        wl.attempt_count = wl.attempt_count.saturating_add(1);
                        worker.current_workload_id = Some(wl.id);
                        worker.current_execution_id = Some(exec_id);
                        self.queued_ids.remove(&entry.workload_id);
                        active_count += 1;

                        let q_lat = now_utc.saturating_sub(entry.enqueued_at_utc).max(0) as u64;
                        self.total_queue_latency_ms =
                            self.total_queue_latency_ms.saturating_add(q_lat);
                        self.diagnostics.total_dispatched += 1;
                        self.diagnostics.average_queue_latency_ms = (self.total_queue_latency_ms
                            as f64)
                            / (self.diagnostics.total_dispatched.max(1) as f64);

                        dispatches.push(ScheduledDispatch {
                            workload_id: wl.id,
                            execution_id: exec_id,
                            worker_id: worker.id,
                            reservation,
                            queue_latency_ms: q_lat,
                        });
                    } else {
                        let _ = resources.release_for_workload(wl.id, workers);
                        remaining.push(entry);
                    }
                }
                Err(_) => {
                    remaining.push(entry);
                }
            }
        }

        self.queue = remaining;
        self.diagnostics.queued_count = self.queue.len();
        self.diagnostics.last_scheduling_pass_ns = pass_start.elapsed().as_nanos() as u64;
        dispatches
    }
}

fn select_best_worker(workers: &[WorkerInfo], wl: &Workload) -> Option<usize> {
    let mut best: Option<(usize, u32, u64, i64, WorkerId)> = None;
    for (idx, w) in workers.iter().enumerate() {
        if !w.status.is_available_for_scheduling() {
            continue;
        }
        if wl.resources.requires_exclusive_worker && w.reserved_cpu_cores > 0 {
            continue;
        }
        let avail_cpu = w.available_cpu_cores();
        let avail_mem = w.available_memory_mb();
        if avail_cpu < wl.resources.cpu_cores || avail_mem < wl.resources.memory_mb {
            continue;
        }
        let candidate = (idx, avail_cpu, avail_mem, w.started_at_utc, w.id);
        let replace = match best {
            None => true,
            Some((_, best_cpu, best_mem, best_started, best_id)) => {
                match avail_cpu.cmp(&best_cpu) {
                    Ordering::Greater => true,
                    Ordering::Less => false,
                    Ordering::Equal => match avail_mem.cmp(&best_mem) {
                        Ordering::Greater => true,
                        Ordering::Less => false,
                        Ordering::Equal => (w.started_at_utc, w.id) < (best_started, best_id),
                    },
                }
            }
        };
        if replace {
            best = Some(candidate);
        }
    }
    best.map(|b| b.0)
}

/// Evaluates workflow task readiness and failure propagation on a DAG.
pub struct WorkflowScheduler;

impl WorkflowScheduler {
    /// Updates workflow task states from their linked workloads, propagates upstream failures,
    /// and returns indices of `Pending` tasks whose dependencies are all `Completed`.
    pub fn evaluate_workflow(
        wf: &mut WorkflowDefinition,
        workloads: &HashMap<WorkloadId, Workload>,
        now_utc: i64,
    ) -> Vec<WorkflowTaskId> {
        if matches!(
            wf.status,
            WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Cancelled
        ) {
            return Vec::new();
        }

        // Sync Dispatched task states from workload states.
        let wf_is_running = wf.status == WorkflowStatus::Running;
        for task in &mut wf.tasks {
            if task.state == WorkflowTaskState::Dispatched
                && let Some(wl_id) = task.workload_id
                && let Some(wl) = workloads.get(&wl_id)
            {
                match wl.status {
                    WorkloadStatus::Completed => {
                        task.state = WorkflowTaskState::Completed;
                    }
                    WorkloadStatus::Failed | WorkloadStatus::TimedOut => {
                        task.state = WorkflowTaskState::Failed;
                    }
                    WorkloadStatus::Cancelled => {
                        task.state = WorkflowTaskState::Cancelled;
                    }
                    WorkloadStatus::Interrupted if !wf_is_running => {}
                    // Only fail task if retry/recovery cannot resume it
                    WorkloadStatus::Interrupted
                        if wl.attempt_count > wl.retry_policy.max_retries =>
                    {
                        task.state = WorkflowTaskState::Failed;
                    }
                    _ => {}
                }
            }
        }

        // Propagate failures downstream in topological order.
        if let Ok(topo) = wf.topological_order() {
            for tid in topo {
                let deps_failed = {
                    let task = wf.tasks.iter().find(|t| t.id == tid);
                    let Some(t) = task else { continue };
                    if t.state != WorkflowTaskState::Pending {
                        false
                    } else {
                        t.depends_on.iter().any(|dep_id| {
                            wf.tasks.iter().any(|dt| {
                                dt.id == *dep_id
                                    && matches!(
                                        dt.state,
                                        WorkflowTaskState::Failed
                                            | WorkflowTaskState::Cancelled
                                            | WorkflowTaskState::SkippedDueToDependencyFailure
                                    )
                            })
                        })
                    }
                };
                if deps_failed && let Some(t) = wf.tasks.iter_mut().find(|t| t.id == tid) {
                    t.state = WorkflowTaskState::SkippedDueToDependencyFailure;
                }
            }
        }

        // Collect ready Pending tasks.
        let mut ready = Vec::new();
        let completed_ids: HashSet<WorkflowTaskId> = wf
            .tasks
            .iter()
            .filter(|t| t.state == WorkflowTaskState::Completed)
            .map(|t| t.id)
            .collect();

        for task in &wf.tasks {
            if task.state == WorkflowTaskState::Pending
                && task.depends_on.iter().all(|d| completed_ids.contains(d))
            {
                ready.push(task.id);
            }
        }

        // Update aggregate workflow status if all tasks finished.
        let any_active_or_pending = wf
            .tasks
            .iter()
            .any(|t| matches!(t.state, WorkflowTaskState::Pending | WorkflowTaskState::Dispatched));
        if !any_active_or_pending {
            let any_failed = wf.tasks.iter().any(|t| {
                matches!(
                    t.state,
                    WorkflowTaskState::Failed | WorkflowTaskState::SkippedDueToDependencyFailure
                )
            });
            let any_cancelled = wf.tasks.iter().any(|t| t.state == WorkflowTaskState::Cancelled);
            wf.status = if any_failed {
                WorkflowStatus::Failed
            } else if any_cancelled {
                WorkflowStatus::Cancelled
            } else {
                WorkflowStatus::Completed
            };
            wf.updated_at_utc = now_utc;
        }

        ready
    }
}

pub fn collect_upstream_artifact_paths(
    task: &WorkflowTaskDefinition,
    all_tasks: &[WorkflowTaskDefinition],
    artifacts_by_workload: &HashMap<WorkloadId, Vec<String>>,
) -> Vec<String> {
    let mut out = Vec::new();
    for dep_id in &task.depends_on {
        if let Some(dep_task) = all_tasks.iter().find(|t| t.id == *dep_id)
            && let Some(wl_id) = dep_task.workload_id
            && let Some(paths) = artifacts_by_workload.get(&wl_id)
        {
            out.extend(paths.iter().cloned());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use helixgrid_domain::{
        CpuBenchmarkInput, MeasurementValue, ResourceRequirements, RetryPolicy, WorkerStatus,
        WorkloadSpec,
    };
    use helixgrid_resource::{HostTelemetrySample, SystemTelemetryProvider};

    struct TestTelemetry;
    impl SystemTelemetryProvider for TestTelemetry {
        fn logical_processors(&self) -> u32 {
            8
        }
        fn sample_host(&mut self) -> HostTelemetrySample {
            HostTelemetrySample {
                logical_processors: 8,
                cpu_utilization_pct: MeasurementValue::Current(15.0),
                total_physical_memory_bytes: MeasurementValue::Current(32 * 1024 * 1024 * 1024),
                available_memory_bytes: MeasurementValue::Current(24 * 1024 * 1024 * 1024),
            }
        }
        fn sample_process(&mut self, _pid: u32) -> (MeasurementValue<f64>, MeasurementValue<u64>) {
            (MeasurementValue::Current(1.0), MeasurementValue::Current(10_000_000))
        }
        fn forget_process(&mut self, _pid: u32) {}
    }

    fn mk_wl(name: &str, prio: Priority, now: i64) -> Workload {
        Workload::new(
            name,
            "desc",
            prio,
            WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                worker_threads: 1,
                iterations: 50,
                sieve_limit: 500,
            }),
            ResourceRequirements::default(),
            10_000,
            RetryPolicy::default(),
            now,
        )
        .unwrap()
    }

    fn mk_worker(name: &str) -> WorkerInfo {
        WorkerInfo {
            id: WorkerId::new(),
            name: name.to_string(),
            pid: Some(100),
            status: WorkerStatus::Idle,
            protocol_version: 1,
            max_cpu_cores: 4,
            max_memory_mb: 2048,
            reserved_cpu_cores: 0,
            reserved_memory_mb: 0,
            current_workload_id: None,
            current_execution_id: None,
            cpu_usage_pct: MeasurementValue::Current(0.0),
            memory_usage_bytes: MeasurementValue::Current(1024),
            started_at_utc: 100,
            last_heartbeat_utc: Some(100),
            completed_executions: 0,
            failed_executions: 0,
            isolation_note: "test".to_string(),
        }
    }

    #[test]
    fn schedules_higher_priority_first_and_enforces_backpressure() {
        let cfg =
            AppConfig { max_queue_depth: 10, max_concurrent_workloads: 1, ..Default::default() };

        let mut sched = SchedulerEngine::new();
        let mut res = ResourceManager::new(Box::new(TestTelemetry));
        let mut workers = vec![mk_worker("w1")];
        let mut map = HashMap::new();

        let mut low = mk_wl("low", Priority::Low, 100);
        let mut crit = mk_wl("crit", Priority::Critical, 101);
        sched.enqueue(&mut low, &cfg, 100).unwrap();
        sched.enqueue(&mut crit, &cfg, 101).unwrap();
        let crit_id = crit.id;
        map.insert(low.id, low);
        map.insert(crit.id, crit);

        let dispatches = sched.schedule_ready(&mut map, &mut workers, &mut res, &cfg, 200);
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].workload_id, crit_id);

        // Fill queue to test backpressure
        for i in 0..9 {
            let mut w = mk_wl(&format!("extra-{i}"), Priority::Normal, 300 + i);
            sched.enqueue(&mut w, &cfg, 300 + i).unwrap();
            map.insert(w.id, w);
        }
        let mut overflow = mk_wl("overflow", Priority::High, 400);
        assert!(matches!(
            sched.enqueue(&mut overflow, &cfg, 400),
            Err(SchedulerError::QueueFull { capacity: 10 })
        ));
    }

    #[test]
    fn workflow_failure_propagates_to_dependents() {
        let t1 = WorkflowTaskId::new();
        let t2 = WorkflowTaskId::new();
        let mut wl1 = mk_wl("stage-1", Priority::Normal, 100);
        wl1.status = WorkloadStatus::Failed;

        let mut wf = WorkflowDefinition::new(
            "Pipeline",
            "Test failure propagation",
            vec![
                WorkflowTaskDefinition {
                    id: t1,
                    name: "Stage 1".into(),
                    depends_on: vec![],
                    priority: Priority::Normal,
                    spec: wl1.spec.clone(),
                    resources: ResourceRequirements::default(),
                    timeout_ms: 5000,
                    retry_policy: RetryPolicy::default(),
                    state: WorkflowTaskState::Dispatched,
                    workload_id: Some(wl1.id),
                },
                WorkflowTaskDefinition {
                    id: t2,
                    name: "Stage 2".into(),
                    depends_on: vec![t1],
                    priority: Priority::Normal,
                    spec: wl1.spec.clone(),
                    resources: ResourceRequirements::default(),
                    timeout_ms: 5000,
                    retry_policy: RetryPolicy::default(),
                    state: WorkflowTaskState::Pending,
                    workload_id: None,
                },
            ],
            100,
        )
        .unwrap();
        wf.status = WorkflowStatus::Running;

        let mut map = HashMap::new();
        map.insert(wl1.id, wl1);

        let ready = WorkflowScheduler::evaluate_workflow(&mut wf, &map, 200);
        assert!(ready.is_empty());
        assert_eq!(wf.tasks[0].state, WorkflowTaskState::Failed);
        assert_eq!(wf.tasks[1].state, WorkflowTaskState::SkippedDueToDependencyFailure);
        assert_eq!(wf.status, WorkflowStatus::Failed);
    }
}
