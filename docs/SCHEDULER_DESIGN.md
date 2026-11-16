# HelixGrid Scheduler Design

`crates/helixgrid-scheduler` implements deterministic workload queuing, backpressure control, resource-aware placement, and multi-stage DAG workflow progression. It has zero UI or Win32 dependencies and operates strictly against domain models and `ResourceManager`.

## Scheduling Policies (`SchedulingPolicy`)

Configured via `AppConfig.scheduling_policy`:

1. **`PriorityResourceAware` (default):**
   - Orders queued workloads primarily by `Priority` descending (`Critical > High > Normal > Low`) and secondarily by monotonic submission sequence ascending (`sequence`), ensuring deterministic tie-breaking and FIFO fairness within each priority class.
2. **`FifoResourceAware`:**
   - Orders queued workloads strictly by submission `sequence` ascending regardless of priority label.

## Queue Admission & Backpressure

- Every workload enters `SchedulerEngine::enqueue` only after validating `WorkloadStatus` transitions (`Submitted -> Validating -> Queued`, or recovery `Retrying -> Queued`).
- Duplicate `WorkloadId` submissions return `SchedulerError::DuplicateWorkload`.
- If `queue.len() >= config.max_queue_depth` (default `500`), admission fails immediately with `SchedulerError::QueueFull` and increments `SchedulerDiagnostics.total_backpressure_rejections` instead of growing memory unboundedly.

## Resource-Aware Placement (`schedule_ready`)

During each scheduling pass:

1. Counts active workloads (`Scheduled`, `Starting`, `Running`). If `active_count >= config.max_concurrent_workloads`, no further dispatches occur on that pass.
2. Iterates queued entries in policy order. If the target workload can transition `Queued -> Scheduled`, evaluates candidate workers in `workers`:
   - Worker must have `status.is_available_for_scheduling()` (specifically `WorkerStatus::Idle`).
   - If `workload.resources.requires_exclusive_worker` is set, the worker must have `reserved_cpu_cores == 0`, `reserved_memory_mb == 0`, and `current_execution_id.is_none()`.
   - `ResourceManager::try_reserve` verifies both **worker headroom** (`available_cpu_cores >= req.cpu_cores` and `available_memory_mb >= req.memory_mb`) and **host ceilings** (`config.cpu_utilization_ceiling_pct`, `config.memory_allocation_ceiling_pct`, `config.reserved_system_memory_mb`).
3. If no worker currently has sufficient capacity, the workload remains queued (non-blocking head-of-line: smaller workloads behind a large unplaceable workload can still dispatch if their requirements fit an available worker, respecting `max_concurrent_workloads`).
4. Records scheduling pass duration (`last_scheduling_pass_ns`), average queue wait latency (`average_queue_latency_ms`), and total dispatched count in `SchedulerDiagnostics`.

## Multi-Stage DAG Workflows (`WorkflowScheduler`)

- `WorkflowDefinition::new` validates task graphs using **Kahn's topological sort** (`topological_order`), rejecting empty workflows, unknown dependencies, self-dependencies, or cycles (`DomainError::InvalidWorkflow`).
- `WorkflowScheduler::advance_workflow` executes in two phases per coordinator tick:
  1. **State Sync:** Updates each `WorkflowTaskDefinition` state from its linked `WorkloadStatus` (`Completed`, `Failed`/`TimedOut` -> `Failed`, `Cancelled`, and `Interrupted` only becomes `Failed` once `attempt_count > retry_policy.max_retries` so safe auto-retry still succeeds).
  2. **Failure Propagation & Ready Dispatch:** Traverses tasks in topological order; any `Pending` task whose dependencies include `Failed`, `Cancelled`, or `SkippedDueToDependencyFailure` transitions immediately to `SkippedDueToDependencyFailure`. Any `Pending` task whose dependencies are all `Completed` collects upstream artifact paths (`collect_upstream_artifact_paths`) into `WorkflowTaskInput` and becomes ready for submission.
- If a running workflow's tasks have all reached terminal/skipped states, the workflow transitions to `WorkflowStatus::Failed` (if any task failed/cancelled/skipped) or `WorkflowStatus::Completed`.
