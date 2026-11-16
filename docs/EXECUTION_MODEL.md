# HelixGrid Execution Model & Lifecycle State Machine

HelixGrid models every execution unit as an immutable-identity `Workload` backed by strict finite state machine transitions in `crates/helixgrid-domain/src/workload.rs` and isolated out-of-process execution attempts (`ExecutionRecord`) managed by `HelixGridEngine`.

## Workload Finite State Machine (`WorkloadStatus`)

Valid transitions enforced by `WorkloadStatus::can_transition_to` / `try_transition`:

| Current State | Allowed Next States |
| --- | --- |
| `Submitted` | `Validating`, `Cancelled`, `Failed` |
| `Validating` | `Queued`, `Failed`, `Cancelled` |
| `Queued` | `Scheduled`, `Cancelled`, `TimedOut`, `Interrupted` |
| `Scheduled` | `Starting`, `Queued`, `Cancelled`, `Interrupted`, `Failed` |
| `Starting` | `Running`, `Failed`, `TimedOut`, `Cancelled`, `Interrupted` |
| `Running` | `Completed`, `Failed`, `Cancelled`, `TimedOut`, `Interrupted`, `Retrying` |
| `Retrying` | `Queued`, `Failed`, `Cancelled` |
| `Interrupted` | `Retrying`, `Failed`, `Cancelled` |
| `Completed` *(terminal)* | *(none)* |
| `Failed` *(terminal)* | *(none)* |
| `Cancelled` *(terminal)* | *(none)* |
| `TimedOut` *(terminal)* | *(none)* |

> **Terminal Immutability Invariant:** Terminal states (`Completed`, `Failed`, `Cancelled`, `TimedOut`) can **never** transition back to `Running`, `Queued`, or `Retrying`. Re-running a completed, cancelled, or failed workload via `SafeRetryWorkload` allocates a new child `Workload` linked by `parent_workload_id` and preserves original attempt history untouched.

## Out-of-Process Dispatch Lifecycle

1. **Submission & Validation:** `HelixGridEngine::submit_workload` creates `Workload` (`Submitted -> Validating`), validates configuration, resource bounds (`cpu_cores` 1..=256, `memory_mb` 16..=524288), executable allowlist/retry-safety rules (`verify_safe_retry`), persists to SQLite, transitions to `Queued`, and enqueues in `SchedulerEngine`.
2. **Scheduling & Reservation:** `SchedulerEngine::schedule_ready` atomically reserves CPU cores and RAM on an `Idle` worker via `ResourceManager`, transitions `Queued -> Scheduled`, and emits a `ScheduledDispatch`.
3. **Worker Dispatch:** Coordinator creates a sandboxed per-workload artifact directory under `artifacts/<workload_id>/attempt_<n>/`, inserts an `ExecutionRecord`, updates worker state to `Busy`, records an `ActiveExecutionClock` deadline, and sends `ExecutionRequest` over the worker's authenticated Named Pipe.
4. **Worker Runtime Execution:** `crates/helixgrid-worker-runtime/src/runner.rs` acknowledges `ExecutionAccepted` and `ExecutionStarted`, runs the workload kernel on `tokio::task::spawn_blocking`, streams `ExecutionProgress` (`Indeterminate` or `Determinate { percent, completed_units, total_units }`), and reports `ExecutionCompleted`, `ExecutionFailed`, or `ExecutionCancelled` with SHA-256 hashed artifacts.

## Implemented Compute Kernels (`crates/helixgrid-worker-runtime/src/compute.rs`)

- **`CpuBenchmark`:** Real multi-threaded prime sieve (`sieve_limit`) + ARX integer mixing across `worker_threads` std threads for `iterations` passes, writing structured JSON benchmark reports (`throughput_ops_per_sec`, `primes_counted`, `duration_ms`).
- **`BatchProcessing`:** Processes real disk directories (`input_dir`) or generates a deterministic disk-backed workspace batch and executes `HashAndValidate`, `NormalizeJsonRecords`, or `AggregateCsvMetrics`, producing validated output artifacts + SHA-256 digests.
- **`NumericalComputation`:** Pure double-precision (`f64`) kernels: `DenseMatrixMultiply`, `ConjugateGradientSolve`, `MonteCarloVariance`, and `Rk4LorenzTrajectory`, outputting numerical residual/checksum metrics and result artifacts.
- **`RegisteredExecutable`:** Launches allowlisted non-shell native executables with scrubbed environments (`build_sanitized_env`), `CREATE_NO_WINDOW`, bounded stdout/stderr capture (`MAX_CAPTURE_BYTES = 2 MiB`), cooperative cancellation termination, and recorded exit codes.
- **`WorkflowTask`:** Wraps an inner `WorkloadSpec` alongside resolved `upstream_artifacts` paths from completed parent DAG tasks.

## Timeout, Cancellation, Crash & Startup Recovery

- **Timeout vs. Cancellation Distinction:** When `ActiveExecutionClock` exceeds `timeout_ms`, the coordinator sets `timed_out = true`, sends `CancelExecutionCommand`, and waits up to a `1200 ms` grace period.
  - If the worker acknowledges `ExecutionCancelled` within grace, the coordinator sees `timed_out == true` and records `WorkloadStatus::TimedOut` (rather than misreporting `Cancelled`).
  - If the worker fails to respond past `1200 ms`, the coordinator force-terminates the worker's Win32 Job Object/process, releases reservations, marks the workload `TimedOut`, and replenishes the worker pool.
- **Worker Crash Recovery:** Unexpected pipe EOF or dead OS process triggers `handle_worker_failure`, marking the worker `Crashed`, freeing its `ResourceReservation`s, and transitioning its active workload to `Interrupted`. Only if `auto_recover_idempotent_interrupted` is enabled, `attempt_count <= max_retries`, and `retry_safety == RetrySafety::SafeIdempotent` does it automatically re-enqueue via `Retrying -> Queued`.
- **Startup Crash Reconciliation:** On coordinator restart (`HelixGridEngine::start`), active executions left `Starting`/`Running` in SQLite are marked `Interrupted` (`RecoverySummary`), interrupted safe-idempotent workloads within retry budget are auto-requeued, and interrupted non-idempotent workloads remain halted in `Interrupted` for explicit operator inspection.
