use crate::error::DomainError;
use crate::ids::{ArtifactId, ExecutionId, WorkerId, WorkflowId, WorkflowTaskId, WorkloadId};
use crate::metrics::ProgressState;
use serde::{Deserialize, Serialize};

/// Initial supported workload categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WorkloadType {
    CpuBenchmark,
    BatchProcessing,
    NumericalComputation,
    RegisteredExecutable,
    WorkflowTask,
}

impl WorkloadType {
    #[must_use]
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::CpuBenchmark => "CPU Benchmark",
            Self::BatchProcessing => "Batch Processing",
            Self::NumericalComputation => "Numerical Computation",
            Self::RegisteredExecutable => "Registered Executable",
            Self::WorkflowTask => "Workflow Task",
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub enum Priority {
    Low = 0,
    #[default]
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl Priority {
    #[must_use]
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Low => "Low",
            Self::Normal => "Normal",
            Self::High => "High",
            Self::Critical => "Critical",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WorkloadStatus {
    Submitted,
    Validating,
    Queued,
    Scheduled,
    Starting,
    Running,
    Retrying,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    Interrupted,
}

impl WorkloadStatus {
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled | Self::TimedOut)
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Scheduled | Self::Starting | Self::Running)
    }

    #[must_use]
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Submitted => "Submitted",
            Self::Validating => "Validating",
            Self::Queued => "Queued",
            Self::Scheduled => "Scheduled",
            Self::Starting => "Starting",
            Self::Running => "Running",
            Self::Retrying => "Retrying",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
            Self::TimedOut => "Timed Out",
            Self::Interrupted => "Interrupted",
        }
    }

    /// Strict legal state transition guard.
    /// Completed workloads NEVER return to any non-terminal or running state.
    #[must_use]
    pub fn can_transition_to(&self, next: Self) -> bool {
        use WorkloadStatus::*;
        if *self == next {
            return false;
        }
        match self {
            Submitted => matches!(next, Validating | Cancelled | Failed),
            Validating => matches!(next, Queued | Failed | Cancelled),
            Queued => matches!(next, Scheduled | Cancelled | TimedOut | Interrupted),
            Scheduled => matches!(next, Starting | Queued | Cancelled | Interrupted | Failed),
            Starting => matches!(next, Running | Failed | TimedOut | Cancelled | Interrupted),
            Running => {
                matches!(next, Completed | Failed | Cancelled | TimedOut | Interrupted | Retrying)
            }
            Retrying => matches!(next, Queued | Failed | Cancelled),
            Interrupted => matches!(next, Retrying | Failed | Cancelled),
            Completed | Failed | Cancelled | TimedOut => false,
        }
    }

    pub fn try_transition(&mut self, next: Self) -> Result<(), DomainError> {
        if self.can_transition_to(next) {
            *self = next;
            Ok(())
        } else {
            Err(DomainError::IllegalStateTransition {
                from: format!("{self:?}"),
                to: format!("{next:?}"),
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RetrySafety {
    /// Deterministic/idempotent computation that is safe to re-run after crash.
    SafeIdempotent,
    /// Side-effecting external process: never automatically retried after partial execution.
    #[default]
    NonIdempotentExternal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub backoff_ms: u64,
    pub retry_safety: RetrySafety,
    pub retry_on_timeout: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0,
            backoff_ms: 500,
            retry_safety: RetrySafety::SafeIdempotent,
            retry_on_timeout: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRequirements {
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub requires_exclusive_worker: bool,
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self { cpu_cores: 1, memory_mb: 128, requires_exclusive_worker: false }
    }
}

impl ResourceRequirements {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.cpu_cores == 0 || self.cpu_cores > 256 {
            return Err(DomainError::InvalidConfiguration(
                "cpu_cores must be between 1 and 256".to_string(),
            ));
        }
        if self.memory_mb < 16 || self.memory_mb > 524_288 {
            return Err(DomainError::InvalidConfiguration(
                "memory_mb must be between 16 and 524288 MB".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchOperation {
    HashAndValidate,
    NormalizeJsonRecords,
    AggregateCsvMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NumericalAlgorithm {
    DenseMatrixMultiply,
    ConjugateGradientSolve,
    MonteCarloVariance,
    Rk4LorenzTrajectory,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CpuBenchmarkInput {
    pub worker_threads: u32,
    pub iterations: u64,
    pub sieve_limit: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchProcessingInput {
    /// If provided, processes real files in this validated directory; otherwise
    /// generates a deterministic batch dataset inside the sandboxed execution workspace.
    pub input_dir: Option<String>,
    pub item_count: u32,
    pub batch_size: u32,
    pub operation: BatchOperation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NumericalComputationInput {
    pub algorithm: NumericalAlgorithm,
    pub dimension: u32,
    pub max_iterations: u32,
    pub tolerance: f64,
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredExecutableInput {
    pub registered_id: String,
    pub executable_path: String,
    pub args: Vec<String>,
    pub working_dir: String,
    pub env_overrides: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTaskInput {
    pub workflow_id: WorkflowId,
    pub task_id: WorkflowTaskId,
    pub stage_name: String,
    pub inner: Box<WorkloadSpec>,
    pub upstream_artifacts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WorkloadSpec {
    CpuBenchmark(CpuBenchmarkInput),
    BatchProcessing(BatchProcessingInput),
    NumericalComputation(NumericalComputationInput),
    RegisteredExecutable(RegisteredExecutableInput),
    WorkflowTask(WorkflowTaskInput),
}

impl WorkloadSpec {
    #[must_use]
    pub fn workload_type(&self) -> WorkloadType {
        match self {
            Self::CpuBenchmark(_) => WorkloadType::CpuBenchmark,
            Self::BatchProcessing(_) => WorkloadType::BatchProcessing,
            Self::NumericalComputation(_) => WorkloadType::NumericalComputation,
            Self::RegisteredExecutable(_) => WorkloadType::RegisteredExecutable,
            Self::WorkflowTask(_) => WorkloadType::WorkflowTask,
        }
    }

    #[must_use]
    pub fn default_retry_safety(&self) -> RetrySafety {
        match self {
            Self::CpuBenchmark(_) | Self::BatchProcessing(_) | Self::NumericalComputation(_) => {
                RetrySafety::SafeIdempotent
            }
            Self::RegisteredExecutable(_) => RetrySafety::NonIdempotentExternal,
            Self::WorkflowTask(t) => t.inner.default_retry_safety(),
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::CpuBenchmark(c) => {
                if c.worker_threads == 0 || c.worker_threads > 64 {
                    return Err(DomainError::InvalidConfiguration(
                        "benchmark worker_threads must be 1..=64".to_string(),
                    ));
                }
                if c.iterations == 0 || c.iterations > 10_000_000 {
                    return Err(DomainError::InvalidConfiguration(
                        "benchmark iterations must be 1..=10_000_000".to_string(),
                    ));
                }
                if c.sieve_limit < 100 || c.sieve_limit > 10_000_000 {
                    return Err(DomainError::InvalidConfiguration(
                        "sieve_limit must be 100..=10_000_000".to_string(),
                    ));
                }
            }
            Self::BatchProcessing(b) => {
                if b.item_count == 0 || b.item_count > 1_000_000 {
                    return Err(DomainError::InvalidConfiguration(
                        "batch item_count must be 1..=1_000_000".to_string(),
                    ));
                }
                if b.batch_size == 0 || b.batch_size > 100_000 {
                    return Err(DomainError::InvalidConfiguration(
                        "batch_size must be 1..=100_000".to_string(),
                    ));
                }
            }
            Self::NumericalComputation(n) => {
                if n.dimension == 0 || n.dimension > 4096 {
                    return Err(DomainError::InvalidConfiguration(
                        "numerical dimension must be 1..=4096".to_string(),
                    ));
                }
                if n.max_iterations == 0 || n.max_iterations > 50_000_000 {
                    return Err(DomainError::InvalidConfiguration(
                        "max_iterations must be 1..=50_000_000".to_string(),
                    ));
                }
                if !n.tolerance.is_finite() || n.tolerance <= 0.0 {
                    return Err(DomainError::InvalidConfiguration(
                        "tolerance must be positive and finite".to_string(),
                    ));
                }
            }
            Self::RegisteredExecutable(e) => {
                if e.registered_id.trim().is_empty() {
                    return Err(DomainError::InvalidConfiguration(
                        "registered_id cannot be empty".to_string(),
                    ));
                }
                if e.executable_path.trim().is_empty() {
                    return Err(DomainError::InvalidConfiguration(
                        "executable_path cannot be empty".to_string(),
                    ));
                }
                if e.args.len() > 128 {
                    return Err(DomainError::InvalidConfiguration(
                        "args length cannot exceed 128".to_string(),
                    ));
                }
            }
            Self::WorkflowTask(t) => {
                if t.stage_name.trim().is_empty() {
                    return Err(DomainError::InvalidConfiguration(
                        "workflow stage_name cannot be empty".to_string(),
                    ));
                }
                if matches!(&*t.inner, Self::WorkflowTask(_)) {
                    return Err(DomainError::InvalidConfiguration(
                        "nested WorkflowTask is disallowed".to_string(),
                    ));
                }
                t.inner.validate()?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkloadResultMetadata {
    pub summary: String,
    pub metrics_json: serde_json::Value,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Workload {
    pub id: WorkloadId,
    pub name: String,
    pub description: String,
    pub workload_type: WorkloadType,
    pub priority: Priority,
    pub spec: WorkloadSpec,
    pub resources: ResourceRequirements,
    pub timeout_ms: u64,
    pub retry_policy: RetryPolicy,
    pub created_at_utc: i64,
    pub updated_at_utc: i64,
    pub status: WorkloadStatus,
    pub progress: ProgressState,
    pub attempt_count: u32,
    pub assigned_worker_id: Option<WorkerId>,
    pub latest_execution_id: Option<ExecutionId>,
    pub parent_workload_id: Option<WorkloadId>,
    pub result_metadata: Option<WorkloadResultMetadata>,
    pub error_message: Option<String>,
}

impl Workload {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        priority: Priority,
        spec: WorkloadSpec,
        resources: ResourceRequirements,
        timeout_ms: u64,
        retry_policy: RetryPolicy,
        now_utc: i64,
    ) -> Result<Self, DomainError> {
        let name = name.into();
        if name.trim().is_empty() || name.len() > 128 {
            return Err(DomainError::InvalidConfiguration(
                "workload name must be 1..=128 characters".to_string(),
            ));
        }
        if !(100..=86_400_000).contains(&timeout_ms) {
            return Err(DomainError::InvalidConfiguration(
                "timeout_ms must be between 100ms and 24h".to_string(),
            ));
        }
        spec.validate()?;
        resources.validate()?;

        let workload_type = spec.workload_type();
        Ok(Self {
            id: WorkloadId::new(),
            name,
            description: description.into(),
            workload_type,
            priority,
            spec,
            resources,
            timeout_ms,
            retry_policy,
            created_at_utc: now_utc,
            updated_at_utc: now_utc,
            status: WorkloadStatus::Submitted,
            progress: ProgressState::Indeterminate { message: None },
            attempt_count: 0,
            assigned_worker_id: None,
            latest_execution_id: None,
            parent_workload_id: None,
            result_metadata: None,
            error_message: None,
        })
    }

    pub fn transition_status(
        &mut self,
        next: WorkloadStatus,
        now_utc: i64,
    ) -> Result<(), DomainError> {
        self.status.try_transition(next)?;
        self.updated_at_utc = now_utc;
        Ok(())
    }
}

/// Explicit distinction among terminal and interrupted execution outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionOutcome {
    Completed,
    Failed,
    Interrupted,
    UnknownOutcome,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub id: ExecutionId,
    pub workload_id: WorkloadId,
    pub worker_id: WorkerId,
    pub attempt_number: u32,
    pub status: WorkloadStatus,
    pub outcome: Option<ExecutionOutcome>,
    pub started_at_utc: i64,
    pub finished_at_utc: Option<i64>,
    pub duration_ms: Option<u64>,
    pub exit_code: Option<i32>,
    pub error_message: Option<String>,
    pub result_json: Option<serde_json::Value>,
    pub log_file_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactKind {
    ResultJson,
    StdoutLog,
    StderrLog,
    BatchOutput,
    DiagnosticReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionArtifact {
    pub id: ArtifactId,
    pub execution_id: ExecutionId,
    pub workload_id: WorkloadId,
    pub name: String,
    pub kind: ArtifactKind,
    pub file_path: String,
    pub size_bytes: u64,
    pub sha256_hex: String,
    pub created_at_utc: i64,
}
