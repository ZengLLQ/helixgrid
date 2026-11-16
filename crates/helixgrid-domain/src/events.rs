use crate::ids::{EventId, WorkerId, WorkflowId, WorkloadId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventSeverity {
    Trace,
    Info,
    Warning,
    Error,
    Critical,
}

impl EventSeverity {
    #[must_use]
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Trace => "Trace",
            Self::Info => "Info",
            Self::Warning => "Warning",
            Self::Error => "Error",
            Self::Critical => "Critical",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemEventKind {
    EngineStarted,
    EngineShutdown,
    WorkerSpawned,
    WorkerConnected,
    WorkerHeartbeatLost,
    WorkerCrashed,
    WorkerRestarted,
    WorkerStopped,
    WorkloadSubmitted,
    WorkloadQueued,
    WorkloadScheduled,
    WorkloadStarted,
    WorkloadCompleted,
    WorkloadFailed,
    WorkloadCancelled,
    WorkloadTimedOut,
    WorkloadInterrupted,
    WorkloadRetrying,
    WorkflowStarted,
    WorkflowCompleted,
    WorkflowFailed,
    SchedulerBackpressure,
    RecoveryReconciled,
    SecurityViolation,
    DatabaseMaintenance,
}

impl SystemEventKind {
    #[must_use]
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::EngineStarted => "Engine Started",
            Self::EngineShutdown => "Engine Shutdown",
            Self::WorkerSpawned => "Worker Spawned",
            Self::WorkerConnected => "Worker Connected",
            Self::WorkerHeartbeatLost => "Heartbeat Lost",
            Self::WorkerCrashed => "Worker Crashed",
            Self::WorkerRestarted => "Worker Restarted",
            Self::WorkerStopped => "Worker Stopped",
            Self::WorkloadSubmitted => "Workload Submitted",
            Self::WorkloadQueued => "Workload Queued",
            Self::WorkloadScheduled => "Workload Scheduled",
            Self::WorkloadStarted => "Workload Started",
            Self::WorkloadCompleted => "Workload Completed",
            Self::WorkloadFailed => "Workload Failed",
            Self::WorkloadCancelled => "Workload Cancelled",
            Self::WorkloadTimedOut => "Workload Timed Out",
            Self::WorkloadInterrupted => "Workload Interrupted",
            Self::WorkloadRetrying => "Workload Retrying",
            Self::WorkflowStarted => "Workflow Started",
            Self::WorkflowCompleted => "Workflow Completed",
            Self::WorkflowFailed => "Workflow Failed",
            Self::SchedulerBackpressure => "Scheduler Backpressure",
            Self::RecoveryReconciled => "Recovery Reconciled",
            Self::SecurityViolation => "Security Violation",
            Self::DatabaseMaintenance => "Database Maintenance",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemEvent {
    pub id: EventId,
    pub timestamp_utc: i64,
    pub kind: SystemEventKind,
    pub severity: EventSeverity,
    pub workload_id: Option<WorkloadId>,
    pub worker_id: Option<WorkerId>,
    pub workflow_id: Option<WorkflowId>,
    pub message: String,
    pub metadata_json: Option<serde_json::Value>,
}

impl SystemEvent {
    #[must_use]
    pub fn new(
        kind: SystemEventKind,
        severity: EventSeverity,
        message: impl Into<String>,
        now_utc: i64,
    ) -> Self {
        Self {
            id: EventId::new(),
            timestamp_utc: now_utc,
            kind,
            severity,
            workload_id: None,
            worker_id: None,
            workflow_id: None,
            message: message.into(),
            metadata_json: None,
        }
    }
}
