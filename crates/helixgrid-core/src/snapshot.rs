use helixgrid_config::AppConfig;
use helixgrid_domain::WorkloadId;
use helixgrid_domain::{
    ExecutionArtifact, ExecutionRecord, SystemEvent, SystemMetricsSnapshot, WorkerInfo,
    WorkflowDefinition, Workload,
};
use helixgrid_scheduler::SchedulerDiagnostics;
use helixgrid_storage::DatabaseStatusReport;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EngineHealth {
    Healthy,
    Degraded,
    Recovering,
    Stopped,
}

impl EngineHealth {
    #[must_use]
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Healthy => "Healthy",
            Self::Degraded => "Degraded",
            Self::Recovering => "Recovering",
            Self::Stopped => "Stopped",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecoverySummary {
    pub interrupted_executions_found: usize,
    pub interrupted_workloads_found: usize,
    pub auto_recovered_workloads: usize,
    pub manual_attention_workloads: usize,
    pub requeued_workloads: usize,
}

/// Immutable UI/API-ready snapshot published by the coordinator without blocking readers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSnapshot {
    pub app_version: String,
    pub health: EngineHealth,
    pub health_reason: String,
    pub config: AppConfig,
    pub latest_metrics: SystemMetricsSnapshot,
    pub historical_metrics: Vec<SystemMetricsSnapshot>,
    pub workers: Vec<WorkerInfo>,
    pub workloads: Vec<Workload>,
    pub workflows: Vec<WorkflowDefinition>,
    pub executions_by_workload: HashMap<WorkloadId, Vec<ExecutionRecord>>,
    pub artifacts_by_workload: HashMap<WorkloadId, Vec<ExecutionArtifact>>,
    pub events: Vec<SystemEvent>,
    pub scheduler_diagnostics: SchedulerDiagnostics,
    pub database_status: DatabaseStatusReport,
    pub recovery_summary: RecoverySummary,
    pub ipc_status: String,
    pub reduced_motion_os: bool,
}
