use crate::ids::{ExecutionId, WorkerId, WorkloadId};
use crate::metrics::MeasurementValue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerStatus {
    Launching,
    Connecting,
    Idle,
    Busy,
    Draining,
    Unhealthy,
    Stopped,
    Crashed,
}

impl WorkerStatus {
    #[must_use]
    pub fn is_available_for_scheduling(&self) -> bool {
        matches!(self, Self::Idle)
    }

    #[must_use]
    pub fn is_alive(&self) -> bool {
        matches!(
            self,
            Self::Launching | Self::Connecting | Self::Idle | Self::Busy | Self::Draining
        )
    }

    #[must_use]
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Launching => "Launching",
            Self::Connecting => "Connecting",
            Self::Idle => "Idle",
            Self::Busy => "Busy",
            Self::Draining => "Draining",
            Self::Unhealthy => "Unhealthy",
            Self::Stopped => "Stopped",
            Self::Crashed => "Crashed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: WorkerId,
    pub name: String,
    pub pid: Option<u32>,
    pub status: WorkerStatus,
    pub protocol_version: u32,
    pub max_cpu_cores: u32,
    pub max_memory_mb: u64,
    pub reserved_cpu_cores: u32,
    pub reserved_memory_mb: u64,
    pub current_workload_id: Option<WorkloadId>,
    pub current_execution_id: Option<ExecutionId>,
    pub cpu_usage_pct: MeasurementValue<f64>,
    pub memory_usage_bytes: MeasurementValue<u64>,
    pub started_at_utc: i64,
    pub last_heartbeat_utc: Option<i64>,
    pub completed_executions: u64,
    pub failed_executions: u64,
    pub isolation_note: String,
}

impl WorkerInfo {
    #[must_use]
    pub fn available_cpu_cores(&self) -> u32 {
        self.max_cpu_cores.saturating_sub(self.reserved_cpu_cores)
    }

    #[must_use]
    pub fn available_memory_mb(&self) -> u64 {
        self.max_memory_mb.saturating_sub(self.reserved_memory_mb)
    }
}
