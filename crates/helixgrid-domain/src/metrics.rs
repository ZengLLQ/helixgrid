use serde::{Deserialize, Serialize};

/// Distinguishes measurement provenance so estimates or missing metrics
/// are never presented as live hardware readings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MeasurementValue<T> {
    Current(T),
    Historical(T),
    Estimate(T),
    Unavailable { reason: String },
}

impl<T> MeasurementValue<T> {
    #[must_use]
    pub fn value(&self) -> Option<&T> {
        match self {
            Self::Current(v) | Self::Historical(v) | Self::Estimate(v) => Some(v),
            Self::Unavailable { .. } => None,
        }
    }

    #[must_use]
    pub fn is_available(&self) -> bool {
        !matches!(self, Self::Unavailable { .. })
    }

    #[must_use]
    pub fn provenance_label(&self) -> &'static str {
        match self {
            Self::Current(_) => "Current",
            Self::Historical(_) => "Historical",
            Self::Estimate(_) => "Estimate",
            Self::Unavailable { .. } => "Unavailable",
        }
    }
}

/// Real progress emitted by an execution; never fabricated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProgressState {
    Indeterminate { message: Option<String> },
    Determinate { percent: f32, completed_units: u64, total_units: u64, message: Option<String> },
}

impl Default for ProgressState {
    fn default() -> Self {
        Self::Indeterminate { message: None }
    }
}

impl ProgressState {
    #[must_use]
    pub fn determinate(completed_units: u64, total_units: u64, message: Option<String>) -> Self {
        if total_units == 0 {
            return Self::Indeterminate { message };
        }
        let pct = ((completed_units.min(total_units) as f64 / total_units as f64) * 100.0) as f32;
        Self::Determinate {
            percent: pct.clamp(0.0, 100.0),
            completed_units: completed_units.min(total_units),
            total_units,
            message,
        }
    }

    #[must_use]
    pub fn percent(&self) -> Option<f32> {
        match self {
            Self::Indeterminate { .. } => None,
            Self::Determinate { percent, .. } => Some(*percent),
        }
    }
}

/// Host & worker resource snapshot captured at a UTC timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemMetricsSnapshot {
    pub captured_at_utc: i64,
    pub logical_processors: u32,
    pub cpu_utilization_pct: MeasurementValue<f64>,
    pub total_physical_memory_bytes: MeasurementValue<u64>,
    pub available_memory_bytes: MeasurementValue<u64>,
    pub active_workers: u32,
    pub active_executions: u32,
    pub queue_depth: u32,
    pub gpu_acceleration_status: GpuCapabilityStatus,
}

/// Honest GPU capability reporting (no fake GPU utilization).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuCapabilityStatus {
    CpuOnlyMode { reason: String },
}

impl Default for GpuCapabilityStatus {
    fn default() -> Self {
        Self::CpuOnlyMode {
            reason: "v1 executes validated CPU backends; GPU offload is disabled".to_string(),
        }
    }
}
