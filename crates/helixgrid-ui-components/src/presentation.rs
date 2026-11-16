use helixgrid_core::EngineSnapshot;
use helixgrid_domain::{WorkerStatus, WorkflowStatus, WorkloadStatus};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatusTone {
    Neutral = 0,
    Accent = 1,
    Success = 2,
    Warning = 3,
    Danger = 4,
}

#[must_use]
pub fn status_tone_for_workload(status: WorkloadStatus) -> StatusTone {
    match status {
        WorkloadStatus::Completed => StatusTone::Success,
        WorkloadStatus::Running | WorkloadStatus::Starting | WorkloadStatus::Scheduled => {
            StatusTone::Accent
        }
        WorkloadStatus::Queued | WorkloadStatus::Validating | WorkloadStatus::Submitted => {
            StatusTone::Neutral
        }
        WorkloadStatus::Retrying | WorkloadStatus::Interrupted => StatusTone::Warning,
        WorkloadStatus::Failed | WorkloadStatus::TimedOut | WorkloadStatus::Cancelled => {
            StatusTone::Danger
        }
    }
}

#[must_use]
pub fn status_tone_for_worker(status: WorkerStatus) -> StatusTone {
    match status {
        WorkerStatus::Idle => StatusTone::Success,
        WorkerStatus::Busy => StatusTone::Accent,
        WorkerStatus::Launching | WorkerStatus::Connecting | WorkerStatus::Draining => {
            StatusTone::Warning
        }
        WorkerStatus::Stopped => StatusTone::Neutral,
        WorkerStatus::Unhealthy | WorkerStatus::Crashed => StatusTone::Danger,
    }
}

#[must_use]
pub fn status_tone_for_workflow(status: WorkflowStatus) -> StatusTone {
    match status {
        WorkflowStatus::Completed => StatusTone::Success,
        WorkflowStatus::Running => StatusTone::Accent,
        WorkflowStatus::Draft => StatusTone::Neutral,
        WorkflowStatus::Failed | WorkflowStatus::Cancelled => StatusTone::Danger,
    }
}

/// Real aggregate analytics computed strictly from persisted engine measurements.
#[derive(Debug, Clone, Default)]
pub struct AnalyticsSummary {
    pub completed_count: u64,
    pub failed_count: u64,
    pub avg_duration_ms: Option<u64>,
    pub p95_duration_ms: Option<u64>,
    pub avg_cpu_pct: Option<f64>,
    pub peak_cpu_pct: Option<f64>,
    pub avg_memory_used_pct: Option<f64>,
    pub throughput_per_minute: Option<f64>,
    pub cpu_series: Vec<f64>,
    pub memory_pct_series: Vec<f64>,
    pub duration_series_ms: Vec<f64>,
}

impl AnalyticsSummary {
    #[must_use]
    pub fn from_snapshot(snap: &EngineSnapshot) -> Self {
        let mut completed = 0u64;
        let mut failed = 0u64;
        let mut durations = Vec::new();
        let mut min_created = i64::MAX;
        let mut max_updated = i64::MIN;

        for wl in &snap.workloads {
            match wl.status {
                WorkloadStatus::Completed => {
                    completed += 1;
                    if let Some(ref res) = wl.result_metadata {
                        durations.push(res.duration_ms);
                    }
                    min_created = min_created.min(wl.created_at_utc);
                    max_updated = max_updated.max(wl.updated_at_utc);
                }
                WorkloadStatus::Failed | WorkloadStatus::TimedOut => {
                    failed += 1;
                }
                _ => {}
            }
        }

        durations.sort_unstable();
        let avg_duration_ms = if durations.is_empty() {
            None
        } else {
            Some(durations.iter().sum::<u64>() / durations.len() as u64)
        };
        let p95_duration_ms = if durations.is_empty() {
            None
        } else {
            let idx = ((durations.len() as f64) * 0.95).ceil() as usize;
            let clamped = idx.saturating_sub(1).min(durations.len() - 1);
            Some(durations[clamped])
        };

        let mut cpu_series = Vec::new();
        let mut memory_pct_series = Vec::new();
        for s in &snap.historical_metrics {
            if let Some(&cpu) = s.cpu_utilization_pct.value() {
                cpu_series.push(cpu);
            }
            if let (Some(&total), Some(&avail)) =
                (s.total_physical_memory_bytes.value(), s.available_memory_bytes.value())
                && total > 0
            {
                let used_pct = ((total.saturating_sub(avail)) as f64 / total as f64) * 100.0;
                memory_pct_series.push(used_pct.clamp(0.0, 100.0));
            }
        }

        let avg_cpu_pct = if cpu_series.is_empty() {
            None
        } else {
            Some(cpu_series.iter().sum::<f64>() / cpu_series.len() as f64)
        };
        let peak_cpu_pct = cpu_series.iter().copied().reduce(f64::max);
        let avg_memory_used_pct = if memory_pct_series.is_empty() {
            None
        } else {
            Some(memory_pct_series.iter().sum::<f64>() / memory_pct_series.len() as f64)
        };

        let throughput_per_minute = if completed > 0 && max_updated > min_created {
            let mins = ((max_updated - min_created) as f64 / 60_000.0).max(1.0 / 60.0);
            Some(completed as f64 / mins)
        } else {
            None
        };

        let duration_series_ms = durations.iter().rev().take(32).rev().map(|&d| d as f64).collect();

        Self {
            completed_count: completed,
            failed_count: failed,
            avg_duration_ms,
            p95_duration_ms,
            avg_cpu_pct,
            peak_cpu_pct,
            avg_memory_used_pct,
            throughput_per_minute,
            cpu_series,
            memory_pct_series,
            duration_series_ms,
        }
    }
}
