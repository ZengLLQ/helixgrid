use helixgrid_config::AppConfig;
use helixgrid_domain::{
    ExecutionId, GpuCapabilityStatus, MeasurementValue, ResourceRequirements,
    SystemMetricsSnapshot, WorkerId, WorkerInfo, WorkloadId,
};
use helixgrid_observability::now_utc_millis;
use helixgrid_platform_windows::telemetry::WindowsSystemTelemetry;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ResourceError {
    #[error("duplicate resource reservation for workload {0}")]
    DuplicateReservation(WorkloadId),

    #[error("insufficient worker capacity on worker {worker_id}: {reason}")]
    InsufficientWorkerCapacity { worker_id: WorkerId, reason: String },

    #[error("host resource ceiling exceeded: {0}")]
    HostCeilingExceeded(String),
}

/// Abstract telemetry provider enabling future distributed nodes and deterministic test doubles.
pub trait SystemTelemetryProvider: Send {
    fn logical_processors(&self) -> u32;
    fn sample_host(&mut self) -> HostTelemetrySample;
    fn sample_process(&mut self, pid: u32) -> (MeasurementValue<f64>, MeasurementValue<u64>);
    fn forget_process(&mut self, pid: u32);
}

#[derive(Debug, Clone)]
pub struct HostTelemetrySample {
    pub logical_processors: u32,
    pub cpu_utilization_pct: MeasurementValue<f64>,
    pub total_physical_memory_bytes: MeasurementValue<u64>,
    pub available_memory_bytes: MeasurementValue<u64>,
}

/// Real Windows OS telemetry backed by `GetSystemInfo`, `GlobalMemoryStatusEx`, `GetSystemTimes`,
/// `GetProcessMemoryInfo`, and `GetProcessTimes`.
pub struct WindowsTelemetryProvider {
    inner: WindowsSystemTelemetry,
}

impl Default for WindowsTelemetryProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowsTelemetryProvider {
    #[must_use]
    pub fn new() -> Self {
        Self { inner: WindowsSystemTelemetry::new() }
    }
}

impl SystemTelemetryProvider for WindowsTelemetryProvider {
    fn logical_processors(&self) -> u32 {
        self.inner.logical_processors()
    }

    fn sample_host(&mut self) -> HostTelemetrySample {
        let cpu = self.inner.sample_cpu_utilization_pct();
        let (total_mem, avail_mem) = match self.inner.memory_bytes() {
            Ok((total, avail)) => {
                (MeasurementValue::Current(total), MeasurementValue::Current(avail))
            }
            Err(e) => (
                MeasurementValue::Unavailable { reason: e.to_string() },
                MeasurementValue::Unavailable { reason: e.to_string() },
            ),
        };
        HostTelemetrySample {
            logical_processors: self.inner.logical_processors(),
            cpu_utilization_pct: cpu,
            total_physical_memory_bytes: total_mem,
            available_memory_bytes: avail_mem,
        }
    }

    fn sample_process(&mut self, pid: u32) -> (MeasurementValue<f64>, MeasurementValue<u64>) {
        self.inner.sample_process_metrics(pid)
    }

    fn forget_process(&mut self, pid: u32) {
        self.inner.forget_process(pid);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceReservation {
    pub workload_id: WorkloadId,
    pub execution_id: ExecutionId,
    pub worker_id: WorkerId,
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub exclusive_worker: bool,
    pub reserved_at_utc: i64,
}

/// Manages host/worker capacity reservations and produces validated resource snapshots.
pub struct ResourceManager {
    telemetry: Box<dyn SystemTelemetryProvider>,
    reservations: HashMap<WorkloadId, ResourceReservation>,
    latest_host: HostTelemetrySample,
    latest_snapshot: SystemMetricsSnapshot,
}

impl ResourceManager {
    #[must_use]
    pub fn new(mut telemetry: Box<dyn SystemTelemetryProvider>) -> Self {
        let latest_host = telemetry.sample_host();
        let latest_snapshot = SystemMetricsSnapshot {
            captured_at_utc: now_utc_millis(),
            logical_processors: latest_host.logical_processors,
            cpu_utilization_pct: latest_host.cpu_utilization_pct.clone(),
            total_physical_memory_bytes: latest_host.total_physical_memory_bytes.clone(),
            available_memory_bytes: latest_host.available_memory_bytes.clone(),
            active_workers: 0,
            active_executions: 0,
            queue_depth: 0,
            gpu_acceleration_status: GpuCapabilityStatus::default(),
        };
        Self { telemetry, reservations: HashMap::new(), latest_host, latest_snapshot }
    }

    #[must_use]
    pub fn new_windows() -> Self {
        Self::new(Box::new(WindowsTelemetryProvider::new()))
    }

    #[must_use]
    pub fn logical_processors(&self) -> u32 {
        self.telemetry.logical_processors()
    }

    #[must_use]
    pub fn latest_snapshot(&self) -> &SystemMetricsSnapshot {
        &self.latest_snapshot
    }

    pub fn refresh_snapshot(
        &mut self,
        workers: &mut [WorkerInfo],
        active_executions: u32,
        queue_depth: u32,
    ) -> SystemMetricsSnapshot {
        let sample = self.telemetry.sample_host();
        let active_workers = workers.iter().filter(|w| w.status.is_alive()).count() as u32;

        for w in workers.iter_mut() {
            if w.status.is_alive() {
                if let Some(pid) = w.pid {
                    let (cpu, mem) = self.telemetry.sample_process(pid);
                    w.cpu_usage_pct = cpu;
                    w.memory_usage_bytes = mem;
                }
            } else if let Some(pid) = w.pid {
                self.telemetry.forget_process(pid);
            }
        }

        let snap = SystemMetricsSnapshot {
            captured_at_utc: now_utc_millis(),
            logical_processors: sample.logical_processors,
            cpu_utilization_pct: sample.cpu_utilization_pct.clone(),
            total_physical_memory_bytes: sample.total_physical_memory_bytes.clone(),
            available_memory_bytes: sample.available_memory_bytes.clone(),
            active_workers,
            active_executions,
            queue_depth,
            gpu_acceleration_status: GpuCapabilityStatus::default(),
        };
        self.latest_host = sample;
        self.latest_snapshot = snap.clone();
        snap
    }

    pub fn check_host_headroom(
        &self,
        config: &AppConfig,
        req: &ResourceRequirements,
    ) -> Result<(), ResourceError> {
        if let Some(&cpu) = self.latest_host.cpu_utilization_pct.value()
            && cpu > config.cpu_utilization_ceiling_pct
        {
            return Err(ResourceError::HostCeilingExceeded(format!(
                "host CPU utilization {cpu:.1}% exceeds configured ceiling {:.1}%",
                config.cpu_utilization_ceiling_pct
            )));
        }

        if let (Some(&total), Some(&avail)) = (
            self.latest_host.total_physical_memory_bytes.value(),
            self.latest_host.available_memory_bytes.value(),
        ) {
            let avail_mb = avail / (1024 * 1024);
            let min_free_after = config.reserved_system_memory_mb.saturating_add(req.memory_mb);
            if avail_mb < min_free_after {
                return Err(ResourceError::HostCeilingExceeded(format!(
                    "available memory {avail_mb} MB < required {min_free_after} MB (including reserved system memory)"
                )));
            }

            let total_reserved_mb: u64 = self.reservations.values().map(|r| r.memory_mb).sum();
            let ceiling_mb = ((total / (1024 * 1024)) as f64
                * (config.memory_allocation_ceiling_pct / 100.0))
                as u64;
            if total_reserved_mb.saturating_add(req.memory_mb) > ceiling_mb {
                return Err(ResourceError::HostCeilingExceeded(format!(
                    "reserved memory {} MB would exceed memory allocation ceiling {ceiling_mb} MB",
                    total_reserved_mb + req.memory_mb
                )));
            }
        }
        Ok(())
    }

    /// Atomically verifies capacity and records a resource reservation on `worker`.
    pub fn reserve(
        &mut self,
        config: &AppConfig,
        worker: &mut WorkerInfo,
        workload_id: WorkloadId,
        execution_id: ExecutionId,
        req: &ResourceRequirements,
        now_utc: i64,
    ) -> Result<ResourceReservation, ResourceError> {
        if self.reservations.contains_key(&workload_id) {
            return Err(ResourceError::DuplicateReservation(workload_id));
        }

        if !worker.status.is_available_for_scheduling() {
            return Err(ResourceError::InsufficientWorkerCapacity {
                worker_id: worker.id,
                reason: format!("worker status is {:?}", worker.status),
            });
        }

        if req.requires_exclusive_worker && worker.reserved_cpu_cores > 0 {
            return Err(ResourceError::InsufficientWorkerCapacity {
                worker_id: worker.id,
                reason: "exclusive worker required while worker holds active reservations"
                    .to_string(),
            });
        }

        if worker.available_cpu_cores() < req.cpu_cores {
            return Err(ResourceError::InsufficientWorkerCapacity {
                worker_id: worker.id,
                reason: format!(
                    "available CPU cores {} < required {}",
                    worker.available_cpu_cores(),
                    req.cpu_cores
                ),
            });
        }

        if worker.available_memory_mb() < req.memory_mb {
            return Err(ResourceError::InsufficientWorkerCapacity {
                worker_id: worker.id,
                reason: format!(
                    "available memory {} MB < required {} MB",
                    worker.available_memory_mb(),
                    req.memory_mb
                ),
            });
        }

        self.check_host_headroom(config, req)?;

        worker.reserved_cpu_cores = worker.reserved_cpu_cores.saturating_add(req.cpu_cores);
        worker.reserved_memory_mb = worker.reserved_memory_mb.saturating_add(req.memory_mb);

        let reservation = ResourceReservation {
            workload_id,
            execution_id,
            worker_id: worker.id,
            cpu_cores: req.cpu_cores,
            memory_mb: req.memory_mb,
            exclusive_worker: req.requires_exclusive_worker,
            reserved_at_utc: now_utc,
        };
        self.reservations.insert(workload_id, reservation.clone());
        Ok(reservation)
    }

    pub fn release_for_workload(
        &mut self,
        workload_id: WorkloadId,
        workers: &mut [WorkerInfo],
    ) -> Option<ResourceReservation> {
        let res = self.reservations.remove(&workload_id)?;
        if let Some(worker) = workers.iter_mut().find(|w| w.id == res.worker_id) {
            worker.reserved_cpu_cores = worker.reserved_cpu_cores.saturating_sub(res.cpu_cores);
            worker.reserved_memory_mb = worker.reserved_memory_mb.saturating_sub(res.memory_mb);
            if worker.current_workload_id == Some(workload_id) {
                worker.current_workload_id = None;
                worker.current_execution_id = None;
            }
        }
        Some(res)
    }

    pub fn release_all_for_worker(
        &mut self,
        worker_id: WorkerId,
        worker: Option<&mut WorkerInfo>,
    ) -> Vec<ResourceReservation> {
        let affected: Vec<WorkloadId> = self
            .reservations
            .values()
            .filter(|r| r.worker_id == worker_id)
            .map(|r| r.workload_id)
            .collect();
        let mut released = Vec::with_capacity(affected.len());
        for wl_id in affected {
            if let Some(r) = self.reservations.remove(&wl_id) {
                released.push(r);
            }
        }
        if let Some(w) = worker {
            w.reserved_cpu_cores = 0;
            w.reserved_memory_mb = 0;
            w.current_workload_id = None;
            w.current_execution_id = None;
        }
        released
    }

    #[must_use]
    pub fn active_reservations(&self) -> Vec<&ResourceReservation> {
        self.reservations.values().collect()
    }

    #[must_use]
    pub fn reservation_count(&self) -> usize {
        self.reservations.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helixgrid_domain::WorkerStatus;

    struct FakeTelemetry {
        cpu: f64,
        total_bytes: u64,
        avail_bytes: u64,
    }

    impl SystemTelemetryProvider for FakeTelemetry {
        fn logical_processors(&self) -> u32 {
            12
        }

        fn sample_host(&mut self) -> HostTelemetrySample {
            HostTelemetrySample {
                logical_processors: 12,
                cpu_utilization_pct: MeasurementValue::Current(self.cpu),
                total_physical_memory_bytes: MeasurementValue::Current(self.total_bytes),
                available_memory_bytes: MeasurementValue::Current(self.avail_bytes),
            }
        }

        fn sample_process(&mut self, _pid: u32) -> (MeasurementValue<f64>, MeasurementValue<u64>) {
            (MeasurementValue::Current(5.0), MeasurementValue::Current(32 * 1024 * 1024))
        }

        fn forget_process(&mut self, _pid: u32) {}
    }

    fn make_worker(cores: u32, mem_mb: u64) -> WorkerInfo {
        WorkerInfo {
            id: WorkerId::new(),
            name: "w1".to_string(),
            pid: Some(1234),
            status: WorkerStatus::Idle,
            protocol_version: 1,
            max_cpu_cores: cores,
            max_memory_mb: mem_mb,
            reserved_cpu_cores: 0,
            reserved_memory_mb: 0,
            current_workload_id: None,
            current_execution_id: None,
            cpu_usage_pct: MeasurementValue::Unavailable { reason: "init".to_string() },
            memory_usage_bytes: MeasurementValue::Unavailable { reason: "init".to_string() },
            started_at_utc: 1000,
            last_heartbeat_utc: Some(1000),
            completed_executions: 0,
            failed_executions: 0,
            isolation_note: "test".to_string(),
        }
    }

    #[test]
    fn prevents_duplicate_and_overcommitted_reservations() {
        let mut mgr = ResourceManager::new(Box::new(FakeTelemetry {
            cpu: 20.0,
            total_bytes: 32 * 1024 * 1024 * 1024,
            avail_bytes: 24 * 1024 * 1024 * 1024,
        }));
        let cfg = AppConfig::default();
        let mut worker = make_worker(4, 2048);
        let wl = WorkloadId::new();
        let exec = ExecutionId::new();
        let req =
            ResourceRequirements { cpu_cores: 2, memory_mb: 512, requires_exclusive_worker: false };

        mgr.reserve(&cfg, &mut worker, wl, exec, &req, 1000).unwrap();
        assert_eq!(worker.reserved_cpu_cores, 2);
        assert_eq!(worker.reserved_memory_mb, 512);

        // Duplicate workload reservation rejected
        assert!(matches!(
            mgr.reserve(&cfg, &mut worker, wl, exec, &req, 1000),
            Err(ResourceError::DuplicateReservation(_))
        ));

        // Overcommit CPU cores rejected
        let big_req =
            ResourceRequirements { cpu_cores: 3, memory_mb: 256, requires_exclusive_worker: false };
        assert!(matches!(
            mgr.reserve(&cfg, &mut worker, WorkloadId::new(), ExecutionId::new(), &big_req, 1000),
            Err(ResourceError::InsufficientWorkerCapacity { .. })
        ));

        let mut workers = vec![worker];
        mgr.release_for_workload(wl, &mut workers);
        assert_eq!(workers[0].reserved_cpu_cores, 0);
        assert_eq!(mgr.reservation_count(), 0);
    }
}
