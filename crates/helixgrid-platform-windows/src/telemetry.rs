use crate::PlatformError;
use helixgrid_domain::MeasurementValue;
use std::collections::HashMap;
use std::time::Instant;
use windows::Win32::Foundation::{CloseHandle, FILETIME};
use windows::Win32::System::ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
use windows::Win32::System::SystemInformation::{
    GetSystemInfo, GlobalMemoryStatusEx, MEMORYSTATUSEX, SYSTEM_INFO,
};
use windows::Win32::System::Threading::{
    GetProcessTimes, GetSystemTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
};

#[derive(Debug, Clone, Copy)]
struct CpuTimesSample {
    idle: u64,
    kernel: u64,
    user: u64,
}

#[derive(Debug, Clone, Copy)]
struct ProcessTimesSample {
    proc_time_100ns: u64,
    sampled_at: Instant,
}

/// Real Windows OS & per-process resource telemetry reader.
/// Reports `MeasurementValue::Unavailable` whenever a hardware/OS delta is not yet available
/// or a process cannot be queried, never fabricating metrics.
pub struct WindowsSystemTelemetry {
    logical_processors: u32,
    prev_system_cpu: Option<CpuTimesSample>,
    prev_process_cpu: HashMap<u32, ProcessTimesSample>,
}

impl Default for WindowsSystemTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowsSystemTelemetry {
    #[must_use]
    pub fn new() -> Self {
        let mut sys_info = SYSTEM_INFO::default();
        // SAFETY: `sys_info` is valid writable memory.
        unsafe { GetSystemInfo(&mut sys_info) };
        let logical_processors = sys_info.dwNumberOfProcessors.max(1);
        let initial_cpu = read_system_times().ok();
        Self { logical_processors, prev_system_cpu: initial_cpu, prev_process_cpu: HashMap::new() }
    }

    #[must_use]
    pub fn logical_processors(&self) -> u32 {
        self.logical_processors
    }

    /// Reads physical total and available RAM from `GlobalMemoryStatusEx`.
    pub fn memory_bytes(&self) -> Result<(u64, u64), PlatformError> {
        let mut mem = MEMORYSTATUSEX::default();
        mem.dwLength = u32::try_from(std::mem::size_of::<MEMORYSTATUSEX>()).unwrap_or(0);
        // SAFETY: `mem.dwLength` is initialized as required by GlobalMemoryStatusEx.
        unsafe { GlobalMemoryStatusEx(&mut mem) }?;
        Ok((mem.ullTotalPhys, mem.ullAvailPhys))
    }

    /// Computes host CPU utilization percentage from `GetSystemTimes` delta since the prior sample.
    pub fn sample_cpu_utilization_pct(&mut self) -> MeasurementValue<f64> {
        let Ok(curr) = read_system_times() else {
            return MeasurementValue::Unavailable { reason: "GetSystemTimes failed".to_string() };
        };
        let Some(prev) = self.prev_system_cpu else {
            self.prev_system_cpu = Some(curr);
            return MeasurementValue::Unavailable {
                reason: "Awaiting second CPU delta sample".to_string(),
            };
        };
        self.prev_system_cpu = Some(curr);

        let idle_delta = curr.idle.saturating_sub(prev.idle);
        let kernel_delta = curr.kernel.saturating_sub(prev.kernel);
        let user_delta = curr.user.saturating_sub(prev.user);
        let total = kernel_delta.saturating_add(user_delta);
        if total == 0 {
            return MeasurementValue::Unavailable {
                reason: "Zero elapsed CPU interval".to_string(),
            };
        }
        // Windows `kernel` time includes `idle` time.
        let busy = total.saturating_sub(idle_delta);
        let pct = ((busy as f64) / (total as f64) * 100.0).clamp(0.0, 100.0);
        MeasurementValue::Current(pct)
    }

    /// Samples real CPU utilization (%) and Working Set memory (bytes) for a worker PID.
    pub fn sample_process_metrics(
        &mut self,
        pid: u32,
    ) -> (MeasurementValue<f64>, MeasurementValue<u64>) {
        // SAFETY: Opens process with minimal read-only query rights.
        let Ok(handle) = (unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid) })
        else {
            self.prev_process_cpu.remove(&pid);
            return (
                MeasurementValue::Unavailable { reason: "Process handle unavailable".to_string() },
                MeasurementValue::Unavailable { reason: "Process handle unavailable".to_string() },
            );
        };

        let mut counters = PROCESS_MEMORY_COUNTERS {
            cb: u32::try_from(std::mem::size_of::<PROCESS_MEMORY_COUNTERS>()).unwrap_or(0),
            ..Default::default()
        };
        let mem_val = if unsafe { GetProcessMemoryInfo(handle, &mut counters, counters.cb) }.is_ok()
        {
            MeasurementValue::Current(counters.WorkingSetSize as u64)
        } else {
            MeasurementValue::Unavailable { reason: "GetProcessMemoryInfo failed".to_string() }
        };

        let mut created = FILETIME::default();
        let mut exited = FILETIME::default();
        let mut kernel = FILETIME::default();
        let mut user = FILETIME::default();
        let cpu_val = if unsafe {
            GetProcessTimes(handle, &mut created, &mut exited, &mut kernel, &mut user)
        }
        .is_ok()
        {
            let now = Instant::now();
            let proc_100ns = filetime_to_u64(kernel).saturating_add(filetime_to_u64(user));
            if let Some(prev) = self
                .prev_process_cpu
                .insert(pid, ProcessTimesSample { proc_time_100ns: proc_100ns, sampled_at: now })
            {
                let wall_100ns =
                    now.saturating_duration_since(prev.sampled_at).as_nanos().saturating_div(100)
                        as u64;
                let capacity_100ns = wall_100ns.saturating_mul(u64::from(self.logical_processors));
                if capacity_100ns > 0 {
                    let delta = proc_100ns.saturating_sub(prev.proc_time_100ns);
                    let pct = ((delta as f64) / (capacity_100ns as f64) * 100.0).clamp(0.0, 100.0);
                    MeasurementValue::Current(pct)
                } else {
                    MeasurementValue::Unavailable { reason: "Zero elapsed interval".to_string() }
                }
            } else {
                MeasurementValue::Unavailable {
                    reason: "Awaiting second process CPU delta sample".to_string(),
                }
            }
        } else {
            MeasurementValue::Unavailable { reason: "GetProcessTimes failed".to_string() }
        };

        // SAFETY: Always close opened process query handle.
        unsafe {
            let _ = CloseHandle(handle);
        }
        (cpu_val, mem_val)
    }

    pub fn forget_process(&mut self, pid: u32) {
        self.prev_process_cpu.remove(&pid);
    }
}

fn read_system_times() -> Result<CpuTimesSample, PlatformError> {
    let mut idle = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: All pointers reference stack FILETIME structs.
    unsafe { GetSystemTimes(Some(&mut idle), Some(&mut kernel), Some(&mut user)) }?;
    Ok(CpuTimesSample {
        idle: filetime_to_u64(idle),
        kernel: filetime_to_u64(kernel),
        user: filetime_to_u64(user),
    })
}

#[inline]
fn filetime_to_u64(ft: FILETIME) -> u64 {
    (u64::from(ft.dwHighDateTime) << 32) | u64::from(ft.dwLowDateTime)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_real_windows_cpu_and_memory() {
        let mut tel = WindowsSystemTelemetry::new();
        assert!(tel.logical_processors() >= 1);
        let (total, avail) = tel.memory_bytes().unwrap();
        assert!(total > 0);
        assert!(avail <= total);

        std::thread::sleep(std::time::Duration::from_millis(40));
        let cpu = tel.sample_cpu_utilization_pct();
        assert!(cpu.is_available());
    }
}
