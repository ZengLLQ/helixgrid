use crate::PlatformError;
use std::ffi::c_void;
use windows::Win32::Foundation::{CloseHandle, HANDLE};
use windows::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_CPU_RATE_CONTROL_ENABLE,
    JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP, JOB_OBJECT_LIMIT_ACTIVE_PROCESS,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOB_OBJECT_LIMIT_PROCESS_MEMORY,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_CPU_RATE_CONTROL_INFORMATION,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicAccountingInformation,
    JobObjectCpuRateControlInformation, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows::core::PCWSTR;

/// RAII wrapper around a Windows Job Object.
///
/// Note on security boundaries: Job Objects provide process-group lifecycle guarantees
/// (`KILL_ON_JOB_CLOSE`), memory limits, active process limits, and CPU rate caps,
/// but do NOT constitute a complete security sandbox on their own.
pub struct WindowsJobObject {
    handle: HANDLE,
    limits_summary: String,
}

// SAFETY: Windows Job Object handles are kernel handles safe to send/share across threads;
// Win32 kernel synchronizes operations on the underlying handle.
unsafe impl Send for WindowsJobObject {}
unsafe impl Sync for WindowsJobObject {}

#[derive(Debug, Clone)]
pub struct JobAccountingSnapshot {
    pub active_processes: u32,
    pub total_processes: u32,
    pub total_user_time_100ns: i64,
    pub total_kernel_time_100ns: i64,
}

impl WindowsJobObject {
    pub fn create(
        max_process_memory_bytes: Option<u64>,
        cpu_hard_cap_pct: Option<u32>,
        active_process_limit: Option<u32>,
    ) -> Result<Self, PlatformError> {
        // SAFETY: CreateJobObjectW returns a valid handle or an error; null name creates an unnamed job.
        let handle = unsafe { CreateJobObjectW(None, PCWSTR::null()) }?;

        let mut ext = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        let mut flags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let mut parts = vec!["KillOnClose".to_string()];

        if let Some(mem_bytes) = max_process_memory_bytes {
            flags |= JOB_OBJECT_LIMIT_PROCESS_MEMORY;
            ext.ProcessMemoryLimit = usize::try_from(mem_bytes).unwrap_or(usize::MAX);
            parts.push(format!("MemLimit={}MB", mem_bytes / (1024 * 1024)));
        }

        if let Some(proc_limit) = active_process_limit {
            flags |= JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
            ext.BasicLimitInformation.ActiveProcessLimit = proc_limit.max(1);
            parts.push(format!("ActiveProcMax={}", proc_limit.max(1)));
        }

        ext.BasicLimitInformation.LimitFlags = flags;

        // SAFETY: `ext` is properly initialized and sized for JobObjectExtendedLimitInformation.
        let res = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                (&ext as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast::<c_void>(),
                u32::try_from(std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                    .unwrap_or(0),
            )
        };
        if let Err(err) = res {
            // SAFETY: Close handle before returning error.
            unsafe {
                let _ = CloseHandle(handle);
            }
            return Err(PlatformError::Win32(err));
        }

        if let Some(pct) = cpu_hard_cap_pct {
            let clamped = pct.clamp(1, 100);
            let mut cpu_info = JOBOBJECT_CPU_RATE_CONTROL_INFORMATION {
                ControlFlags: JOB_OBJECT_CPU_RATE_CONTROL_ENABLE
                    | JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP,
                ..Default::default()
            };
            cpu_info.Anonymous.CpuRate = clamped * 100;

            // SAFETY: `cpu_info` is properly initialized and sized for JobObjectCpuRateControlInformation.
            let cpu_res = unsafe {
                SetInformationJobObject(
                    handle,
                    JobObjectCpuRateControlInformation,
                    (&cpu_info as *const JOBOBJECT_CPU_RATE_CONTROL_INFORMATION).cast::<c_void>(),
                    u32::try_from(std::mem::size_of::<JOBOBJECT_CPU_RATE_CONTROL_INFORMATION>())
                        .unwrap_or(0),
                )
            };
            if cpu_res.is_ok() {
                parts.push(format!("CpuCap={clamped}%"));
            }
        }

        Ok(Self { handle, limits_summary: format!("Win32 JobObject ({})", parts.join(", ")) })
    }

    pub fn assign_process_handle(&self, process_handle: HANDLE) -> Result<(), PlatformError> {
        // SAFETY: Both `self.handle` (owned valid job handle) and `process_handle` are valid handles.
        unsafe { AssignProcessToJobObject(self.handle, process_handle) }?;
        Ok(())
    }

    pub fn query_accounting(&self) -> Result<JobAccountingSnapshot, PlatformError> {
        let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        // SAFETY: `info` buffer matches JobObjectBasicAccountingInformation size.
        unsafe {
            QueryInformationJobObject(
                Some(self.handle),
                JobObjectBasicAccountingInformation,
                (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast::<c_void>(),
                u32::try_from(std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>())
                    .unwrap_or(0),
                None,
            )
        }?;
        Ok(JobAccountingSnapshot {
            active_processes: info.ActiveProcesses,
            total_processes: info.TotalProcesses,
            total_user_time_100ns: info.TotalUserTime,
            total_kernel_time_100ns: info.TotalKernelTime,
        })
    }

    pub fn terminate(&self, exit_code: u32) -> Result<(), PlatformError> {
        // SAFETY: Terminates all processes within this owned Job Object.
        unsafe { TerminateJobObject(self.handle, exit_code) }?;
        Ok(())
    }

    #[must_use]
    pub fn limits_summary(&self) -> &str {
        &self.limits_summary
    }
}

impl Drop for WindowsJobObject {
    fn drop(&mut self) {
        if !self.handle.is_invalid() {
            // SAFETY: CloseHandle on owned handle; KILL_ON_JOB_CLOSE terminates child processes.
            unsafe {
                let _ = CloseHandle(self.handle);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_and_queries_job_object() {
        let job = WindowsJobObject::create(Some(256 * 1024 * 1024), Some(80), Some(4)).unwrap();
        let snap = job.query_accounting().unwrap();
        assert_eq!(snap.active_processes, 0);
        assert!(job.limits_summary().contains("KillOnClose"));
    }
}
