use crate::PlatformError;
use crate::job::WindowsJobObject;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use windows::Win32::Foundation::{CloseHandle, HANDLE, STILL_ACTIVE, WAIT_OBJECT_0};
use windows::Win32::System::Threading::{
    CREATE_NO_WINDOW, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
    GetExitCodeProcess, PROCESS_INFORMATION, ResumeThread, STARTUPINFOW, TerminateProcess,
    WaitForSingleObject,
};
use windows::core::{PCWSTR, PWSTR};

/// Owned handle to a Windows worker process launched suspended and bound to a Job Object.
pub struct WindowsWorkerProcess {
    pid: u32,
    process_handle: HANDLE,
}

// SAFETY: Win32 process handles are thread-safe kernel handles; access is synchronized by OS.
unsafe impl Send for WindowsWorkerProcess {}
unsafe impl Sync for WindowsWorkerProcess {}

impl WindowsWorkerProcess {
    /// Launches `exe_path` with explicit argument array (never concatenated into a shell),
    /// starts it suspended without a console window, assigns it to `job`, and resumes execution.
    pub fn spawn_in_job(
        exe_path: &Path,
        args: &[String],
        working_dir: &Path,
        env_map: Option<&BTreeMap<String, String>>,
        job: &WindowsJobObject,
    ) -> Result<Self, PlatformError> {
        let mut cmdline_str = quote_windows_arg(exe_path.as_os_str());
        for arg in args {
            cmdline_str.push(' ');
            cmdline_str.push_str(&quote_windows_arg(OsStr::new(arg)));
        }

        let mut cmdline_wide: Vec<u16> = cmdline_str.encode_utf16().chain(Some(0)).collect();
        let app_wide: Vec<u16> = exe_path.as_os_str().encode_wide().chain(Some(0)).collect();
        let dir_wide: Vec<u16> = working_dir.as_os_str().encode_wide().chain(Some(0)).collect();

        let env_block: Option<Vec<u16>> = env_map.map(|m| {
            let mut block = Vec::new();
            for (k, v) in m {
                block.extend(k.encode_utf16());
                block.push(b'=' as u16);
                block.extend(v.encode_utf16());
                block.push(0);
            }
            block.push(0);
            block
        });

        let si = STARTUPINFOW {
            cb: u32::try_from(std::mem::size_of::<STARTUPINFOW>()).unwrap_or(0),
            ..Default::default()
        };
        let mut pi = PROCESS_INFORMATION::default();

        let mut flags = CREATE_NO_WINDOW | CREATE_SUSPENDED;
        let env_ptr = if let Some(ref block) = env_block {
            flags |= CREATE_UNICODE_ENVIRONMENT;
            Some(block.as_ptr().cast())
        } else {
            None
        };

        // SAFETY: All wide buffers outlive the CreateProcessW call, cmdline_wide is mutable and
        // null-terminated as required by CreateProcessW.
        unsafe {
            CreateProcessW(
                PCWSTR(app_wide.as_ptr()),
                Some(PWSTR(cmdline_wide.as_mut_ptr())),
                None,
                None,
                false,
                flags,
                env_ptr,
                PCWSTR(dir_wide.as_ptr()),
                &si,
                &mut pi,
            )
        }?;

        let assign_res = job.assign_process_handle(pi.hProcess);
        if let Err(err) = assign_res {
            // SAFETY: Terminate and close handles if Job assignment fails before resuming.
            unsafe {
                let _ = TerminateProcess(pi.hProcess, 1);
                let _ = CloseHandle(pi.hThread);
                let _ = CloseHandle(pi.hProcess);
            }
            return Err(err);
        }

        // SAFETY: Process is already inside the Job Object; resume primary thread now.
        let prev_suspend = unsafe { ResumeThread(pi.hThread) };
        unsafe {
            let _ = CloseHandle(pi.hThread);
        }
        if prev_suspend == u32::MAX {
            unsafe {
                let _ = TerminateProcess(pi.hProcess, 1);
                let _ = CloseHandle(pi.hProcess);
            }
            return Err(PlatformError::Win32(windows::core::Error::from_thread()));
        }

        Ok(Self { pid: pi.dwProcessId, process_handle: pi.hProcess })
    }

    #[must_use]
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Non-blocking check returning `Some(exit_code)` if the process has exited, or `None` if still alive.
    pub fn try_exit_code(&self) -> Result<Option<i32>, PlatformError> {
        // SAFETY: WaitForSingleObject with timeout 0 polls handle state without blocking.
        let wait_res = unsafe { WaitForSingleObject(self.process_handle, 0) };
        if wait_res != WAIT_OBJECT_0 {
            return Ok(None);
        }
        let mut code: u32 = 0;
        // SAFETY: `self.process_handle` is valid and `code` is writable.
        unsafe { GetExitCodeProcess(self.process_handle, &mut code) }?;
        if code == STILL_ACTIVE.0 as u32 { Ok(None) } else { Ok(Some(code as i32)) }
    }

    /// Forcefully terminates the process with `exit_code`.
    pub fn terminate(&self, exit_code: u32) -> Result<(), PlatformError> {
        // SAFETY: `self.process_handle` is a valid owned process handle.
        unsafe { TerminateProcess(self.process_handle, exit_code) }?;
        Ok(())
    }
}

impl Drop for WindowsWorkerProcess {
    fn drop(&mut self) {
        if !self.process_handle.is_invalid() {
            // SAFETY: Closing process handle on drop; Job Object also ensures child termination on job close.
            unsafe {
                let _ = CloseHandle(self.process_handle);
            }
        }
    }
}

/// Quotes a Windows command-line argument following CommandLineToArgvW rules.
fn quote_windows_arg(arg: &OsStr) -> String {
    let s = arg.to_string_lossy();
    if !s.is_empty() && !s.contains([' ', '\t', '"']) {
        return s.into_owned();
    }
    let mut out = String::from("\"");
    let mut backslashes = 0usize;
    for ch in s.chars() {
        if ch == '\\' {
            backslashes += 1;
        } else if ch == '"' {
            out.push_str(&"\\".repeat(backslashes * 2 + 1));
            out.push('"');
            backslashes = 0;
        } else {
            out.push_str(&"\\".repeat(backslashes));
            out.push(ch);
            backslashes = 0;
        }
    }
    out.push_str(&"\\".repeat(backslashes * 2));
    out.push('"');
    out
}
