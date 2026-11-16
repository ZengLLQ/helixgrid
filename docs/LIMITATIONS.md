# HelixGrid v1 Scope & Known Limitations

HelixGrid prioritizes engineering honesty over inflated claims. The boundaries below are explicit in v1:

1. **Single-Machine Execution Only:**
   - All orchestration, workers, Named Pipe IPC, Job Objects, and SQLite persistence operate exclusively on the local Windows 11 x64 host. Remote worker discovery, TLS network transport, and multi-node scheduling are deferred beyond v1.
2. **Windows Job Objects Are Not a Complete Security Sandbox:**
   - `WindowsJobObject` enforces `KILL_ON_JOB_CLOSE`, memory limits, active process limits, and CPU rate caps, ensuring reliable teardown and resource governance. However, worker processes still execute under the invoking user's standard Windows token without AppContainer virtualization, Restricted Token SID filtering, or virtualized file/registry namespaces.
3. **CPU-Only Validated Compute Backend:**
   - No CUDA, Vulkan Compute, DirectML, or OpenCL GPU dispatch backend is implemented in v1. `GpuCapabilityStatus::CpuOnlyMode` reports this transparently in telemetry and Diagnostics.
4. **Registered External Executables Require Explicit Non-Shell Allowlisting:**
   - Arbitrary shell commands, scripts (`cmd.exe`, `powershell.exe`, `pwsh.exe`, `bash.exe`, `wscript.exe`, etc.), and non-allowlisted executables are intentionally blocked (`SecurityError`).
5. **Windows 11 x64 Primary Target:**
   - Tested and verified on Windows 11 Pro x64 (`10.0.26300`). Linux, macOS, ARM64 Windows, and Windows 10 were not validated in this build.
6. **Code Signing Not Included for Unmanaged Builds:**
   - `helixgrid.exe`, `helixgrid-worker.exe`, and `helixgrid-0.1.0-x64.msi` are compiled locally without an Authenticode code-signing certificate; production distribution requires signing with an organization certificate (`signtool`).
7. **MSI Desktop UI Interaction:**
   - Automated verification confirmed Win32 HWND creation, live worker spawning, `--smoke-test`, and clean `WM_CLOSE` teardown under both `winit-femtovg` and `winit-software`, alongside end-to-end Rust engine tests covering all core workflows; interactive mouse/keyboard inspection on physical hardware monitors should complement automated CI checks before public release.
