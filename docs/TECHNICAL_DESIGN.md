# HelixGrid Technical Design

This document captures key internal mechanisms, design trade-offs, UI architecture, and platform contracts across the HelixGrid workspace.

## 1. Control Plane Coordinator Actor (`crates/helixgrid-core`)

- `HelixGridEngine` owns the canonical runtime state inside a dedicated multi-threaded Tokio background runtime (`bg_thread` in `apps/helixgrid-studio/src/main.rs`), ensuring the Slint UI thread never awaits disk, IPC, or worker subprocess I/O.
- State mutations flow in via `mpsc::Sender<EngineCommand>` (from callers / UI `UiAction` bridge) and `ExecutorEvent` updates from `WorkerPoolSupervisor`.
- Read-only observers subscribe to `watch::Receiver<Arc<EngineSnapshot>>`. Cached execution records (`executions_by_workload`), artifact lists (`artifacts_by_workload`), and `DatabaseStatusReport` are maintained incrementally in memory and synchronized with SQLite write-through persistence so snapshot emission remains sub-millisecond even under 100+ concurrent workloads.

## 2. IPC Protocol (`crates/helixgrid-protocol`)

- **Transport:** Windows Named Pipes (`\\.\pipe\helixgrid-...`) using `tokio::net::windows::named_pipe`.
- **Framing:** Length-prefixed big/little-endian contract `[u32 little-endian payload length][UTF-8 JSON payload]`, strictly bounded by `MAX_FRAME_BYTES` (`4 MiB`), with protocol compatibility check (`PROTOCOL_VERSION = 1`).
- **Message Set (`IpcMessage`):** 13 strongly typed variants covering worker registration (`WorkerRegistration`, `WorkerReady`), telemetry heartbeats (`Heartbeat`), dispatch & lifecycle (`ExecutionRequest`, `ExecutionAccepted`, `ExecutionStarted`, `ExecutionProgress`, `ExecutionCompleted`, `ExecutionFailed`, `ExecutionCancelled`), cooperative control (`CancelExecutionCommand`), shutdown (`WorkerShutdown`), and structured diagnostics (`DiagnosticEvent`).

## 3. Windows OS Integration (`crates/helixgrid-platform-windows`)

| Primitive | Win32 / COM APIs Used | Purpose |
| --- | --- | --- |
| **Job Objects** | `CreateJobObjectW`, `SetInformationJobObject`, `AssignProcessToJobObject`, `QueryInformationJobObject`, `TerminateJobObject` | Process-group teardown (`KILL_ON_JOB_CLOSE`), memory limit, active process limit, CPU rate hard cap |
| **Process Spawn** | `CreateProcessW` (`CREATE_NO_WINDOW | CREATE_SUSPENDED`), `ResumeThread`, `GetExitCodeProcess` | Attaches child worker to Job Object before user-mode execution starts |
| **Pipe Security** | `OpenProcessToken`, `GetTokenInformation(TokenUser)`, `ConvertSidToStringSidW`, `ConvertStringSecurityDescriptorToSecurityDescriptorW` | Current-user SDDL DACL `D:(A;;GA;;;{user_sid})` + local-only pipe creation |
| **OS & Process Telemetry** | `GetSystemInfo`, `GlobalMemoryStatusEx`, `GetSystemTimes`, `GetProcessMemoryInfo`, `GetProcessTimes` | Real CPU utilization, RAM capacity/availability, and per-worker memory/CPU time |
| **Native File Dialogs** | COM `IFileOpenDialog`, `IFileSaveDialog` (`FOS_PICKFOLDERS` when picking folder) | Authentic Windows file/folder selection for batch workloads, SQLite backup, allowlist config |
| **Accessibility** | `SystemParametersInfoW(SPI_GETCLIENTAREAANIMATION)` | Detects Windows animation/reduced-motion preference on startup |

## 4. Telemetry Provenance & Honest Reporting (`helixgrid-domain/src/metrics.rs`)

Metrics never fabricate missing numbers or silently turn estimates into sensor data:

- `MeasurementValue<T>` separates `Current(T)`, `Historical(T)`, `Estimate(T)`, and `Unavailable { reason }`.
- `ProgressState` separates `Indeterminate { message }` from `Determinate { percent, completed_units, total_units, message }`.
- `GpuCapabilityStatus::CpuOnlyMode { reason }` explicitly communicates CPU-only execution mode without fake GPU utilization meters.

## 5. Desktop UI Architecture (`ui/` + `apps/helixgrid-studio`)

- **Framework:** Slint `1.19.0` (`backend-winit` + `renderer-femtovg` + `renderer-software`), exposing an 8-screen desktop shell (`Overview`, `Workers`, `Workloads`, `Workflows`, `Analytics`, `Activity`, `Settings`, `Diagnostics`) and modal dialogs (`Submit Workload`, `Create Workflow`, `Register Executable`, `Confirm Action`, `About`).
- **Typography & Theme System (`ui/themes/tokens.slint`):** Bundles **Poppins** (`Regular`, `Medium`, `SemiBold`, `Bold`, SIL OFL 1.1) exclusively across all screens with zero monospace fonts anywhere in the UI. Implements Dark (`#111214` / `#1B1D20` / `#24272B` / `#32353A` / `#F4F4F2` / `#A1A5AB` / `#7C9CF5`) and Light (`#F5F5F3` / `#FFFFFF` / `#E5E5E1` / `#17181A` / `#74777D` / `#526FC5`) tokens with accessible focus/keyboard navigation (`Ctrl+1..8`, `Ctrl+N`, `Ctrl+,`, `F5`, `Esc`) and reduced-motion support (`0ms` transitions when OS reduced motion is active or overridden).
- **Licensing Compliance:** `HxAboutDialog` embeds `AboutSlint` (`LicenseRef-Slint-Royalty-free-2.0` desktop attribution) alongside license notices in `LICENSE`.
