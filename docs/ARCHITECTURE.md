# HelixGrid Architecture

HelixGrid is a native Windows 11 x64 single-machine compute orchestration platform written in Rust with a declarative Slint desktop interface. It transforms a local workstation into an isolated, resource-aware, observable execution environment by separating UI presentation, orchestration state, resource accounting, persistence, Windows OS primitives, and out-of-process worker execution.

> **Scope Boundary (v1):** HelixGrid v1 is strictly single-machine. There are no remote worker agents, network sockets, cluster membership protocols, or distributed schedulers in this release.

## High-Level System Topology

```
+-------------------------------------------------------------------+
| helixgrid.exe (apps/helixgrid-studio)                             |
|                                                                   |
|  +-----------------------------+    UiAction (bounded mpsc)       |
|  | Slint UI Thread             | -------------------------------> |
|  | (ui/*.slint, Poppins only,  |                                  |
|  |  FemtoVG / Software)        | <--- EngineSnapshot (watch) ---- |
|  +-----------------------------+                                  |
|                                                                   |
|  +--------------------------------------------------------------+ |
|  | Background Tokio Runtime (HelixGridEngine Coordinator Actor) | |
|  |  - Domain & State Machine (helixgrid-domain)                 | |
|  |  - Priority/FIFO & DAG Scheduler (helixgrid-scheduler)       | |
|  |  - CPU/Memory Accounting (helixgrid-resource)                | |
|  |  - SQLite WAL Persistence (helixgrid-storage)                | |
|  |  - Worker Pool Supervisor (helixgrid-executor)               | |
|  +--------------------------------------------------------------+ |
+----------------------------------|--------------------------------+
                                   |
     Windows Named Pipes (\\.\pipe\helixgrid-*, SDDL current-user DACL,
     256-bit constant-time auth token, 4 MiB bounded JSON frames)
                                   |
        +--------------------------+--------------------------+
        |                                                     |
        v                                                     v
+-------------------------------+             +-------------------------------+
| helixgrid-worker.exe (#1)     |             | helixgrid-worker.exe (#N)     |
| Assigned to Win32 Job Object: |             | Assigned to Win32 Job Object: |
|  - KILL_ON_JOB_CLOSE          |             |  - KILL_ON_JOB_CLOSE          |
|  - Process Memory Limit       |             |  - Process Memory Limit       |
|  - CPU Rate Hard Cap          |             |  - CPU Rate Hard Cap          |
|  - Active Process Limit       |             |  - Active Process Limit       |
+-------------------------------+             +-------------------------------+
```

## Workspace Crate Boundaries

| Layer | Crate / App | Responsibility | Key Dependencies / Isolation Rule |
| --- | --- | --- | --- |
| **Domain** | `helixgrid-domain` | IDs, state machines, `Workload`, `WorkflowDefinition` (Kahn DAG sort), `MeasurementValue`, `SystemEvent` | Pure Rust; no Slint, no SQLite, no Win32 APIs |
| **Protocol** | `helixgrid-protocol` | Versioned (`v1`) length-prefixed JSON IPC codec, bounded at `4 MiB` | Depends only on `helixgrid-domain` |
| **Config** | `helixgrid-config` | `AppConfig` validation, TOML persistence, `AppPaths` (`%LocalAppData%\HelixGrid\HelixGrid\data` via `directories`) | Depends only on `helixgrid-domain` |
| **Observability** | `helixgrid-observability` | `BoundedRotatingWriter`, `EventRingBuffer`, JSON tracing, `DiagnosticBundle` export | Depends on `helixgrid-domain` |
| **Security** | `helixgrid-security` | `safe_join_under`, executable allowlist, disallowed shell checks, env scrubbing, retry safety, SHA-256 | Depends on `helixgrid-domain`, `helixgrid-config` |
| **Platform (Win32)** | `helixgrid-platform-windows` | `WindowsJobObject`, suspended+Job `CreateProcessW`, SDDL Named Pipe server, OS/process telemetry, `IFileOpenDialog`/`IFileSaveDialog`, `SPI_GETCLIENTAREAANIMATION` | Only crate invoking Win32/COM unsafe APIs via `windows` |
| **Storage** | `helixgrid-storage` | Bundled SQLite WAL engine, migrations, transactions, retention pruning, online `Backup`, integrity verification | Depends on `helixgrid-domain`, `rusqlite` |
| **Resource** | `helixgrid-resource` | `SystemTelemetryProvider` trait, `WindowsTelemetryProvider`, atomic CPU/memory reservation tracking & host ceilings | Depends on `helixgrid-domain`, `helixgrid-config`, `helixgrid-platform-windows` |
| **Scheduler** | `helixgrid-scheduler` | `SchedulerEngine` (Priority/FIFO + backpressure), `WorkflowScheduler` (DAG progression & upstream artifact forwarding) | Pure deterministic Rust; zero Slint or Win32 dependency |
| **Worker Runtime** | `helixgrid-worker-runtime` | Pipe client, heartbeat sender, real compute kernels & cooperative cancellation | Depends on `domain`, `protocol`, `config`, `security`, `observability`, `platform-windows` |
| **Executor** | `helixgrid-executor` | `WorkerPoolSupervisor` managing child process spawn, pipe auth handshake, exit polling, graceful/forced shutdown | Coordinator-side process & IPC lifecycle |
| **Core** | `helixgrid-core` | `HelixGridEngine` actor, startup crash reconciliation, execution timeout tracking, snapshot broadcast (`watch`) | Combines domain, storage, scheduler, resource, executor, security |
| **UI Helpers** | `helixgrid-ui-components` | Display formatters, `AnalyticsSummary`, SVG path builders for time-series charts, `StatusTone` mapping | Pure Rust; no Slint runtime coupling |
| **Desktop App** | `apps/helixgrid-studio` | `helixgrid.exe` binary (`windows` subsystem in release), Slint `AppWindow`, `presenter.rs`, `--smoke-test` | Owns Slint UI event loop; delegates all mutations via `UiAction` channel |
| **Worker Binary** | `apps/helixgrid-worker` | `helixgrid-worker.exe` standalone CLI process invoked by coordinator | Isolated out-of-process compute host |

## Strict UI / Engine Threading Separation

- `apps/helixgrid-studio/src/main.rs` constructs the Slint `AppWindow` on the main GUI thread while spawning a dedicated multi-threaded Tokio background thread (`bg_thread`).
- UI interactions never touch SQLite, Job Objects, or scheduler state directly. Button callbacks push lightweight `UiAction` commands onto a bounded `tokio::sync::mpsc` channel (capacity `128`).
- Coordinator state changes emit immutable `Arc<EngineSnapshot>` updates over a `tokio::sync::watch` channel, which a background listener forwards to the UI thread via `slint::invoke_from_event_loop`.
- Heavy compute never shares the coordinator's Tokio worker threads: `helixgrid-worker.exe` runs in separate OS processes and executes CPU-intensive kernels on `tokio::task::spawn_blocking`.

## Windows Platform Boundaries vs. Slint UI

- Slint renders all application chrome and widgets itself (via `winit-femtovg` OpenGL or `winit-software` fallback); HelixGrid does not confuse or describe Slint widgets as native Win32/Common Controls.
- Genuine OS integration lives strictly in `helixgrid-platform-windows`: Job Objects, Named Pipe DACLs, `CreateProcessW`, Win32 CPU/memory telemetry (`GetSystemInfo`, `GlobalMemoryStatusEx`, `GetSystemTimes`, `GetProcessMemoryInfo`, `GetProcessTimes`), `IFileOpenDialog` / `IFileSaveDialog`, and `SystemParametersInfoW(SPI_GETCLIENTAREAANIMATION)`.
- Every Job Object wrapper explicitly documents that while Win32 Job Objects enforce process-tree teardown (`KILL_ON_JOB_CLOSE`), memory caps, active process limits, and CPU rate hard caps, a Job Object alone is **not** a complete OS security sandbox.
