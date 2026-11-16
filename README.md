# HelixGrid

**Native Windows 11 x64 Compute Orchestration Platform in Rust + Slint**

HelixGrid turns a single Windows workstation into an observable, resource-aware compute environment. It coordinates multiple isolated worker processes (`helixgrid-worker.exe`) attached to Windows Job Objects over user-scoped Named Pipes, persists full job and execution history in embedded WAL SQLite, and presents a calm, high-contrast Slint desktop studio (`helixgrid.exe`) styled exclusively with Poppins typography.

> **Single-Machine Scope (v1):** HelixGrid v1 runs exclusively on a single Windows 11 x64 computer. Remote cluster nodes, multi-machine networking, and fake GPU/benchmark metrics are intentionally excluded.

---

## Key Capabilities

- **Isolated Multi-Process Worker Pool:** Spawns standalone `helixgrid-worker.exe` child processes suspended, assigns each to an unnamed Win32 Job Object (`KILL_ON_JOB_CLOSE`, memory limits, active process cap, CPU rate hard cap), and resumes cleanly.
- **Hardened Local IPC:** Versioned (`PROTOCOL_VERSION = 1`) length-prefixed JSON protocol (`<= 4 MiB` per frame) over Windows Named Pipes (`reject_remote_clients(true)`, SDDL current-user DACL `D:(A;;GA;;;{user_sid})`, 256-bit constant-time session token handshake).
- **Resource-Aware Priority/FIFO & DAG Scheduler:** Enforces bounded backpressure (`max_queue_depth`), host CPU/memory ceilings, worker CPU/memory capacity reservations, and Kahn-validated multi-stage DAG workflows with failure propagation and upstream artifact passing.
- **Real Compute Kernels:**
  - **CPU Benchmark:** Multi-threaded prime sieve + ARX integer mixing with ops/sec throughput metrics.
  - **Batch Processing:** Disk-backed `HashAndValidate`, `NormalizeJsonRecords`, and `AggregateCsvMetrics`.
  - **Numerical Computation:** Double-precision `DenseMatrixMultiply`, `ConjugateGradientSolve`, `MonteCarloVariance`, and `Rk4LorenzTrajectory`.
  - **Registered External Executables:** Allowlisted native binaries with canonical path validation, shell interpreter blocking (`cmd.exe`, `powershell.exe`, `pwsh.exe`, etc.), scrubbed environments, and retry-safety enforcement.
- **Crash-Safe SQLite WAL Persistence:** 10 relational tables with `FOREIGN KEY` constraints, bounded snapshot/event retention, startup `PRAGMA integrity_check`, online SQLite backups, and automatic crash/restart reconciliation (`Interrupted` vs. safe idempotent auto-recovery).
- **Native Slint Desktop Interface (`helixgrid.exe`):** 8 clean navigation views (`Overview`, `Workers`, `Workloads`, `Workflows`, `Analytics`, `Activity`, `Settings`, `Diagnostics`), Dark & Light color systems, bundled **Poppins** fonts (`SIL OFL 1.1`), keyboard shortcuts (`Ctrl+1..8`, `Ctrl+N`, `Ctrl+,`, `F5`, `Esc`), Windows animation awareness (`SPI_GETCLIENTAREAANIMATION`), and native `IFileOpenDialog` / `IFileSaveDialog` pickers.
- **Per-User Windows MSI Installer:** UAC-free per-user WiX v5 MSI (`helixgrid-0.1.0-x64.msi`) installing into `%LocalAppData%\Programs\HelixGrid` while preserving `%LocalAppData%\HelixGrid\HelixGrid\data` across upgrades and uninstalls.

---

## Architecture Overview

| Layer | Crates / Apps | Responsibility |
| --- | --- | --- |
| Presentation | `apps/helixgrid-studio`, `crates/helixgrid-ui-components`, `ui/` | Slint `AppWindow`, Poppins design system, formatters, SVG chart paths |
| Control Plane | `crates/helixgrid-core`, `crates/helixgrid-config`, `crates/helixgrid-observability` | Async coordinator actor (`HelixGridEngine`), TOML config, rolling JSON logs & diagnostic bundles |
| Orchestration | `crates/helixgrid-domain`, `crates/helixgrid-scheduler`, `crates/helixgrid-resource`, `crates/helixgrid-security` | 12-state finite state machine, Priority/FIFO & DAG schedulers, capacity reservations, path/env/allowlist guards |
| Worker & IPC | `apps/helixgrid-worker`, `crates/helixgrid-worker-runtime`, `crates/helixgrid-executor`, `crates/helixgrid-protocol` | Isolated worker runtime, compute kernels, `WorkerPoolSupervisor`, versioned IPC codec |
| Infrastructure | `crates/helixgrid-storage`, `crates/helixgrid-platform-windows` | Bundled SQLite WAL + online backup, Win32 Job Objects, SDDL Named Pipes, OS telemetry, dialogs |

---

## Quick Start (Windows 11 x64)

### Build, Lint & Test

```powershell
# Verify formatting, strict Clippy lints, and all 33 unit + integration + stress tests
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# Build release binaries (helixgrid.exe + helixgrid-worker.exe)
cargo build --release -p helixgrid-worker -p helixgrid-studio

# Run headless end-to-end verification
.\target\release\helixgrid.exe --smoke-test

# Launch the desktop application
.\target\release\helixgrid.exe
```

### Build Per-User MSI Installer

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\packaging\build-msi.ps1
```

Produces `packaging\out\helixgrid-0.1.0-x64.msi`.

---

## Verified Benchmarks & Quality Metrics

Measured on Windows 11 Pro x64 (`10.0.26300`, AMD Ryzen 5 5600 6-Core / 12 logical CPUs, 32 GiB RAM):

- **`cargo test --workspace`:** **33 passed, 0 failed, 0 ignored** (the full 5-test `stress_and_recovery` suite — including 100 concurrent workloads across 4 worker processes, worker crash fault injection, coordinator DB restart recovery, and property-based checks — completed in `4.96 s`).
- **Criterion Benchmarks (`cargo bench -p helixgrid-benchmarks`):**
  - `scheduler_enqueue_and_dispatch_100`: `66.407 µs` median (`~0.66 µs`/workload)
  - `ipc_frame_encode_decode`: `1.2752 µs` median
  - `ipc_named_pipe_roundtrip`: `6.8282 µs` median
  - `sqlite_insert_and_get_workload`: `181.31 µs` median
  - `compute_cg_solve_500`: `789.83 µs` median

---

## Documentation

- [Architecture Overview](docs/ARCHITECTURE.md)
- [Technical Design](docs/TECHNICAL_DESIGN.md)
- [Dependency Audit](docs/DEPENDENCY_AUDIT.md)
- [Database Schema](docs/DATABASE_SCHEMA.md)
- [Scheduler Design](docs/SCHEDULER_DESIGN.md)
- [Execution Model & State Machine](docs/EXECUTION_MODEL.md)
- [Security Model & Trust Boundaries](docs/SECURITY_MODEL.md)
- [Performance & Benchmarks](docs/PERFORMANCE.md)
- [Testing & Verification](docs/TESTING.md)
- [Building from Source](docs/BUILDING.md)
- [Installation & MSI Packaging](docs/INSTALLATION.md)
- [Known Limitations](docs/LIMITATIONS.md)
- [Roadmap](docs/ROADMAP.md)

---

## License

HelixGrid original code is dual-licensed under **MIT OR Apache-2.0** ([LICENSE](LICENSE)). Third-party binary obligations include Slint `1.19.0` attribution (`AboutSlint`), bundled Poppins font files (`SIL OFL 1.1`, [OFL.txt](assets/fonts/OFL.txt)), and SQLite (`Public Domain`).
