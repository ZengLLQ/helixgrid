# HelixGrid Performance Benchmarks & Bottleneck Analysis

All measurements below were collected on the target Windows 11 x64 environment (`Microsoft Windows 11 Pro 10.0.26300`, `AMD Ryzen 5 5600 6-Core Processor`, 12 logical processors, 32 GiB RAM) using release-optimized Criterion benchmarks (`cargo bench -p helixgrid-benchmarks`) and end-to-end integration stress tests (`cargo test --workspace`).

## Criterion Micro-Benchmarks (`benchmarks/benches/engine_benchmarks.rs`)

Warmup: `3.0 s` · Samples: `100` per benchmark (`criterion 0.8.2`, `bench` optimized profile):

| Benchmark | Lower Bound | Median | Upper Bound | Notes |
| --- | --- | --- | --- | --- |
| `scheduler_enqueue_and_dispatch_100` | `65.298 µs` | **`66.407 µs`** | `67.527 µs` | Enqueues 100 workloads + runs `schedule_ready` (~`0.66 µs` per workload) |
| `ipc_frame_encode_decode` | `1.2300 µs` | **`1.2752 µs`** | `1.3272 µs` | Length-prefixed JSON `IpcEnvelope` heartbeat encode + decode |
| `ipc_named_pipe_roundtrip` | `6.6930 µs` | **`6.8282 µs`** | `6.9858 µs` | Full async Windows Named Pipe frame write + read over `\\.\pipe\helixgrid-bench-*` |
| `sqlite_insert_and_get_workload` | `173.16 µs` | **`181.31 µs`** | `190.16 µs` | Disk-backed SQLite WAL `insert_workload` + `get_workload` round-trip |
| `compute_cg_solve_500` | `760.89 µs` | **`789.83 µs`** | `821.75 µs` | Real `ConjugateGradientSolve` (`dimension = 500`) + artifact output & SHA-256 |

## Application Binary Footprint & Runtime Memory (Observed)

| Artifact / Process | Measured Value | How Measured |
| --- | --- | --- |
| `helixgrid.exe` (release, stripped) | `25,414,144 bytes` (`~24.24 MiB`) | `cargo build --release -p helixgrid-studio` |
| `helixgrid-worker.exe` (release, stripped) | `2,922,496 bytes` (`~2.79 MiB`) | `cargo build --release -p helixgrid-worker` |
| `helixgrid-0.1.0-x64.msi` (high compression) | `9,601,024 bytes` (`~9.16 MiB`) | WiX v5 `packaging/build-msi.ps1` |
| Studio GUI memory (`winit-femtovg`) | `166.45 MB` Working Set | Live Win32 GUI startup + 2 active worker processes |
| Studio GUI memory (`winit-software`) | `46.15 MB` Working Set | Live Win32 GUI startup + 2 active worker processes |

## Real Bottlenecks Identified & Eliminated During Stress Validation

Running `processes_100_concurrent_queued_workloads_and_releases_all_reservations` exposed a genuine coordinator scalability bottleneck:

- **Problem:** Initially, `HelixGridEngine::build_snapshot` ran on every IPC progress/completion message and executed `PRAGMA integrity_check` (`status_report()`) plus 200 individual SQLite `SELECT` queries (`list_executions_for_workload` + `list_artifacts_for_workload`). Under 100 concurrent workloads firing rapid progress updates, the coordinator thread became I/O-bound and timed out before draining the queue.
- **Fix Applied (`crates/helixgrid-core/src/engine.rs`):**
  1. Cached `executions_by_workload`, `artifacts_by_workload`, and `cached_db_status` directly inside `HelixGridEngine` memory synchronized with write-through persistence updates (`upsert_cached_execution`, insert on dispatch, artifact insertion).
  2. Moved full `StorageEngine::status_report()` checks to startup and periodic `metrics_sample_interval_ms` ticks instead of per-IPC message emission.
- **Measured Result:** All 100 queued real compute workloads across 4 OS worker processes now execute end-to-end, persist WAL rows, generate artifacts, and release 100% of resource reservations, completing alongside all other stress/recovery tests in `4.96 seconds` for the full 5-test `stress_and_recovery` suite (see [TESTING.md](TESTING.md)).
