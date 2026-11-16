# HelixGrid Database Schema & Persistence Design

HelixGrid persists operational state, execution history, artifacts, resource telemetry, and audit events in a local embedded SQLite database managed by `crates/helixgrid-storage`.

- **Driver:** `rusqlite` `0.43.0` compiled with `bundled` (native C SQLite statically linked via `libsqlite3-sys`) and `backup` features.
- **Default Database File:** `%LocalAppData%\HelixGrid\HelixGrid\data\helixgrid.db` (overrideable via `--data-dir <PATH>`).
- **Schema Source:** `migrations/0001_initial.sql`, embedded at compile time via `include_str!` in `crates/helixgrid-storage/src/migrations.rs`.

## Connection Invariants

Every SQLite connection opened through `StorageEngine::open` executes:

1. `PRAGMA journal_mode = WAL;` — Enables concurrent readers without writer starvation and atomic crash-safe commits.
2. `PRAGMA synchronous = NORMAL;` — Balances durability and WAL commit throughput.
3. `PRAGMA foreign_keys = ON;` — Enforces relational integrity across all parent/child tables.
4. `PRAGMA busy_timeout = 5000;` — Prevents spurious lock contention failures under concurrent operations.
5. Startup `PRAGMA integrity_check` — Fails fast with `StorageError::Corruption` if database pages fail structural verification.

## Tables (`migrations/0001_initial.sql`, Version 1)

| Table | Purpose | Foreign Keys & Indexes |
| --- | --- | --- |
| `schema_migrations` | Applied migration versions (`version`, `name`, `applied_at_utc`) | Primary key `version` |
| `application_settings` | Key/value JSON application settings store (`key`, `value_json`, `updated_at_utc`) | Primary key `key` |
| `workers` | Last known state, capacities, PID, heartbeat, and counters per worker | Primary key `id` |
| `workflow_definitions` | Top-level multi-stage DAG workflow definitions (`status`, `name`, `description`) | Primary key `id` |
| `workloads` | Workload specs, priority, resource requirements, retry policy, status, progress, result metadata | `assigned_worker_id -> workers(id)` (`ON DELETE SET NULL`), `parent_workload_id -> workloads(id)` (`ON DELETE SET NULL`); index `idx_workloads_status_priority` on `(status, priority DESC, created_at_utc ASC)` |
| `executions` | Per-attempt execution records (`attempt_number`, `outcome`, timestamps, `duration_ms`, `exit_code`, `result_json`) | `workload_id -> workloads(id)` (`ON DELETE CASCADE`), `worker_id -> workers(id)` (`ON DELETE RESTRICT`); indexes `idx_executions_workload`, `idx_executions_status` |
| `workflow_tasks` | DAG task definitions (`depends_on_json`, `spec_json`, `state`, linked `workload_id`) | `workflow_id -> workflow_definitions(id)` (`ON DELETE CASCADE`), `workload_id -> workloads(id)` (`ON DELETE SET NULL`); index `idx_workflow_tasks_workflow` |
| `execution_artifacts` | Output artifacts (`kind`, `file_path`, `size_bytes`, `sha256_hex`) produced by executions | `execution_id -> executions(id)` (`ON DELETE CASCADE`), `workload_id -> workloads(id)` (`ON DELETE CASCADE`); index `idx_artifacts_workload` |
| `resource_snapshots` | Historical CPU/memory telemetry samples with explicit provenance strings (`cpu_provenance`, `memory_provenance`) | Auto-increment `id`; index `idx_resource_snapshots_time` |
| `scheduler_events` | Structured lifecycle, security, and failure audit trail | Nullable FK references (`ON DELETE SET NULL`) to `workloads`, `workers`, `workflow_definitions`; indexes `idx_scheduler_events_time`, `idx_scheduler_events_severity` |

## Retention, Backups & Integrity

- **Bounded Growth:** Telemetry (`resource_snapshots`) and audit events (`scheduler_events`) automatically prune oldest rows past `snapshot_retention_max_rows` (default `10,000`) and `event_retention_max_rows` (default `20,000`).
- **Online Consistent Backups:** `StorageEngine::backup_to(dest_path)` invokes SQLite's online `Backup` API (`rusqlite::backup::Backup`) using 256-page steps with `2 ms` pauses so backups remain transactionally consistent while workers and snapshots continue committing in WAL mode.
- **Health Checks:** Startup and throttled periodic checks query `PRAGMA journal_mode`, `PRAGMA foreign_keys`, schema version, and `PRAGMA integrity_check` (cached on `HelixGridEngine` so per-message IPC handling stays fast without saturating disk I/O).
