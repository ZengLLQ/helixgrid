-- HelixGrid Initial Embedded SQLite Schema (Version 1)
-- Foreign keys and WAL mode are enforced on every connection open.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at_utc INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS application_settings (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at_utc INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    pid INTEGER,
    status TEXT NOT NULL,
    protocol_version INTEGER NOT NULL,
    max_cpu_cores INTEGER NOT NULL,
    max_memory_mb INTEGER NOT NULL,
    reserved_cpu_cores INTEGER NOT NULL,
    reserved_memory_mb INTEGER NOT NULL,
    started_at_utc INTEGER NOT NULL,
    last_heartbeat_utc INTEGER,
    completed_executions INTEGER NOT NULL DEFAULT 0,
    failed_executions INTEGER NOT NULL DEFAULT 0,
    isolation_note TEXT NOT NULL,
    updated_at_utc INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_definitions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at_utc INTEGER NOT NULL,
    updated_at_utc INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS workloads (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    workload_type TEXT NOT NULL,
    priority INTEGER NOT NULL,
    spec_json TEXT NOT NULL,
    resources_json TEXT NOT NULL,
    timeout_ms INTEGER NOT NULL,
    retry_policy_json TEXT NOT NULL,
    created_at_utc INTEGER NOT NULL,
    updated_at_utc INTEGER NOT NULL,
    status TEXT NOT NULL,
    progress_json TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    assigned_worker_id TEXT REFERENCES workers(id) ON DELETE SET NULL,
    latest_execution_id TEXT,
    parent_workload_id TEXT REFERENCES workloads(id) ON DELETE SET NULL,
    result_metadata_json TEXT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_workloads_status_priority
    ON workloads (status, priority DESC, created_at_utc ASC);

CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY,
    workload_id TEXT NOT NULL REFERENCES workloads(id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    outcome TEXT,
    started_at_utc INTEGER NOT NULL,
    finished_at_utc INTEGER,
    duration_ms INTEGER,
    exit_code INTEGER,
    error_message TEXT,
    result_json TEXT,
    log_file_path TEXT
);

CREATE INDEX IF NOT EXISTS idx_executions_workload
    ON executions (workload_id, attempt_number DESC);

CREATE INDEX IF NOT EXISTS idx_executions_status
    ON executions (status);

CREATE TABLE IF NOT EXISTS workflow_tasks (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflow_definitions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    depends_on_json TEXT NOT NULL,
    priority INTEGER NOT NULL,
    spec_json TEXT NOT NULL,
    resources_json TEXT NOT NULL,
    timeout_ms INTEGER NOT NULL,
    retry_policy_json TEXT NOT NULL,
    state TEXT NOT NULL,
    workload_id TEXT REFERENCES workloads(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_tasks_workflow
    ON workflow_tasks (workflow_id);

CREATE TABLE IF NOT EXISTS execution_artifacts (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    workload_id TEXT NOT NULL REFERENCES workloads(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    file_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256_hex TEXT NOT NULL,
    created_at_utc INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_artifacts_workload
    ON execution_artifacts (workload_id, created_at_utc DESC);

CREATE TABLE IF NOT EXISTS resource_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at_utc INTEGER NOT NULL,
    logical_processors INTEGER NOT NULL,
    cpu_utilization_pct REAL,
    cpu_provenance TEXT NOT NULL,
    total_physical_memory_bytes INTEGER,
    available_memory_bytes INTEGER,
    memory_provenance TEXT NOT NULL,
    active_workers INTEGER NOT NULL,
    active_executions INTEGER NOT NULL,
    queue_depth INTEGER NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_resource_snapshots_time
    ON resource_snapshots (captured_at_utc DESC);

CREATE TABLE IF NOT EXISTS scheduler_events (
    id TEXT PRIMARY KEY,
    timestamp_utc INTEGER NOT NULL,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    workload_id TEXT REFERENCES workloads(id) ON DELETE SET NULL,
    worker_id TEXT REFERENCES workers(id) ON DELETE SET NULL,
    workflow_id TEXT REFERENCES workflow_definitions(id) ON DELETE SET NULL,
    message TEXT NOT NULL,
    metadata_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_scheduler_events_time
    ON scheduler_events (timestamp_utc DESC);

CREATE INDEX IF NOT EXISTS idx_scheduler_events_severity
    ON scheduler_events (severity, timestamp_utc DESC);
