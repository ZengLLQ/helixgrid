pub mod migrations;

use crate::migrations::{StorageError, apply_migrations};
use helixgrid_domain::{
    ArtifactId, ArtifactKind, EventId, EventSeverity, ExecutionArtifact, ExecutionId,
    ExecutionOutcome, ExecutionRecord, MeasurementValue, Priority, ProgressState,
    ResourceRequirements, RetryPolicy, SystemEvent, SystemEventKind, SystemMetricsSnapshot,
    WorkerId, WorkerInfo, WorkflowDefinition, WorkflowId, WorkflowStatus, WorkflowTaskDefinition,
    WorkflowTaskId, WorkflowTaskState, Workload, WorkloadId, WorkloadResultMetadata, WorkloadSpec,
    WorkloadStatus, WorkloadType,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseStatusReport {
    pub path: String,
    pub journal_mode: String,
    pub foreign_keys_enabled: bool,
    pub schema_version: i64,
    pub integrity_ok: bool,
    pub integrity_details: Vec<String>,
    pub workload_count: u64,
    pub execution_count: u64,
    pub worker_count: u64,
    pub snapshot_count: u64,
    pub event_count: u64,
    pub file_size_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub struct EventFilter {
    pub severity: Option<EventSeverity>,
    pub kind: Option<SystemEventKind>,
    pub workload_id: Option<WorkloadId>,
    pub worker_id: Option<WorkerId>,
    pub since_utc: Option<i64>,
    pub limit: usize,
}

#[derive(Clone)]
pub struct StorageEngine {
    db_path: PathBuf,
    conn: Arc<Mutex<Connection>>,
}

impl StorageEngine {
    /// Opens (or creates) an embedded SQLite database, enforces foreign keys + WAL,
    /// verifies integrity, and runs pending schema migrations.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut conn = Connection::open(path)?;
        configure_connection(&conn)?;
        let integrity = check_integrity_on(&conn)?;
        if integrity != ["ok"] {
            return Err(StorageError::Corruption(integrity.join("; ")));
        }
        let now_utc = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        apply_migrations(&mut conn, now_utc)?;
        Ok(Self { db_path: path.to_path_buf(), conn: Arc::new(Mutex::new(conn)) })
    }

    pub fn open_in_memory() -> Result<Self, StorageError> {
        let mut conn = Connection::open_in_memory()?;
        configure_connection(&conn)?;
        let now_utc = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        apply_migrations(&mut conn, now_utc)?;
        Ok(Self { db_path: PathBuf::from(":memory:"), conn: Arc::new(Mutex::new(conn)) })
    }

    fn lock_conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StorageError> {
        self.conn.lock().map_err(|_| StorageError::LockPoisoned)
    }

    #[must_use]
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn status_report(&self) -> Result<DatabaseStatusReport, StorageError> {
        let conn = self.lock_conn()?;
        let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        let fk: i64 = conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?;
        let schema_version: i64 =
            conn.query_row("SELECT COALESCE(MAX(version), 0) FROM schema_migrations", [], |r| {
                r.get(0)
            })?;
        let integrity = check_integrity_on(&conn)?;
        let integrity_ok = integrity == ["ok"];
        let workload_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM workloads", [], |r| r.get(0))?;
        let execution_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM executions", [], |r| r.get(0))?;
        let worker_count: i64 = conn.query_row("SELECT COUNT(*) FROM workers", [], |r| r.get(0))?;
        let snapshot_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM resource_snapshots", [], |r| r.get(0))?;
        let event_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM scheduler_events", [], |r| r.get(0))?;
        let file_size_bytes = fs::metadata(&self.db_path).map(|m| m.len()).unwrap_or(0);

        Ok(DatabaseStatusReport {
            path: self.db_path.to_string_lossy().into_owned(),
            journal_mode,
            foreign_keys_enabled: fk == 1,
            schema_version,
            integrity_ok,
            integrity_details: integrity,
            workload_count: workload_count.max(0) as u64,
            execution_count: execution_count.max(0) as u64,
            worker_count: worker_count.max(0) as u64,
            snapshot_count: snapshot_count.max(0) as u64,
            event_count: event_count.max(0) as u64,
            file_size_bytes,
        })
    }

    /// Creates a consistent online SQLite backup at `dest_path` using SQLite's backup API.
    pub fn backup_to(&self, dest_path: &Path) -> Result<(), StorageError> {
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temp_dest = dest_path.with_extension("bak.tmp");
        if temp_dest.exists() {
            fs::remove_file(&temp_dest)?;
        }
        {
            let src = self.lock_conn()?;
            let mut dst = Connection::open(&temp_dest)?;
            {
                let backup = rusqlite::backup::Backup::new(&src, &mut dst)?;
                backup.run_to_completion(64, std::time::Duration::from_millis(5), None)?;
            }
            let integrity = check_integrity_on(&dst)?;
            if integrity != ["ok"] {
                return Err(StorageError::Corruption(format!(
                    "backup integrity check failed: {}",
                    integrity.join("; ")
                )));
            }
        }
        if dest_path.exists() {
            fs::remove_file(dest_path)?;
        }
        fs::rename(&temp_dest, dest_path)?;
        Ok(())
    }

    /// Validates a backup SQLite file and restores it onto `target_db_path`.
    /// Must be called when no active open connection holds an exclusive lock on `target_db_path`,
    /// or via re-opening after restore.
    pub fn verify_and_restore_file(
        backup_path: &Path,
        target_db_path: &Path,
    ) -> Result<(), StorageError> {
        {
            let conn = Connection::open(backup_path)?;
            let integrity = check_integrity_on(&conn)?;
            if integrity != ["ok"] {
                return Err(StorageError::Corruption(format!(
                    "backup file failed integrity check: {}",
                    integrity.join("; ")
                )));
            }
            let has_migrations: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='workloads'",
                [],
                |r| r.get(0),
            )?;
            if has_migrations != 1 {
                return Err(StorageError::Corruption(
                    "backup file does not contain HelixGrid schema".to_string(),
                ));
            }
        }
        if let Some(parent) = target_db_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let wal = PathBuf::from(format!("{}-wal", target_db_path.display()));
        let shm = PathBuf::from(format!("{}-shm", target_db_path.display()));
        if wal.exists() {
            let _ = fs::remove_file(wal);
        }
        if shm.exists() {
            let _ = fs::remove_file(shm);
        }
        fs::copy(backup_path, target_db_path)?;
        Ok(())
    }

    pub fn upsert_worker(&self, worker: &WorkerInfo, now_utc: i64) -> Result<(), StorageError> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT INTO workers (
                id, name, pid, status, protocol_version, max_cpu_cores, max_memory_mb,
                reserved_cpu_cores, reserved_memory_mb, started_at_utc, last_heartbeat_utc,
                completed_executions, failed_executions, isolation_note, updated_at_utc
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                pid = excluded.pid,
                status = excluded.status,
                max_cpu_cores = excluded.max_cpu_cores,
                max_memory_mb = excluded.max_memory_mb,
                reserved_cpu_cores = excluded.reserved_cpu_cores,
                reserved_memory_mb = excluded.reserved_memory_mb,
                last_heartbeat_utc = excluded.last_heartbeat_utc,
                completed_executions = excluded.completed_executions,
                failed_executions = excluded.failed_executions,
                isolation_note = excluded.isolation_note,
                updated_at_utc = excluded.updated_at_utc",
            params![
                worker.id.to_string(),
                worker.name,
                worker.pid.map(i64::from),
                serde_json::to_string(&worker.status)?,
                i64::from(worker.protocol_version),
                i64::from(worker.max_cpu_cores),
                worker.max_memory_mb as i64,
                i64::from(worker.reserved_cpu_cores),
                worker.reserved_memory_mb as i64,
                worker.started_at_utc,
                worker.last_heartbeat_utc,
                worker.completed_executions as i64,
                worker.failed_executions as i64,
                worker.isolation_note,
                now_utc
            ],
        )?;
        Ok(())
    }

    pub fn list_workers(&self, limit: usize) -> Result<Vec<WorkerInfo>, StorageError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, pid, status, protocol_version, max_cpu_cores, max_memory_mb,
                    reserved_cpu_cores, reserved_memory_mb, started_at_utc, last_heartbeat_utc,
                    completed_executions, failed_executions, isolation_note
             FROM workers ORDER BY started_at_utc DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            let id_s: String = r.get(0)?;
            let status_s: String = r.get(3)?;
            let pid_opt: Option<i64> = r.get(2)?;
            Ok((
                id_s,
                r.get::<_, String>(1)?,
                pid_opt,
                status_s,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, i64>(7)?,
                r.get::<_, i64>(8)?,
                r.get::<_, i64>(9)?,
                r.get::<_, Option<i64>>(10)?,
                r.get::<_, i64>(11)?,
                r.get::<_, i64>(12)?,
                r.get::<_, String>(13)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (
                id_s,
                name,
                pid_opt,
                status_s,
                proto,
                max_cpu,
                max_mem,
                res_cpu,
                res_mem,
                started_at,
                last_hb,
                completed,
                failed,
                isolation_note,
            ) = row?;
            out.push(WorkerInfo {
                id: id_s.parse()?,
                name,
                pid: pid_opt.and_then(|p| u32::try_from(p).ok()),
                status: serde_json::from_str(&status_s)?,
                protocol_version: proto as u32,
                max_cpu_cores: max_cpu as u32,
                max_memory_mb: max_mem as u64,
                reserved_cpu_cores: res_cpu as u32,
                reserved_memory_mb: res_mem as u64,
                current_workload_id: None,
                current_execution_id: None,
                cpu_usage_pct: MeasurementValue::Unavailable {
                    reason: "historical database record".to_string(),
                },
                memory_usage_bytes: MeasurementValue::Unavailable {
                    reason: "historical database record".to_string(),
                },
                started_at_utc: started_at,
                last_heartbeat_utc: last_hb,
                completed_executions: completed as u64,
                failed_executions: failed as u64,
                isolation_note,
            });
        }
        Ok(out)
    }

    pub fn insert_workload(&self, workload: &Workload) -> Result<(), StorageError> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT INTO workloads (
                id, name, description, workload_type, priority, spec_json, resources_json,
                timeout_ms, retry_policy_json, created_at_utc, updated_at_utc, status,
                progress_json, attempt_count, assigned_worker_id, latest_execution_id,
                parent_workload_id, result_metadata_json, error_message
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
            params![
                workload.id.to_string(),
                workload.name,
                workload.description,
                serde_json::to_string(&workload.workload_type)?,
                workload.priority as i64,
                serde_json::to_string(&workload.spec)?,
                serde_json::to_string(&workload.resources)?,
                workload.timeout_ms as i64,
                serde_json::to_string(&workload.retry_policy)?,
                workload.created_at_utc,
                workload.updated_at_utc,
                serde_json::to_string(&workload.status)?,
                serde_json::to_string(&workload.progress)?,
                i64::from(workload.attempt_count),
                workload.assigned_worker_id.map(|w| w.to_string()),
                workload.latest_execution_id.map(|e| e.to_string()),
                workload.parent_workload_id.map(|p| p.to_string()),
                workload
                    .result_metadata
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()?,
                workload.error_message,
            ],
        )?;
        Ok(())
    }

    pub fn update_workload(&self, workload: &Workload) -> Result<(), StorageError> {
        let conn = self.lock_conn()?;
        conn.execute(
            "UPDATE workloads SET
                updated_at_utc = ?2,
                status = ?3,
                progress_json = ?4,
                attempt_count = ?5,
                assigned_worker_id = ?6,
                latest_execution_id = ?7,
                result_metadata_json = ?8,
                error_message = ?9
            WHERE id = ?1",
            params![
                workload.id.to_string(),
                workload.updated_at_utc,
                serde_json::to_string(&workload.status)?,
                serde_json::to_string(&workload.progress)?,
                i64::from(workload.attempt_count),
                workload.assigned_worker_id.map(|w| w.to_string()),
                workload.latest_execution_id.map(|e| e.to_string()),
                workload.result_metadata.as_ref().map(serde_json::to_string).transpose()?,
                workload.error_message,
            ],
        )?;
        Ok(())
    }

    pub fn get_workload(&self, id: WorkloadId) -> Result<Option<Workload>, StorageError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, description, workload_type, priority, spec_json, resources_json,
                    timeout_ms, retry_policy_json, created_at_utc, updated_at_utc, status,
                    progress_json, attempt_count, assigned_worker_id, latest_execution_id,
                    parent_workload_id, result_metadata_json, error_message
             FROM workloads WHERE id = ?1",
        )?;
        let opt = stmt.query_row(params![id.to_string()], row_to_workload_tuple).optional()?;
        opt.map(parse_workload_tuple).transpose()
    }

    pub fn list_workloads(&self, limit: usize) -> Result<Vec<Workload>, StorageError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, description, workload_type, priority, spec_json, resources_json,
                    timeout_ms, retry_policy_json, created_at_utc, updated_at_utc, status,
                    progress_json, attempt_count, assigned_worker_id, latest_execution_id,
                    parent_workload_id, result_metadata_json, error_message
             FROM workloads ORDER BY created_at_utc DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], row_to_workload_tuple)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(parse_workload_tuple(r?)?);
        }
        Ok(out)
    }

    /// Finds workloads left in non-terminal states from a previous coordinator process.
    pub fn list_incomplete_workloads(&self) -> Result<Vec<Workload>, StorageError> {
        let workloads = self.list_workloads(2_000)?;
        Ok(workloads
            .into_iter()
            .filter(|w| !w.status.is_terminal() && w.status != WorkloadStatus::Interrupted)
            .collect())
    }

    pub fn insert_execution(&self, exec: &ExecutionRecord) -> Result<(), StorageError> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT INTO executions (
                id, workload_id, worker_id, attempt_number, status, outcome,
                started_at_utc, finished_at_utc, duration_ms, exit_code,
                error_message, result_json, log_file_path
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                exec.id.to_string(),
                exec.workload_id.to_string(),
                exec.worker_id.to_string(),
                i64::from(exec.attempt_number),
                serde_json::to_string(&exec.status)?,
                exec.outcome.as_ref().map(serde_json::to_string).transpose()?,
                exec.started_at_utc,
                exec.finished_at_utc,
                exec.duration_ms.map(|d| d as i64),
                exec.exit_code.map(i64::from),
                exec.error_message,
                exec.result_json.as_ref().map(serde_json::to_string).transpose()?,
                exec.log_file_path,
            ],
        )?;
        Ok(())
    }

    pub fn update_execution(&self, exec: &ExecutionRecord) -> Result<(), StorageError> {
        let conn = self.lock_conn()?;
        conn.execute(
            "UPDATE executions SET
                status = ?2,
                outcome = ?3,
                finished_at_utc = ?4,
                duration_ms = ?5,
                exit_code = ?6,
                error_message = ?7,
                result_json = ?8,
                log_file_path = ?9
            WHERE id = ?1",
            params![
                exec.id.to_string(),
                serde_json::to_string(&exec.status)?,
                exec.outcome.as_ref().map(serde_json::to_string).transpose()?,
                exec.finished_at_utc,
                exec.duration_ms.map(|d| d as i64),
                exec.exit_code.map(i64::from),
                exec.error_message,
                exec.result_json.as_ref().map(serde_json::to_string).transpose()?,
                exec.log_file_path,
            ],
        )?;
        Ok(())
    }

    pub fn list_executions_for_workload(
        &self,
        workload_id: WorkloadId,
    ) -> Result<Vec<ExecutionRecord>, StorageError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, workload_id, worker_id, attempt_number, status, outcome,
                    started_at_utc, finished_at_utc, duration_ms, exit_code,
                    error_message, result_json, log_file_path
             FROM executions WHERE workload_id = ?1 ORDER BY attempt_number DESC",
        )?;
        let rows = stmt.query_map(params![workload_id.to_string()], row_to_exec_tuple)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(parse_exec_tuple(r?)?);
        }
        Ok(out)
    }

    /// Marks any still-open execution attempts as Interrupted during startup reconciliation.
    pub fn mark_open_executions_interrupted(&self, now_utc: i64) -> Result<usize, StorageError> {
        let conn = self.lock_conn()?;
        let interrupted_status = serde_json::to_string(&WorkloadStatus::Interrupted)?;
        let interrupted_outcome = serde_json::to_string(&ExecutionOutcome::Interrupted)?;
        let changed = conn.execute(
            "UPDATE executions
             SET status = ?1, outcome = ?2, finished_at_utc = ?3,
                 error_message = COALESCE(error_message, 'Coordinator terminated before completion')
             WHERE outcome IS NULL",
            params![interrupted_status, interrupted_outcome, now_utc],
        )?;
        Ok(changed)
    }

    pub fn insert_artifact(&self, art: &ExecutionArtifact) -> Result<(), StorageError> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT INTO execution_artifacts (
                id, execution_id, workload_id, name, kind, file_path,
                size_bytes, sha256_hex, created_at_utc
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                art.id.to_string(),
                art.execution_id.to_string(),
                art.workload_id.to_string(),
                art.name,
                serde_json::to_string(&art.kind)?,
                art.file_path,
                art.size_bytes as i64,
                art.sha256_hex,
                art.created_at_utc,
            ],
        )?;
        Ok(())
    }

    pub fn list_artifacts_for_workload(
        &self,
        workload_id: WorkloadId,
    ) -> Result<Vec<ExecutionArtifact>, StorageError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, execution_id, workload_id, name, kind, file_path,
                    size_bytes, sha256_hex, created_at_utc
             FROM execution_artifacts WHERE workload_id = ?1 ORDER BY created_at_utc DESC",
        )?;
        let rows = stmt.query_map(params![workload_id.to_string()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, i64>(8)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (id_s, exec_s, wl_s, name, kind_s, file_path, size_b, sha, created) = row?;
            let id: ArtifactId = id_s.parse()?;
            let kind: ArtifactKind = serde_json::from_str(&kind_s)?;
            out.push(ExecutionArtifact {
                id,
                execution_id: exec_s.parse()?,
                workload_id: wl_s.parse()?,
                name,
                kind,
                file_path,
                size_bytes: size_b as u64,
                sha256_hex: sha,
                created_at_utc: created,
            });
        }
        Ok(out)
    }

    pub fn insert_workflow(&self, wf: &WorkflowDefinition) -> Result<(), StorageError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO workflow_definitions (id, name, description, status, created_at_utc, updated_at_utc)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                wf.id.to_string(),
                wf.name,
                wf.description,
                serde_json::to_string(&wf.status)?,
                wf.created_at_utc,
                wf.updated_at_utc,
            ],
        )?;
        for task in &wf.tasks {
            tx.execute(
                "INSERT INTO workflow_tasks (
                    id, workflow_id, name, depends_on_json, priority, spec_json,
                    resources_json, timeout_ms, retry_policy_json, state, workload_id
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    task.id.to_string(),
                    wf.id.to_string(),
                    task.name,
                    serde_json::to_string(&task.depends_on)?,
                    task.priority as i64,
                    serde_json::to_string(&task.spec)?,
                    serde_json::to_string(&task.resources)?,
                    task.timeout_ms as i64,
                    serde_json::to_string(&task.retry_policy)?,
                    serde_json::to_string(&task.state)?,
                    task.workload_id.map(|w| w.to_string()),
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn update_workflow_state(&self, wf: &WorkflowDefinition) -> Result<(), StorageError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE workflow_definitions SET status = ?2, updated_at_utc = ?3 WHERE id = ?1",
            params![wf.id.to_string(), serde_json::to_string(&wf.status)?, wf.updated_at_utc,],
        )?;
        for task in &wf.tasks {
            tx.execute(
                "UPDATE workflow_tasks SET state = ?2, workload_id = ?3 WHERE id = ?1",
                params![
                    task.id.to_string(),
                    serde_json::to_string(&task.state)?,
                    task.workload_id.map(|w| w.to_string()),
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_workflows(&self, limit: usize) -> Result<Vec<WorkflowDefinition>, StorageError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, description, status, created_at_utc, updated_at_utc
             FROM workflow_definitions ORDER BY created_at_utc DESC LIMIT ?1",
        )?;
        let wf_rows = stmt.query_map(params![limit as i64], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })?;
        let mut out = Vec::new();
        for wf_row in wf_rows {
            let (id_s, name, desc, status_s, created, updated) = wf_row?;
            let wf_id: WorkflowId = id_s.parse()?;
            let mut task_stmt = conn.prepare(
                "SELECT id, name, depends_on_json, priority, spec_json, resources_json,
                        timeout_ms, retry_policy_json, state, workload_id
                 FROM workflow_tasks WHERE workflow_id = ?1",
            )?;
            let task_rows = task_stmt.query_map(params![id_s], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, String>(8)?,
                    r.get::<_, Option<String>>(9)?,
                ))
            })?;
            let mut tasks = Vec::new();
            for tr in task_rows {
                let (tid_s, tname, deps_s, prio_i, spec_s, res_s, to_ms, retry_s, state_s, wl_opt) =
                    tr?;
                let tid: WorkflowTaskId = tid_s.parse()?;
                let deps: Vec<WorkflowTaskId> = serde_json::from_str(&deps_s)?;
                let prio = int_to_priority(prio_i);
                let spec: WorkloadSpec = serde_json::from_str(&spec_s)?;
                let res: ResourceRequirements = serde_json::from_str(&res_s)?;
                let retry: RetryPolicy = serde_json::from_str(&retry_s)?;
                let state: WorkflowTaskState = serde_json::from_str(&state_s)?;
                let workload_id = wl_opt.map(|s| s.parse()).transpose()?;
                tasks.push(WorkflowTaskDefinition {
                    id: tid,
                    name: tname,
                    depends_on: deps,
                    priority: prio,
                    spec,
                    resources: res,
                    timeout_ms: to_ms as u64,
                    retry_policy: retry,
                    state,
                    workload_id,
                });
            }
            let status: WorkflowStatus = serde_json::from_str(&status_s)?;
            out.push(WorkflowDefinition {
                id: wf_id,
                name,
                description: desc,
                status,
                tasks,
                created_at_utc: created,
                updated_at_utc: updated,
            });
        }
        Ok(out)
    }

    pub fn insert_resource_snapshot(
        &self,
        snap: &SystemMetricsSnapshot,
        max_rows: u64,
    ) -> Result<(), StorageError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let cpu_val = snap.cpu_utilization_pct.value().copied();
        let cpu_prov = snap.cpu_utilization_pct.provenance_label();
        let total_mem = snap.total_physical_memory_bytes.value().map(|v| *v as i64);
        let avail_mem = snap.available_memory_bytes.value().map(|v| *v as i64);
        let mem_prov = snap.available_memory_bytes.provenance_label();

        tx.execute(
            "INSERT INTO resource_snapshots (
                captured_at_utc, logical_processors, cpu_utilization_pct, cpu_provenance,
                total_physical_memory_bytes, available_memory_bytes, memory_provenance,
                active_workers, active_executions, queue_depth, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                snap.captured_at_utc,
                i64::from(snap.logical_processors),
                cpu_val,
                cpu_prov,
                total_mem,
                avail_mem,
                mem_prov,
                i64::from(snap.active_workers),
                i64::from(snap.active_executions),
                i64::from(snap.queue_depth),
                serde_json::to_string(snap)?,
            ],
        )?;

        tx.execute(
            "DELETE FROM resource_snapshots WHERE id NOT IN (
                SELECT id FROM resource_snapshots ORDER BY id DESC LIMIT ?1
            )",
            params![max_rows as i64],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn list_recent_snapshots(
        &self,
        limit: usize,
    ) -> Result<Vec<SystemMetricsSnapshot>, StorageError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT payload_json FROM resource_snapshots ORDER BY captured_at_utc DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            let s: SystemMetricsSnapshot = serde_json::from_str(&row?)?;
            out.push(s);
        }
        out.reverse();
        Ok(out)
    }

    pub fn insert_event(&self, ev: &SystemEvent, max_rows: u64) -> Result<(), StorageError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO scheduler_events (
                id, timestamp_utc, kind, severity, workload_id, worker_id, workflow_id,
                message, metadata_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                ev.id.to_string(),
                ev.timestamp_utc,
                serde_json::to_string(&ev.kind)?,
                serde_json::to_string(&ev.severity)?,
                ev.workload_id.map(|w| w.to_string()),
                ev.worker_id.map(|w| w.to_string()),
                ev.workflow_id.map(|w| w.to_string()),
                ev.message,
                ev.metadata_json.as_ref().map(serde_json::to_string).transpose()?,
            ],
        )?;
        tx.execute(
            "DELETE FROM scheduler_events WHERE id NOT IN (
                SELECT id FROM scheduler_events ORDER BY timestamp_utc DESC LIMIT ?1
            )",
            params![max_rows as i64],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn query_events(&self, filter: &EventFilter) -> Result<Vec<SystemEvent>, StorageError> {
        let conn = self.lock_conn()?;
        let limit = if filter.limit == 0 { 500 } else { filter.limit };
        let mut stmt = conn.prepare(
            "SELECT id, timestamp_utc, kind, severity, workload_id, worker_id, workflow_id,
                    message, metadata_json
             FROM scheduler_events
             WHERE (?1 IS NULL OR severity = ?1)
               AND (?2 IS NULL OR kind = ?2)
               AND (?3 IS NULL OR workload_id = ?3)
               AND (?4 IS NULL OR worker_id = ?4)
               AND (?5 IS NULL OR timestamp_utc >= ?5)
             ORDER BY timestamp_utc DESC
             LIMIT ?6",
        )?;
        let sev_s = filter.severity.as_ref().map(serde_json::to_string).transpose()?;
        let kind_s = filter.kind.as_ref().map(serde_json::to_string).transpose()?;
        let wl_s = filter.workload_id.map(|w| w.to_string());
        let wk_s = filter.worker_id.map(|w| w.to_string());

        let rows = stmt.query_map(
            params![sev_s, kind_s, wl_s, wk_s, filter.since_utc, limit as i64],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, Option<String>>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, Option<String>>(8)?,
                ))
            },
        )?;
        let mut out = Vec::new();
        for row in rows {
            let (id_s, ts, kind_s, sev_s, wl_opt, wk_opt, wf_opt, msg, meta_opt) = row?;
            let id: EventId = id_s.parse()?;
            out.push(SystemEvent {
                id,
                timestamp_utc: ts,
                kind: serde_json::from_str(&kind_s)?,
                severity: serde_json::from_str(&sev_s)?,
                workload_id: wl_opt.map(|s| s.parse()).transpose()?,
                worker_id: wk_opt.map(|s| s.parse()).transpose()?,
                workflow_id: wf_opt.map(|s| s.parse()).transpose()?,
                message: msg,
                metadata_json: meta_opt.map(|s| serde_json::from_str(&s)).transpose()?,
            });
        }
        Ok(out)
    }

    pub fn set_setting(
        &self,
        key: &str,
        value_json: &str,
        now_utc: i64,
    ) -> Result<(), StorageError> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT INTO application_settings (key, value_json, updated_at_utc)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET
                value_json = excluded.value_json,
                updated_at_utc = excluded.updated_at_utc",
            params![key, value_json, now_utc],
        )?;
        Ok(())
    }

    pub fn get_setting(&self, key: &str) -> Result<Option<String>, StorageError> {
        let conn = self.lock_conn()?;
        let val = conn
            .query_row(
                "SELECT value_json FROM application_settings WHERE key = ?1",
                params![key],
                |r| r.get(0),
            )
            .optional()?;
        Ok(val)
    }
}

fn configure_connection(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(
        "PRAGMA foreign_keys = ON;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;",
    )?;
    Ok(())
}

fn check_integrity_on(conn: &Connection) -> Result<Vec<String>, StorageError> {
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

type WorkloadRowTuple = (
    String,
    String,
    String,
    String,
    i64,
    String,
    String,
    i64,
    String,
    i64,
    i64,
    String,
    String,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn row_to_workload_tuple(r: &rusqlite::Row<'_>) -> rusqlite::Result<WorkloadRowTuple> {
    Ok((
        r.get(0)?,
        r.get(1)?,
        r.get(2)?,
        r.get(3)?,
        r.get(4)?,
        r.get(5)?,
        r.get(6)?,
        r.get(7)?,
        r.get(8)?,
        r.get(9)?,
        r.get(10)?,
        r.get(11)?,
        r.get(12)?,
        r.get(13)?,
        r.get(14)?,
        r.get(15)?,
        r.get(16)?,
        r.get(17)?,
        r.get(18)?,
    ))
}

fn parse_workload_tuple(t: WorkloadRowTuple) -> Result<Workload, StorageError> {
    let (
        id_s,
        name,
        description,
        wtype_s,
        prio_i,
        spec_s,
        res_s,
        timeout_ms,
        retry_s,
        created_at,
        updated_at,
        status_s,
        progress_s,
        attempts,
        worker_opt,
        exec_opt,
        parent_opt,
        result_opt,
        error_msg,
    ) = t;

    let workload_type: WorkloadType = serde_json::from_str(&wtype_s)?;
    let spec: WorkloadSpec = serde_json::from_str(&spec_s)?;
    let resources: ResourceRequirements = serde_json::from_str(&res_s)?;
    let retry_policy: RetryPolicy = serde_json::from_str(&retry_s)?;
    let status: WorkloadStatus = serde_json::from_str(&status_s)?;
    let progress: ProgressState = serde_json::from_str(&progress_s)?;
    let result_metadata: Option<WorkloadResultMetadata> =
        result_opt.map(|s| serde_json::from_str(&s)).transpose()?;

    Ok(Workload {
        id: id_s.parse()?,
        name,
        description,
        workload_type,
        priority: int_to_priority(prio_i),
        spec,
        resources,
        timeout_ms: timeout_ms as u64,
        retry_policy,
        created_at_utc: created_at,
        updated_at_utc: updated_at,
        status,
        progress,
        attempt_count: attempts as u32,
        assigned_worker_id: worker_opt.map(|s| s.parse()).transpose()?,
        latest_execution_id: exec_opt.map(|s| s.parse()).transpose()?,
        parent_workload_id: parent_opt.map(|s| s.parse()).transpose()?,
        result_metadata,
        error_message: error_msg,
    })
}

type ExecRowTuple = (
    String,
    String,
    String,
    i64,
    String,
    Option<String>,
    i64,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn row_to_exec_tuple(r: &rusqlite::Row<'_>) -> rusqlite::Result<ExecRowTuple> {
    Ok((
        r.get(0)?,
        r.get(1)?,
        r.get(2)?,
        r.get(3)?,
        r.get(4)?,
        r.get(5)?,
        r.get(6)?,
        r.get(7)?,
        r.get(8)?,
        r.get(9)?,
        r.get(10)?,
        r.get(11)?,
        r.get(12)?,
    ))
}

fn parse_exec_tuple(t: ExecRowTuple) -> Result<ExecutionRecord, StorageError> {
    let (
        id_s,
        wl_s,
        wk_s,
        attempt,
        status_s,
        outcome_opt,
        started,
        finished,
        duration,
        exit_code,
        err_msg,
        res_opt,
        log_path,
    ) = t;
    let id: ExecutionId = id_s.parse()?;
    let workload_id: WorkloadId = wl_s.parse()?;
    let worker_id: WorkerId = wk_s.parse()?;
    Ok(ExecutionRecord {
        id,
        workload_id,
        worker_id,
        attempt_number: attempt as u32,
        status: serde_json::from_str(&status_s)?,
        outcome: outcome_opt.map(|s| serde_json::from_str(&s)).transpose()?,
        started_at_utc: started,
        finished_at_utc: finished,
        duration_ms: duration.map(|d| d as u64),
        exit_code: exit_code.map(|c| c as i32),
        error_message: err_msg,
        result_json: res_opt.map(|s| serde_json::from_str(&s)).transpose()?,
        log_file_path: log_path,
    })
}

fn int_to_priority(v: i64) -> Priority {
    match v {
        0 => Priority::Low,
        2 => Priority::High,
        3 => Priority::Critical,
        _ => Priority::Normal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helixgrid_domain::{CpuBenchmarkInput, GpuCapabilityStatus};

    #[test]
    fn migrations_wal_foreign_keys_and_restart_persistence() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("helixgrid.db");

        let wl_id;
        {
            let store = StorageEngine::open(&db_path).unwrap();
            let report = store.status_report().unwrap();
            assert!(report.foreign_keys_enabled);
            assert!(report.integrity_ok);
            assert_eq!(report.journal_mode.to_ascii_lowercase(), "wal");
            assert_eq!(report.schema_version, 1);

            let wl = Workload::new(
                "Prime Bench",
                "Test restart persistence",
                Priority::High,
                WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                    worker_threads: 2,
                    iterations: 1000,
                    sieve_limit: 5000,
                }),
                ResourceRequirements::default(),
                10_000,
                RetryPolicy::default(),
                1_700_000_000_000,
            )
            .unwrap();
            wl_id = wl.id;
            store.insert_workload(&wl).unwrap();
        }

        // Reopen database and confirm persistence + backup + restore
        let reopened = StorageEngine::open(&db_path).unwrap();
        let loaded = reopened.get_workload(wl_id).unwrap().unwrap();
        assert_eq!(loaded.name, "Prime Bench");

        let backup_path = tmp.path().join("backup.db");
        reopened.backup_to(&backup_path).unwrap();
        drop(reopened);

        let restored_path = tmp.path().join("restored.db");
        StorageEngine::verify_and_restore_file(&backup_path, &restored_path).unwrap();
        let restored = StorageEngine::open(&restored_path).unwrap();
        assert_eq!(restored.get_workload(wl_id).unwrap().unwrap().name, "Prime Bench");
    }

    #[test]
    fn detects_corrupted_sqlite_file() {
        let tmp = tempfile::tempdir().unwrap();
        let bad_path = tmp.path().join("corrupt.db");
        fs::write(&bad_path, b"not a valid sqlite database file at all").unwrap();
        assert!(StorageEngine::open(&bad_path).is_err());
    }

    #[test]
    fn enforces_snapshot_and_event_retention() {
        let store = StorageEngine::open_in_memory().unwrap();
        for i in 0..150 {
            store
                .insert_resource_snapshot(
                    &SystemMetricsSnapshot {
                        captured_at_utc: 1000 + i,
                        logical_processors: 12,
                        cpu_utilization_pct: MeasurementValue::Current(25.0),
                        total_physical_memory_bytes: MeasurementValue::Current(32_000_000_000),
                        available_memory_bytes: MeasurementValue::Current(20_000_000_000),
                        active_workers: 2,
                        active_executions: 1,
                        queue_depth: 0,
                        gpu_acceleration_status: GpuCapabilityStatus::default(),
                    },
                    100,
                )
                .unwrap();
        }
        assert_eq!(store.status_report().unwrap().snapshot_count, 100);
    }
}
