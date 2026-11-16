use helixgrid_config::{AppConfig, AppPaths};
use helixgrid_domain::{
    EventSeverity, ExecutionId, MeasurementValue, WorkerId, WorkerInfo, WorkerStatus, WorkloadId,
};
use helixgrid_observability::now_utc_millis;
use helixgrid_platform_windows::job::WindowsJobObject;
use helixgrid_platform_windows::pipe::create_secure_pipe_server;
use helixgrid_platform_windows::process::WindowsWorkerProcess;
use helixgrid_protocol::{
    CancelExecutionCommandMsg, ExecutionRequestMsg, IpcEnvelope, IpcMessage,
    MAX_IPC_CHANNEL_CAPACITY, PROTOCOL_VERSION, WorkerShutdownMsg, read_frame, write_frame,
};
use helixgrid_security::{constant_time_eq, generate_ipc_token};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("worker executable not found at '{0}'")]
    WorkerBinaryNotFound(String),

    #[error("worker {0} not found or not connected")]
    WorkerUnavailable(WorkerId),

    #[error("IPC dispatch queue full or closed for worker {0}")]
    IpcSendFailed(WorkerId),

    #[error("platform error: {0}")]
    Platform(#[from] helixgrid_platform_windows::PlatformError),
}

#[derive(Debug, Clone)]
pub enum ExecutorEvent {
    WorkerConnected {
        worker_id: WorkerId,
        pid: u32,
    },
    WorkerReady {
        worker_id: WorkerId,
        logical_processors: u32,
    },
    WorkerHeartbeat {
        worker_id: WorkerId,
        active_execution_id: Option<ExecutionId>,
        cpu_usage_pct: Option<f64>,
        memory_usage_bytes: Option<u64>,
    },
    WorkerIpcAuthRejected {
        worker_id: WorkerId,
        reason: String,
    },
    WorkerDisconnected {
        worker_id: WorkerId,
        reason: String,
    },
    IpcMessageReceived {
        worker_id: WorkerId,
        message: IpcMessage,
    },
}

enum WorkerCommand {
    Send(IpcMessage),
    Shutdown(WorkerShutdownMsg),
}

struct ManagedWorkerHandle {
    info: WorkerInfo,
    process: WindowsWorkerProcess,
    job: WindowsJobObject,
    cmd_tx: mpsc::Sender<WorkerCommand>,
    pipe_name: String,
}

/// Manages real isolated Windows worker processes, Job Objects, and local Named Pipe IPC sessions.
pub struct WorkerPoolSupervisor {
    worker_exe_path: PathBuf,
    paths: AppPaths,
    workers: HashMap<WorkerId, ManagedWorkerHandle>,
    event_tx: mpsc::Sender<ExecutorEvent>,
    next_worker_ordinal: u32,
}

impl WorkerPoolSupervisor {
    pub fn new(
        worker_exe_path: PathBuf,
        paths: AppPaths,
        event_tx: mpsc::Sender<ExecutorEvent>,
    ) -> Self {
        Self { worker_exe_path, paths, workers: HashMap::new(), event_tx, next_worker_ordinal: 1 }
    }

    /// Resolves `helixgrid-worker.exe` adjacent to `helixgrid.exe` (or cargo target dir in dev/test).
    pub fn resolve_worker_executable() -> Result<PathBuf, ExecutorError> {
        if let Ok(explicit) = std::env::var("HELIXGRID_WORKER_EXE") {
            let p = PathBuf::from(&explicit);
            if p.is_file() {
                return Ok(p);
            }
        }
        if let Ok(exe) = std::env::current_exe()
            && let Some(dir) = exe.parent()
        {
            let sibling = dir.join("helixgrid-worker.exe");
            if sibling.is_file() {
                return Ok(sibling);
            }
            // In `cargo test`, test binaries live in `target/debug/deps/`
            if let Some(parent) = dir.parent() {
                let target_bin = parent.join("helixgrid-worker.exe");
                if target_bin.is_file() {
                    return Ok(target_bin);
                }
            }
        }
        let default_guess = PathBuf::from("helixgrid-worker.exe");
        Err(ExecutorError::WorkerBinaryNotFound(default_guess.to_string_lossy().into_owned()))
    }

    pub fn spawn_worker(
        &mut self,
        config: &AppConfig,
        logical_processors: u32,
        per_worker_memory_mb: u64,
    ) -> Result<WorkerInfo, ExecutorError> {
        if !self.worker_exe_path.is_file() {
            return Err(ExecutorError::WorkerBinaryNotFound(
                self.worker_exe_path.to_string_lossy().into_owned(),
            ));
        }

        let worker_id = WorkerId::new();
        let ordinal = self.next_worker_ordinal;
        self.next_worker_ordinal = self.next_worker_ordinal.saturating_add(1);
        let worker_name = format!("worker-{ordinal:02}");

        let pipe_name = format!(r"\\.\pipe\helixgrid-{}-{}", std::process::id(), worker_id);
        let auth_token = generate_ipc_token();

        let job = WindowsJobObject::create(
            Some(per_worker_memory_mb.saturating_mul(1024 * 1024)),
            Some(config.cpu_utilization_ceiling_pct.round() as u32),
            Some(16),
        )?;
        let isolation_note = format!("{} | PipeDACL=CurrentUser", job.limits_summary());

        let pipe_server = create_secure_pipe_server(&pipe_name, true)?;
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<WorkerCommand>(MAX_IPC_CHANNEL_CAPACITY);

        let args = vec![
            "--worker-id".to_string(),
            worker_id.to_string(),
            "--pipe-name".to_string(),
            pipe_name.clone(),
            "--auth-token".to_string(),
            auth_token.clone(),
            "--logs-dir".to_string(),
            self.paths.logs_dir.to_string_lossy().into_owned(),
            "--config-path".to_string(),
            self.paths.config_file.to_string_lossy().into_owned(),
            "--max-cpu-cores".to_string(),
            logical_processors.max(1).to_string(),
            "--max-memory-mb".to_string(),
            per_worker_memory_mb.to_string(),
            "--heartbeat-interval-ms".to_string(),
            config.heartbeat_interval_ms.to_string(),
        ];

        let process = WindowsWorkerProcess::spawn_in_job(
            &self.worker_exe_path,
            &args,
            &self.paths.root_dir,
            None,
            &job,
        )?;
        let pid = process.pid();
        let now_utc = now_utc_millis();

        let info = WorkerInfo {
            id: worker_id,
            name: worker_name,
            pid: Some(pid),
            status: WorkerStatus::Connecting,
            protocol_version: PROTOCOL_VERSION,
            max_cpu_cores: logical_processors.max(1),
            max_memory_mb: per_worker_memory_mb,
            reserved_cpu_cores: 0,
            reserved_memory_mb: 0,
            current_workload_id: None,
            current_execution_id: None,
            cpu_usage_pct: MeasurementValue::Unavailable {
                reason: "Awaiting worker telemetry".to_string(),
            },
            memory_usage_bytes: MeasurementValue::Unavailable {
                reason: "Awaiting worker telemetry".to_string(),
            },
            started_at_utc: now_utc,
            last_heartbeat_utc: None,
            completed_executions: 0,
            failed_executions: 0,
            isolation_note,
        };

        let event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            let connect_res =
                tokio::time::timeout(Duration::from_secs(8), pipe_server.connect()).await;
            match connect_res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let _ = event_tx
                        .send(ExecutorEvent::WorkerDisconnected {
                            worker_id,
                            reason: format!("Named pipe accept failed: {e}"),
                        })
                        .await;
                    return;
                }
                Err(_) => {
                    let _ = event_tx
                        .send(ExecutorEvent::WorkerDisconnected {
                            worker_id,
                            reason: "Worker failed to connect within 8s handshake timeout"
                                .to_string(),
                        })
                        .await;
                    return;
                }
            }

            let (mut reader, mut writer) = tokio::io::split(pipe_server);
            // Authenticate first frame as WorkerRegistration
            let first_frame =
                match tokio::time::timeout(Duration::from_secs(5), read_frame(&mut reader)).await {
                    Ok(Ok(env)) => env,
                    Ok(Err(e)) => {
                        let _ = event_tx
                            .send(ExecutorEvent::WorkerDisconnected {
                                worker_id,
                                reason: format!("Handshake read error: {e}"),
                            })
                            .await;
                        return;
                    }
                    Err(_) => {
                        let _ = event_tx
                            .send(ExecutorEvent::WorkerDisconnected {
                                worker_id,
                                reason: "Worker registration frame timeout".to_string(),
                            })
                            .await;
                        return;
                    }
                };

            match first_frame.payload {
                IpcMessage::WorkerRegistration(reg) => {
                    if reg.worker_id != worker_id || !constant_time_eq(&reg.auth_token, &auth_token)
                    {
                        let _ = event_tx
                            .send(ExecutorEvent::WorkerIpcAuthRejected {
                                worker_id,
                                reason: "Worker registration token/id mismatch".to_string(),
                            })
                            .await;
                        return;
                    }
                    let _ = event_tx
                        .send(ExecutorEvent::WorkerConnected { worker_id, pid: reg.pid })
                        .await;
                }
                other => {
                    let _ = event_tx
                        .send(ExecutorEvent::WorkerIpcAuthRejected {
                            worker_id,
                            reason: format!("Expected WorkerRegistration, got {other:?}"),
                        })
                        .await;
                    return;
                }
            }

            let seq = Arc::new(AtomicU64::new(1));
            let writer_task = tokio::spawn(async move {
                while let Some(cmd) = cmd_rx.recv().await {
                    let msg = match cmd {
                        WorkerCommand::Send(m) => m,
                        WorkerCommand::Shutdown(s) => IpcMessage::WorkerShutdown(s),
                    };
                    let is_shutdown = matches!(msg, IpcMessage::WorkerShutdown(_));
                    let env = IpcEnvelope::new(
                        seq.fetch_add(1, Ordering::Relaxed),
                        now_utc_millis(),
                        msg,
                    );
                    if let Err(err) = write_frame(&mut writer, &env).await {
                        warn!("coordinator pipe write failed for {worker_id}: {err}");
                        break;
                    }
                    if is_shutdown {
                        break;
                    }
                }
            });

            loop {
                match read_frame(&mut reader).await {
                    Ok(env) => match env.payload {
                        IpcMessage::WorkerReady(r) => {
                            let _ = event_tx
                                .send(ExecutorEvent::WorkerReady {
                                    worker_id,
                                    logical_processors: r.logical_processors,
                                })
                                .await;
                        }
                        IpcMessage::Heartbeat(hb) => {
                            let _ = event_tx
                                .send(ExecutorEvent::WorkerHeartbeat {
                                    worker_id,
                                    active_execution_id: hb.active_execution_id,
                                    cpu_usage_pct: hb.cpu_usage_pct,
                                    memory_usage_bytes: hb.memory_usage_bytes,
                                })
                                .await;
                        }
                        other => {
                            let _ = event_tx
                                .send(ExecutorEvent::IpcMessageReceived {
                                    worker_id,
                                    message: other,
                                })
                                .await;
                        }
                    },
                    Err(err) => {
                        let _ = event_tx
                            .send(ExecutorEvent::WorkerDisconnected {
                                worker_id,
                                reason: err.to_string(),
                            })
                            .await;
                        break;
                    }
                }
            }

            writer_task.abort();
        });

        let ret = info.clone();
        self.workers
            .insert(worker_id, ManagedWorkerHandle { info, process, job, cmd_tx, pipe_name });
        info!("Spawned isolated worker {} (pid={pid})", ret.name);
        Ok(ret)
    }

    pub async fn dispatch_execution(
        &self,
        worker_id: WorkerId,
        req: ExecutionRequestMsg,
    ) -> Result<(), ExecutorError> {
        let handle =
            self.workers.get(&worker_id).ok_or(ExecutorError::WorkerUnavailable(worker_id))?;
        handle
            .cmd_tx
            .send(WorkerCommand::Send(IpcMessage::ExecutionRequest(req)))
            .await
            .map_err(|_| ExecutorError::IpcSendFailed(worker_id))
    }

    pub async fn cancel_execution(
        &self,
        worker_id: WorkerId,
        workload_id: WorkloadId,
        execution_id: ExecutionId,
        reason: &str,
    ) -> Result<(), ExecutorError> {
        let handle =
            self.workers.get(&worker_id).ok_or(ExecutorError::WorkerUnavailable(worker_id))?;
        handle
            .cmd_tx
            .send(WorkerCommand::Send(IpcMessage::CancelExecutionCommand(
                CancelExecutionCommandMsg { workload_id, execution_id, reason: reason.to_string() },
            )))
            .await
            .map_err(|_| ExecutorError::IpcSendFailed(worker_id))
    }

    pub async fn graceful_shutdown_worker(
        &mut self,
        worker_id: WorkerId,
        reason: &str,
    ) -> Result<(), ExecutorError> {
        if let Some(handle) = self.workers.get_mut(&worker_id) {
            handle.info.status = WorkerStatus::Draining;
            let _ = handle
                .cmd_tx
                .send(WorkerCommand::Shutdown(WorkerShutdownMsg {
                    reason: reason.to_string(),
                    graceful_timeout_ms: 1500,
                }))
                .await;
            for _ in 0..20 {
                if let Ok(Some(_)) = handle.process.try_exit_code() {
                    handle.info.status = WorkerStatus::Stopped;
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let _ = handle.job.terminate(0);
            handle.info.status = WorkerStatus::Stopped;
            Ok(())
        } else {
            Err(ExecutorError::WorkerUnavailable(worker_id))
        }
    }

    /// Force-terminates a worker via its Windows Job Object (used for fault injection & emergency kill).
    pub fn force_terminate_worker(&mut self, worker_id: WorkerId) -> Result<(), ExecutorError> {
        let handle =
            self.workers.get_mut(&worker_id).ok_or(ExecutorError::WorkerUnavailable(worker_id))?;
        let _ = handle.job.terminate(0xC000_0001);
        handle.info.status = WorkerStatus::Crashed;
        Ok(())
    }

    /// Checks OS process handles directly for unexpected termination.
    pub fn poll_exited_workers(&mut self) -> Vec<(WorkerId, i32)> {
        let mut exited = Vec::new();
        for (id, handle) in &mut self.workers {
            if handle.info.status.is_alive()
                && let Ok(Some(code)) = handle.process.try_exit_code()
            {
                if handle.info.status != WorkerStatus::Draining {
                    handle.info.status = WorkerStatus::Crashed;
                } else {
                    handle.info.status = WorkerStatus::Stopped;
                }
                exited.push((*id, code));
            }
        }
        exited
    }

    #[must_use]
    pub fn pipe_name(&self, worker_id: WorkerId) -> Option<&str> {
        self.workers.get(&worker_id).map(|h| h.pipe_name.as_str())
    }

    #[must_use]
    pub fn worker_infos(&self) -> Vec<WorkerInfo> {
        self.workers.values().map(|h| h.info.clone()).collect()
    }

    #[must_use]
    pub fn worker_info_mut(&mut self, id: WorkerId) -> Option<&mut WorkerInfo> {
        self.workers.get_mut(&id).map(|h| &mut h.info)
    }

    pub fn update_worker_infos_snapshot(&mut self, updated: &[WorkerInfo]) {
        for u in updated {
            if let Some(h) = self.workers.get_mut(&u.id) {
                h.info = u.clone();
            }
        }
    }

    pub fn remove_worker(&mut self, id: WorkerId) {
        self.workers.remove(&id);
    }

    pub async fn shutdown_all(&mut self) {
        let ids: Vec<WorkerId> = self.workers.keys().copied().collect();
        for id in ids {
            let _ = self.graceful_shutdown_worker(id, "Coordinator shutting down").await;
        }
        self.workers.clear();
    }
}

pub fn severity_from_ipc_diagnostic(sev: EventSeverity) -> EventSeverity {
    sev
}
