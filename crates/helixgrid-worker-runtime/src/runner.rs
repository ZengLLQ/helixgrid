use crate::compute::{ComputeError, execute_spec_blocking};
use helixgrid_config::AppConfig;
use helixgrid_domain::{ExecutionId, ExecutionOutcome, ProgressState, WorkerId, WorkloadId};
use helixgrid_observability::now_utc_millis;
use helixgrid_platform_windows::telemetry::WindowsSystemTelemetry;
use helixgrid_protocol::{
    ExecutionAcceptedMsg, ExecutionCancelledMsg, ExecutionCompletedMsg, ExecutionFailedMsg,
    ExecutionProgressMsg, ExecutionStartedMsg, HeartbeatMsg, IpcEnvelope, IpcMessage,
    MAX_IPC_CHANNEL_CAPACITY, PROTOCOL_VERSION, WorkerReadyMsg, WorkerRegistrationMsg, read_frame,
    write_frame,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::net::windows::named_pipe::ClientOptions;
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info, warn};

#[derive(Debug, Error)]
pub enum WorkerRuntimeError {
    #[error("IPC transport error: {0}")]
    Ipc(#[from] helixgrid_protocol::ProtocolError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct WorkerRuntimeConfig {
    pub worker_id: WorkerId,
    pub pipe_name: String,
    pub auth_token: String,
    pub config_path: Option<PathBuf>,
    pub max_cpu_cores: u32,
    pub max_memory_mb: u64,
    pub heartbeat_interval_ms: u64,
}

/// Runs the standalone worker event loop over the local Windows Named Pipe.
pub async fn run_worker_runtime(cfg: WorkerRuntimeConfig) -> Result<(), WorkerRuntimeError> {
    let started_instant = Instant::now();
    let seq = Arc::new(AtomicU64::new(1));
    let app_config: Option<AppConfig> =
        cfg.config_path.as_ref().and_then(|p| AppConfig::load_or_create(p).ok());

    let client = connect_pipe_with_retry(&cfg.pipe_name).await?;
    let (mut reader, mut writer) = tokio::io::split(client);

    let (tx, mut rx) = mpsc::channel::<IpcMessage>(MAX_IPC_CHANNEL_CAPACITY);
    type ActiveExecSlot = Arc<Mutex<Option<(ExecutionId, Arc<AtomicBool>)>>>;
    let active_exec_id: ActiveExecSlot = Arc::new(Mutex::new(None));

    // Writer task serializes all outbound IPC messages cleanly on bounded channel.
    let seq_w = Arc::clone(&seq);
    let writer_handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let s = seq_w.fetch_add(1, Ordering::Relaxed);
            let env = IpcEnvelope::new(s, now_utc_millis(), msg);
            if let Err(err) = write_frame(&mut writer, &env).await {
                error!("worker pipe write failed: {err}");
                break;
            }
        }
    });

    let mut telemetry = WindowsSystemTelemetry::new();
    let logical_processors = telemetry.logical_processors();

    tx.send(IpcMessage::WorkerRegistration(WorkerRegistrationMsg {
        worker_id: cfg.worker_id,
        pid: std::process::id(),
        auth_token: cfg.auth_token.clone(),
        worker_version: env!("CARGO_PKG_VERSION").to_string(),
        max_cpu_cores: cfg.max_cpu_cores.max(1),
        max_memory_mb: cfg.max_memory_mb.max(16),
    }))
    .await
    .ok();

    tx.send(IpcMessage::WorkerReady(WorkerReadyMsg {
        worker_id: cfg.worker_id,
        logical_processors,
    }))
    .await
    .ok();

    // Heartbeat loop reporting real OS process CPU and memory metrics.
    let hb_tx = tx.clone();
    let hb_active = Arc::clone(&active_exec_id);
    let hb_worker_id = cfg.worker_id;
    let hb_interval = cfg.heartbeat_interval_ms.max(250);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let hb_shutdown = Arc::clone(&shutdown_flag);

    let hb_handle = tokio::spawn(async move {
        let pid = std::process::id();
        let mut ticker = tokio::time::interval(Duration::from_millis(hb_interval));
        loop {
            ticker.tick().await;
            if hb_shutdown.load(Ordering::Relaxed) {
                break;
            }
            let active_id = hb_active.lock().await.as_ref().map(|(id, _)| *id);
            let (cpu_val, mem_val) = telemetry.sample_process_metrics(pid);
            let msg = IpcMessage::Heartbeat(HeartbeatMsg {
                worker_id: hb_worker_id,
                active_execution_id: active_id,
                cpu_usage_pct: cpu_val.value().copied(),
                memory_usage_bytes: mem_val.value().copied(),
                uptime_ms: started_instant.elapsed().as_millis() as u64,
            });
            if hb_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Inbound command loop from coordinator.
    loop {
        let env = match read_frame(&mut reader).await {
            Ok(e) => e,
            Err(err) => {
                warn!("worker pipe read terminated: {err}");
                break;
            }
        };
        if env.protocol_version != PROTOCOL_VERSION {
            warn!("rejected mismatched protocol version {}", env.protocol_version);
            continue;
        }

        match env.payload {
            IpcMessage::ExecutionRequest(req) => {
                let cancel_flag = Arc::new(AtomicBool::new(false));
                {
                    let mut guard = active_exec_id.lock().await;
                    if guard.is_some() {
                        let _ = tx
                            .send(IpcMessage::ExecutionFailed(ExecutionFailedMsg {
                                workload_id: req.workload_id,
                                execution_id: req.execution_id,
                                worker_id: cfg.worker_id,
                                outcome: ExecutionOutcome::Failed,
                                reason: "worker already executing another workload".to_string(),
                                exit_code: None,
                                duration_ms: 0,
                                artifacts: vec![],
                            }))
                            .await;
                        continue;
                    }
                    *guard = Some((req.execution_id, Arc::clone(&cancel_flag)));
                }

                let _ = tx
                    .send(IpcMessage::ExecutionAccepted(ExecutionAcceptedMsg {
                        workload_id: req.workload_id,
                        execution_id: req.execution_id,
                        worker_id: cfg.worker_id,
                    }))
                    .await;

                let _ = tx
                    .send(IpcMessage::ExecutionStarted(ExecutionStartedMsg {
                        workload_id: req.workload_id,
                        execution_id: req.execution_id,
                        worker_id: cfg.worker_id,
                        started_at_utc: now_utc_millis(),
                    }))
                    .await;

                let exec_tx = tx.clone();
                let exec_active = Arc::clone(&active_exec_id);
                let worker_id = cfg.worker_id;
                let cfg_clone = app_config.clone();

                tokio::spawn(async move {
                    let wl_id: WorkloadId = req.workload_id;
                    let ex_id: ExecutionId = req.execution_id;
                    let timeout_ms = req.timeout_ms;
                    let artifact_dir = PathBuf::from(&req.artifact_dir);
                    let spec = req.spec;
                    let start = Instant::now();
                    let prog_tx = exec_tx.clone();
                    let cancel_inner = Arc::clone(&cancel_flag);

                    let compute_res = tokio::task::spawn_blocking(move || {
                        execute_spec_blocking(
                            &spec,
                            Path::new(&artifact_dir),
                            timeout_ms,
                            cfg_clone.as_ref(),
                            &cancel_inner,
                            |progress: ProgressState| {
                                let _ = prog_tx.try_send(IpcMessage::ExecutionProgress(
                                    ExecutionProgressMsg {
                                        workload_id: wl_id,
                                        execution_id: ex_id,
                                        progress,
                                    },
                                ));
                            },
                        )
                    })
                    .await;

                    {
                        let mut guard = exec_active.lock().await;
                        if guard.as_ref().map(|(id, _)| *id) == Some(ex_id) {
                            *guard = None;
                        }
                    }

                    let elapsed_ms = start.elapsed().as_millis() as u64;
                    let msg = match compute_res {
                        Ok(Ok(outcome)) => IpcMessage::ExecutionCompleted(ExecutionCompletedMsg {
                            workload_id: wl_id,
                            execution_id: ex_id,
                            worker_id,
                            result: outcome.result,
                            artifacts: outcome.artifacts,
                        }),
                        Ok(Err(ComputeError::Cancelled)) => {
                            IpcMessage::ExecutionCancelled(ExecutionCancelledMsg {
                                workload_id: wl_id,
                                execution_id: ex_id,
                                worker_id,
                                reason: "Cancelled by coordinator".to_string(),
                                duration_ms: elapsed_ms,
                            })
                        }
                        Ok(Err(err)) => IpcMessage::ExecutionFailed(ExecutionFailedMsg {
                            workload_id: wl_id,
                            execution_id: ex_id,
                            worker_id,
                            outcome: ExecutionOutcome::Failed,
                            reason: err.to_string(),
                            exit_code: None,
                            duration_ms: elapsed_ms,
                            artifacts: vec![],
                        }),
                        Err(join_err) => IpcMessage::ExecutionFailed(ExecutionFailedMsg {
                            workload_id: wl_id,
                            execution_id: ex_id,
                            worker_id,
                            outcome: ExecutionOutcome::Failed,
                            reason: format!("worker execution panicked: {join_err}"),
                            exit_code: None,
                            duration_ms: elapsed_ms,
                            artifacts: vec![],
                        }),
                    };
                    let _ = exec_tx.send(msg).await;
                });
            }
            IpcMessage::CancelExecutionCommand(cmd) => {
                let guard = active_exec_id.lock().await;
                if let Some((active_id, ref flag)) = *guard
                    && active_id == cmd.execution_id
                {
                    flag.store(true, Ordering::Relaxed);
                }
            }
            IpcMessage::WorkerShutdown(sh) => {
                info!("worker received shutdown: {}", sh.reason);
                if let Some((_, ref flag)) = *active_exec_id.lock().await {
                    flag.store(true, Ordering::Relaxed);
                }
                break;
            }
            _ => {}
        }
    }

    shutdown_flag.store(true, Ordering::Relaxed);
    drop(tx);
    let _ = hb_handle.await;
    let _ = writer_handle.await;
    Ok(())
}

async fn connect_pipe_with_retry(
    pipe_name: &str,
) -> Result<tokio::net::windows::named_pipe::NamedPipeClient, WorkerRuntimeError> {
    let mut attempts = 0u32;
    loop {
        match ClientOptions::new().open(pipe_name) {
            Ok(client) => return Ok(client),
            Err(err) => {
                attempts += 1;
                if attempts >= 40 {
                    return Err(WorkerRuntimeError::Io(err));
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
}
