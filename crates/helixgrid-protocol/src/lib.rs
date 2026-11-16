use helixgrid_domain::{
    EventSeverity, ExecutionId, ExecutionOutcome, ProgressState, ResourceRequirements, WorkerId,
    WorkloadId, WorkloadResultMetadata, WorkloadSpec,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Current IPC wire protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Maximum bounded IPC frame size (4 MiB). Prevents unbounded memory allocation on malformed peers.
pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

/// Maximum number of queued inbound/outbound IPC messages per worker channel.
pub const MAX_IPC_CHANNEL_CAPACITY: usize = 256;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("unsupported protocol version: expected {expected}, got {actual}")]
    UnsupportedVersion { expected: u32, actual: u32 },

    #[error("frame exceeds bounded limit of {max} bytes (got {actual} bytes)")]
    FrameTooLarge { max: usize, actual: usize },

    #[error("invalid protocol payload: {0}")]
    Validation(String),

    #[error("serialization failure: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("I/O failure: {0}")]
    Io(#[from] std::io::Error),
}

/// Top-level versioned envelope transmitted over local Windows Named Pipes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IpcEnvelope {
    pub protocol_version: u32,
    pub sequence_id: u64,
    pub sent_at_utc: i64,
    pub payload: IpcMessage,
}

impl IpcEnvelope {
    #[must_use]
    pub fn new(sequence_id: u64, sent_at_utc: i64, payload: IpcMessage) -> Self {
        Self { protocol_version: PROTOCOL_VERSION, sequence_id, sent_at_utc, payload }
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion {
                expected: PROTOCOL_VERSION,
                actual: self.protocol_version,
            });
        }
        self.payload.validate()
    }
}

/// Complete set of versioned local coordinator <-> worker IPC messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IpcMessage {
    WorkerRegistration(WorkerRegistrationMsg),
    WorkerReady(WorkerReadyMsg),
    Heartbeat(HeartbeatMsg),
    ExecutionRequest(ExecutionRequestMsg),
    ExecutionAccepted(ExecutionAcceptedMsg),
    ExecutionStarted(ExecutionStartedMsg),
    ExecutionProgress(ExecutionProgressMsg),
    ExecutionCompleted(ExecutionCompletedMsg),
    ExecutionFailed(ExecutionFailedMsg),
    ExecutionCancelled(ExecutionCancelledMsg),
    WorkerShutdown(WorkerShutdownMsg),
    DiagnosticEvent(DiagnosticEventMsg),
    /// Coordinator control signal asking a worker to abort an active execution.
    CancelExecutionCommand(CancelExecutionCommandMsg),
}

impl IpcMessage {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::WorkerRegistration(m) => {
                if m.auth_token.len() < 16 || m.auth_token.len() > 256 {
                    return Err(ProtocolError::Validation(
                        "auth_token length must be 16..=256".to_string(),
                    ));
                }
                if m.max_cpu_cores == 0 || m.max_memory_mb < 16 {
                    return Err(ProtocolError::Validation(
                        "worker capacity must be >= 1 core and >= 16 MB".to_string(),
                    ));
                }
            }
            Self::WorkerReady(_) => {}
            Self::Heartbeat(m) => {
                if let Some(pct) = m.cpu_usage_pct
                    && (!pct.is_finite() || !(0.0..=100.0).contains(&pct))
                {
                    return Err(ProtocolError::Validation(
                        "heartbeat cpu_usage_pct must be finite in 0..=100".to_string(),
                    ));
                }
            }
            Self::ExecutionRequest(m) => {
                m.spec.validate().map_err(|e| ProtocolError::Validation(e.to_string()))?;
                m.resources.validate().map_err(|e| ProtocolError::Validation(e.to_string()))?;
                if m.timeout_ms < 100 {
                    return Err(ProtocolError::Validation(
                        "execution timeout_ms must be >= 100".to_string(),
                    ));
                }
                if m.artifact_dir.trim().is_empty() {
                    return Err(ProtocolError::Validation(
                        "artifact_dir cannot be empty".to_string(),
                    ));
                }
            }
            Self::ExecutionAccepted(_) | Self::ExecutionStarted(_) => {}
            Self::ExecutionProgress(m) => {
                if let ProgressState::Determinate { percent, .. } = m.progress
                    && (!percent.is_finite() || !(0.0..=100.0).contains(&percent))
                {
                    return Err(ProtocolError::Validation(
                        "progress percent must be finite in 0..=100".to_string(),
                    ));
                }
            }
            Self::ExecutionCompleted(m) => {
                if m.result.summary.len() > 16_384 {
                    return Err(ProtocolError::Validation(
                        "result summary exceeds 16 KiB limit".to_string(),
                    ));
                }
            }
            Self::ExecutionFailed(m) => {
                if m.reason.trim().is_empty() {
                    return Err(ProtocolError::Validation(
                        "failure reason cannot be empty".to_string(),
                    ));
                }
            }
            Self::ExecutionCancelled(_)
            | Self::WorkerShutdown(_)
            | Self::CancelExecutionCommand(_) => {}
            Self::DiagnosticEvent(m) => {
                if m.message.len() > 16_384 {
                    return Err(ProtocolError::Validation(
                        "diagnostic message exceeds 16 KiB limit".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRegistrationMsg {
    pub worker_id: WorkerId,
    pub pid: u32,
    pub auth_token: String,
    pub worker_version: String,
    pub max_cpu_cores: u32,
    pub max_memory_mb: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerReadyMsg {
    pub worker_id: WorkerId,
    pub logical_processors: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeartbeatMsg {
    pub worker_id: WorkerId,
    pub active_execution_id: Option<ExecutionId>,
    pub cpu_usage_pct: Option<f64>,
    pub memory_usage_bytes: Option<u64>,
    pub uptime_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRequestMsg {
    pub workload_id: WorkloadId,
    pub execution_id: ExecutionId,
    pub attempt_number: u32,
    pub spec: WorkloadSpec,
    pub resources: ResourceRequirements,
    pub timeout_ms: u64,
    pub artifact_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionAcceptedMsg {
    pub workload_id: WorkloadId,
    pub execution_id: ExecutionId,
    pub worker_id: WorkerId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStartedMsg {
    pub workload_id: WorkloadId,
    pub execution_id: ExecutionId,
    pub worker_id: WorkerId,
    pub started_at_utc: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionProgressMsg {
    pub workload_id: WorkloadId,
    pub execution_id: ExecutionId,
    pub progress: ProgressState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactReport {
    pub name: String,
    pub kind: String,
    pub file_path: String,
    pub size_bytes: u64,
    pub sha256_hex: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionCompletedMsg {
    pub workload_id: WorkloadId,
    pub execution_id: ExecutionId,
    pub worker_id: WorkerId,
    pub result: WorkloadResultMetadata,
    pub artifacts: Vec<ArtifactReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionFailedMsg {
    pub workload_id: WorkloadId,
    pub execution_id: ExecutionId,
    pub worker_id: WorkerId,
    pub outcome: ExecutionOutcome,
    pub reason: String,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub artifacts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionCancelledMsg {
    pub workload_id: WorkloadId,
    pub execution_id: ExecutionId,
    pub worker_id: WorkerId,
    pub reason: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelExecutionCommandMsg {
    pub workload_id: WorkloadId,
    pub execution_id: ExecutionId,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerShutdownMsg {
    pub reason: String,
    pub graceful_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticEventMsg {
    pub worker_id: WorkerId,
    pub workload_id: Option<WorkloadId>,
    pub severity: EventSeverity,
    pub message: String,
}

/// Encodes an envelope as a length-prefixed bounded JSON frame `[u32 LE len][json bytes]`.
pub fn encode_frame(envelope: &IpcEnvelope) -> Result<Vec<u8>, ProtocolError> {
    envelope.validate()?;
    let payload = serde_json::to_vec(envelope)?;
    if payload.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge { max: MAX_FRAME_BYTES, actual: payload.len() });
    }
    let len = u32::try_from(payload.len()).map_err(|_| ProtocolError::FrameTooLarge {
        max: MAX_FRAME_BYTES,
        actual: payload.len(),
    })?;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decodes a length-prefixed bounded JSON frame from raw bytes.
pub fn decode_frame_payload(payload: &[u8]) -> Result<IpcEnvelope, ProtocolError> {
    if payload.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge { max: MAX_FRAME_BYTES, actual: payload.len() });
    }
    let env: IpcEnvelope = serde_json::from_slice(payload)?;
    env.validate()?;
    Ok(env)
}

/// Writes a bounded frame asynchronously to any Tokio `AsyncWrite`.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    envelope: &IpcEnvelope,
) -> Result<(), ProtocolError> {
    let bytes = encode_frame(envelope)?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads and validates a bounded frame asynchronously from any Tokio `AsyncRead`.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<IpcEnvelope, ProtocolError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len == 0 {
        return Err(ProtocolError::Validation("zero-length IPC frame is invalid".to_string()));
    }
    if len > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge { max: MAX_FRAME_BYTES, actual: len });
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    decode_frame_payload(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use helixgrid_domain::CpuBenchmarkInput;
    use proptest::prelude::*;

    #[test]
    fn rejects_oversized_frames_and_version_mismatch() {
        let mut bad_env = IpcEnvelope::new(
            1,
            1000,
            IpcMessage::WorkerReady(WorkerReadyMsg {
                worker_id: WorkerId::new(),
                logical_processors: 12,
            }),
        );
        bad_env.protocol_version = 999;
        assert!(matches!(
            encode_frame(&bad_env),
            Err(ProtocolError::UnsupportedVersion { expected: 1, actual: 999 })
        ));

        let oversized = vec![b' '; MAX_FRAME_BYTES + 1];
        assert!(matches!(
            decode_frame_payload(&oversized),
            Err(ProtocolError::FrameTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn async_frame_roundtrip_over_duplex() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let msg = IpcEnvelope::new(
            42,
            1_700_000_000,
            IpcMessage::ExecutionRequest(ExecutionRequestMsg {
                workload_id: WorkloadId::new(),
                execution_id: ExecutionId::new(),
                attempt_number: 1,
                spec: WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                    worker_threads: 2,
                    iterations: 500,
                    sieve_limit: 5000,
                }),
                resources: ResourceRequirements::default(),
                timeout_ms: 10_000,
                artifact_dir: "C:\\Temp\\artifacts".to_string(),
            }),
        );
        write_frame(&mut client, &msg).await.unwrap();
        let decoded = read_frame(&mut server).await.unwrap();
        assert_eq!(msg, decoded);
    }

    proptest! {
        #[test]
        fn proptest_heartbeat_roundtrip(
            seq in any::<u64>(),
            ts in any::<i64>(),
            cpu in 0.0f64..=100.0f64,
            mem in any::<u64>(),
            uptime in any::<u64>()
        ) {
            let env = IpcEnvelope::new(
                seq,
                ts,
                IpcMessage::Heartbeat(HeartbeatMsg {
                    worker_id: WorkerId::new(),
                    active_execution_id: None,
                    cpu_usage_pct: Some(cpu),
                    memory_usage_bytes: Some(mem),
                    uptime_ms: uptime,
                }),
            );
            let encoded = encode_frame(&env).unwrap();
            let decoded = decode_frame_payload(&encoded[4..]).unwrap();
            prop_assert_eq!(env.sequence_id, decoded.sequence_id);
        }
    }
}
