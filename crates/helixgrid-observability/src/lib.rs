use helixgrid_domain::{EventSeverity, SystemEvent, SystemMetricsSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use time::OffsetDateTime;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::writer::MakeWriter;

#[derive(Debug, Error)]
pub enum ObservabilityError {
    #[error("observability I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("tracing initialization failed: {0}")]
    TracingInit(String),
}

/// Bounded size-rotating file writer for structured JSON logs.
#[derive(Clone)]
pub struct BoundedRotatingWriter {
    inner: Arc<Mutex<RotatingState>>,
}

struct RotatingState {
    dir: PathBuf,
    prefix: String,
    max_file_bytes: u64,
    max_files: u32,
    current_size: u64,
    file: File,
}

impl BoundedRotatingWriter {
    pub fn new(
        dir: &Path,
        prefix: &str,
        max_file_bytes: u64,
        max_files: u32,
    ) -> Result<Self, ObservabilityError> {
        fs::create_dir_all(dir)?;
        let active_path = dir.join(format!("{prefix}.log"));
        let current_size = fs::metadata(&active_path).map(|m| m.len()).unwrap_or(0);
        let file = OpenOptions::new().create(true).append(true).open(&active_path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(RotatingState {
                dir: dir.to_path_buf(),
                prefix: prefix.to_string(),
                max_file_bytes: max_file_bytes.max(64 * 1024),
                max_files: max_files.max(1),
                current_size,
                file,
            })),
        })
    }

    fn rotate_locked(state: &mut RotatingState) -> io::Result<()> {
        state.file.flush()?;
        for idx in (1..state.max_files).rev() {
            let src = state.dir.join(format!("{}.{}.log", state.prefix, idx));
            let dst = state.dir.join(format!("{}.{}.log", state.prefix, idx + 1));
            if src.exists() {
                if dst.exists() {
                    let _ = fs::remove_file(&dst);
                }
                fs::rename(&src, &dst)?;
            }
        }
        let active = state.dir.join(format!("{}.log", state.prefix));
        let first_archive = state.dir.join(format!("{}.1.log", state.prefix));
        if first_archive.exists() {
            let _ = fs::remove_file(&first_archive);
        }
        if active.exists() {
            fs::rename(&active, &first_archive)?;
        }
        state.file = OpenOptions::new().create(true).write(true).truncate(true).open(&active)?;
        state.current_size = 0;
        Ok(())
    }
}

pub struct RotatingGuard {
    inner: Arc<Mutex<RotatingState>>,
}

impl Write for RotatingGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self.inner.lock().map_err(|_| io::Error::other("log mutex poisoned"))?;
        if guard.current_size + (buf.len() as u64) > guard.max_file_bytes {
            BoundedRotatingWriter::rotate_locked(&mut guard)?;
        }
        let n = guard.file.write(buf)?;
        guard.current_size += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self.inner.lock().map_err(|_| io::Error::other("log mutex poisoned"))?;
        guard.file.flush()
    }
}

impl<'a> MakeWriter<'a> for BoundedRotatingWriter {
    type Writer = RotatingGuard;

    fn make_writer(&'a self) -> Self::Writer {
        RotatingGuard { inner: Arc::clone(&self.inner) }
    }
}

/// Initializes structured JSON tracing into bounded local log files.
pub fn init_tracing(
    logs_dir: &Path,
    prefix: &str,
    max_file_bytes: u64,
    max_files: u32,
) -> Result<(), ObservabilityError> {
    let writer = BoundedRotatingWriter::new(logs_dir, prefix, max_file_bytes, max_files)?;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_writer(writer)
        .with_target(true)
        .with_current_span(false)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
    Ok(())
}

/// Thread-safe bounded ring buffer for recent system events and errors.
#[derive(Debug, Clone)]
pub struct EventRingBuffer {
    inner: Arc<Mutex<VecDeque<SystemEvent>>>,
    capacity: usize,
}

impl EventRingBuffer {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity: capacity.max(16),
        }
    }

    pub fn push(&self, event: SystemEvent) {
        if let Ok(mut guard) = self.inner.lock() {
            if guard.len() >= self.capacity {
                guard.pop_front();
            }
            guard.push_back(event);
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> Vec<SystemEvent> {
        self.inner.lock().map(|g| g.iter().rev().cloned().collect()).unwrap_or_default()
    }

    #[must_use]
    pub fn recent_errors(&self) -> Vec<SystemEvent> {
        self.inner
            .lock()
            .map(|g| {
                g.iter()
                    .rev()
                    .filter(|e| {
                        matches!(e.severity, EventSeverity::Error | EventSeverity::Critical)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Diagnostic bundle payload written on user diagnostic export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticBundle {
    pub generated_at_utc: i64,
    pub application_version: String,
    pub os_description: String,
    pub database_status: String,
    pub ipc_status: String,
    pub latest_metrics: Option<SystemMetricsSnapshot>,
    pub active_worker_count: usize,
    pub recent_errors: Vec<SystemEvent>,
    pub recent_events: Vec<SystemEvent>,
}

pub fn export_diagnostic_bundle(
    exports_dir: &Path,
    bundle: &DiagnosticBundle,
) -> Result<PathBuf, ObservabilityError> {
    fs::create_dir_all(exports_dir)?;
    let ts = OffsetDateTime::now_utc().unix_timestamp();
    let filename = format!("helixgrid-diagnostics-{ts}.json");
    let path = exports_dir.join(filename);
    let json = serde_json::to_string_pretty(bundle)?;
    fs::write(&path, json)?;
    Ok(path)
}

#[must_use]
pub fn now_utc_millis() -> i64 {
    let now = OffsetDateTime::now_utc();
    (now.unix_timestamp_nanos() / 1_000_000) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use helixgrid_domain::SystemEventKind;

    #[test]
    fn bounded_log_rotation_respects_size_and_count() {
        let tmp = tempfile::tempdir().unwrap();
        let writer = BoundedRotatingWriter::new(tmp.path(), "test", 64 * 1024, 2).unwrap();
        let payload = vec![b'A'; 40_000];
        for _ in 0..5 {
            let mut g = writer.make_writer();
            g.write_all(&payload).unwrap();
            g.write_all(b"\n").unwrap();
        }
        assert!(tmp.path().join("test.log").exists());
        assert!(tmp.path().join("test.1.log").exists());
        assert!(!tmp.path().join("test.3.log").exists());
    }

    #[test]
    fn event_ring_buffer_filters_errors() {
        let buf = EventRingBuffer::new(32);
        buf.push(SystemEvent::new(SystemEventKind::EngineStarted, EventSeverity::Info, "ok", 10));
        buf.push(SystemEvent::new(
            SystemEventKind::WorkerCrashed,
            EventSeverity::Critical,
            "boom",
            20,
        ));
        assert_eq!(buf.snapshot().len(), 2);
        assert_eq!(buf.recent_errors().len(), 1);
    }
}
