use directories::ProjectDirs;
use helixgrid_domain::RetrySafety;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("unable to determine local application data directory")]
    NoAppDataDir,

    #[error("configuration validation error: {0}")]
    Validation(String),

    #[error("configuration I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML parse error: {0}")]
    TomlDecode(#[from] toml::de::Error),

    #[error("TOML serialize error: {0}")]
    TomlEncode(#[from] toml::ser::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ThemePreference {
    #[default]
    Dark,
    Light,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SchedulingPolicy {
    #[default]
    PriorityResourceAware,
    FifoResourceAware,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredExecutableEntry {
    pub id: String,
    pub display_name: String,
    pub executable_path: String,
    pub allowed_working_dir: String,
    pub retry_safety: RetrySafety,
    pub enabled: bool,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppConfig {
    pub theme: ThemePreference,
    pub reduced_motion_override: Option<bool>,
    pub scheduling_policy: SchedulingPolicy,
    pub max_workers: u32,
    pub max_concurrent_workloads: u32,
    pub max_queue_depth: u32,
    pub cpu_utilization_ceiling_pct: f64,
    pub memory_allocation_ceiling_pct: f64,
    pub reserved_system_memory_mb: u64,
    pub default_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub metrics_sample_interval_ms: u64,
    pub snapshot_retention_max_rows: u64,
    pub event_retention_max_rows: u64,
    pub max_log_file_bytes: u64,
    pub max_rotated_log_files: u32,
    pub require_explicit_executable_allowlist: bool,
    pub auto_recover_idempotent_interrupted: bool,
    pub registered_executables: Vec<RegisteredExecutableEntry>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            theme: ThemePreference::Dark,
            reduced_motion_override: None,
            scheduling_policy: SchedulingPolicy::PriorityResourceAware,
            max_workers: 4,
            max_concurrent_workloads: 4,
            max_queue_depth: 500,
            cpu_utilization_ceiling_pct: 85.0,
            memory_allocation_ceiling_pct: 80.0,
            reserved_system_memory_mb: 2048,
            default_timeout_ms: 120_000,
            heartbeat_interval_ms: 1_500,
            heartbeat_timeout_ms: 8_000,
            metrics_sample_interval_ms: 1_500,
            snapshot_retention_max_rows: 10_000,
            event_retention_max_rows: 20_000,
            max_log_file_bytes: 10 * 1024 * 1024,
            max_rotated_log_files: 5,
            require_explicit_executable_allowlist: true,
            auto_recover_idempotent_interrupted: true,
            registered_executables: Vec::new(),
        }
    }
}

impl AppConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_workers == 0 || self.max_workers > 64 {
            return Err(ConfigError::Validation(
                "max_workers must be between 1 and 64".to_string(),
            ));
        }
        if self.max_concurrent_workloads == 0 || self.max_concurrent_workloads > 128 {
            return Err(ConfigError::Validation(
                "max_concurrent_workloads must be between 1 and 128".to_string(),
            ));
        }
        if self.max_queue_depth < 10 || self.max_queue_depth > 50_000 {
            return Err(ConfigError::Validation(
                "max_queue_depth must be between 10 and 50000".to_string(),
            ));
        }
        if !self.cpu_utilization_ceiling_pct.is_finite()
            || !(10.0..=100.0).contains(&self.cpu_utilization_ceiling_pct)
        {
            return Err(ConfigError::Validation(
                "cpu_utilization_ceiling_pct must be between 10.0 and 100.0".to_string(),
            ));
        }
        if !self.memory_allocation_ceiling_pct.is_finite()
            || !(10.0..=95.0).contains(&self.memory_allocation_ceiling_pct)
        {
            return Err(ConfigError::Validation(
                "memory_allocation_ceiling_pct must be between 10.0 and 95.0".to_string(),
            ));
        }
        if self.reserved_system_memory_mb < 256 {
            return Err(ConfigError::Validation(
                "reserved_system_memory_mb must be at least 256 MB".to_string(),
            ));
        }
        if self.default_timeout_ms < 500 {
            return Err(ConfigError::Validation("default_timeout_ms must be >= 500".to_string()));
        }
        if self.heartbeat_timeout_ms <= self.heartbeat_interval_ms {
            return Err(ConfigError::Validation(
                "heartbeat_timeout_ms must exceed heartbeat_interval_ms".to_string(),
            ));
        }
        if self.snapshot_retention_max_rows < 100 || self.event_retention_max_rows < 100 {
            return Err(ConfigError::Validation("retention row limits must be >= 100".to_string()));
        }
        if self.max_log_file_bytes < 64 * 1024 {
            return Err(ConfigError::Validation(
                "max_log_file_bytes must be >= 64 KiB".to_string(),
            ));
        }
        Ok(())
    }

    pub fn load_or_create(path: &Path) -> Result<Self, ConfigError> {
        if path.exists() {
            let content = fs::read_to_string(path)?;
            let cfg: Self = toml::from_str(&content)?;
            cfg.validate()?;
            Ok(cfg)
        } else {
            let cfg = Self::default();
            cfg.save(path)?;
            Ok(cfg)
        }
    }

    pub fn save(&self, path: &Path) -> Result<(), ConfigError> {
        self.validate()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let body = toml::to_string_pretty(self)?;
        fs::write(path, body)?;
        Ok(())
    }
}

/// Resolved local directories for HelixGrid persistence, logs, artifacts, backups, and config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppPaths {
    pub root_dir: PathBuf,
    pub config_file: PathBuf,
    pub database_file: PathBuf,
    pub logs_dir: PathBuf,
    pub artifacts_dir: PathBuf,
    pub backups_dir: PathBuf,
    pub exports_dir: PathBuf,
}

impl AppPaths {
    pub fn resolve_default() -> Result<Self, ConfigError> {
        let proj =
            ProjectDirs::from("io", "HelixGrid", "HelixGrid").ok_or(ConfigError::NoAppDataDir)?;
        Ok(Self::from_root(proj.data_local_dir().to_path_buf()))
    }

    #[must_use]
    pub fn from_root(root_dir: PathBuf) -> Self {
        Self {
            config_file: root_dir.join("config.toml"),
            database_file: root_dir.join("helixgrid.db"),
            logs_dir: root_dir.join("logs"),
            artifacts_dir: root_dir.join("artifacts"),
            backups_dir: root_dir.join("backups"),
            exports_dir: root_dir.join("exports"),
            root_dir,
        }
    }

    pub fn ensure_directories(&self) -> Result<(), ConfigError> {
        fs::create_dir_all(&self.root_dir)?;
        fs::create_dir_all(&self.logs_dir)?;
        fs::create_dir_all(&self.artifacts_dir)?;
        fs::create_dir_all(&self.backups_dir)?;
        fs::create_dir_all(&self.exports_dir)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_roundtrip_and_validation() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = AppPaths::from_root(tmp.path().to_path_buf());
        paths.ensure_directories().unwrap();

        let cfg = AppConfig::load_or_create(&paths.config_file).unwrap();
        assert_eq!(cfg.max_workers, 4);

        let mut bad = cfg.clone();
        bad.max_workers = 0;
        assert!(bad.validate().is_err());

        let mut bad_hb = cfg;
        bad_hb.heartbeat_timeout_ms = 500;
        assert!(bad_hb.validate().is_err());
    }
}
