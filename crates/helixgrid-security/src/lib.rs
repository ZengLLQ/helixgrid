use helixgrid_config::{AppConfig, RegisteredExecutableEntry};
use helixgrid_domain::{RegisteredExecutableInput, RetryPolicy, RetrySafety, WorkloadSpec};
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SecurityError {
    #[error("path traversal or unauthorized path rejected: {0}")]
    PathViolation(String),

    #[error("executable not registered or disabled in allowlist: {0}")]
    UnregisteredExecutable(String),

    #[error("executable validation failed: {0}")]
    InvalidExecutable(String),

    #[error("unsafe retry policy violation: {0}")]
    UnsafeRetry(String),

    #[error("environment policy violation: {0}")]
    EnvironmentViolation(String),

    #[error("IPC authentication failed")]
    IpcAuthFailed,

    #[error("filesystem error during validation: {0}")]
    Io(String),
}

const DISALLOWED_SHELL_BINARIES: &[&str] = &[
    "cmd.exe",
    "cmd",
    "powershell.exe",
    "powershell",
    "pwsh.exe",
    "pwsh",
    "wscript.exe",
    "cscript.exe",
    "mshta.exe",
    "sh.exe",
    "bash.exe",
    "wsl.exe",
];

const SAFE_INHERITED_ENV_VARS: &[&str] = &[
    "SYSTEMROOT",
    "SYSTEMDRIVE",
    "WINDIR",
    "OS",
    "COMSPEC",
    "NUMBER_OF_PROCESSORS",
    "PROCESSOR_ARCHITECTURE",
];

const SENSITIVE_ENV_PREFIXES: &[&str] = &[
    "AWS_",
    "AZURE_",
    "GCP_",
    "GOOGLE_",
    "GITHUB_",
    "GITLAB_",
    "OPENAI_",
    "ANTHROPIC_",
    "STRIPE_",
    "DATABASE_URL",
    "SECRET",
    "TOKEN",
    "PASSWORD",
    "PRIVATE_KEY",
];

/// Generates a 256-bit cryptographic session token using OS CSPRNG-backed UUID v4 entropy.
#[must_use]
pub fn generate_ipc_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

/// Constant-time string comparison for IPC session tokens.
#[must_use]
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    if ab.len() != bb.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (&x, &y) in ab.iter().zip(bb.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Resolves a relative path safely inside `base_dir`, rejecting absolute paths,
/// parent traversal (`..`), NUL bytes, and symlink/junction escapes outside `base_dir`.
pub fn safe_join_under(base_dir: &Path, relative: &str) -> Result<PathBuf, SecurityError> {
    if relative.is_empty() || relative.contains('\0') {
        return Err(SecurityError::PathViolation(
            "relative path is empty or contains NUL".to_string(),
        ));
    }

    let rel_path = Path::new(relative);
    if rel_path.is_absolute() {
        return Err(SecurityError::PathViolation(format!(
            "absolute path '{relative}' is not permitted here"
        )));
    }

    for comp in rel_path.components() {
        match comp {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(SecurityError::PathViolation(format!(
                    "disallowed path component in '{relative}'"
                )));
            }
        }
    }

    let canonical_base = fs::canonicalize(base_dir)
        .map_err(|e| SecurityError::Io(format!("cannot canonicalize base dir: {e}")))?;
    let candidate = canonical_base.join(rel_path);
    if candidate.exists() {
        let canonical_target = fs::canonicalize(&candidate)
            .map_err(|e| SecurityError::Io(format!("cannot canonicalize target: {e}")))?;
        if !canonical_target.starts_with(&canonical_base) {
            return Err(SecurityError::PathViolation(
                "resolved path escapes base directory".to_string(),
            ));
        }
        Ok(canonical_target)
    } else {
        Ok(candidate)
    }
}

/// Ensures an existing directory path resides inside an allowed root directory.
pub fn validate_dir_within(
    allowed_root: &Path,
    target_dir: &Path,
) -> Result<PathBuf, SecurityError> {
    let canon_root = fs::canonicalize(allowed_root)
        .map_err(|e| SecurityError::Io(format!("allowed root invalid: {e}")))?;
    let canon_target = fs::canonicalize(target_dir)
        .map_err(|e| SecurityError::Io(format!("target directory invalid: {e}")))?;
    if !canon_target.is_dir() {
        return Err(SecurityError::PathViolation("target path is not a directory".to_string()));
    }
    if !canon_target.starts_with(&canon_root) {
        return Err(SecurityError::PathViolation(format!(
            "directory '{}' is outside allowed root '{}'",
            canon_target.display(),
            canon_root.display()
        )));
    }
    Ok(canon_target)
}

/// Validates a registered executable against the application allowlist and OS safety checks.
pub fn validate_registered_executable<'a>(
    config: &'a AppConfig,
    input: &RegisteredExecutableInput,
) -> Result<(&'a RegisteredExecutableEntry, PathBuf, PathBuf), SecurityError> {
    let entry = config
        .registered_executables
        .iter()
        .find(|e| e.id == input.registered_id)
        .ok_or_else(|| SecurityError::UnregisteredExecutable(input.registered_id.clone()))?;

    if !entry.enabled {
        return Err(SecurityError::UnregisteredExecutable(format!(
            "{} (disabled)",
            input.registered_id
        )));
    }

    let reg_exe = fs::canonicalize(&entry.executable_path)
        .map_err(|e| SecurityError::InvalidExecutable(format!("registered path invalid: {e}")))?;
    let req_exe = fs::canonicalize(&input.executable_path)
        .map_err(|e| SecurityError::InvalidExecutable(format!("requested path invalid: {e}")))?;

    if config.require_explicit_executable_allowlist && reg_exe != req_exe {
        return Err(SecurityError::InvalidExecutable(
            "requested executable path does not match registered allowlist path".to_string(),
        ));
    }

    if !req_exe.is_file() {
        return Err(SecurityError::InvalidExecutable(
            "executable path is not a regular file".to_string(),
        ));
    }

    if let Some(file_name) = req_exe.file_name().and_then(|s| s.to_str()) {
        let lower = file_name.to_ascii_lowercase();
        if DISALLOWED_SHELL_BINARIES.contains(&lower.as_str()) {
            return Err(SecurityError::InvalidExecutable(format!(
                "shell/interpreter binary '{file_name}' is prohibited to prevent command injection"
            )));
        }
    }

    for arg in &input.args {
        if arg.contains('\0') || arg.len() > 8192 {
            return Err(SecurityError::InvalidExecutable(
                "argument contains NUL byte or exceeds 8 KiB".to_string(),
            ));
        }
    }

    let allowed_wd = Path::new(&entry.allowed_working_dir);
    let requested_wd = Path::new(&input.working_dir);
    let safe_wd = validate_dir_within(allowed_wd, requested_wd)?;

    Ok((entry, req_exe, safe_wd))
}

/// Builds a scrubbed child-process environment so coordinator secrets are never leaked.
pub fn build_sanitized_env(
    overrides: &[(String, String)],
    artifact_temp_dir: &Path,
) -> Result<BTreeMap<String, String>, SecurityError> {
    let mut env = BTreeMap::new();
    for key in SAFE_INHERITED_ENV_VARS {
        if let Ok(val) = std::env::var(key) {
            env.insert((*key).to_string(), val);
        }
    }

    let temp_str = artifact_temp_dir.to_string_lossy().into_owned();
    env.insert("TEMP".to_string(), temp_str.clone());
    env.insert("TMP".to_string(), temp_str);

    for (k, v) in overrides {
        let upper = k.trim().to_ascii_uppercase();
        if upper.is_empty() || k.contains('=') || k.contains('\0') || v.contains('\0') {
            return Err(SecurityError::EnvironmentViolation(format!(
                "invalid environment variable '{k}'"
            )));
        }
        if SENSITIVE_ENV_PREFIXES.iter().any(|p| upper.starts_with(p) || upper.contains(p)) {
            return Err(SecurityError::EnvironmentViolation(format!(
                "override key '{k}' matches restricted secret pattern"
            )));
        }
        env.insert(k.clone(), v.clone());
    }

    Ok(env)
}

/// Validates that a workload retry is policy-safe (prevents automatic retries of non-idempotent external programs).
pub fn verify_safe_retry(spec: &WorkloadSpec, policy: &RetryPolicy) -> Result<(), SecurityError> {
    if policy.max_retries > 0
        && matches!(spec, WorkloadSpec::RegisteredExecutable(_))
        && policy.retry_safety != RetrySafety::SafeIdempotent
    {
        return Err(SecurityError::UnsafeRetry(
            "external executables cannot enable automatic retries unless classified SafeIdempotent"
                .to_string(),
        ));
    }
    Ok(())
}

/// Minimal FIPS 180-4 SHA-256 for artifact integrity hashing without extra dependencies.
pub fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = data.to_vec();
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
        state[5] = state[5].wrapping_add(f);
        state[6] = state[6].wrapping_add(g);
        state[7] = state[7].wrapping_add(h);
    }

    let mut out = [0u8; 32];
    for (i, word) in state.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[must_use]
pub fn sha256_hex(data: &[u8]) -> String {
    let digest = sha256_bytes(data);
    let mut s = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

pub fn sha256_file_hex(path: &Path) -> Result<String, SecurityError> {
    let mut file = fs::File::open(path).map_err(|e| SecurityError::Io(e.to_string()))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).map_err(|e| SecurityError::Io(e.to_string()))?;
    Ok(sha256_hex(&buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nist_sha256_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn rejects_path_traversal() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(safe_join_under(tmp.path(), "../escape.txt").is_err());
        assert!(safe_join_under(tmp.path(), "C:\\Windows\\System32\\cmd.exe").is_err());
        assert!(safe_join_under(tmp.path(), "sub/ok.json").is_ok());
    }

    #[test]
    fn rejects_shell_binary_and_unsafe_retry() {
        let tmp = tempfile::tempdir().unwrap();
        let fake_cmd = tmp.path().join("cmd.exe");
        fs::write(&fake_cmd, b"stub").unwrap();
        let mut cfg = AppConfig::default();
        cfg.registered_executables.push(RegisteredExecutableEntry {
            id: "shell".to_string(),
            display_name: "Shell".to_string(),
            executable_path: fake_cmd.to_string_lossy().into_owned(),
            allowed_working_dir: tmp.path().to_string_lossy().into_owned(),
            retry_safety: RetrySafety::NonIdempotentExternal,
            enabled: true,
            description: "Disallowed".to_string(),
        });
        let input = RegisteredExecutableInput {
            registered_id: "shell".to_string(),
            executable_path: fake_cmd.to_string_lossy().into_owned(),
            args: vec![],
            working_dir: tmp.path().to_string_lossy().into_owned(),
            env_overrides: vec![],
        };
        assert!(matches!(
            validate_registered_executable(&cfg, &input),
            Err(SecurityError::InvalidExecutable(_))
        ));

        let spec = WorkloadSpec::RegisteredExecutable(input);
        let unsafe_policy = RetryPolicy {
            max_retries: 2,
            backoff_ms: 500,
            retry_safety: RetrySafety::NonIdempotentExternal,
            retry_on_timeout: false,
        };
        assert!(matches!(
            verify_safe_retry(&spec, &unsafe_policy),
            Err(SecurityError::UnsafeRetry(_))
        ));
    }

    #[test]
    fn scrubs_secret_env_vars() {
        let tmp = tempfile::tempdir().unwrap();
        let env = build_sanitized_env(&[("SAFE_ARG".into(), "1".into())], tmp.path()).unwrap();
        assert_eq!(env.get("SAFE_ARG").map(String::as_str), Some("1"));

        let bad = build_sanitized_env(&[("AWS_SECRET_ACCESS_KEY".into(), "x".into())], tmp.path());
        assert!(matches!(bad, Err(SecurityError::EnvironmentViolation(_))));
    }
}
