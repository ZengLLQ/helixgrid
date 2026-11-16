use helixgrid_config::{AppConfig, RegisteredExecutableEntry};
use helixgrid_domain::{
    CpuBenchmarkInput, Priority, RegisteredExecutableInput, ResourceRequirements, RetryPolicy,
    RetrySafety, WorkerId, Workload, WorkloadSpec,
};
use helixgrid_platform_windows::pipe::create_secure_pipe_server;
use helixgrid_protocol::{IpcEnvelope, IpcMessage, WorkerRegistrationMsg, write_frame};
use helixgrid_security::{
    SecurityError, build_sanitized_env, safe_join_under, validate_registered_executable,
    verify_safe_retry,
};
use helixgrid_storage::StorageEngine;
use std::fs;
use tokio::net::windows::named_pipe::ClientOptions;
use uuid::Uuid;

#[test]
fn blocks_shell_executables_traversal_and_secret_env_leakage() {
    let tmp = tempfile::tempdir().unwrap();

    // Path traversal checks
    assert!(matches!(
        safe_join_under(tmp.path(), "..\\..\\Windows\\win.ini"),
        Err(SecurityError::PathViolation(_))
    ));
    assert!(matches!(
        safe_join_under(tmp.path(), "C:\\Windows\\System32\\cmd.exe"),
        Err(SecurityError::PathViolation(_))
    ));

    // Shell binary blocked even if explicitly registered
    let powershell_stub = tmp.path().join("powershell.exe");
    fs::write(&powershell_stub, b"stub").unwrap();
    let mut cfg = AppConfig::default();
    cfg.registered_executables.push(RegisteredExecutableEntry {
        id: "ps".into(),
        display_name: "PowerShell".into(),
        executable_path: powershell_stub.to_string_lossy().into_owned(),
        allowed_working_dir: tmp.path().to_string_lossy().into_owned(),
        retry_safety: RetrySafety::NonIdempotentExternal,
        enabled: true,
        description: "blocked".into(),
    });
    let input = RegisteredExecutableInput {
        registered_id: "ps".into(),
        executable_path: powershell_stub.to_string_lossy().into_owned(),
        args: vec!["-Command".into(), "calc.exe".into()],
        working_dir: tmp.path().to_string_lossy().into_owned(),
        env_overrides: vec![],
    };
    assert!(matches!(
        validate_registered_executable(&cfg, &input),
        Err(SecurityError::InvalidExecutable(_))
    ));

    // Secret environment leakage prevention
    assert!(matches!(
        build_sanitized_env(&[("GITHUB_TOKEN".into(), "ghp_secret".into())], tmp.path()),
        Err(SecurityError::EnvironmentViolation(_))
    ));

    // Unsafe retry prevention on external processes
    let spec = WorkloadSpec::RegisteredExecutable(input);
    let bad_retry = RetryPolicy {
        max_retries: 1,
        backoff_ms: 100,
        retry_safety: RetrySafety::NonIdempotentExternal,
        retry_on_timeout: false,
    };
    assert!(matches!(verify_safe_retry(&spec, &bad_retry), Err(SecurityError::UnsafeRetry(_))));
}

#[tokio::test]
async fn rejects_malformed_or_unauthenticated_ipc_registration() {
    let pipe_name = format!(r"\\.\pipe\helixgrid-sec-{}", Uuid::new_v4());
    let server = create_secure_pipe_server(&pipe_name, true).unwrap();
    let mut client = ClientOptions::new().open(&pipe_name).unwrap();
    server.connect().await.unwrap();

    let fake_token = "0".repeat(64);
    let env = IpcEnvelope::new(
        1,
        1000,
        IpcMessage::WorkerRegistration(WorkerRegistrationMsg {
            worker_id: WorkerId::new(),
            pid: 9999,
            auth_token: fake_token,
            worker_version: "0.1.0".into(),
            max_cpu_cores: 2,
            max_memory_mb: 512,
        }),
    );
    write_frame(&mut client, &env).await.unwrap();
    let mut srv = server;
    let received = helixgrid_protocol::read_frame(&mut srv).await.unwrap();
    match received.payload {
        IpcMessage::WorkerRegistration(reg) => {
            assert!(!helixgrid_security::constant_time_eq(
                &reg.auth_token,
                "expected-valid-coordinator-secret-token-64"
            ));
        }
        other => panic!("unexpected msg: {other:?}"),
    }
}

#[test]
fn database_backup_restore_and_corruption_handling() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("primary.db");
    let backup_path = tmp.path().join("backup.db");
    let restored_path = tmp.path().join("restored.db");
    let corrupt_path = tmp.path().join("corrupt.db");

    let wl_id;
    {
        let db = StorageEngine::open(&db_path).unwrap();
        let wl = Workload::new(
            "Persisted Workload",
            "Verify online SQLite backup & restore",
            Priority::Normal,
            WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                worker_threads: 1,
                iterations: 100,
                sieve_limit: 500,
            }),
            ResourceRequirements::default(),
            5_000,
            RetryPolicy::default(),
            1_700_000_000,
        )
        .unwrap();
        wl_id = wl.id;
        db.insert_workload(&wl).unwrap();
        db.backup_to(&backup_path).unwrap();
    }

    StorageEngine::verify_and_restore_file(&backup_path, &restored_path).unwrap();
    let restored = StorageEngine::open(&restored_path).unwrap();
    assert_eq!(restored.get_workload(wl_id).unwrap().unwrap().id, wl_id);

    fs::write(&corrupt_path, b"CORRUPTED_BYTES").unwrap();
    assert!(StorageEngine::verify_and_restore_file(&corrupt_path, &restored_path).is_err());
}
