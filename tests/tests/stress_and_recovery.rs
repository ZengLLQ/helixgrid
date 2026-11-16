use helixgrid_config::{AppConfig, AppPaths};
use helixgrid_core::{HelixGridEngine, SubmitWorkloadRequest};
use helixgrid_domain::{
    CpuBenchmarkInput, Priority, ResourceRequirements, RetryPolicy, RetrySafety, WorkerStatus,
    Workload, WorkloadSpec, WorkloadStatus,
};
use helixgrid_storage::StorageEngine;
use proptest::prelude::*;
use std::path::PathBuf;
use std::time::Duration;

fn locate_worker_exe() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    let dir = exe.parent().unwrap();
    for candidate in
        [dir.join("helixgrid-worker.exe"), dir.parent().unwrap().join("helixgrid-worker.exe")]
    {
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("helixgrid-worker.exe not found");
}

#[tokio::test]
async fn processes_100_concurrent_queued_workloads_and_releases_all_reservations() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_root(tmp.path().to_path_buf());
    paths.ensure_directories().unwrap();

    let cfg = AppConfig {
        max_workers: 4,
        max_concurrent_workloads: 4,
        max_queue_depth: 200,
        ..Default::default()
    };
    cfg.save(&paths.config_file).unwrap();

    let engine = HelixGridEngine::start(paths, Some(locate_worker_exe()), Some(4)).await.unwrap();

    for _ in 0..80 {
        if engine.snapshot().workers.iter().filter(|w| w.status == WorkerStatus::Idle).count() >= 2
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let mut ids = Vec::with_capacity(100);
    for i in 0..100 {
        let prio = match i % 4 {
            0 => Priority::Critical,
            1 => Priority::High,
            2 => Priority::Normal,
            _ => Priority::Low,
        };
        let id = engine
            .submit_workload(SubmitWorkloadRequest {
                name: format!("Stress-100-{i:03}"),
                description: "100 queued workload throughput test".into(),
                priority: prio,
                spec: WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                    worker_threads: 1,
                    iterations: 20,
                    sieve_limit: 500,
                }),
                resources: ResourceRequirements {
                    cpu_cores: 1,
                    memory_mb: 32,
                    requires_exclusive_worker: false,
                },
                timeout_ms: Some(30_000),
                retry_policy: None,
            })
            .await
            .unwrap();
        ids.push(id);
    }

    let mut all_done = false;
    for _ in 0..400 {
        let snap = engine.snapshot();
        let done_count = snap
            .workloads
            .iter()
            .filter(|w| ids.contains(&w.id) && w.status == WorkloadStatus::Completed)
            .count();
        if done_count == 100 {
            all_done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(all_done, "Not all 100 queued workloads completed in time");

    let snap = engine.snapshot();
    for w in &snap.workers {
        assert_eq!(w.reserved_cpu_cores, 0);
        assert_eq!(w.reserved_memory_mb, 0);
    }
    engine.shutdown().await;
}

#[tokio::test]
async fn fault_injection_worker_crash_triggers_interrupted_and_safe_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_root(tmp.path().to_path_buf());
    paths.ensure_directories().unwrap();

    let cfg = AppConfig {
        max_workers: 2,
        max_concurrent_workloads: 2,
        auto_recover_idempotent_interrupted: true,
        ..Default::default()
    };
    cfg.save(&paths.config_file).unwrap();

    let engine = HelixGridEngine::start(paths, Some(locate_worker_exe()), Some(2)).await.unwrap();

    for _ in 0..80 {
        if engine.snapshot().workers.iter().filter(|w| w.status == WorkerStatus::Idle).count() == 2
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let wl_id = engine
        .submit_workload(SubmitWorkloadRequest {
            name: "Crash Recovery Target".into(),
            description: "Killed mid-execution and safely recovered onto surviving worker".into(),
            priority: Priority::High,
            spec: WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                worker_threads: 1,
                iterations: 50_000,
                sieve_limit: 20_000,
            }),
            resources: ResourceRequirements::default(),
            timeout_ms: Some(30_000),
            retry_policy: Some(RetryPolicy {
                max_retries: 2,
                backoff_ms: 100,
                retry_safety: RetrySafety::SafeIdempotent,
                retry_on_timeout: false,
            }),
        })
        .await
        .unwrap();

    // Wait until assigned to a worker and running, then force-kill that worker via Job Object.
    let mut killed_worker = None;
    for _ in 0..100 {
        let snap = engine.snapshot();
        if let Some(wl) = snap.workloads.iter().find(|w| w.id == wl_id)
            && matches!(wl.status, WorkloadStatus::Starting | WorkloadStatus::Running)
            && let Some(wid) = wl.assigned_worker_id
        {
            engine.force_kill_worker(wid).await.unwrap();
            killed_worker = Some(wid);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let killed_id = killed_worker.expect("Workload never reached active state");

    // Surviving worker should pick up the auto-recovered retry and complete it.
    let mut completed = false;
    for _ in 0..250 {
        let snap = engine.snapshot();
        if let Some(wl) = snap.workloads.iter().find(|w| w.id == wl_id)
            && wl.status == WorkloadStatus::Completed
        {
            assert_ne!(wl.assigned_worker_id, Some(killed_id));
            assert!(snap.recovery_summary.auto_recovered_workloads >= 1);
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(completed, "Recovered workload did not complete on surviving worker");

    let snap = engine.snapshot();
    let killed_info = snap.workers.iter().find(|w| w.id == killed_id).unwrap();
    assert_eq!(killed_info.reserved_cpu_cores, 0);
    assert_eq!(killed_info.reserved_memory_mb, 0);

    engine.shutdown().await;
}

#[tokio::test]
async fn coordinator_restart_reconciles_interrupted_db_records() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_root(tmp.path().to_path_buf());
    paths.ensure_directories().unwrap();

    // Seed SQLite with a workload left in Running state as if coordinator crashed mid-flight,
    // AND one non-idempotent ExternalExecutable left in Running state.
    {
        let store = StorageEngine::open(&paths.database_file).unwrap();

        let mut safe_wl = Workload::new(
            "Interrupted Idempotent",
            "Should auto-recover on restart",
            Priority::High,
            WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                worker_threads: 1,
                iterations: 50,
                sieve_limit: 500,
            }),
            ResourceRequirements::default(),
            10_000,
            RetryPolicy {
                max_retries: 2,
                backoff_ms: 100,
                retry_safety: RetrySafety::SafeIdempotent,
                retry_on_timeout: false,
            },
            1_700_000_000,
        )
        .unwrap();
        safe_wl.status = WorkloadStatus::Running;
        safe_wl.attempt_count = 1;
        store.insert_workload(&safe_wl).unwrap();

        let mut unsafe_wl = Workload::new(
            "Interrupted Non-Idempotent",
            "Must NOT auto-retry",
            Priority::Normal,
            WorkloadSpec::RegisteredExecutable(helixgrid_domain::RegisteredExecutableInput {
                registered_id: "tool".into(),
                executable_path: "C:\\Windows\\System32\\hostname.exe".into(),
                args: vec![],
                working_dir: tmp.path().to_string_lossy().into_owned(),
                env_overrides: vec![],
            }),
            ResourceRequirements::default(),
            10_000,
            RetryPolicy {
                max_retries: 0,
                backoff_ms: 100,
                retry_safety: RetrySafety::NonIdempotentExternal,
                retry_on_timeout: false,
            },
            1_700_000_001,
        )
        .unwrap();
        unsafe_wl.status = WorkloadStatus::Running;
        unsafe_wl.attempt_count = 1;
        store.insert_workload(&unsafe_wl).unwrap();
    }

    let engine = HelixGridEngine::start(paths, Some(locate_worker_exe()), Some(1)).await.unwrap();

    let mut both_reconciled = false;
    for _ in 0..160 {
        let snap = engine.snapshot();
        let safe = snap.workloads.iter().find(|w| w.name == "Interrupted Idempotent");
        let unsafe_w = snap.workloads.iter().find(|w| w.name == "Interrupted Non-Idempotent");
        if let (Some(s), Some(u)) = (safe, unsafe_w)
            && s.status == WorkloadStatus::Completed
            && u.status == WorkloadStatus::Interrupted
        {
            both_reconciled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(both_reconciled);
    engine.shutdown().await;
}

#[test]
fn ten_thousand_lifecycle_transitions_obey_state_machine_invariants() {
    use WorkloadStatus::*;
    let all_states = [
        Submitted,
        Validating,
        Queued,
        Scheduled,
        Starting,
        Running,
        Retrying,
        Completed,
        Failed,
        Cancelled,
        TimedOut,
        Interrupted,
    ];

    let mut legal_count = 0usize;
    let mut state_idx = 0usize;
    for step in 0..10_000usize {
        let mut current = all_states[state_idx % all_states.len()];
        let was_terminal = current.is_terminal();
        let target = all_states[(step * 7 + 3) % all_states.len()];
        let allowed = current.can_transition_to(target);
        let res = current.try_transition(target);
        assert_eq!(allowed, res.is_ok());

        if was_terminal {
            assert!(res.is_err(), "Terminal state must never transition");
        }
        if res.is_ok() {
            legal_count += 1;
            state_idx = all_states.iter().position(|&s| s == current).unwrap();
        } else {
            state_idx = (state_idx + 1) % all_states.len();
        }
    }
    assert!(legal_count > 1000);
}

proptest! {
    #[test]
    fn proptest_terminal_states_are_absorbing(next_idx in 0usize..12usize) {
        let all = [
            WorkloadStatus::Submitted,
            WorkloadStatus::Validating,
            WorkloadStatus::Queued,
            WorkloadStatus::Scheduled,
            WorkloadStatus::Starting,
            WorkloadStatus::Running,
            WorkloadStatus::Retrying,
            WorkloadStatus::Completed,
            WorkloadStatus::Failed,
            WorkloadStatus::Cancelled,
            WorkloadStatus::TimedOut,
            WorkloadStatus::Interrupted,
        ];
        let target = all[next_idx];
        for term in [
            WorkloadStatus::Completed,
            WorkloadStatus::Failed,
            WorkloadStatus::Cancelled,
            WorkloadStatus::TimedOut,
        ] {
            prop_assert!(!term.can_transition_to(target));
        }
    }
}
