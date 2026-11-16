use helixgrid_config::{AppConfig, AppPaths, RegisteredExecutableEntry};
use helixgrid_core::{HelixGridEngine, SubmitWorkloadRequest};
use helixgrid_domain::{
    BatchOperation, BatchProcessingInput, CpuBenchmarkInput, NumericalAlgorithm,
    NumericalComputationInput, Priority, RegisteredExecutableInput, ResourceRequirements,
    RetryPolicy, RetrySafety, WorkerStatus, WorkflowStatus, WorkflowTaskDefinition, WorkflowTaskId,
    WorkflowTaskState, WorkloadSpec, WorkloadStatus,
};
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
    panic!(
        "helixgrid-worker.exe not found next to {:?}; run `cargo build --workspace` before integration tests",
        exe
    );
}

async fn wait_for_idle_worker(engine: &HelixGridEngine) {
    for _ in 0..80 {
        let snap = engine.snapshot();
        if snap.workers.iter().any(|w| w.status == WorkerStatus::Idle) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("Timed out waiting for worker to become Idle");
}

async fn wait_for_workload_terminal(
    engine: &HelixGridEngine,
    id: helixgrid_domain::WorkloadId,
    timeout_ms: u64,
) -> helixgrid_domain::Workload {
    let steps = (timeout_ms / 50).max(1);
    for _ in 0..steps {
        let snap = engine.snapshot();
        if let Some(wl) = snap.workloads.iter().find(|w| w.id == id)
            && wl.status.is_terminal()
        {
            return wl.clone();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let snap = engine.snapshot();
    let state = snap.workloads.iter().find(|w| w.id == id).map(|w| w.status);
    panic!("Timed out waiting for workload {id}; last status = {state:?}");
}

#[tokio::test]
async fn executes_all_real_workload_categories_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_root(tmp.path().to_path_buf());
    paths.ensure_directories().unwrap();

    // Register benign system binary hostname.exe for ExternalExecutable test
    let hostname_exe = PathBuf::from(r"C:\Windows\System32\hostname.exe");
    let mut cfg = AppConfig::default();
    if hostname_exe.is_file() {
        cfg.registered_executables.push(RegisteredExecutableEntry {
            id: "sys-hostname".into(),
            display_name: "System Hostname".into(),
            executable_path: hostname_exe.to_string_lossy().into_owned(),
            allowed_working_dir: tmp.path().to_string_lossy().into_owned(),
            retry_safety: RetrySafety::NonIdempotentExternal,
            enabled: true,
            description: "Test registered non-shell binary".into(),
        });
    }
    cfg.save(&paths.config_file).unwrap();

    let engine =
        HelixGridEngine::start(paths.clone(), Some(locate_worker_exe()), Some(2)).await.unwrap();
    wait_for_idle_worker(&engine).await;

    // 1. CPU Benchmark
    let bench_id = engine
        .submit_workload(SubmitWorkloadRequest {
            name: "E2E CPU Benchmark".into(),
            description: "Real prime sieve + ARX compute".into(),
            priority: Priority::High,
            spec: WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                worker_threads: 2,
                iterations: 400,
                sieve_limit: 5000,
            }),
            resources: ResourceRequirements::default(),
            timeout_ms: Some(15_000),
            retry_policy: None,
        })
        .await
        .unwrap();

    // 2. Batch Processing
    let batch_id = engine
        .submit_workload(SubmitWorkloadRequest {
            name: "E2E Batch Processing".into(),
            description: "Real disk-backed JSONL batch transform".into(),
            priority: Priority::Normal,
            spec: WorkloadSpec::BatchProcessing(BatchProcessingInput {
                input_dir: None,
                item_count: 500,
                batch_size: 100,
                operation: BatchOperation::HashAndValidate,
            }),
            resources: ResourceRequirements::default(),
            timeout_ms: Some(15_000),
            retry_policy: None,
        })
        .await
        .unwrap();

    // 3. Numerical Computation
    let num_id = engine
        .submit_workload(SubmitWorkloadRequest {
            name: "E2E CG Solve".into(),
            description: "Conjugate Gradient SPD linear solver".into(),
            priority: Priority::Normal,
            spec: WorkloadSpec::NumericalComputation(NumericalComputationInput {
                algorithm: NumericalAlgorithm::ConjugateGradientSolve,
                dimension: 200,
                max_iterations: 500,
                tolerance: 1e-8,
                seed: 9,
            }),
            resources: ResourceRequirements::default(),
            timeout_ms: Some(15_000),
            retry_policy: None,
        })
        .await
        .unwrap();

    let bench_wl = wait_for_workload_terminal(&engine, bench_id, 20_000).await;
    assert_eq!(bench_wl.status, WorkloadStatus::Completed);
    assert!(bench_wl.result_metadata.is_some());

    let batch_wl = wait_for_workload_terminal(&engine, batch_id, 20_000).await;
    assert_eq!(batch_wl.status, WorkloadStatus::Completed);

    let num_wl = wait_for_workload_terminal(&engine, num_id, 20_000).await;
    assert_eq!(num_wl.status, WorkloadStatus::Completed);

    // 4. Registered External Executable
    if hostname_exe.is_file() {
        let ext_id = engine
            .submit_workload(SubmitWorkloadRequest {
                name: "E2E Hostname".into(),
                description: "Registered external executable".into(),
                priority: Priority::High,
                spec: WorkloadSpec::RegisteredExecutable(RegisteredExecutableInput {
                    registered_id: "sys-hostname".into(),
                    executable_path: hostname_exe.to_string_lossy().into_owned(),
                    args: vec![],
                    working_dir: tmp.path().to_string_lossy().into_owned(),
                    env_overrides: vec![],
                }),
                resources: ResourceRequirements::default(),
                timeout_ms: Some(10_000),
                retry_policy: None,
            })
            .await
            .unwrap();
        let ext_wl = wait_for_workload_terminal(&engine, ext_id, 15_000).await;
        assert_eq!(ext_wl.status, WorkloadStatus::Completed);
        let snap = engine.snapshot();
        let arts = snap.artifacts_by_workload.get(&ext_id).unwrap();
        assert!(arts.iter().any(|a| a.name == "stdout.log" && a.size_bytes > 0));
    }

    // 5. Multi-stage DAG Workflow
    let t1 = WorkflowTaskId::new();
    let t2 = WorkflowTaskId::new();
    let wf_id = engine
        .submit_and_start_workflow(
            "E2E Workflow".into(),
            "2-stage DAG".into(),
            vec![
                WorkflowTaskDefinition {
                    id: t1,
                    name: "Stage 1".into(),
                    depends_on: vec![],
                    priority: Priority::High,
                    spec: WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                        worker_threads: 1,
                        iterations: 100,
                        sieve_limit: 1000,
                    }),
                    resources: ResourceRequirements::default(),
                    timeout_ms: 10_000,
                    retry_policy: RetryPolicy::default(),
                    state: WorkflowTaskState::Pending,
                    workload_id: None,
                },
                WorkflowTaskDefinition {
                    id: t2,
                    name: "Stage 2".into(),
                    depends_on: vec![t1],
                    priority: Priority::Normal,
                    spec: WorkloadSpec::NumericalComputation(NumericalComputationInput {
                        algorithm: NumericalAlgorithm::MonteCarloVariance,
                        dimension: 16,
                        max_iterations: 20_000,
                        tolerance: 1e-6,
                        seed: 123,
                    }),
                    resources: ResourceRequirements::default(),
                    timeout_ms: 10_000,
                    retry_policy: RetryPolicy::default(),
                    state: WorkflowTaskState::Pending,
                    workload_id: None,
                },
            ],
        )
        .await
        .unwrap();

    let mut wf_done = false;
    for _ in 0..160 {
        let snap = engine.snapshot();
        if let Some(wf) = snap.workflows.iter().find(|w| w.id == wf_id)
            && wf.status == WorkflowStatus::Completed
        {
            wf_done = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(wf_done, "DAG workflow did not complete in time");

    engine.shutdown().await;
}

#[tokio::test]
async fn timeout_and_cancellation_work_on_real_workers() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_root(tmp.path().to_path_buf());
    paths.ensure_directories().unwrap();

    let engine = HelixGridEngine::start(paths, Some(locate_worker_exe()), Some(2)).await.unwrap();
    wait_for_idle_worker(&engine).await;

    // 1. Active execution timeout
    let to_id = engine
        .submit_workload(SubmitWorkloadRequest {
            name: "Timeout Test".into(),
            description: "Heavy workload with 250ms timeout".into(),
            priority: Priority::Normal,
            spec: WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                worker_threads: 1,
                iterations: 5_000_000,
                sieve_limit: 500_000,
            }),
            resources: ResourceRequirements::default(),
            timeout_ms: Some(250),
            retry_policy: None,
        })
        .await
        .unwrap();

    let to_wl = wait_for_workload_terminal(&engine, to_id, 15_000).await;
    assert_eq!(to_wl.status, WorkloadStatus::TimedOut);

    // 2. Active mid-flight cancellation
    let cancel_id = engine
        .submit_workload(SubmitWorkloadRequest {
            name: "Cancel Test".into(),
            description: "Long workload cancelled mid-run".into(),
            priority: Priority::Normal,
            spec: WorkloadSpec::NumericalComputation(NumericalComputationInput {
                algorithm: NumericalAlgorithm::DenseMatrixMultiply,
                dimension: 1024,
                max_iterations: 1000,
                tolerance: 1e-8,
                seed: 7,
            }),
            resources: ResourceRequirements::default(),
            timeout_ms: Some(60_000),
            retry_policy: None,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    engine.cancel_workload(cancel_id).await.unwrap();
    let cancelled_wl = wait_for_workload_terminal(&engine, cancel_id, 10_000).await;
    assert_eq!(cancelled_wl.status, WorkloadStatus::Cancelled);

    // Verify all reservations released
    let snap = engine.snapshot();
    for w in &snap.workers {
        assert_eq!(w.reserved_cpu_cores, 0);
        assert_eq!(w.reserved_memory_mb, 0);
    }

    engine.shutdown().await;
}
