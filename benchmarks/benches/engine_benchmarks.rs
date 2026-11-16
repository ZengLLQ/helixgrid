use criterion::{Criterion, criterion_group, criterion_main};
use helixgrid_config::AppConfig;
use helixgrid_domain::{
    CpuBenchmarkInput, MeasurementValue, NumericalAlgorithm, NumericalComputationInput, Priority,
    ResourceRequirements, RetryPolicy, WorkerId, WorkerInfo, WorkerStatus, Workload, WorkloadSpec,
};
use helixgrid_platform_windows::pipe::create_secure_pipe_server;
use helixgrid_protocol::{
    HeartbeatMsg, IpcEnvelope, IpcMessage, decode_frame_payload, encode_frame, read_frame,
    write_frame,
};
use helixgrid_resource::{HostTelemetrySample, ResourceManager, SystemTelemetryProvider};
use helixgrid_scheduler::SchedulerEngine;
use helixgrid_storage::StorageEngine;
use helixgrid_worker_runtime::compute::execute_spec_blocking;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::net::windows::named_pipe::ClientOptions;
use uuid::Uuid;

struct BenchTelemetry;
impl SystemTelemetryProvider for BenchTelemetry {
    fn logical_processors(&self) -> u32 {
        12
    }
    fn sample_host(&mut self) -> HostTelemetrySample {
        HostTelemetrySample {
            logical_processors: 12,
            cpu_utilization_pct: MeasurementValue::Current(20.0),
            total_physical_memory_bytes: MeasurementValue::Current(32 * 1024 * 1024 * 1024),
            available_memory_bytes: MeasurementValue::Current(24 * 1024 * 1024 * 1024),
        }
    }
    fn sample_process(&mut self, _pid: u32) -> (MeasurementValue<f64>, MeasurementValue<u64>) {
        (MeasurementValue::Current(1.0), MeasurementValue::Current(32 * 1024 * 1024))
    }
    fn forget_process(&mut self, _pid: u32) {}
}

fn bench_scheduler_100_workloads(c: &mut Criterion) {
    c.bench_function("scheduler_enqueue_and_dispatch_100", |b| {
        b.iter(|| {
            let cfg = AppConfig { max_concurrent_workloads: 64, ..Default::default() };
            let mut sched = SchedulerEngine::new();
            let mut res = ResourceManager::new(Box::new(BenchTelemetry));
            let mut workers = (0..4)
                .map(|i| WorkerInfo {
                    id: WorkerId::new(),
                    name: format!("w-{i}"),
                    pid: Some(1000 + i),
                    status: WorkerStatus::Idle,
                    protocol_version: 1,
                    max_cpu_cores: 16,
                    max_memory_mb: 8192,
                    reserved_cpu_cores: 0,
                    reserved_memory_mb: 0,
                    current_workload_id: None,
                    current_execution_id: None,
                    cpu_usage_pct: MeasurementValue::Current(0.0),
                    memory_usage_bytes: MeasurementValue::Current(1024),
                    started_at_utc: 100 + i64::from(i),
                    last_heartbeat_utc: Some(100),
                    completed_executions: 0,
                    failed_executions: 0,
                    isolation_note: "bench".into(),
                })
                .collect::<Vec<_>>();

            let mut map = HashMap::new();
            for i in 0..100 {
                let mut wl = Workload::new(
                    format!("wl-{i}"),
                    "",
                    if i % 4 == 0 { Priority::High } else { Priority::Normal },
                    WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                        worker_threads: 1,
                        iterations: 50,
                        sieve_limit: 500,
                    }),
                    ResourceRequirements {
                        cpu_cores: 1,
                        memory_mb: 64,
                        requires_exclusive_worker: false,
                    },
                    10_000,
                    RetryPolicy::default(),
                    1000 + i,
                )
                .unwrap();
                sched.enqueue(&mut wl, &cfg, 1000 + i).unwrap();
                map.insert(wl.id, wl);
            }
            let dispatches = sched.schedule_ready(&mut map, &mut workers, &mut res, &cfg, 2000);
            black_box(dispatches.len());
        });
    });
}

fn bench_ipc_codec_and_named_pipe(c: &mut Criterion) {
    let env = IpcEnvelope::new(
        1,
        1_700_000_000,
        IpcMessage::Heartbeat(HeartbeatMsg {
            worker_id: WorkerId::new(),
            active_execution_id: None,
            cpu_usage_pct: Some(34.5),
            memory_usage_bytes: Some(64 * 1024 * 1024),
            uptime_ms: 12_345,
        }),
    );

    c.bench_function("ipc_frame_encode_decode", |b| {
        b.iter(|| {
            let bytes = encode_frame(black_box(&env)).unwrap();
            let decoded = decode_frame_payload(black_box(&bytes[4..])).unwrap();
            black_box(decoded);
        });
    });

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let pipe_name = format!(r"\\.\pipe\helixgrid-bench-{}", Uuid::new_v4());
    let (mut srv_side, mut cli_side) = rt.block_on(async {
        let srv = create_secure_pipe_server(&pipe_name, true).unwrap();
        let cli = ClientOptions::new().open(&pipe_name).unwrap();
        srv.connect().await.unwrap();
        (srv, cli)
    });

    c.bench_function("ipc_named_pipe_roundtrip", |b| {
        b.iter(|| {
            rt.block_on(async {
                write_frame(&mut cli_side, &env).await.unwrap();
                let got = read_frame(&mut srv_side).await.unwrap();
                black_box(got);
            });
        });
    });
}

fn bench_sqlite_operations(c: &mut Criterion) {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("bench.db");
    let store = StorageEngine::open(&db_path).unwrap();

    c.bench_function("sqlite_insert_and_get_workload", |b| {
        b.iter(|| {
            let wl = Workload::new(
                "SQLite Bench",
                "desc",
                Priority::Normal,
                WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                    worker_threads: 1,
                    iterations: 100,
                    sieve_limit: 500,
                }),
                ResourceRequirements::default(),
                10_000,
                RetryPolicy::default(),
                1_700_000_000,
            )
            .unwrap();
            store.insert_workload(&wl).unwrap();
            let got = store.get_workload(wl.id).unwrap().unwrap();
            black_box(got);
        });
    });
}

fn bench_compute_kernels(c: &mut Criterion) {
    let tmp = tempfile::tempdir().unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let spec = WorkloadSpec::NumericalComputation(NumericalComputationInput {
        algorithm: NumericalAlgorithm::ConjugateGradientSolve,
        dimension: 500,
        max_iterations: 500,
        tolerance: 1e-8,
        seed: 7,
    });
    c.bench_function("compute_cg_solve_500", |b| {
        b.iter(|| {
            let res =
                execute_spec_blocking(&spec, tmp.path(), 10_000, None, &cancel, |_| {}).unwrap();
            black_box(res.result.duration_ms);
        });
    });
}

criterion_group!(
    benches,
    bench_scheduler_100_workloads,
    bench_ipc_codec_and_named_pipe,
    bench_sqlite_operations,
    bench_compute_kernels
);
criterion_main!(benches);
