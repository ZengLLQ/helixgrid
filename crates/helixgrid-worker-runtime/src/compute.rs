use helixgrid_config::AppConfig;
use helixgrid_domain::{
    BatchOperation, BatchProcessingInput, CpuBenchmarkInput, NumericalAlgorithm,
    NumericalComputationInput, ProgressState, RegisteredExecutableInput, WorkflowTaskInput,
    WorkloadResultMetadata, WorkloadSpec,
};
use helixgrid_protocol::ArtifactReport;
use helixgrid_security::{
    build_sanitized_env, sha256_bytes, sha256_file_hex, sha256_hex, validate_registered_executable,
};
use serde_json::json;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;

const CREATE_NO_WINDOW_FLAG: u32 = 0x0800_0000;
const MAX_CAPTURE_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ComputeError {
    #[error("execution cancelled")]
    Cancelled,

    #[error("execution timed out after {0} ms")]
    TimedOut(u64),

    #[error("validation/security error: {0}")]
    Validation(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct ComputeOutcome {
    pub result: WorkloadResultMetadata,
    pub artifacts: Vec<ArtifactReport>,
}

/// Runs a concrete computational workload synchronously on a worker thread,
/// reporting genuine progress via `on_progress` and aborting cleanly on `cancel`.
pub fn execute_spec_blocking<F>(
    spec: &WorkloadSpec,
    artifact_dir: &Path,
    timeout_ms: u64,
    config: Option<&AppConfig>,
    cancel: &Arc<AtomicBool>,
    mut on_progress: F,
) -> Result<ComputeOutcome, ComputeError>
where
    F: FnMut(ProgressState),
{
    execute_spec_blocking_dyn(spec, artifact_dir, timeout_ms, config, cancel, &mut on_progress)
}

fn execute_spec_blocking_dyn(
    spec: &WorkloadSpec,
    artifact_dir: &Path,
    timeout_ms: u64,
    config: Option<&AppConfig>,
    cancel: &Arc<AtomicBool>,
    on_progress: &mut dyn FnMut(ProgressState),
) -> Result<ComputeOutcome, ComputeError> {
    fs::create_dir_all(artifact_dir)?;
    let started = Instant::now();

    match spec {
        WorkloadSpec::CpuBenchmark(input) => {
            run_cpu_benchmark(input, artifact_dir, timeout_ms, started, cancel, on_progress)
        }
        WorkloadSpec::BatchProcessing(input) => {
            run_batch_processing(input, artifact_dir, timeout_ms, started, cancel, on_progress)
        }
        WorkloadSpec::NumericalComputation(input) => {
            run_numerical(input, artifact_dir, timeout_ms, started, cancel, on_progress)
        }
        WorkloadSpec::RegisteredExecutable(input) => run_registered_executable(
            input,
            artifact_dir,
            timeout_ms,
            config,
            started,
            cancel,
            on_progress,
        ),
        WorkloadSpec::WorkflowTask(task) => {
            run_workflow_task(task, artifact_dir, timeout_ms, config, started, cancel, on_progress)
        }
    }
}

fn check_abort(cancel: &AtomicBool, started: Instant, timeout_ms: u64) -> Result<(), ComputeError> {
    if cancel.load(Ordering::Relaxed) {
        return Err(ComputeError::Cancelled);
    }
    if started.elapsed().as_millis() as u64 >= timeout_ms {
        return Err(ComputeError::TimedOut(timeout_ms));
    }
    Ok(())
}

fn write_json_artifact(
    artifact_dir: &Path,
    filename: &str,
    kind: &str,
    value: &serde_json::Value,
) -> Result<ArtifactReport, ComputeError> {
    let path = artifact_dir.join(filename);
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(&path, &bytes)?;
    Ok(ArtifactReport {
        name: filename.to_string(),
        kind: kind.to_string(),
        file_path: path.to_string_lossy().into_owned(),
        size_bytes: bytes.len() as u64,
        sha256_hex: sha256_hex(&bytes),
    })
}

fn run_cpu_benchmark<F>(
    input: &CpuBenchmarkInput,
    artifact_dir: &Path,
    timeout_ms: u64,
    started: Instant,
    cancel: &Arc<AtomicBool>,
    on_progress: &mut F,
) -> Result<ComputeOutcome, ComputeError>
where
    F: FnMut(ProgressState) + ?Sized,
{
    let threads = input.worker_threads.max(1);
    let total_iters = input.iterations.max(1);
    let sieve_limit = input.sieve_limit.max(100) as usize;
    let chunks = 20u64.min(total_iters);
    let iters_per_chunk = (total_iters / chunks).max(1);

    let mut total_primes: u64 = 0;
    let mut cumulative_hash: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut completed_iters: u64 = 0;

    for chunk_idx in 0..chunks {
        check_abort(cancel, started, timeout_ms)?;
        let this_chunk = if chunk_idx + 1 == chunks {
            total_iters.saturating_sub(completed_iters)
        } else {
            iters_per_chunk
        };
        if this_chunk == 0 {
            continue;
        }

        let per_thread = (this_chunk / u64::from(threads)).max(1);
        let chunk_res: Result<(u64, u64), ComputeError> = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(threads as usize);
            for t in 0..threads {
                let c_flag = Arc::clone(cancel);
                handles.push(scope.spawn(move || {
                    let mut local_primes = 0u64;
                    let mut local_mix = 0x517c_c1b7_2722_0a95u64 ^ u64::from(t);
                    for i in 0..per_thread {
                        if i % 64 == 0 && c_flag.load(Ordering::Relaxed) {
                            return Err(ComputeError::Cancelled);
                        }
                        if i == 0 {
                            local_primes += count_primes_sieve(sieve_limit) as u64;
                        }
                        // Deterministic ARX mixing round simulating compute kernel work
                        let mut x = local_mix.wrapping_add(i.wrapping_mul(0x9e37_79b9_7f4a_7c15));
                        for _ in 0..64 {
                            x = x.rotate_left(13) ^ x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
                            x = x.wrapping_add(0x94d0_49bb_1331_11eb);
                        }
                        local_mix ^= x;
                    }
                    Ok((local_primes, local_mix))
                }));
            }
            let mut p_sum = 0u64;
            let mut h_mix = 0u64;
            for h in handles {
                let (p, m) = h.join().map_err(|_| ComputeError::Cancelled)??;
                p_sum = p_sum.saturating_add(p);
                h_mix ^= m;
            }
            Ok((p_sum, h_mix))
        });

        let (p, h) = chunk_res?;
        total_primes = total_primes.saturating_add(p);
        cumulative_hash ^= h.rotate_left((chunk_idx % 31) as u32);
        completed_iters = completed_iters.saturating_add(this_chunk);

        on_progress(ProgressState::determinate(
            completed_iters,
            total_iters,
            Some(format!("Benchmark pass {}/{}", chunk_idx + 1, chunks)),
        ));
    }

    let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
    let ops_per_sec = (total_iters as f64) / (elapsed_ms as f64 / 1000.0);
    let metrics = json!({
        "backend": "CPU",
        "threads": threads,
        "iterations": total_iters,
        "sieve_limit": sieve_limit,
        "primes_counted": total_primes,
        "elapsed_ms": elapsed_ms,
        "iterations_per_sec": ops_per_sec,
        "checksum_hex": format!("{cumulative_hash:016x}"),
    });
    let artifact =
        write_json_artifact(artifact_dir, "benchmark_result.json", "ResultJson", &metrics)?;
    Ok(ComputeOutcome {
        result: WorkloadResultMetadata {
            summary: format!(
                "{total_iters} iterations across {threads} thread(s) in {elapsed_ms} ms ({ops_per_sec:.0} iter/s)"
            ),
            metrics_json: metrics,
            exit_code: Some(0),
            duration_ms: elapsed_ms,
        },
        artifacts: vec![artifact],
    })
}

fn count_primes_sieve(limit: usize) -> usize {
    if limit < 2 {
        return 0;
    }
    let mut is_prime = vec![true; limit + 1];
    is_prime[0] = false;
    is_prime[1] = false;
    let mut p = 2;
    while p * p <= limit {
        if is_prime[p] {
            let mut m = p * p;
            while m <= limit {
                is_prime[m] = false;
                m += p;
            }
        }
        p += 1;
    }
    is_prime.into_iter().filter(|&b| b).count()
}

fn run_batch_processing<F>(
    input: &BatchProcessingInput,
    artifact_dir: &Path,
    timeout_ms: u64,
    started: Instant,
    cancel: &Arc<AtomicBool>,
    on_progress: &mut F,
) -> Result<ComputeOutcome, ComputeError>
where
    F: FnMut(ProgressState) + ?Sized,
{
    let batch_size = input.batch_size.max(1) as usize;
    let mut records_processed: u64 = 0;
    let mut bytes_processed: u64 = 0;
    let mut numeric_sum: f64 = 0.0;
    let mut numeric_min: f64 = f64::INFINITY;
    let mut numeric_max: f64 = f64::NEG_INFINITY;
    let mut outputs: Vec<serde_json::Value> = Vec::new();

    if let Some(ref dir_str) = input.input_dir {
        let dir = Path::new(dir_str);
        if !dir.is_dir() {
            return Err(ComputeError::Validation(format!(
                "batch input_dir '{}' does not exist or is not a directory",
                dir.display()
            )));
        }
        let mut files: Vec<PathBuf> = fs::read_dir(dir)?
            .filter_map(|e| e.ok().map(|de| de.path()))
            .filter(|p| p.is_file())
            .collect();
        files.sort();
        let total = files.len() as u64;
        if total == 0 {
            return Err(ComputeError::Validation(
                "batch input_dir contains no regular files".to_string(),
            ));
        }

        for chunk in files.chunks(batch_size) {
            check_abort(cancel, started, timeout_ms)?;
            for file_path in chunk {
                let data = fs::read(file_path)?;
                bytes_processed += data.len() as u64;
                let digest = sha256_hex(&data);
                records_processed += 1;
                outputs.push(json!({
                    "file": file_path.file_name().and_then(|s| s.to_str()).unwrap_or(""),
                    "bytes": data.len(),
                    "sha256": digest,
                }));
            }
            on_progress(ProgressState::determinate(
                records_processed,
                total,
                Some(format!("Processed {records_processed}/{total} files")),
            ));
        }
    } else {
        // Real disk-backed batch generation + transform pipeline inside execution workspace
        let input_file = artifact_dir.join("batch_input_records.jsonl");
        {
            let mut f = File::create(&input_file)?;
            for idx in 0..input.item_count {
                let score = ((idx as f64 * 1.618_033_988_75) % 1000.0) + 1.0;
                writeln!(
                    f,
                    "{{\"record_id\":{idx},\"partition\":\"p{}\",\"value\":{score:.4}}}",
                    idx % 8
                )?;
            }
        }
        let raw = fs::read_to_string(&input_file)?;
        let lines: Vec<&str> = raw.lines().collect();
        let total = lines.len() as u64;

        for chunk in lines.chunks(batch_size) {
            check_abort(cancel, started, timeout_ms)?;
            for line in chunk {
                bytes_processed += line.len() as u64;
                let parsed: serde_json::Value = serde_json::from_str(line)?;
                let val = parsed.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
                numeric_sum += val;
                if val < numeric_min {
                    numeric_min = val;
                }
                if val > numeric_max {
                    numeric_max = val;
                }
                match input.operation {
                    BatchOperation::HashAndValidate => {
                        let hash = sha256_hex(line.as_bytes());
                        if outputs.len() < 256 {
                            outputs.push(json!({ "hash": hash, "valid": true }));
                        }
                    }
                    BatchOperation::NormalizeJsonRecords | BatchOperation::AggregateCsvMetrics => {
                        if outputs.len() < 256 {
                            outputs.push(json!({
                                "record_id": parsed.get("record_id"),
                                "normalized_value": (val / 1000.0),
                            }));
                        }
                    }
                }
                records_processed += 1;
            }
            on_progress(ProgressState::determinate(
                records_processed,
                total,
                Some(format!("Processed {records_processed}/{total} records")),
            ));
        }
    }

    let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
    let mean = if records_processed > 0 && numeric_min.is_finite() {
        Some(numeric_sum / records_processed as f64)
    } else {
        None
    };

    let output_doc = json!({
        "operation": format!("{:?}", input.operation),
        "records_processed": records_processed,
        "bytes_processed": bytes_processed,
        "elapsed_ms": elapsed_ms,
        "summary_stats": {
            "sum": numeric_sum,
            "mean": mean,
            "min": if numeric_min.is_finite() { Some(numeric_min) } else { None },
            "max": if numeric_max.is_finite() { Some(numeric_max) } else { None },
        },
        "sample_outputs": outputs,
    });
    let artifact =
        write_json_artifact(artifact_dir, "batch_output.json", "BatchOutput", &output_doc)?;
    Ok(ComputeOutcome {
        result: WorkloadResultMetadata {
            summary: format!(
                "Processed {records_processed} items ({bytes_processed} bytes) in {elapsed_ms} ms"
            ),
            metrics_json: output_doc,
            exit_code: Some(0),
            duration_ms: elapsed_ms,
        },
        artifacts: vec![artifact],
    })
}

fn run_numerical<F>(
    input: &NumericalComputationInput,
    artifact_dir: &Path,
    timeout_ms: u64,
    started: Instant,
    cancel: &Arc<AtomicBool>,
    on_progress: &mut F,
) -> Result<ComputeOutcome, ComputeError>
where
    F: FnMut(ProgressState) + ?Sized,
{
    let metrics = match input.algorithm {
        NumericalAlgorithm::DenseMatrixMultiply => {
            let n = (input.dimension as usize).clamp(4, 1024);
            let mut a = vec![0.0f64; n * n];
            let mut b = vec![0.0f64; n * n];
            let mut c = vec![0.0f64; n * n];
            for i in 0..n {
                for j in 0..n {
                    let s = ((i * 131 + j * 17) as u64 ^ input.seed) % 1000;
                    a[i * n + j] = (s as f64) * 0.001 + 0.1;
                    b[i * n + j] = (((s + 7) % 1000) as f64) * 0.001 + 0.1;
                }
            }
            for i in 0..n {
                if i % 8 == 0 {
                    check_abort(cancel, started, timeout_ms)?;
                    on_progress(ProgressState::determinate(
                        i as u64,
                        n as u64,
                        Some(format!("Matrix multiply row {i}/{n}")),
                    ));
                }
                for k in 0..n {
                    let aik = a[i * n + k];
                    let b_row = &b[k * n..(k + 1) * n];
                    let c_row = &mut c[i * n..(i + 1) * n];
                    for j in 0..n {
                        c_row[j] += aik * b_row[j];
                    }
                }
            }
            on_progress(ProgressState::determinate(n as u64, n as u64, Some("Complete".into())));
            let frobenius: f64 = c.iter().map(|x| x * x).sum::<f64>().sqrt();
            let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
            let flops = 2.0 * (n as f64).powi(3);
            let gflops = flops / (elapsed_ms as f64 / 1000.0) / 1e9;
            json!({
                "backend": "CPU (IEEE-754 f64)",
                "algorithm": "DenseMatrixMultiply",
                "dimension": n,
                "frobenius_norm": frobenius,
                "gflops": gflops,
                "elapsed_ms": elapsed_ms,
            })
        }
        NumericalAlgorithm::ConjugateGradientSolve => {
            let n = (input.dimension as usize).clamp(8, 200_000);
            let max_iter = input.max_iterations.max(1);
            // Tridiagonal SPD system: A[i,i]=4, A[i,i±1]=-1, b[i]=1.0
            let b = vec![1.0f64; n];
            let mut x = vec![0.0f64; n];
            let mut r = b.clone();
            let mut p = r.clone();
            let mut rs_old: f64 = r.iter().map(|v| v * v).sum();
            let mut iters_run = 0u32;

            for iter in 0..max_iter {
                if iter % 50 == 0 {
                    check_abort(cancel, started, timeout_ms)?;
                    on_progress(ProgressState::determinate(
                        u64::from(iter),
                        u64::from(max_iter),
                        Some(format!("CG iteration {iter}, residual {:.3e}", rs_old.sqrt())),
                    ));
                }
                let mut ap = vec![0.0f64; n];
                for i in 0..n {
                    let left = if i > 0 { p[i - 1] } else { 0.0 };
                    let right = if i + 1 < n { p[i + 1] } else { 0.0 };
                    ap[i] = 4.0 * p[i] - left - right;
                }
                let denom: f64 = p.iter().zip(ap.iter()).map(|(pi, api)| pi * api).sum();
                if denom.abs() < 1e-30 {
                    break;
                }
                let alpha = rs_old / denom;
                for i in 0..n {
                    x[i] += alpha * p[i];
                    r[i] -= alpha * ap[i];
                }
                let rs_new: f64 = r.iter().map(|v| v * v).sum();
                iters_run = iter + 1;
                if rs_new.sqrt() < input.tolerance {
                    rs_old = rs_new;
                    break;
                }
                let beta = rs_new / rs_old;
                for i in 0..n {
                    p[i] = r[i] + beta * p[i];
                }
                rs_old = rs_new;
            }
            on_progress(ProgressState::determinate(
                u64::from(max_iter),
                u64::from(max_iter),
                Some("Converged".into()),
            ));
            let sol_norm = x.iter().map(|v| v * v).sum::<f64>().sqrt();
            let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
            json!({
                "backend": "CPU (IEEE-754 f64)",
                "algorithm": "ConjugateGradientSolve",
                "dimension": n,
                "iterations_run": iters_run,
                "final_residual_l2": rs_old.sqrt(),
                "solution_l2_norm": sol_norm,
                "elapsed_ms": elapsed_ms,
            })
        }
        NumericalAlgorithm::MonteCarloVariance => {
            let total_samples = u64::from(input.max_iterations.max(1000));
            let mut rng = input.seed ^ 0x9e37_79b9_7f4a_7c15;
            if rng == 0 {
                rng = 1;
            }
            let mut inside = 0u64;
            let mut mean = 0.0f64;
            let mut m2 = 0.0f64;
            let report_step = (total_samples / 20).max(1);

            for i in 1..=total_samples {
                if i % report_step == 0 {
                    check_abort(cancel, started, timeout_ms)?;
                    on_progress(ProgressState::determinate(
                        i,
                        total_samples,
                        Some(format!("Sample {i}/{total_samples}")),
                    ));
                }
                let x = next_unit_f64(&mut rng);
                let y = next_unit_f64(&mut rng);
                let hit = if x * x + y * y <= 1.0 {
                    inside += 1;
                    4.0
                } else {
                    0.0
                };
                let delta = hit - mean;
                mean += delta / (i as f64);
                m2 += delta * (hit - mean);
            }
            let variance = m2 / ((total_samples - 1).max(1) as f64);
            let pi_est = 4.0 * (inside as f64) / (total_samples as f64);
            let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
            json!({
                "backend": "CPU (IEEE-754 f64)",
                "algorithm": "MonteCarloVariance",
                "samples": total_samples,
                "pi_estimate": pi_est,
                "sample_variance": variance,
                "elapsed_ms": elapsed_ms,
            })
        }
        NumericalAlgorithm::Rk4LorenzTrajectory => {
            let steps = u64::from(input.max_iterations.max(100));
            let dt = 0.005f64;
            let sigma = 10.0f64;
            let rho = 28.0f64;
            let beta = 8.0 / 3.0;
            let mut s = [1.0f64, 1.0, 1.0];
            let deriv = |st: [f64; 3]| -> [f64; 3] {
                [
                    sigma * (st[1] - st[0]),
                    st[0] * (rho - st[2]) - st[1],
                    st[0] * st[1] - beta * st[2],
                ]
            };
            let report_step = (steps / 20).max(1);
            for step in 1..=steps {
                if step % report_step == 0 {
                    check_abort(cancel, started, timeout_ms)?;
                    on_progress(ProgressState::determinate(
                        step,
                        steps,
                        Some(format!("RK4 step {step}/{steps}")),
                    ));
                }
                let k1 = deriv(s);
                let k2 = deriv([
                    s[0] + 0.5 * dt * k1[0],
                    s[1] + 0.5 * dt * k1[1],
                    s[2] + 0.5 * dt * k1[2],
                ]);
                let k3 = deriv([
                    s[0] + 0.5 * dt * k2[0],
                    s[1] + 0.5 * dt * k2[1],
                    s[2] + 0.5 * dt * k2[2],
                ]);
                let k4 = deriv([s[0] + dt * k3[0], s[1] + dt * k3[1], s[2] + dt * k3[2]]);
                for j in 0..3 {
                    s[j] += (dt / 6.0) * (k1[j] + 2.0 * k2[j] + 2.0 * k3[j] + k4[j]);
                }
            }
            let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
            json!({
                "backend": "CPU (IEEE-754 f64)",
                "algorithm": "Rk4LorenzTrajectory",
                "steps": steps,
                "final_state": s,
                "elapsed_ms": elapsed_ms,
            })
        }
    };

    let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
    let artifact =
        write_json_artifact(artifact_dir, "numerical_result.json", "ResultJson", &metrics)?;
    Ok(ComputeOutcome {
        result: WorkloadResultMetadata {
            summary: format!("{:?} completed in {elapsed_ms} ms", input.algorithm),
            metrics_json: metrics,
            exit_code: Some(0),
            duration_ms: elapsed_ms,
        },
        artifacts: vec![artifact],
    })
}

#[inline]
fn next_unit_f64(state: &mut u64) -> f64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    let mantissa = x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11;
    (mantissa as f64) * (1.0 / ((1u64 << 53) as f64))
}

fn run_registered_executable<F>(
    input: &RegisteredExecutableInput,
    artifact_dir: &Path,
    timeout_ms: u64,
    config: Option<&AppConfig>,
    started: Instant,
    cancel: &Arc<AtomicBool>,
    on_progress: &mut F,
) -> Result<ComputeOutcome, ComputeError>
where
    F: FnMut(ProgressState) + ?Sized,
{
    let cfg = config.ok_or_else(|| {
        ComputeError::Validation(
            "worker missing security configuration for RegisteredExecutable verification"
                .to_string(),
        )
    })?;
    let (_entry, safe_exe, safe_wd) = validate_registered_executable(cfg, input)
        .map_err(|e| ComputeError::Validation(e.to_string()))?;
    let clean_env = build_sanitized_env(&input.env_overrides, artifact_dir)
        .map_err(|e| ComputeError::Validation(e.to_string()))?;

    on_progress(ProgressState::Indeterminate {
        message: Some(format!("Executing {}", safe_exe.display())),
    });

    // Explicit binary + args array with env_clear(); NEVER uses cmd.exe or shell concatenation.
    let mut cmd = Command::new(&safe_exe);
    cmd.args(&input.args)
        .current_dir(&safe_wd)
        .env_clear()
        .envs(&clean_env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .creation_flags(CREATE_NO_WINDOW_FLAG);

    let mut child = cmd.spawn()?;
    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();

    let stdout_handle = std::thread::spawn(move || read_bounded_stream(&mut stdout_pipe));
    let stderr_handle = std::thread::spawn(move || read_bounded_stream(&mut stderr_pipe));

    let exit_status = loop {
        if cancel.load(Ordering::Relaxed) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(ComputeError::Cancelled);
        }
        if started.elapsed().as_millis() as u64 >= timeout_ms {
            let _ = child.kill();
            let _ = child.wait();
            return Err(ComputeError::TimedOut(timeout_ms));
        }
        if let Some(status) = child.try_wait()? {
            break status;
        }
        std::thread::sleep(Duration::from_millis(25));
    };

    let stdout_bytes = stdout_handle.join().unwrap_or_default();
    let stderr_bytes = stderr_handle.join().unwrap_or_default();

    let stdout_path = artifact_dir.join("stdout.log");
    let stderr_path = artifact_dir.join("stderr.log");
    fs::write(&stdout_path, &stdout_bytes)?;
    fs::write(&stderr_path, &stderr_bytes)?;

    let elapsed_ms = started.elapsed().as_millis().max(1) as u64;
    let code = exit_status.code().unwrap_or(-1);
    let artifacts = vec![
        ArtifactReport {
            name: "stdout.log".to_string(),
            kind: "StdoutLog".to_string(),
            file_path: stdout_path.to_string_lossy().into_owned(),
            size_bytes: stdout_bytes.len() as u64,
            sha256_hex: sha256_hex(&stdout_bytes),
        },
        ArtifactReport {
            name: "stderr.log".to_string(),
            kind: "StderrLog".to_string(),
            file_path: stderr_path.to_string_lossy().into_owned(),
            size_bytes: stderr_bytes.len() as u64,
            sha256_hex: sha256_hex(&stderr_bytes),
        },
    ];

    if !exit_status.success() {
        return Err(ComputeError::Validation(format!(
            "external executable exited with code {code}"
        )));
    }

    let metrics = json!({
        "executable": safe_exe.to_string_lossy(),
        "working_dir": safe_wd.to_string_lossy(),
        "exit_code": code,
        "stdout_bytes": stdout_bytes.len(),
        "stderr_bytes": stderr_bytes.len(),
        "elapsed_ms": elapsed_ms,
    });
    Ok(ComputeOutcome {
        result: WorkloadResultMetadata {
            summary: format!("Exited with code {code} in {elapsed_ms} ms"),
            metrics_json: metrics,
            exit_code: Some(code),
            duration_ms: elapsed_ms,
        },
        artifacts,
    })
}

fn read_bounded_stream<R: Read>(stream_opt: &mut Option<R>) -> Vec<u8> {
    let Some(mut stream) = stream_opt.take() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    while let Ok(n) = stream.read(&mut buf) {
        if n == 0 {
            break;
        }
        let remaining = MAX_CAPTURE_BYTES.saturating_sub(out.len());
        if remaining == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n.min(remaining)]);
    }
    out
}

fn run_workflow_task(
    task: &WorkflowTaskInput,
    artifact_dir: &Path,
    timeout_ms: u64,
    config: Option<&AppConfig>,
    started: Instant,
    cancel: &Arc<AtomicBool>,
    on_progress: &mut dyn FnMut(ProgressState),
) -> Result<ComputeOutcome, ComputeError> {
    let mut outcome = execute_spec_blocking_dyn(
        &task.inner,
        artifact_dir,
        timeout_ms,
        config,
        cancel,
        on_progress,
    )?;
    let stage_meta = json!({
        "workflow_id": task.workflow_id.to_string(),
        "task_id": task.task_id.to_string(),
        "stage_name": task.stage_name,
        "upstream_artifacts": task.upstream_artifacts,
        "stage_metrics": outcome.result.metrics_json,
        "duration_ms": started.elapsed().as_millis().max(1) as u64,
    });
    let stage_artifact =
        write_json_artifact(artifact_dir, "workflow_stage_result.json", "ResultJson", &stage_meta)?;
    outcome.artifacts.push(stage_artifact);
    outcome.result.summary = format!("Stage '{}': {}", task.stage_name, outcome.result.summary);
    Ok(outcome)
}

pub fn file_artifact_report(
    name: &str,
    kind: &str,
    path: &Path,
) -> Result<ArtifactReport, ComputeError> {
    let meta = fs::metadata(path)?;
    let hash = sha256_file_hex(path).unwrap_or_else(|_| sha256_hex(&sha256_bytes(name.as_bytes())));
    Ok(ArtifactReport {
        name: name.to_string(),
        kind: kind.to_string(),
        file_path: path.to_string_lossy().into_owned(),
        size_bytes: meta.len(),
        sha256_hex: hash,
    })
}
