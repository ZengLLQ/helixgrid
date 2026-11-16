use anyhow::Result;
use clap::Parser;
use helixgrid_domain::WorkerId;
use helixgrid_observability::init_tracing;
use helixgrid_worker_runtime::{WorkerRuntimeConfig, run_worker_runtime};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "helixgrid-worker", version, about = "HelixGrid isolated worker runtime")]
struct Args {
    #[arg(long)]
    worker_id: String,

    #[arg(long)]
    pipe_name: String,

    #[arg(long)]
    auth_token: String,

    #[arg(long)]
    logs_dir: PathBuf,

    #[arg(long)]
    config_path: Option<PathBuf>,

    #[arg(long, default_value_t = 4)]
    max_cpu_cores: u32,

    #[arg(long, default_value_t = 2048)]
    max_memory_mb: u64,

    #[arg(long, default_value_t = 1500)]
    heartbeat_interval_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let _ = init_tracing(&args.logs_dir, "helixgrid-worker", 10 * 1024 * 1024, 3);

    let worker_id: WorkerId = args.worker_id.parse()?;
    let runtime_cfg = WorkerRuntimeConfig {
        worker_id,
        pipe_name: args.pipe_name,
        auth_token: args.auth_token,
        config_path: args.config_path,
        max_cpu_cores: args.max_cpu_cores,
        max_memory_mb: args.max_memory_mb,
        heartbeat_interval_ms: args.heartbeat_interval_ms,
    };

    run_worker_runtime(runtime_cfg).await?;
    Ok(())
}
