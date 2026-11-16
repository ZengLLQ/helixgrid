#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod presenter;

use anyhow::Result;
use clap::Parser;
use helixgrid_config::{
    AppConfig, AppPaths, RegisteredExecutableEntry, SchedulingPolicy, ThemePreference,
};
use helixgrid_core::{EngineSnapshot, HelixGridEngine, SubmitWorkloadRequest};
use helixgrid_domain::{
    CpuBenchmarkInput, Priority, ResourceRequirements, RetryPolicy, RetrySafety, WorkloadSpec,
    WorkloadStatus,
};
use helixgrid_observability::init_tracing;
use helixgrid_platform_windows::dialogs::{FileDialogMode, show_native_file_dialog};
use presenter::{
    apply_activity_filter, apply_snapshot_to_window, apply_workloads_filter,
    build_workflow_tasks_from_dialog, build_workload_spec_from_dialog,
};
use slint::ComponentHandle;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

slint::include_modules!();

#[derive(Debug, Parser)]
#[command(name = "helixgrid", version, about = "HelixGrid Desktop Compute Studio")]
struct Cli {
    /// Override local application data directory (defaults to %LocalAppData%\HelixGrid)
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Override path to helixgrid-worker.exe
    #[arg(long)]
    worker_exe: Option<PathBuf>,

    /// Run headless startup, submit a real CPU benchmark workload, verify completion, and exit
    #[arg(long)]
    smoke_test: bool,
}

enum UiAction {
    Refresh,
    SpawnWorker,
    ShutdownWorker(String),
    RestartWorker(String),
    SubmitWorkload(SubmitWorkloadRequest),
    CancelWorkload(String),
    SafeRetryWorkload(String),
    CreateWorkflow {
        name: String,
        description: String,
        tasks: Vec<helixgrid_domain::WorkflowTaskDefinition>,
    },
    CancelWorkflow(String),
    UpdateConfig(AppConfig),
    RegisterExecutable(RegisteredExecutableEntry),
    RemoveRegisteredExecutable(String),
    BackupDatabase(PathBuf),
    ExportDiagnostics,
    Shutdown,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let paths = match cli.data_dir {
        Some(dir) => AppPaths::from_root(dir),
        None => AppPaths::resolve_default()?,
    };
    paths.ensure_directories()?;
    let _ = init_tracing(&paths.logs_dir, "helixgrid-studio", 10 * 1024 * 1024, 5);

    if cli.smoke_test {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        return rt.block_on(run_smoke_test(paths, cli.worker_exe));
    }

    let window = AppWindow::new()?;
    window.set_data_dir_path(paths.root_dir.to_string_lossy().as_ref().into());

    let latest_snap: Arc<Mutex<Option<Arc<EngineSnapshot>>>> = Arc::new(Mutex::new(None));
    let (ui_action_tx, mut ui_action_rx) = mpsc::channel::<UiAction>(128);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

    let win_weak = window.as_weak();
    let snap_store = Arc::clone(&latest_snap);
    let worker_override = cli.worker_exe.clone();
    let paths_bg = paths.clone();

    // Dedicated background Tokio runtime keeping UI event loop completely non-blocking.
    let bg_thread = std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => {
                let _ = ready_tx.send(Err(e.to_string()));
                return;
            }
        };

        rt.block_on(async move {
            let engine = match HelixGridEngine::start(paths_bg, worker_override, None).await {
                Ok(e) => {
                    let _ = ready_tx.send(Ok(()));
                    e
                }
                Err(err) => {
                    let _ = ready_tx.send(Err(err.to_string()));
                    return;
                }
            };

            let mut snap_rx = engine.subscribe_snapshots();
            {
                let initial = engine.snapshot();
                if let Ok(mut g) = snap_store.lock() {
                    *g = Some(initial.clone());
                }
                let w = win_weak.clone();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(app) = w.upgrade() {
                        apply_snapshot_to_window(&app, &initial, true);
                    }
                });
            }

            let snap_watch_store = Arc::clone(&snap_store);
            let snap_watch_win = win_weak.clone();
            let watch_task = tokio::spawn(async move {
                while snap_rx.changed().await.is_ok() {
                    let s = snap_rx.borrow().clone();
                    if let Ok(mut g) = snap_watch_store.lock() {
                        *g = Some(s.clone());
                    }
                    let w = snap_watch_win.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(app) = w.upgrade() {
                            apply_snapshot_to_window(&app, &s, false);
                        }
                    });
                }
            });

            while let Some(action) = ui_action_rx.recv().await {
                match action {
                    UiAction::Shutdown => {
                        engine.shutdown().await;
                        break;
                    }
                    other => handle_ui_action(&engine, other, &win_weak).await,
                }
            }
            watch_task.abort();
        });
    });

    if let Ok(Err(err)) = ready_rx.recv() {
        window.set_toast_is_error(true);
        window.set_toast_message(format!("Engine startup failed: {err}").into());
    }

    wire_window_callbacks(&window, ui_action_tx.clone(), Arc::clone(&latest_snap));
    window.run()?;

    let _ = ui_action_tx.blocking_send(UiAction::Shutdown);
    let _ = bg_thread.join();
    Ok(())
}

async fn handle_ui_action(
    engine: &HelixGridEngine,
    action: UiAction,
    win_weak: &slint::Weak<AppWindow>,
) {
    match action {
        UiAction::Refresh => {
            if let Err(e) = engine.refresh_snapshot().await {
                show_toast(win_weak, format!("Refresh failed: {e}"), true);
            }
        }
        UiAction::SpawnWorker => match engine.spawn_worker().await {
            Ok(w) => show_toast(win_weak, format!("Spawned {} (PID {:?})", w.name, w.pid), false),
            Err(e) => show_toast(win_weak, format!("Spawn worker failed: {e}"), true),
        },
        UiAction::ShutdownWorker(id_s) => {
            if let Ok(id) = id_s.parse() {
                match engine.shutdown_worker(id).await {
                    Ok(()) => show_toast(win_weak, "Worker shut down cleanly".into(), false),
                    Err(e) => show_toast(win_weak, format!("Worker shutdown failed: {e}"), true),
                }
            }
        }
        UiAction::RestartWorker(id_s) => {
            if let Ok(id) = id_s.parse() {
                match engine.restart_worker(id).await {
                    Ok(w) => show_toast(win_weak, format!("Restarted as {}", w.name), false),
                    Err(e) => show_toast(win_weak, format!("Worker restart failed: {e}"), true),
                }
            }
        }
        UiAction::SubmitWorkload(req) => match engine.submit_workload(req).await {
            Ok(_) => show_toast(win_weak, "Workload queued for execution".into(), false),
            Err(e) => show_toast(win_weak, format!("Workload rejected: {e}"), true),
        },
        UiAction::CancelWorkload(id_s) => {
            if let Ok(id) = id_s.parse() {
                match engine.cancel_workload(id).await {
                    Ok(()) => show_toast(win_weak, "Cancellation requested".into(), false),
                    Err(e) => show_toast(win_weak, format!("Cancel failed: {e}"), true),
                }
            }
        }
        UiAction::SafeRetryWorkload(id_s) => {
            if let Ok(id) = id_s.parse() {
                match engine.safe_retry_workload(id).await {
                    Ok(_) => show_toast(win_weak, "Safe retry queued".into(), false),
                    Err(e) => show_toast(win_weak, format!("Safe retry rejected: {e}"), true),
                }
            }
        }
        UiAction::CreateWorkflow { name, description, tasks } => {
            match engine.submit_and_start_workflow(name, description, tasks).await {
                Ok(_) => show_toast(win_weak, "Workflow launched".into(), false),
                Err(e) => show_toast(win_weak, format!("Workflow failed: {e}"), true),
            }
        }
        UiAction::CancelWorkflow(id_s) => {
            if let Ok(id) = id_s.parse() {
                match engine.cancel_workflow(id).await {
                    Ok(()) => show_toast(win_weak, "Workflow cancelled".into(), false),
                    Err(e) => show_toast(win_weak, format!("Workflow cancel failed: {e}"), true),
                }
            }
        }
        UiAction::UpdateConfig(cfg) => match engine.update_config(cfg).await {
            Ok(()) => show_toast(win_weak, "Settings saved".into(), false),
            Err(e) => show_toast(win_weak, format!("Invalid settings: {e}"), true),
        },
        UiAction::RegisterExecutable(entry) => match engine.register_executable(entry).await {
            Ok(()) => show_toast(win_weak, "Executable registered in allowlist".into(), false),
            Err(e) => show_toast(win_weak, format!("Registration failed: {e}"), true),
        },
        UiAction::RemoveRegisteredExecutable(id) => {
            match engine.remove_registered_executable(id).await {
                Ok(()) => show_toast(win_weak, "Executable removed from allowlist".into(), false),
                Err(e) => show_toast(win_weak, format!("Remove failed: {e}"), true),
            }
        }
        UiAction::BackupDatabase(dest) => match engine.backup_database(dest).await {
            Ok(p) => show_toast(win_weak, format!("Backup written to {}", p.display()), false),
            Err(e) => show_toast(win_weak, format!("Backup failed: {e}"), true),
        },
        UiAction::ExportDiagnostics => match engine.export_diagnostics(None).await {
            Ok(p) => {
                show_toast(win_weak, format!("Diagnostics exported to {}", p.display()), false)
            }
            Err(e) => show_toast(win_weak, format!("Export failed: {e}"), true),
        },
        UiAction::Shutdown => {}
    }
}

fn show_toast(win_weak: &slint::Weak<AppWindow>, msg: String, is_err: bool) {
    let w = win_weak.clone();
    let _ = slint::invoke_from_event_loop(move || {
        if let Some(app) = w.upgrade() {
            app.set_toast_is_error(is_err);
            app.set_toast_message(msg.into());
        }
    });
}

fn wire_window_callbacks(
    window: &AppWindow,
    tx: mpsc::Sender<UiAction>,
    snap_store: Arc<Mutex<Option<Arc<EngineSnapshot>>>>,
) {
    let tx_r = tx.clone();
    window.on_refresh_requested(move || {
        let _ = tx_r.try_send(UiAction::Refresh);
    });

    let tx_sw = tx.clone();
    window.on_spawn_worker(move || {
        let _ = tx_sw.try_send(UiAction::SpawnWorker);
    });

    let tx_sd = tx.clone();
    window.on_shutdown_worker(move |id| {
        let _ = tx_sd.try_send(UiAction::ShutdownWorker(id.into()));
    });

    let tx_rw = tx.clone();
    window.on_restart_worker(move |id| {
        let _ = tx_rw.try_send(UiAction::RestartWorker(id.into()));
    });

    let w_sub = window.as_weak();
    let s_sub = Arc::clone(&snap_store);
    let tx_sub = tx.clone();
    window.on_submit_workload(move || {
        let Some(app) = w_sub.upgrade() else { return };
        let cfg = s_sub
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|s| s.config.clone()))
            .unwrap_or_default();
        match build_workload_spec_from_dialog(&app, &cfg) {
            Ok((spec, priority, resources, timeout_ms)) => {
                let name = app.get_sub_name().to_string();
                if name.trim().is_empty() {
                    app.set_sub_error("Workload name cannot be empty".into());
                    return;
                }
                let retry_policy = Some(RetryPolicy {
                    max_retries: 0,
                    backoff_ms: 500,
                    retry_safety: spec.default_retry_safety(),
                    retry_on_timeout: false,
                });
                app.set_sub_error("".into());
                app.set_show_submit_dialog(false);
                let _ = tx_sub.try_send(UiAction::SubmitWorkload(SubmitWorkloadRequest {
                    name,
                    description: String::new(),
                    priority,
                    spec,
                    resources,
                    timeout_ms: Some(timeout_ms),
                    retry_policy,
                }));
            }
            Err(err) => {
                app.set_sub_error(err.into());
            }
        }
    });

    let w_pbf = window.as_weak();
    window.on_pick_batch_folder(move || {
        let w = w_pbf.clone();
        std::thread::spawn(move || {
            if let Ok(Some(path)) =
                show_native_file_dialog(FileDialogMode::OpenFolder, "Select Folder", None)
            {
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(app) = w.upgrade() {
                        app.set_sub_param_c(path.to_string_lossy().as_ref().into());
                    }
                });
            }
        });
    });

    let tx_cw = tx.clone();
    window.on_cancel_workload(move |id| {
        let _ = tx_cw.try_send(UiAction::CancelWorkload(id.into()));
    });

    let tx_sr = tx.clone();
    window.on_safe_retry_workload(move |id| {
        let _ = tx_sr.try_send(UiAction::SafeRetryWorkload(id.into()));
    });

    let w_wf = window.as_weak();
    let tx_wf = tx.clone();
    window.on_create_workflow(move || {
        let Some(app) = w_wf.upgrade() else { return };
        match build_workflow_tasks_from_dialog(&app) {
            Ok((name, description, tasks)) => {
                app.set_wf_error("".into());
                app.set_show_workflow_dialog(false);
                let _ = tx_wf.try_send(UiAction::CreateWorkflow { name, description, tasks });
            }
            Err(err) => app.set_wf_error(err.into()),
        }
    });

    let tx_cwf = tx.clone();
    window.on_cancel_workflow(move |id| {
        let _ = tx_cwf.try_send(UiAction::CancelWorkflow(id.into()));
    });

    let w_wf_flt = window.as_weak();
    let s_wf_flt = Arc::clone(&snap_store);
    window.on_workload_filter_changed(move || {
        if let (Some(app), Ok(g)) = (w_wf_flt.upgrade(), s_wf_flt.lock())
            && let Some(ref snap) = *g
        {
            apply_workloads_filter(&app, snap);
        }
    });

    let w_act_flt = window.as_weak();
    let s_act_flt = Arc::clone(&snap_store);
    window.on_activity_filter_changed(move || {
        if let (Some(app), Ok(g)) = (w_act_flt.upgrade(), s_act_flt.lock())
            && let Some(ref snap) = *g
        {
            apply_activity_filter(&app, snap);
        }
    });

    let w_sel_wk = window.as_weak();
    let s_sel_wk = Arc::clone(&snap_store);
    window.on_select_worker(move |id| {
        if let (Some(app), Ok(g)) = (w_sel_wk.upgrade(), s_sel_wk.lock())
            && let Some(ref snap) = *g
        {
            app.set_selected_worker_id(id);
            apply_snapshot_to_window(&app, snap, false);
        }
    });

    let w_sel_wl = window.as_weak();
    let s_sel_wl = Arc::clone(&snap_store);
    window.on_select_workload(move |id| {
        if let (Some(app), Ok(g)) = (w_sel_wl.upgrade(), s_sel_wl.lock())
            && let Some(ref snap) = *g
        {
            app.set_selected_workload_id(id);
            apply_snapshot_to_window(&app, snap, false);
        }
    });

    let w_sel_wf = window.as_weak();
    let s_sel_wf = Arc::clone(&snap_store);
    window.on_select_workflow(move |id| {
        if let (Some(app), Ok(g)) = (w_sel_wf.upgrade(), s_sel_wf.lock())
            && let Some(ref snap) = *g
        {
            app.set_selected_workflow_id(id);
            apply_snapshot_to_window(&app, snap, false);
        }
    });

    let w_th = window.as_weak();
    let s_th = Arc::clone(&snap_store);
    let tx_th = tx.clone();
    window.on_theme_changed(move |idx| {
        if let Some(app) = w_th.upgrade() {
            app.set_dark_mode(idx == 0);
        }
        if let Ok(g) = s_th.lock()
            && let Some(ref snap) = *g
        {
            let mut cfg = snap.config.clone();
            cfg.theme = if idx == 0 { ThemePreference::Dark } else { ThemePreference::Light };
            let _ = tx_th.try_send(UiAction::UpdateConfig(cfg));
        }
    });

    let w_rm = window.as_weak();
    let s_rm = Arc::clone(&snap_store);
    let tx_rm = tx.clone();
    window.on_reduced_motion_changed(move |idx| {
        let override_val = match idx {
            1 => Some(true),
            2 => Some(false),
            _ => None,
        };
        if let (Some(app), Ok(g)) = (w_rm.upgrade(), s_rm.lock()) {
            let os_rm = g.as_ref().is_some_and(|s| s.reduced_motion_os);
            app.set_reduced_motion(override_val.unwrap_or(os_rm));
            if let Some(ref snap) = *g {
                let mut cfg = snap.config.clone();
                cfg.reduced_motion_override = override_val;
                let _ = tx_rm.try_send(UiAction::UpdateConfig(cfg));
            }
        }
    });

    let w_save = window.as_weak();
    let s_save = Arc::clone(&snap_store);
    let tx_save = tx.clone();
    window.on_save_settings(move || {
        let Some(app) = w_save.upgrade() else { return };
        let base_cfg = s_save
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|s| s.config.clone()))
            .unwrap_or_default();
        match parse_settings_form(&app, base_cfg) {
            Ok(cfg) => {
                app.set_settings_error("".into());
                let _ = tx_save.try_send(UiAction::UpdateConfig(cfg));
            }
            Err(e) => app.set_settings_error(e.into()),
        }
    });

    let tx_bak = tx.clone();
    window.on_backup_database(move || {
        let tx_inner = tx_bak.clone();
        std::thread::spawn(move || {
            if let Ok(Some(path)) = show_native_file_dialog(
                FileDialogMode::SaveFile,
                "Backup HelixGrid SQLite Database",
                Some("helixgrid-backup.db"),
            ) {
                let _ = tx_inner.try_send(UiAction::BackupDatabase(path));
            }
        });
    });

    let tx_exp = tx.clone();
    window.on_export_diagnostics(move || {
        let _ = tx_exp.try_send(UiAction::ExportDiagnostics);
    });

    let w_pef = window.as_weak();
    window.on_pick_reg_exe_file(move || {
        let w = w_pef.clone();
        std::thread::spawn(move || {
            if let Ok(Some(path)) =
                show_native_file_dialog(FileDialogMode::OpenFile, "Select Allowed Executable", None)
            {
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(app) = w.upgrade() {
                        app.set_reg_path(path.to_string_lossy().as_ref().into());
                    }
                });
            }
        });
    });

    let w_ped = window.as_weak();
    window.on_pick_reg_exe_dir(move || {
        let w = w_ped.clone();
        std::thread::spawn(move || {
            if let Ok(Some(path)) = show_native_file_dialog(
                FileDialogMode::OpenFolder,
                "Select Allowed Working Directory",
                None,
            ) {
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(app) = w.upgrade() {
                        app.set_reg_wd(path.to_string_lossy().as_ref().into());
                    }
                });
            }
        });
    });

    let w_sre = window.as_weak();
    let tx_sre = tx.clone();
    window.on_save_registered_exe(move || {
        let Some(app) = w_sre.upgrade() else { return };
        let id = app.get_reg_id().trim().to_string();
        let display_name = app.get_reg_name().trim().to_string();
        let executable_path = app.get_reg_path().trim().to_string();
        let allowed_working_dir = app.get_reg_wd().trim().to_string();
        let description = app.get_reg_desc().trim().to_string();

        if id.is_empty()
            || display_name.is_empty()
            || executable_path.is_empty()
            || allowed_working_dir.is_empty()
        {
            app.set_reg_error("All fields except notes are required".into());
            return;
        }
        app.set_reg_error("".into());
        app.set_show_register_exe_dialog(false);
        let _ = tx_sre.try_send(UiAction::RegisterExecutable(RegisteredExecutableEntry {
            id,
            display_name,
            executable_path,
            allowed_working_dir,
            retry_safety: RetrySafety::NonIdempotentExternal,
            enabled: true,
            description,
        }));
    });

    let tx_rre = tx;
    window.on_remove_registered_exe(move |id| {
        let _ = tx_rre.try_send(UiAction::RemoveRegisteredExecutable(id.into()));
    });
}

fn parse_settings_form(app: &AppWindow, mut cfg: AppConfig) -> Result<AppConfig, String> {
    cfg.theme = if app.get_settings_theme_idx() == 0 {
        ThemePreference::Dark
    } else {
        ThemePreference::Light
    };
    cfg.reduced_motion_override = match app.get_settings_reduced_motion_idx() {
        1 => Some(true),
        2 => Some(false),
        _ => None,
    };
    cfg.scheduling_policy = if app.get_settings_scheduling_policy_idx() == 0 {
        SchedulingPolicy::PriorityResourceAware
    } else {
        SchedulingPolicy::FifoResourceAware
    };
    cfg.max_queue_depth = app
        .get_settings_max_queue_depth()
        .trim()
        .parse()
        .map_err(|_| "Max queue depth must be a positive integer")?;
    cfg.max_workers =
        app.get_settings_max_workers().trim().parse().map_err(|_| "Max workers must be 1..=64")?;
    cfg.max_concurrent_workloads = app
        .get_settings_max_concurrent_workloads()
        .trim()
        .parse()
        .map_err(|_| "Max concurrent workloads must be 1..=128")?;
    cfg.cpu_utilization_ceiling_pct = app
        .get_settings_cpu_ceiling_pct()
        .trim()
        .parse()
        .map_err(|_| "CPU ceiling % must be a number 10..=100")?;
    cfg.memory_allocation_ceiling_pct = app
        .get_settings_mem_ceiling_pct()
        .trim()
        .parse()
        .map_err(|_| "Memory ceiling % must be a number 10..=95")?;
    cfg.reserved_system_memory_mb = app
        .get_settings_reserved_mem_mb()
        .trim()
        .parse()
        .map_err(|_| "Reserved system memory (MB) must be >= 256")?;
    cfg.default_timeout_ms = app
        .get_settings_default_timeout_sec()
        .trim()
        .parse::<u64>()
        .map_err(|_| "Default timeout must be a positive integer (seconds)")?
        .saturating_mul(1000);
    cfg.auto_recover_idempotent_interrupted = app.get_settings_auto_recover();
    cfg.heartbeat_interval_ms = app
        .get_settings_heartbeat_interval_ms()
        .trim()
        .parse()
        .map_err(|_| "Heartbeat interval must be integer ms")?;
    cfg.heartbeat_timeout_ms = app
        .get_settings_heartbeat_timeout_ms()
        .trim()
        .parse()
        .map_err(|_| "Heartbeat timeout must be integer ms")?;
    cfg.snapshot_retention_max_rows = app
        .get_settings_snapshot_retention()
        .trim()
        .parse()
        .map_err(|_| "Snapshot retention must be >= 100")?;
    cfg.event_retention_max_rows = app
        .get_settings_event_retention()
        .trim()
        .parse()
        .map_err(|_| "Event retention must be >= 100")?;
    cfg.max_log_file_bytes = app
        .get_settings_max_log_mb()
        .trim()
        .parse::<u64>()
        .map_err(|_| "Max log file size must be integer MiB")?
        .saturating_mul(1024 * 1024);
    cfg.validate().map_err(|e| e.to_string())?;
    Ok(cfg)
}

async fn run_smoke_test(paths: AppPaths, worker_exe: Option<PathBuf>) -> Result<()> {
    let engine = HelixGridEngine::start(paths, worker_exe, Some(2)).await?;
    // Wait briefly until at least one worker reaches Idle
    for _ in 0..40 {
        let snap = engine.snapshot();
        if snap.workers.iter().any(|w| w.status.is_available_for_scheduling()) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let wl_id = engine
        .submit_workload(SubmitWorkloadRequest {
            name: "Smoke Test Benchmark".into(),
            description: "End-to-end verification".into(),
            priority: Priority::High,
            spec: WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                worker_threads: 2,
                iterations: 200,
                sieve_limit: 2000,
            }),
            resources: ResourceRequirements::default(),
            timeout_ms: Some(15_000),
            retry_policy: Some(RetryPolicy {
                max_retries: 1,
                backoff_ms: 100,
                retry_safety: RetrySafety::SafeIdempotent,
                retry_on_timeout: false,
            }),
        })
        .await?;

    let mut completed = false;
    for _ in 0..150 {
        let snap = engine.snapshot();
        if let Some(wl) = snap.workloads.iter().find(|w| w.id == wl_id)
            && wl.status == WorkloadStatus::Completed
        {
            completed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    engine.shutdown().await;
    if !completed {
        anyhow::bail!("Smoke test workload did not reach Completed state");
    }
    Ok(())
}
