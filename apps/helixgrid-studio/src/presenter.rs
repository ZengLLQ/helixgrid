use crate::{
    AppWindow, EventRowData, RegisteredExeRowData, WorkerRowData, WorkflowRowData, WorkloadRowData,
};
use helixgrid_config::{SchedulingPolicy, ThemePreference};
use helixgrid_core::{EngineHealth, EngineSnapshot};
use helixgrid_domain::{
    BatchOperation, EventSeverity, NumericalAlgorithm, Priority, ProgressState,
    ResourceRequirements, RetryPolicy, RetrySafety, WorkflowStatus, WorkflowTaskDefinition,
    WorkflowTaskId, WorkflowTaskState, Workload, WorkloadSpec, WorkloadStatus,
};
use helixgrid_observability::now_utc_millis;
use helixgrid_ui_components::{
    AnalyticsSummary, StatusTone, build_area_path_commands, build_line_path_commands, format_bytes,
    format_duration_ms, format_measurement_pct, format_short_id, format_utc_millis_hms,
    format_utc_millis_iso, status_tone_for_worker, status_tone_for_workflow,
    status_tone_for_workload,
};
use slint::{ModelRc, VecModel};
use std::fmt::Write as _;
use std::fs;

pub fn apply_snapshot_to_window(
    win: &AppWindow,
    snap: &EngineSnapshot,
    sync_settings_inputs: bool,
) {
    let now_utc = now_utc_millis();

    win.set_app_version(snap.app_version.as_str().into());
    win.set_health_text(snap.health.display_name().into());
    win.set_health_tone(match snap.health {
        EngineHealth::Healthy => StatusTone::Success as i32,
        EngineHealth::Degraded => StatusTone::Warning as i32,
        EngineHealth::Recovering => StatusTone::Accent as i32,
        EngineHealth::Stopped => StatusTone::Danger as i32,
    });
    win.set_health_reason(snap.health_reason.as_str().into());

    // Appearance & motion
    let is_dark = snap.config.theme == ThemePreference::Dark;
    win.set_dark_mode(is_dark);
    let reduced = snap.config.reduced_motion_override.unwrap_or(snap.reduced_motion_os);
    win.set_reduced_motion(reduced);

    if sync_settings_inputs {
        win.set_settings_theme_idx(if is_dark { 0 } else { 1 });
        win.set_settings_reduced_motion_idx(match snap.config.reduced_motion_override {
            None => 0,
            Some(true) => 1,
            Some(false) => 2,
        });
        win.set_settings_scheduling_policy_idx(match snap.config.scheduling_policy {
            SchedulingPolicy::PriorityResourceAware => 0,
            SchedulingPolicy::FifoResourceAware => 1,
        });
        win.set_settings_max_queue_depth(snap.config.max_queue_depth.to_string().into());
        win.set_settings_max_workers(snap.config.max_workers.to_string().into());
        win.set_settings_max_concurrent_workloads(
            snap.config.max_concurrent_workloads.to_string().into(),
        );
        win.set_settings_cpu_ceiling_pct(
            format!("{:.0}", snap.config.cpu_utilization_ceiling_pct).into(),
        );
        win.set_settings_mem_ceiling_pct(
            format!("{:.0}", snap.config.memory_allocation_ceiling_pct).into(),
        );
        win.set_settings_reserved_mem_mb(snap.config.reserved_system_memory_mb.to_string().into());
        win.set_settings_default_timeout_sec(
            (snap.config.default_timeout_ms / 1000).to_string().into(),
        );
        win.set_settings_auto_recover(snap.config.auto_recover_idempotent_interrupted);
        win.set_settings_heartbeat_interval_ms(
            snap.config.heartbeat_interval_ms.to_string().into(),
        );
        win.set_settings_heartbeat_timeout_ms(snap.config.heartbeat_timeout_ms.to_string().into());
        win.set_settings_snapshot_retention(
            snap.config.snapshot_retention_max_rows.to_string().into(),
        );
        win.set_settings_event_retention(snap.config.event_retention_max_rows.to_string().into());
        win.set_settings_max_log_mb(
            (snap.config.max_log_file_bytes / (1024 * 1024)).to_string().into(),
        );
    }

    let reg_rows: Vec<RegisteredExeRowData> = snap
        .config
        .registered_executables
        .iter()
        .map(|e| RegisteredExeRowData {
            id: e.id.as_str().into(),
            display_name: e.display_name.as_str().into(),
            executable_path: e.executable_path.as_str().into(),
            working_dir: e.allowed_working_dir.as_str().into(),
            retry_safety: match e.retry_safety {
                RetrySafety::SafeIdempotent => "Idempotent".into(),
                RetrySafety::NonIdempotentExternal => "Non-Idempotent".into(),
            },
            enabled: e.enabled,
        })
        .collect();
    win.set_registered_executables(ModelRc::new(VecModel::from(reg_rows)));

    // Resource summary tiles
    let m = &snap.latest_metrics;
    win.set_cpu_text(format_measurement_pct(&m.cpu_utilization_pct).into());
    win.set_cpu_sub(
        format!(
            "{} logical processors · Ceiling {:.0}%",
            m.logical_processors, snap.config.cpu_utilization_ceiling_pct
        )
        .into(),
    );

    let (mem_text, mem_sub) =
        match (m.total_physical_memory_bytes.value(), m.available_memory_bytes.value()) {
            (Some(&total), Some(&avail)) if total > 0 => {
                let used = total.saturating_sub(avail);
                let pct = (used as f64 / total as f64) * 100.0;
                (
                    format!("{pct:.1}%").into(),
                    format!("{} used of {}", format_bytes(used), format_bytes(total)).into(),
                )
            }
            _ => ("Unavailable".into(), "Awaiting OS memory telemetry".into()),
        };
    win.set_mem_text(mem_text);
    win.set_mem_sub(mem_sub);

    let alive_workers = snap.workers.iter().filter(|w| w.status.is_alive()).count();
    win.set_active_workers_count(alive_workers as i32);
    win.set_workers_text(format!("{alive_workers} / {}", snap.config.max_workers).into());
    let idle_workers =
        snap.workers.iter().filter(|w| w.status.is_available_for_scheduling()).count();
    win.set_workers_sub(
        format!("{idle_workers} idle · {} total tracked", snap.workers.len()).into(),
    );

    let running_count = snap.workloads.iter().filter(|w| w.status.is_active()).count();
    let queued_count = snap
        .workloads
        .iter()
        .filter(|w| matches!(w.status, WorkloadStatus::Queued | WorkloadStatus::Retrying))
        .count();
    win.set_running_workloads_count(running_count as i32);
    win.set_queue_text(format!("{running_count} running").into());
    win.set_queue_sub(
        format!("{queued_count} queued · {} max concurrent", snap.config.max_concurrent_workloads)
            .into(),
    );

    let interrupted_count =
        snap.workloads.iter().filter(|w| w.status == WorkloadStatus::Interrupted).count();
    let failed_count = snap
        .workloads
        .iter()
        .filter(|w| matches!(w.status, WorkloadStatus::Failed | WorkloadStatus::TimedOut))
        .count();
    let attention = if interrupted_count > 0 {
        format!("{interrupted_count} interrupted workload(s) require manual review or safe retry.")
    } else if failed_count > 0 && snap.health == EngineHealth::Degraded {
        format!("{failed_count} failed workload(s) recorded; engine status degraded.")
    } else if snap.health == EngineHealth::Degraded {
        snap.health_reason.clone()
    } else {
        String::new()
    };
    win.set_attention_message(attention.into());

    // Analytics & Charts
    let analytics = AnalyticsSummary::from_snapshot(snap);
    let cpu_line = build_line_path_commands(&analytics.cpu_series, 480.0, 120.0, 0.0, 100.0);
    let cpu_area = build_area_path_commands(&analytics.cpu_series, 480.0, 120.0, 0.0, 100.0);
    win.set_cpu_chart_has_data(!cpu_line.is_empty());
    win.set_cpu_chart_line(cpu_line.into());
    win.set_cpu_chart_area(cpu_area.into());

    let mem_line = build_line_path_commands(&analytics.memory_pct_series, 480.0, 120.0, 0.0, 100.0);
    let mem_area = build_area_path_commands(&analytics.memory_pct_series, 480.0, 120.0, 0.0, 100.0);
    win.set_mem_chart_has_data(!mem_line.is_empty());
    win.set_mem_chart_line(mem_line.into());
    win.set_mem_chart_area(mem_area.into());

    let max_dur =
        analytics.duration_series_ms.iter().copied().reduce(f64::max).unwrap_or(100.0).max(10.0);
    let dur_line =
        build_line_path_commands(&analytics.duration_series_ms, 480.0, 120.0, 0.0, max_dur);
    let dur_area =
        build_area_path_commands(&analytics.duration_series_ms, 480.0, 120.0, 0.0, max_dur);
    win.set_dur_chart_has_data(!dur_line.is_empty());
    win.set_dur_chart_line(dur_line.into());
    win.set_dur_chart_area(dur_area.into());
    win.set_dur_chart_max_label(format_duration_ms(max_dur.round() as u64).into());

    win.set_throughput_text(
        analytics
            .throughput_per_minute
            .map(|t| format!("{t:.1} / min"))
            .unwrap_or_else(|| "Unavailable".to_string())
            .into(),
    );
    win.set_completion_ratio_text(
        format!("{} completed · {} failed", analytics.completed_count, analytics.failed_count)
            .into(),
    );
    win.set_queue_latency_text(
        format!("{:.1} ms", snap.scheduler_diagnostics.average_queue_latency_ms).into(),
    );
    win.set_avg_duration_text(
        analytics
            .avg_duration_ms
            .map(format_duration_ms)
            .unwrap_or_else(|| "Unavailable".to_string())
            .into(),
    );
    let total_cores: u32 =
        snap.workers.iter().filter(|w| w.status.is_alive()).map(|w| w.max_cpu_cores).sum();
    let res_cores: u32 =
        snap.workers.iter().filter(|w| w.status.is_alive()).map(|w| w.reserved_cpu_cores).sum();
    let util_pct =
        if total_cores > 0 { (res_cores as f64 / total_cores as f64) * 100.0 } else { 0.0 };
    win.set_worker_util_text(format!("{util_pct:.0}%").into());
    win.set_cpu_avg_text(
        analytics
            .avg_cpu_pct
            .map(|v| format!("{v:.1}%"))
            .unwrap_or_else(|| "Unavailable".to_string())
            .into(),
    );
    win.set_cpu_peak_text(
        analytics
            .peak_cpu_pct
            .map(|v| format!("{v:.1}%"))
            .unwrap_or_else(|| "Unavailable".to_string())
            .into(),
    );
    win.set_mem_avg_text(
        analytics
            .avg_memory_used_pct
            .map(|v| format!("{v:.1}%"))
            .unwrap_or_else(|| "Unavailable".to_string())
            .into(),
    );

    // Workers list + inspector sync
    let worker_rows: Vec<WorkerRowData> = snap
        .workers
        .iter()
        .map(|w| {
            let cur_wl = w
                .current_workload_id
                .and_then(|wid| snap.workloads.iter().find(|wl| wl.id == wid))
                .map(|wl| wl.name.clone())
                .unwrap_or_else(|| "Idle".to_string());
            let uptime_ms = now_utc.saturating_sub(w.started_at_utc).max(0) as u64;
            let hb_text = w
                .last_heartbeat_utc
                .map(|ts| {
                    let age = now_utc.saturating_sub(ts).max(0) as u64;
                    if age < 1000 {
                        "Just now".to_string()
                    } else {
                        format!("{} ago", format_duration_ms(age))
                    }
                })
                .unwrap_or_else(|| "Pending".to_string());

            WorkerRowData {
                id: w.id.to_string().into(),
                name: w.name.as_str().into(),
                pid_text: w.pid.map(|p| p.to_string()).unwrap_or_else(|| "—".to_string()).into(),
                status_text: w.status.display_name().into(),
                status_tone: status_tone_for_worker(w.status) as i32,
                cpu_text: format_measurement_pct(&w.cpu_usage_pct).into(),
                memory_text: w
                    .memory_usage_bytes
                    .value()
                    .map(|b| format_bytes(*b))
                    .unwrap_or_else(|| "Unavailable".to_string())
                    .into(),
                current_workload: cur_wl.into(),
                uptime_text: format_duration_ms(uptime_ms).into(),
                heartbeat_text: hb_text.into(),
                reserved_text: format!(
                    "{}/{} cores · {}/{} MiB",
                    w.reserved_cpu_cores, w.max_cpu_cores, w.reserved_memory_mb, w.max_memory_mb
                )
                .into(),
                completed_count: w.completed_executions as i32,
                failed_count: w.failed_executions as i32,
                isolation_note: w.isolation_note.as_str().into(),
                is_alive: w.status.is_alive(),
            }
        })
        .collect();

    let sel_worker_id = win.get_selected_worker_id().to_string();
    if let Some(found) = worker_rows.iter().find(|r| r.id.as_str() == sel_worker_id) {
        win.set_selected_worker(found.clone());
        win.set_has_selected_worker(true);
    } else if !sel_worker_id.is_empty() {
        win.set_has_selected_worker(false);
    }
    win.set_workers(ModelRc::new(VecModel::from(worker_rows)));

    // Workloads (filtered + active overview)
    apply_workloads_filter(win, snap);

    // Workflows
    let wf_rows: Vec<WorkflowRowData> = snap
        .workflows
        .iter()
        .map(|wf| {
            let completed_t =
                wf.tasks.iter().filter(|t| t.state == WorkflowTaskState::Completed).count();
            let mut detail = String::new();
            for t in &wf.tasks {
                let deps = if t.depends_on.is_empty() {
                    "root".to_string()
                } else {
                    t.depends_on
                        .iter()
                        .filter_map(|did| wf.tasks.iter().find(|dt| dt.id == *did))
                        .map(|dt| dt.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                let _ = writeln!(&mut detail, "• {} [{:?}] (after: {deps})", t.name, t.state);
            }
            WorkflowRowData {
                id: wf.id.to_string().into(),
                short_id: format_short_id(&wf.id).into(),
                name: wf.name.as_str().into(),
                description: wf.description.as_str().into(),
                status_text: wf.status.display_name().into(),
                status_tone: status_tone_for_workflow(wf.status) as i32,
                tasks_summary: format!("{completed_t}/{} stages completed", wf.tasks.len()).into(),
                created_text: format_utc_millis_hms(wf.created_at_utc).into(),
                stages_detail: detail.into(),
                can_cancel: wf.status == WorkflowStatus::Running,
            }
        })
        .collect();

    let sel_wf_id = win.get_selected_workflow_id().to_string();
    if let Some(found) = wf_rows.iter().find(|r| r.id.as_str() == sel_wf_id) {
        win.set_selected_workflow(found.clone());
        win.set_has_selected_workflow(true);
    } else if !sel_wf_id.is_empty() {
        win.set_has_selected_workflow(false);
    }
    win.set_workflows(ModelRc::new(VecModel::from(wf_rows)));

    // Events (recent + filtered activity + recent errors)
    let recent_ev: Vec<EventRowData> = snap.events.iter().take(6).map(event_to_row).collect();
    win.set_recent_events(ModelRc::new(VecModel::from(recent_ev)));

    let err_rows: Vec<EventRowData> = snap
        .events
        .iter()
        .filter(|e| {
            matches!(
                e.severity,
                EventSeverity::Warning | EventSeverity::Error | EventSeverity::Critical
            )
        })
        .take(20)
        .map(event_to_row)
        .collect();
    win.set_recent_errors(ModelRc::new(VecModel::from(err_rows)));
    apply_activity_filter(win, snap);

    // Diagnostics metadata
    win.set_db_path(snap.database_status.path.as_str().into());
    win.set_db_status_summary(if snap.database_status.integrity_ok {
        "Integrity OK · WAL".into()
    } else {
        "Integrity Issue".into()
    });
    win.set_db_metrics(
        format!(
            "v{} · {} workloads · {} snapshots · {}",
            snap.database_status.schema_version,
            snap.database_status.workload_count,
            snap.database_status.snapshot_count,
            format_bytes(snap.database_status.file_size_bytes)
        )
        .into(),
    );
    win.set_worker_pool_summary(
        format!(
            "{} alive / {} configured max · {} queued dispatches",
            alive_workers, snap.config.max_workers, snap.scheduler_diagnostics.total_dispatched
        )
        .into(),
    );
    win.set_ipc_status(snap.ipc_status.as_str().into());
    win.set_recovery_summary_text(
        format!(
            "Startup recovery: {} interrupted, {} auto-recovered, {} manual",
            snap.recovery_summary.interrupted_workloads_found,
            snap.recovery_summary.auto_recovered_workloads,
            snap.recovery_summary.manual_attention_workloads
        )
        .into(),
    );
}

pub fn apply_workloads_filter(win: &AppWindow, snap: &EngineSnapshot) {
    let filter_idx = win.get_workload_status_filter();
    let search = win.get_workload_search_query().to_string().to_ascii_lowercase();
    let now_utc = now_utc_millis();

    let mut active_rows = Vec::new();
    let mut filtered_rows = Vec::new();

    for wl in &snap.workloads {
        let row = workload_to_row(wl, snap, now_utc);
        if matches!(
            wl.status,
            WorkloadStatus::Queued
                | WorkloadStatus::Scheduled
                | WorkloadStatus::Starting
                | WorkloadStatus::Running
                | WorkloadStatus::Retrying
        ) {
            active_rows.push(row.clone());
        }

        let status_ok = match filter_idx {
            1 => matches!(
                wl.status,
                WorkloadStatus::Submitted
                    | WorkloadStatus::Validating
                    | WorkloadStatus::Queued
                    | WorkloadStatus::Scheduled
                    | WorkloadStatus::Starting
                    | WorkloadStatus::Running
                    | WorkloadStatus::Retrying
            ),
            2 => wl.status == WorkloadStatus::Completed,
            3 => matches!(
                wl.status,
                WorkloadStatus::Failed
                    | WorkloadStatus::TimedOut
                    | WorkloadStatus::Interrupted
                    | WorkloadStatus::Cancelled
            ),
            _ => true,
        };
        let search_ok = search.is_empty()
            || wl.name.to_ascii_lowercase().contains(&search)
            || wl.workload_type.display_name().to_ascii_lowercase().contains(&search)
            || wl.id.to_string().to_ascii_lowercase().contains(&search);

        if status_ok && search_ok {
            filtered_rows.push(row);
        }
    }

    let sel_id = win.get_selected_workload_id().to_string();
    if let Some(found) = filtered_rows.iter().find(|r| r.id.as_str() == sel_id) {
        win.set_selected_workload(found.clone());
        win.set_has_selected_workload(true);
    } else if !sel_id.is_empty()
        && let Some(wl) = snap.workloads.iter().find(|w| w.id.to_string() == sel_id)
    {
        win.set_selected_workload(workload_to_row(wl, snap, now_utc));
        win.set_has_selected_workload(true);
    }

    win.set_active_workloads(ModelRc::new(VecModel::from(active_rows)));
    win.set_workloads(ModelRc::new(VecModel::from(filtered_rows)));
}

pub fn apply_activity_filter(win: &AppWindow, snap: &EngineSnapshot) {
    let sev_idx = win.get_activity_severity_filter();
    let win_idx = win.get_activity_window_filter();
    let query = win.get_activity_search_query().to_string().to_ascii_lowercase();
    let now_utc = now_utc_millis();

    let min_ts = match win_idx {
        1 => now_utc.saturating_sub(5 * 60 * 1000),
        2 => now_utc.saturating_sub(60 * 60 * 1000),
        _ => 0,
    };

    let rows: Vec<EventRowData> = snap
        .events
        .iter()
        .filter(|e| e.timestamp_utc >= min_ts)
        .filter(|e| match sev_idx {
            1 => e.severity == EventSeverity::Info,
            2 => e.severity == EventSeverity::Warning,
            3 => e.severity == EventSeverity::Error,
            4 => e.severity == EventSeverity::Critical,
            _ => true,
        })
        .filter(|e| {
            if query.is_empty() {
                return true;
            }
            e.message.to_ascii_lowercase().contains(&query)
                || e.kind.display_name().to_ascii_lowercase().contains(&query)
                || e.workload_id
                    .is_some_and(|w| w.to_string().to_ascii_lowercase().contains(&query))
                || e.worker_id.is_some_and(|w| w.to_string().to_ascii_lowercase().contains(&query))
        })
        .take(300)
        .map(event_to_row)
        .collect();

    win.set_events(ModelRc::new(VecModel::from(rows)));
}

fn workload_to_row(wl: &Workload, snap: &EngineSnapshot, now_utc: i64) -> WorkloadRowData {
    let worker_text = wl
        .assigned_worker_id
        .and_then(|wid| snap.workers.iter().find(|w| w.id == wid))
        .map(|w| w.name.clone())
        .unwrap_or_else(|| "Unassigned".to_string());

    let (progress_pct, progress_indeterminate, progress_text) = match &wl.progress {
        ProgressState::Determinate { percent, completed_units, total_units, message } => (
            *percent,
            false,
            message
                .clone()
                .unwrap_or_else(|| format!("{percent:.0}% ({completed_units}/{total_units})")),
        ),
        ProgressState::Indeterminate { message } => (
            0.0,
            wl.status.is_active(),
            message.clone().unwrap_or_else(|| wl.status.display_name().to_string()),
        ),
    };

    let duration_text = if let Some(ref res) = wl.result_metadata {
        format_duration_ms(res.duration_ms)
    } else if wl.status.is_active() {
        format_duration_ms(now_utc.saturating_sub(wl.updated_at_utc).max(0) as u64)
    } else {
        "—".to_string()
    };

    let can_cancel = !wl.status.is_terminal();
    let can_retry = !wl.status.is_active()
        && wl.status != WorkloadStatus::Completed
        && wl.retry_policy.retry_safety == RetrySafety::SafeIdempotent;

    let mut timeline = String::new();
    let _ = writeln!(&mut timeline, "Submitted: {}", format_utc_millis_iso(wl.created_at_utc));
    let _ = writeln!(&mut timeline, "Last Updated: {}", format_utc_millis_iso(wl.updated_at_utc));
    let _ = writeln!(
        &mut timeline,
        "Attempts: {} (Safety: {:?})",
        wl.attempt_count, wl.retry_policy.retry_safety
    );
    if let Some(execs) = snap.executions_by_workload.get(&wl.id) {
        for ex in execs {
            let dur =
                ex.duration_ms.map(format_duration_ms).unwrap_or_else(|| "in-progress".into());
            let _ = writeln!(
                &mut timeline,
                "• Attempt #{}: {:?} ({dur})",
                ex.attempt_number, ex.status
            );
        }
    }

    let mut artifacts_text = String::new();
    let mut logs_text = String::new();
    if let Some(arts) = snap.artifacts_by_workload.get(&wl.id) {
        for art in arts {
            let _ = writeln!(
                &mut artifacts_text,
                "• {} [{}] ({}) · SHA256 {}",
                art.name,
                format_bytes(art.size_bytes),
                art.file_path,
                &art.sha256_hex[..12.min(art.sha256_hex.len())]
            );
            if matches!(
                art.kind,
                helixgrid_domain::ArtifactKind::StdoutLog
                    | helixgrid_domain::ArtifactKind::StderrLog
            ) && let Ok(content) = fs::read_to_string(&art.file_path)
            {
                let snippet: String = content.chars().take(1200).collect();
                let _ = writeln!(&mut logs_text, "--- {} ---\n{}", art.name, snippet);
            }
        }
    }

    for ev in snap.events.iter().filter(|e| e.workload_id == Some(wl.id)).take(12) {
        let _ = writeln!(
            &mut logs_text,
            "[{}] {:?}: {}",
            format_utc_millis_hms(ev.timestamp_utc),
            ev.severity,
            ev.message
        );
    }

    let config_summary = format!(
        "Type: {} | Reserved: {} core(s), {} MiB RAM | Timeout: {} s\n{}",
        wl.workload_type.display_name(),
        wl.resources.cpu_cores,
        wl.resources.memory_mb,
        wl.timeout_ms / 1000,
        serde_json::to_string_pretty(&wl.spec).unwrap_or_default()
    );

    let (result_summary, metrics_pretty) = match &wl.result_metadata {
        Some(res) => (
            res.summary.clone(),
            serde_json::to_string_pretty(&res.metrics_json).unwrap_or_default(),
        ),
        None => (String::new(), String::new()),
    };

    WorkloadRowData {
        id: wl.id.to_string().into(),
        short_id: format_short_id(&wl.id).into(),
        name: wl.name.as_str().into(),
        type_text: wl.workload_type.display_name().into(),
        status_text: wl.status.display_name().into(),
        status_tone: status_tone_for_workload(wl.status) as i32,
        priority_text: wl.priority.display_name().into(),
        worker_text: worker_text.into(),
        submitted_text: format_utc_millis_hms(wl.created_at_utc).into(),
        duration_text: duration_text.into(),
        progress_pct,
        progress_indeterminate,
        progress_text: progress_text.into(),
        can_cancel,
        can_retry,
        description: wl.description.as_str().into(),
        config_summary: config_summary.into(),
        result_summary: result_summary.into(),
        error_text: wl.error_message.as_deref().unwrap_or("").into(),
        metrics_pretty: metrics_pretty.into(),
        timeline_text: timeline.into(),
        artifacts_text: artifacts_text.into(),
        logs_text: logs_text.into(),
    }
}

fn event_to_row(ev: &helixgrid_domain::SystemEvent) -> EventRowData {
    let mut ctx = Vec::new();
    if let Some(wl) = ev.workload_id {
        ctx.push(format!("workload {}", format_short_id(&wl)));
    }
    if let Some(wk) = ev.worker_id {
        ctx.push(format!("worker {}", format_short_id(&wk)));
    }
    if let Some(wf) = ev.workflow_id {
        ctx.push(format!("workflow {}", format_short_id(&wf)));
    }
    EventRowData {
        id: ev.id.to_string().into(),
        time_text: format_utc_millis_hms(ev.timestamp_utc).into(),
        severity_text: ev.severity.display_name().into(),
        severity_tone: match ev.severity {
            EventSeverity::Trace | EventSeverity::Info => StatusTone::Neutral as i32,
            EventSeverity::Warning => StatusTone::Warning as i32,
            EventSeverity::Error | EventSeverity::Critical => StatusTone::Danger as i32,
        },
        kind_text: ev.kind.display_name().into(),
        message: ev.message.as_str().into(),
        context_text: ctx.join(" · ").into(),
    }
}

pub fn build_workload_spec_from_dialog(
    win: &AppWindow,
    config: &helixgrid_config::AppConfig,
) -> Result<(WorkloadSpec, Priority, ResourceRequirements, u64), String> {
    let type_idx = win.get_sub_type_idx();
    let a = win.get_sub_param_a().to_string();
    let b = win.get_sub_param_b().to_string();
    let c = win.get_sub_param_c().to_string();

    let cores: u32 = win
        .get_sub_cpu_cores()
        .trim()
        .parse()
        .map_err(|_| "Reserved cores must be a positive integer".to_string())?;
    let memory_mb: u64 = win
        .get_sub_memory_mb()
        .trim()
        .parse()
        .map_err(|_| "Reserved memory (MB) must be a positive integer".to_string())?;
    let timeout_sec: u64 = win
        .get_sub_timeout_sec()
        .trim()
        .parse()
        .map_err(|_| "Timeout (seconds) must be a positive integer".to_string())?;

    let priority = match win.get_sub_priority_idx() {
        0 => Priority::Low,
        2 => Priority::High,
        3 => Priority::Critical,
        _ => Priority::Normal,
    };

    let spec = match type_idx {
        0 => {
            let worker_threads = a.trim().parse().map_err(|_| "Threads must be 1..=64")?;
            let iterations = b.trim().parse().map_err(|_| "Iterations must be >= 1")?;
            let sieve_limit = c.trim().parse().map_err(|_| "Sieve limit must be >= 100")?;
            WorkloadSpec::CpuBenchmark(helixgrid_domain::CpuBenchmarkInput {
                worker_threads,
                iterations,
                sieve_limit,
            })
        }
        1 => {
            let item_count = a.trim().parse().map_err(|_| "Record count must be >= 1")?;
            let batch_size = b.trim().parse().map_err(|_| "Batch size must be >= 1")?;
            let input_dir = if c.trim().is_empty() { None } else { Some(c.trim().to_string()) };
            WorkloadSpec::BatchProcessing(helixgrid_domain::BatchProcessingInput {
                input_dir,
                item_count,
                batch_size,
                operation: BatchOperation::HashAndValidate,
            })
        }
        2 => {
            let dimension = a.trim().parse().map_err(|_| "Dimension must be 1..=4096")?;
            let max_iterations = b.trim().parse().map_err(|_| "Max iterations must be >= 1")?;
            let algo_idx: i32 = c.trim().parse().unwrap_or(1);
            let algorithm = match algo_idx {
                0 => NumericalAlgorithm::DenseMatrixMultiply,
                2 => NumericalAlgorithm::MonteCarloVariance,
                3 => NumericalAlgorithm::Rk4LorenzTrajectory,
                _ => NumericalAlgorithm::ConjugateGradientSolve,
            };
            WorkloadSpec::NumericalComputation(helixgrid_domain::NumericalComputationInput {
                algorithm,
                dimension,
                max_iterations,
                tolerance: 1e-8,
                seed: 42,
            })
        }
        _ => {
            let reg_id = a.trim();
            let entry =
                config.registered_executables.iter().find(|e| e.id == reg_id).ok_or_else(|| {
                    format!("Executable ID '{reg_id}' not found in Settings -> Security allowlist")
                })?;
            let args: Vec<String> = b.split_whitespace().map(str::to_string).collect();
            let working_dir = if c.trim().is_empty() {
                entry.allowed_working_dir.clone()
            } else {
                c.trim().to_string()
            };
            WorkloadSpec::RegisteredExecutable(helixgrid_domain::RegisteredExecutableInput {
                registered_id: entry.id.clone(),
                executable_path: entry.executable_path.clone(),
                args,
                working_dir,
                env_overrides: vec![],
            })
        }
    };

    spec.validate().map_err(|e| e.to_string())?;
    let res =
        ResourceRequirements { cpu_cores: cores, memory_mb, requires_exclusive_worker: false };
    res.validate().map_err(|e| e.to_string())?;
    Ok((spec, priority, res, timeout_sec.saturating_mul(1000).max(500)))
}

pub fn build_workflow_tasks_from_dialog(
    win: &AppWindow,
) -> Result<(String, String, Vec<WorkflowTaskDefinition>), String> {
    let name = win.get_wf_name().to_string();
    let desc = win.get_wf_desc().to_string();
    let tmpl = win.get_wf_template_idx();
    let iters: u64 = win
        .get_wf_stage1_iters()
        .trim()
        .parse()
        .map_err(|_| "Stage 1 iterations must be a positive integer")?;
    let dim: u32 = win
        .get_wf_stage2_dim()
        .trim()
        .parse()
        .map_err(|_| "Stage 2 matrix dimension must be a positive integer")?;
    let items: u32 = win
        .get_wf_stage3_items()
        .trim()
        .parse()
        .map_err(|_| "Final batch records must be a positive integer")?;

    let t1 = WorkflowTaskId::new();
    let t2 = WorkflowTaskId::new();
    let t3 = WorkflowTaskId::new();

    let tasks = if tmpl == 0 {
        vec![
            WorkflowTaskDefinition {
                id: t1,
                name: "1. CPU Prep Benchmark".into(),
                depends_on: vec![],
                priority: Priority::High,
                spec: WorkloadSpec::CpuBenchmark(helixgrid_domain::CpuBenchmarkInput {
                    worker_threads: 2,
                    iterations: iters,
                    sieve_limit: 5000,
                }),
                resources: ResourceRequirements::default(),
                timeout_ms: 60_000,
                retry_policy: RetryPolicy {
                    max_retries: 1,
                    backoff_ms: 250,
                    retry_safety: RetrySafety::SafeIdempotent,
                    retry_on_timeout: false,
                },
                state: WorkflowTaskState::Pending,
                workload_id: None,
            },
            WorkflowTaskDefinition {
                id: t2,
                name: "2. Dense Matrix Solve".into(),
                depends_on: vec![t1],
                priority: Priority::Normal,
                spec: WorkloadSpec::NumericalComputation(
                    helixgrid_domain::NumericalComputationInput {
                        algorithm: NumericalAlgorithm::DenseMatrixMultiply,
                        dimension: dim,
                        max_iterations: 1000,
                        tolerance: 1e-8,
                        seed: 7,
                    },
                ),
                resources: ResourceRequirements::default(),
                timeout_ms: 60_000,
                retry_policy: RetryPolicy::default(),
                state: WorkflowTaskState::Pending,
                workload_id: None,
            },
            WorkflowTaskDefinition {
                id: t3,
                name: "3. Batch Aggregation".into(),
                depends_on: vec![t2],
                priority: Priority::Normal,
                spec: WorkloadSpec::BatchProcessing(helixgrid_domain::BatchProcessingInput {
                    input_dir: None,
                    item_count: items,
                    batch_size: 250,
                    operation: BatchOperation::NormalizeJsonRecords,
                }),
                resources: ResourceRequirements::default(),
                timeout_ms: 60_000,
                retry_policy: RetryPolicy::default(),
                state: WorkflowTaskState::Pending,
                workload_id: None,
            },
        ]
    } else {
        let t2b = WorkflowTaskId::new();
        vec![
            WorkflowTaskDefinition {
                id: t1,
                name: "1. Seed Benchmark".into(),
                depends_on: vec![],
                priority: Priority::High,
                spec: WorkloadSpec::CpuBenchmark(helixgrid_domain::CpuBenchmarkInput {
                    worker_threads: 2,
                    iterations: iters,
                    sieve_limit: 5000,
                }),
                resources: ResourceRequirements::default(),
                timeout_ms: 60_000,
                retry_policy: RetryPolicy::default(),
                state: WorkflowTaskState::Pending,
                workload_id: None,
            },
            WorkflowTaskDefinition {
                id: t2,
                name: "2a. Matrix Multiply".into(),
                depends_on: vec![t1],
                priority: Priority::Normal,
                spec: WorkloadSpec::NumericalComputation(
                    helixgrid_domain::NumericalComputationInput {
                        algorithm: NumericalAlgorithm::DenseMatrixMultiply,
                        dimension: dim,
                        max_iterations: 1000,
                        tolerance: 1e-8,
                        seed: 11,
                    },
                ),
                resources: ResourceRequirements::default(),
                timeout_ms: 60_000,
                retry_policy: RetryPolicy::default(),
                state: WorkflowTaskState::Pending,
                workload_id: None,
            },
            WorkflowTaskDefinition {
                id: t2b,
                name: "2b. Monte Carlo Variance".into(),
                depends_on: vec![t1],
                priority: Priority::Normal,
                spec: WorkloadSpec::NumericalComputation(
                    helixgrid_domain::NumericalComputationInput {
                        algorithm: NumericalAlgorithm::MonteCarloVariance,
                        dimension: 64,
                        max_iterations: 200_000,
                        tolerance: 1e-6,
                        seed: 99,
                    },
                ),
                resources: ResourceRequirements::default(),
                timeout_ms: 60_000,
                retry_policy: RetryPolicy::default(),
                state: WorkflowTaskState::Pending,
                workload_id: None,
            },
            WorkflowTaskDefinition {
                id: t3,
                name: "3. Final Batch Consolidation".into(),
                depends_on: vec![t2, t2b],
                priority: Priority::High,
                spec: WorkloadSpec::BatchProcessing(helixgrid_domain::BatchProcessingInput {
                    input_dir: None,
                    item_count: items,
                    batch_size: 250,
                    operation: BatchOperation::HashAndValidate,
                }),
                resources: ResourceRequirements::default(),
                timeout_ms: 60_000,
                retry_policy: RetryPolicy::default(),
                state: WorkflowTaskState::Pending,
                workload_id: None,
            },
        ]
    };

    Ok((name, desc, tasks))
}
