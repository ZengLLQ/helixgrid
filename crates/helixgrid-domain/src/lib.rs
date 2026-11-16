pub mod error;
pub mod events;
pub mod ids;
pub mod metrics;
pub mod worker;
pub mod workflow;
pub mod workload;

pub use error::DomainError;
pub use events::{EventSeverity, SystemEvent, SystemEventKind};
pub use ids::{ArtifactId, EventId, ExecutionId, WorkerId, WorkflowId, WorkflowTaskId, WorkloadId};
pub use metrics::{GpuCapabilityStatus, MeasurementValue, ProgressState, SystemMetricsSnapshot};
pub use worker::{WorkerInfo, WorkerStatus};
pub use workflow::{WorkflowDefinition, WorkflowStatus, WorkflowTaskDefinition, WorkflowTaskState};
pub use workload::{
    ArtifactKind, BatchOperation, BatchProcessingInput, CpuBenchmarkInput, ExecutionArtifact,
    ExecutionOutcome, ExecutionRecord, NumericalAlgorithm, NumericalComputationInput, Priority,
    RegisteredExecutableInput, ResourceRequirements, RetryPolicy, RetrySafety, WorkflowTaskInput,
    Workload, WorkloadResultMetadata, WorkloadSpec, WorkloadStatus, WorkloadType,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_workload_never_transitions_back_to_running() {
        let mut status = WorkloadStatus::Submitted;
        status.try_transition(WorkloadStatus::Validating).unwrap();
        status.try_transition(WorkloadStatus::Queued).unwrap();
        status.try_transition(WorkloadStatus::Scheduled).unwrap();
        status.try_transition(WorkloadStatus::Starting).unwrap();
        status.try_transition(WorkloadStatus::Running).unwrap();
        status.try_transition(WorkloadStatus::Completed).unwrap();

        assert!(status.is_terminal());
        for candidate in [
            WorkloadStatus::Submitted,
            WorkloadStatus::Validating,
            WorkloadStatus::Queued,
            WorkloadStatus::Scheduled,
            WorkloadStatus::Starting,
            WorkloadStatus::Running,
            WorkloadStatus::Retrying,
            WorkloadStatus::Interrupted,
            WorkloadStatus::Failed,
            WorkloadStatus::Cancelled,
            WorkloadStatus::TimedOut,
        ] {
            assert!(
                !status.can_transition_to(candidate),
                "Completed must not transition to {candidate:?}"
            );
        }
    }

    #[test]
    fn workflow_detects_cycles_and_orders_dag() {
        let t1 = WorkflowTaskId::new();
        let t2 = WorkflowTaskId::new();
        let t3 = WorkflowTaskId::new();

        let mk_task = |id, name: &str, deps| WorkflowTaskDefinition {
            id,
            name: name.to_string(),
            depends_on: deps,
            priority: Priority::Normal,
            spec: WorkloadSpec::CpuBenchmark(CpuBenchmarkInput {
                worker_threads: 1,
                iterations: 100,
                sieve_limit: 1000,
            }),
            resources: ResourceRequirements::default(),
            timeout_ms: 5_000,
            retry_policy: RetryPolicy::default(),
            state: WorkflowTaskState::Pending,
            workload_id: None,
        };

        let valid_wf = WorkflowDefinition::new(
            "Valid DAG",
            "A -> B -> C",
            vec![
                mk_task(t1, "Stage A", vec![]),
                mk_task(t2, "Stage B", vec![t1]),
                mk_task(t3, "Stage C", vec![t1, t2]),
            ],
            1000,
        )
        .unwrap();
        let order = valid_wf.topological_order().unwrap();
        assert_eq!(order, vec![t1, t2, t3]);

        // Cycle: t1 -> t2 -> t1
        let cyclic = WorkflowDefinition::new(
            "Cyclic DAG",
            "Should reject",
            vec![mk_task(t1, "Stage A", vec![t2]), mk_task(t2, "Stage B", vec![t1])],
            1000,
        );
        assert!(matches!(cyclic, Err(DomainError::CyclicWorkflowDependency(_))));
    }
}
