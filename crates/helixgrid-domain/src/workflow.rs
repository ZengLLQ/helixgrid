use crate::error::DomainError;
use crate::ids::{WorkflowId, WorkflowTaskId, WorkloadId};
use crate::workload::{Priority, ResourceRequirements, RetryPolicy, WorkloadSpec};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowStatus {
    Draft,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl WorkflowStatus {
    #[must_use]
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Draft => "Draft",
            Self::Running => "Running",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowTaskState {
    Pending,
    Dispatched,
    Completed,
    Failed,
    SkippedDueToDependencyFailure,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTaskDefinition {
    pub id: WorkflowTaskId,
    pub name: String,
    pub depends_on: Vec<WorkflowTaskId>,
    pub priority: Priority,
    pub spec: WorkloadSpec,
    pub resources: ResourceRequirements,
    pub timeout_ms: u64,
    pub retry_policy: RetryPolicy,
    pub state: WorkflowTaskState,
    pub workload_id: Option<WorkloadId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub id: WorkflowId,
    pub name: String,
    pub description: String,
    pub status: WorkflowStatus,
    pub tasks: Vec<WorkflowTaskDefinition>,
    pub created_at_utc: i64,
    pub updated_at_utc: i64,
}

impl WorkflowDefinition {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        tasks: Vec<WorkflowTaskDefinition>,
        now_utc: i64,
    ) -> Result<Self, DomainError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(DomainError::InvalidWorkflow("workflow name cannot be empty".to_string()));
        }
        let wf = Self {
            id: WorkflowId::new(),
            name,
            description: description.into(),
            status: WorkflowStatus::Draft,
            tasks,
            created_at_utc: now_utc,
            updated_at_utc: now_utc,
        };
        wf.topological_order()?;
        Ok(wf)
    }

    /// Validates DAG integrity and computes topological execution order (Kahn's algorithm).
    pub fn topological_order(&self) -> Result<Vec<WorkflowTaskId>, DomainError> {
        if self.tasks.is_empty() {
            return Err(DomainError::InvalidWorkflow(
                "workflow must contain at least one task".to_string(),
            ));
        }

        let mut task_ids = HashSet::with_capacity(self.tasks.len());
        let mut indegree: HashMap<WorkflowTaskId, usize> = HashMap::with_capacity(self.tasks.len());
        let mut dependents: HashMap<WorkflowTaskId, Vec<WorkflowTaskId>> =
            HashMap::with_capacity(self.tasks.len());

        for task in &self.tasks {
            if task.name.trim().is_empty() {
                return Err(DomainError::InvalidWorkflow(
                    "workflow task name cannot be empty".to_string(),
                ));
            }
            if !task_ids.insert(task.id) {
                return Err(DomainError::InvalidWorkflow(format!(
                    "duplicate workflow task id {}",
                    task.id
                )));
            }
            task.spec.validate()?;
            task.resources.validate()?;
            indegree.insert(task.id, 0);
            dependents.insert(task.id, Vec::new());
        }

        for task in &self.tasks {
            let mut seen_deps = HashSet::new();
            for dep in &task.depends_on {
                if *dep == task.id {
                    return Err(DomainError::CyclicWorkflowDependency(task.name.clone()));
                }
                if !task_ids.contains(dep) {
                    return Err(DomainError::InvalidWorkflow(format!(
                        "task '{}' depends on unknown task {}",
                        task.name, dep
                    )));
                }
                if seen_deps.insert(*dep) {
                    *indegree.entry(task.id).or_insert(0) += 1;
                    dependents.entry(*dep).or_default().push(task.id);
                }
            }
        }

        let mut queue = VecDeque::new();
        for task in &self.tasks {
            if indegree.get(&task.id).copied().unwrap_or(0) == 0 {
                queue.push_back(task.id);
            }
        }

        let mut ordered = Vec::with_capacity(self.tasks.len());
        while let Some(id) = queue.pop_front() {
            ordered.push(id);
            if let Some(children) = dependents.get(&id) {
                for child in children {
                    if let Some(deg) = indegree.get_mut(child) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(*child);
                        }
                    }
                }
            }
        }

        if ordered.len() != self.tasks.len() {
            let stuck_name = self
                .tasks
                .iter()
                .find(|t| indegree.get(&t.id).copied().unwrap_or(0) > 0)
                .map(|t| t.name.clone())
                .unwrap_or_else(|| "unknown".to_string());
            return Err(DomainError::CyclicWorkflowDependency(stuck_name));
        }

        Ok(ordered)
    }
}
