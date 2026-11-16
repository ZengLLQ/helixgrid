use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Domain-level errors enforcing HelixGrid invariants.
#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainError {
    #[error("illegal workload state transition from {from:?} to {to:?}")]
    IllegalStateTransition { from: String, to: String },

    #[error("invalid workload configuration: {0}")]
    InvalidConfiguration(String),

    #[error("invalid workflow definition: {0}")]
    InvalidWorkflow(String),

    #[error("cyclic dependency detected in workflow involving task '{0}'")]
    CyclicWorkflowDependency(String),

    #[error("unsafe retry rejected for non-idempotent workload: {0}")]
    UnsafeRetryRejected(String),

    #[error("invalid identifier: {0}")]
    InvalidId(String),
}
