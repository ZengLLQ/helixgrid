use crate::error::DomainError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

macro_rules! define_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            #[must_use]
            pub fn from_uuid(id: Uuid) -> Self {
                Self(id)
            }

            #[must_use]
            pub fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = DomainError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(s)
                    .map(Self)
                    .map_err(|e| DomainError::InvalidId(format!("{}: {}", stringify!($name), e)))
            }
        }
    };
}

define_id!(WorkloadId, "Unique identifier for a submitted workload.");
define_id!(ExecutionId, "Unique identifier for a single execution attempt.");
define_id!(WorkerId, "Unique identifier for a managed worker process.");
define_id!(WorkflowId, "Unique identifier for a multi-stage DAG workflow.");
define_id!(WorkflowTaskId, "Unique identifier for a task within a workflow.");
define_id!(ArtifactId, "Unique identifier for an execution artifact.");
define_id!(EventId, "Unique identifier for an observability event.");
