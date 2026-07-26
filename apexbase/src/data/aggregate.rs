use serde::{Deserialize, Serialize};

/// Aggregate functions shared by SQL planning and storage fast paths.
///
/// This type lives below both query and storage so storage kernels do not
/// depend on the query runtime merely to describe an aggregate operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl std::fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateFunc::Count => write!(f, "COUNT"),
            AggregateFunc::Sum => write!(f, "SUM"),
            AggregateFunc::Avg => write!(f, "AVG"),
            AggregateFunc::Min => write!(f, "MIN"),
            AggregateFunc::Max => write!(f, "MAX"),
        }
    }
}
