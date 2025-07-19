use crate::{Query, Table};
use std::collections::HashMap;

pub struct QueryPlanner {}

impl QueryPlanner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn optimize(&self, query: Query, table: &Table) -> Query {
        self.optimize_query(query, table)
    }

    fn optimize_query(&self, query: Query, table: &Table) -> Query {
        match query {
            Query::And(mut queries) => {
                queries.sort_by_key(|q| self.estimate_cost(q, table));
                Query::And(queries)
            }
            _ => query,
        }
    }

    fn estimate_cost(&self, query: &Query, table: &Table) -> u64 {
        match query {
            Query::Condition(condition) => {
                if table.indexes.contains_key(&condition.column) {
                    1 // Low cost for indexed columns
                } else {
                    10 // High cost for non-indexed columns
                }
            }
            Query::And(queries) => queries.iter().map(|q| self.estimate_cost(q, table)).sum(),
            Query::Or(queries) => queries.iter().map(|q| self.estimate_cost(q, table)).sum(),
            _ => 100, // Default high cost for other query types
        }
    }
}
