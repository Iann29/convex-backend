//! Bounded, read-only snapshots for diagnostic nested-call accounting.
use std::collections::{
    BTreeMap,
    BTreeSet,
};

use common::{
    components::ComponentPath,
    explain::ReadCounters,
};

use crate::FunctionUsageTracker;

const TABLE_LIMIT: usize = 128;

pub struct ReadSnapshot {
    pub reads: ReadCounters,
    pub tables: BTreeMap<(ComponentPath, String), ReadCounters>,
    pub truncated: bool,
}

fn difference(after: &ReadCounters, before: &ReadCounters) -> Option<ReadCounters> {
    Some(ReadCounters {
        database_read_bytes: after
            .database_read_bytes
            .checked_sub(before.database_read_bytes)?,
        database_io_read_bytes: after
            .database_io_read_bytes
            .checked_sub(before.database_io_read_bytes)?,
        database_read_documents: after
            .database_read_documents
            .checked_sub(before.database_read_documents)?,
    })
}

impl ReadSnapshot {
    pub fn delta_since(&self, before: &Self) -> Option<Self> {
        let reads = difference(&self.reads, &before.reads)?;
        let mut tables = BTreeMap::new();
        for (key, after) in &self.tables {
            // Tracker maps only grow. A key in the final first128 but absent
            // from the initial first128 is new, not a dropped initial key.
            let zero = ReadCounters::default();
            let delta = difference(after, before.tables.get(key).unwrap_or(&zero))?;
            if delta.database_read_bytes != 0
                || delta.database_io_read_bytes != 0
                || delta.database_read_documents != 0
            {
                tables.insert(key.clone(), delta);
            }
        }
        Some(Self {
            reads,
            tables,
            truncated: self.truncated || before.truncated,
        })
    }
}

impl FunctionUsageTracker {
    /// Clones no storage/network state and never changes billing counters.
    /// Totals cover every table; only the first128 table details are retained.
    pub fn explain_read_snapshot(&self) -> ReadSnapshot {
        let state = self.state.lock();
        let mut keys = BTreeSet::new();
        for map in [
            &state.database_egress,
            &state.database_egress_v2,
            &state.database_egress_rows,
        ] {
            for key in map.keys().take(TABLE_LIMIT + 1) {
                if keys.contains(key) {
                    continue;
                }
                if keys.len() == TABLE_LIMIT + 1 {
                    if keys.last().is_some_and(|last| key >= last) {
                        continue;
                    }
                    keys.pop_last();
                }
                keys.insert(key.clone());
            }
        }
        let truncated = keys.len() > TABLE_LIMIT;
        let tables = keys
            .into_iter()
            .take(TABLE_LIMIT)
            .map(|key| {
                let counts = ReadCounters {
                    database_read_bytes: state
                        .database_egress
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                    database_io_read_bytes: state
                        .database_egress_v2
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                    database_read_documents: state
                        .database_egress_rows
                        .get(&key)
                        .copied()
                        .unwrap_or_default(),
                };
                (key, counts)
            })
            .collect();
        ReadSnapshot {
            reads: ReadCounters {
                database_read_bytes: state.database_egress.values().sum(),
                database_io_read_bytes: state.database_egress_v2.values().sum(),
                database_read_documents: state.database_egress_rows.values().sum(),
            },
            tables,
            truncated,
        }
    }
}

#[cfg(test)]
mod tests {
    use common::components::ComponentPath;

    use super::*;

    fn read(tracker: &FunctionUsageTracker, table: &str, rows: u64) {
        let table = table.parse().unwrap();
        tracker.track_database_egress(ComponentPath::root(), &table, rows * 10, false);
        tracker.track_database_egress_v2(ComponentPath::root(), &table, rows * 11, false);
        tracker.track_database_egress_rows(ComponentPath::root(), &table, rows, false);
    }

    #[test]
    fn nested_reads_exclude_prior_parent_and_sibling_work() {
        let tracker = FunctionUsageTracker::new();
        read(&tracker, "parent", 4);
        let child = tracker.explain_read_snapshot();
        read(&tracker, "child", 1);
        let after_child = tracker.explain_read_snapshot();
        let delta = after_child.delta_since(&child).unwrap();
        assert_eq!(delta.reads.database_read_documents, 1);
        assert_eq!(delta.reads.database_read_bytes, 10);
        assert_eq!(delta.tables.len(), 1);
        assert_eq!(after_child.reads.database_read_documents, 5);
        read(&tracker, "sibling", 2);
        assert_eq!(
            tracker
                .explain_read_snapshot()
                .delta_since(&after_child)
                .unwrap()
                .reads
                .database_read_documents,
            2
        );
        // Reads remain attempted work even when the caller rolls back writes.
        assert_eq!(
            tracker
                .explain_read_snapshot()
                .delta_since(&child)
                .unwrap()
                .reads
                .database_read_documents,
            3
        );
    }

    #[test]
    fn bounded_tables_keep_exact_totals_and_new_keys_do_not_recount_old_reads() {
        let tracker = FunctionUsageTracker::new();
        for i in 0..140 {
            read(&tracker, &format!("t{i:03}"), 1);
        }
        let before = tracker.explain_read_snapshot();
        assert_eq!(before.tables.len(), 128);
        assert!(before.truncated);
        assert_eq!(before.reads.database_read_documents, 140);
        read(&tracker, "aaa", 2);
        let delta = tracker
            .explain_read_snapshot()
            .delta_since(&before)
            .unwrap();
        assert_eq!(delta.reads.database_read_documents, 2);
        assert_eq!(delta.tables.len(), 1);
        assert!(delta.truncated);
    }
}
