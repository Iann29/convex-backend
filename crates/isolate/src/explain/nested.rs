//! Inclusive reads within the sequential database-UDF transaction boundary.
use std::sync::atomic::Ordering;

use common::explain::{
    ExplainEvent,
    Outcome,
    TableReadUsage,
    TraceHandle,
};
use usage_tracking::{
    explain::ReadSnapshot,
    FunctionUsageTracker,
};

use super::{
    ExplainSlot,
    MAX_PENDING_EXPLAIN_SYSCALLS,
    PENDING_EXPLAIN_SYSCALLS,
};

pub(crate) struct NestedReadGuard {
    _slot: ExplainSlot,
    trace: Option<TraceHandle>,
    tracker: FunctionUsageTracker,
    before: ReadSnapshot,
}

impl NestedReadGuard {
    pub(crate) fn start(trace: TraceHandle, tracker: &FunctionUsageTracker) -> Option<Self> {
        if !trace.is_recording() {
            return None;
        }
        if PENDING_EXPLAIN_SYSCALLS
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < MAX_PENDING_EXPLAIN_SYSCALLS).then_some(count + 1)
            })
            .is_err()
        {
            trace.record_detail_drop();
            return None;
        }
        Some(Self {
            _slot: ExplainSlot,
            before: tracker.explain_read_snapshot(),
            tracker: tracker.clone(),
            trace: Some(trace),
        })
    }

    pub(crate) fn finish(mut self, complete: bool, outcome: Outcome) {
        self.emit(complete, complete.then_some(outcome));
    }

    fn emit(&mut self, complete: bool, outcome: Option<Outcome>) {
        let Some(trace) = self.trace.take() else {
            return;
        };
        if !trace.is_recording() {
            return;
        }
        let after = self.tracker.explain_read_snapshot();
        let Some(delta) = after.delta_since(&self.before) else {
            // A reset would invalidate subtraction. Do not manufacture zeros.
            trace.record_detail_drop();
            return;
        };
        if delta.truncated {
            trace.record_detail_drop();
        }
        let tables = delta
            .tables
            .into_iter()
            .map(|((component, table), reads)| TableReadUsage {
                component: component.to_string(),
                table,
                read_bytes: reads.database_read_bytes,
                io_read_bytes: reads.database_io_read_bytes,
                read_documents: reads.database_read_documents,
            })
            .collect();
        trace.record(ExplainEvent::NestedAccounting {
            root_execution_id: trace.root_execution_id().to_string(),
            scope: "nested_attempt_inclusive",
            complete,
            outcome,
            reads: delta.reads,
            tables,
            writes: "shared_transaction",
        });
    }
}

impl Drop for NestedReadGuard {
    fn drop(&mut self) {
        // Cancellation may drop the future without returning its transaction.
        // The cloned tracker still contains only work observed up to this drop.
        self.emit(false, None);
    }
}

#[cfg(test)]
mod tests {
    use common::{
        components::ComponentPath,
        execution_context::{
            ExecutionContext,
            RequestContext,
        },
        explain::{
            ExplainRecorder,
            StartCapture,
        },
        types::FunctionCaller,
        RequestId,
    };

    use super::*;

    fn read(tracker: &FunctionUsageTracker, rows: u64) {
        let table = "records".parse().unwrap();
        tracker.track_database_egress(ComponentPath::root(), &table, rows * 10, false);
        tracker.track_database_egress_v2(ComponentPath::root(), &table, rows * 11, false);
        tracker.track_database_egress_rows(ComponentPath::root(), &table, rows, false);
    }

    #[test]
    fn child_reads_and_abandoned_sibling_are_separate_without_fabricated_completion() {
        let recorder = ExplainRecorder::default();
        let capture = recorder
            .start(StartCapture {
                function: Some("child:lookup".into()),
                ..Default::default()
            })
            .unwrap();
        let context = ExecutionContext::new(
            RequestContext::new_for_system_request(RequestId::new()),
            &FunctionCaller::HttpEndpoint,
        );
        let root = recorder
            .begin(&context, "parent:f", "", "query", None)
            .unwrap();
        let tracker = FunctionUsageTracker::new();
        read(&tracker, 4);
        let child = root
            .handle()
            .begin_nested("child:lookup", "child", "query")
            .unwrap();
        let usage = NestedReadGuard::start(child.handle(), &tracker).unwrap();
        read(&tracker, 1);
        usage.finish(true, Outcome::Success);
        child.finish(Outcome::Success);
        assert_eq!(
            tracker
                .explain_read_snapshot()
                .reads
                .database_read_documents,
            5
        );
        let sibling = root
            .handle()
            .begin_nested("child:lookup", "child", "mutation")
            .unwrap();
        let usage = NestedReadGuard::start(sibling.handle(), &tracker).unwrap();
        read(&tracker, 2);
        // Models a canceled await: no normal return hook, no fabricated success.
        drop(usage);
        drop(sibling);
        let failed = root
            .handle()
            .begin_nested("child:lookup", "child", "mutation")
            .unwrap();
        let usage = NestedReadGuard::start(failed.handle(), &tracker).unwrap();
        read(&tracker, 3);
        usage.finish(true, Outcome::Error);
        failed.finish(Outcome::Error);
        let snapshot = recorder.poll(&capture.session_id, 0, None, false).unwrap();
        let accounting: Vec<_> = snapshot
            .events
            .iter()
            .filter_map(|event| match &event.event {
                ExplainEvent::NestedAccounting {
                    root_execution_id,
                    complete,
                    outcome,
                    reads,
                    tables,
                    ..
                } => Some((root_execution_id, complete, outcome, reads, tables)),
                _ => None,
            })
            .collect();
        assert_eq!(accounting.len(), 3);
        assert_eq!(accounting[0].0, &context.execution_id.to_string());
        assert!(*accounting[0].1);
        assert!(matches!(accounting[0].2, Some(Outcome::Success)));
        assert_eq!(accounting[0].3.database_read_documents, 1);
        assert_eq!(accounting[0].4[0].read_bytes, 10);
        assert!(!accounting[1].1);
        assert!(accounting[1].2.is_none());
        assert_eq!(accounting[1].3.database_read_documents, 2);
        assert!(*accounting[2].1);
        assert!(matches!(accounting[2].2, Some(Outcome::Error)));
        assert_eq!(accounting[2].3.database_read_documents, 3);
    }
}
