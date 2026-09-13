//! Opt-in, deployment-owned execution capture. Only typed metadata enters the
//! buffer.
use std::{
    collections::{
        BTreeMap,
        VecDeque,
    },
    sync::{
        atomic::{
            AtomicBool,
            AtomicU64,
            AtomicU8,
            AtomicUsize,
            Ordering,
        },
        Arc,
        LazyLock,
        OnceLock,
        Weak,
    },
    time::{
        Duration,
        Instant,
        SystemTime,
        UNIX_EPOCH,
    },
};

use errors::ErrorMetadata;
use parking_lot::Mutex;
use serde::{
    Deserialize,
    Serialize,
};
use uuid::Uuid;
use value::{
    id_v6::DeveloperDocumentId,
    sha256::Sha256,
};

use crate::execution_context::{
    ExecutionContext,
    ExecutionId,
};

const MAX_SESSIONS: usize = 4;
const MAX_BINDINGS: usize = 10_000;
const GRACE: Duration = Duration::from_secs(300);
const MAX_ITEMS: usize = 128;
const MAX_STRING: usize = 1024;
static CLOCK: LazyLock<Instant> = LazyLock::new(Instant::now);
static PROCESS_ID: LazyLock<String> = LazyLock::new(|| Uuid::new_v4().to_string());
static BINDING_COUNT: AtomicUsize = AtomicUsize::new(0);
static ACTIVE_RECORDERS: AtomicUsize = AtomicUsize::new(0);
static BINDINGS: LazyLock<Mutex<BTreeMap<ExecutionId, Weak<Trace>>>> =
    LazyLock::new(Default::default);

/// Process-local retry coordinator. Owns at most the latest attempt and its
/// bounded completion until the caller decides whether the error is retriable.
#[derive(Clone, Default)]
pub struct ExplainInvocation(Arc<Mutex<InvocationState>>);
#[derive(Default)]
struct InvocationState {
    logical_id: Option<ExecutionId>,
    attempt: u64,
    trace: Option<TraceHandle>,
}
impl std::fmt::Debug for ExplainInvocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExplainInvocation")
    }
}
impl PartialEq for ExplainInvocation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for ExplainInvocation {}
impl ExplainInvocation {
    fn identity(&self, execution_id: ExecutionId) -> (ExecutionId, u64) {
        let mut state = self.0.lock();
        (
            *state.logical_id.get_or_insert(execution_id),
            state.attempt + 1,
        )
    }

    /// Must follow the coordinator's retry decision, before waiting or
    /// returning.
    pub fn finish_attempt(&self, outcome: Outcome) -> Option<TraceHandle> {
        let mut state = self.0.lock();
        if matches!(outcome, Outcome::Retry) {
            state.attempt += 1;
        }
        let trace = state.trace.take()?;
        drop(state);
        trace.finish_coordinated(outcome);
        Some(trace)
    }
}
impl Drop for InvocationState {
    fn drop(&mut self) {
        if let Some(trace) = self.trace.take() {
            trace.finish_coordinated(Outcome::Abandoned);
        }
    }
}

pub fn captures_active() -> bool {
    ACTIVE_RECORDERS.load(Ordering::Acquire) > 0
}

pub fn monotonic_offset() -> Duration {
    CLOCK.elapsed()
}
pub fn monotonic_ms() -> f64 {
    monotonic_offset().as_secs_f64() * 1000.0
}
fn safe_request_id(context: &ExecutionContext) -> String {
    Sha256::hash(context.request_id.as_str().as_bytes()).as_hex()
}
fn timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartCapture {
    pub function: Option<String>,
    pub component: Option<String>,
    pub duration_seconds: Option<u64>,
    pub max_events: Option<usize>,
    pub max_bytes: Option<usize>,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureLimits {
    pub duration_seconds: u64,
    pub max_events: usize,
    pub max_bytes: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct CaptureFilter {
    pub function: Option<String>,
    pub component: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureState {
    Capturing,
    Stopped,
    Expired,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureCoverage {
    pub dropped_events: u64,
    pub truncated_events: u64,
    pub lost_bindings: u64,
    pub dropped_detail_events: u64,
    pub earliest_cursor: u64,
    pub continuity_lost: bool,
    pub retained_bytes: usize,
    pub matching_events: u64,
    pub sampling: &'static str,
    pub external_process_spans: &'static str,
    pub request_id_encoding: &'static str,
    pub call_boundary: &'static str,
    pub system_function_counters: &'static str,
    pub parent_context_provenance: &'static str,
    pub http_cancellation_scope: &'static str,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureSnapshot {
    pub schema_version: u32,
    pub process_id: String,
    pub session_id: String,
    pub state: CaptureState,
    pub started_at: f64,
    pub expires_at: f64,
    pub filter: CaptureFilter,
    pub limits: CaptureLimits,
    pub coverage: CaptureCoverage,
    /// Shared with the session buffer: a poll bumps refcounts under the
    /// recorder lock instead of deep-cloning every retained event there.
    #[serde(serialize_with = "serialize_shared_events")]
    pub events: Vec<Arc<CapturedEvent>>,
    pub next_cursor: u64,
}

fn serialize_shared_events<S: serde::Serializer>(
    events: &Vec<Arc<CapturedEvent>>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(events.iter().map(|event| &**event))
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapturedEvent {
    pub cursor: u64,
    pub timestamp: f64,
    pub monotonic_ms: f64,
    pub request_id: String,
    pub execution_id: String,
    pub logical_call_id: String,
    pub attempt_number: u64,
    pub execution_identity: &'static str,
    pub cache_iteration: Option<u64>,
    pub function: String,
    pub component: String,
    pub udf_type: String,
    pub event: ExplainEvent,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceLocation {
    pub file: String,
    pub line: u32,
    pub column: u32,
    pub mapped: bool,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SourceUnavailable {
    SourceMapMissing,
    TokenMissing,
    UserFrameUnavailable,
    SourceLimitExceeded,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SyscallTiming {
    Synchronous,
    AsyncBatch,
    AsyncTask,
}
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Phase {
    Admission,
    Execution,
    Commit,
    Persistence,
    RetryBackoff,
    External,
    Total,
    CacheLookup,
    CacheWait,
    CacheValidation,
    CommitQueue,
    CommitValidation,
    PersistenceQueue,
    PersistenceBackoff,
}
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Success,
    Error,
    Retry,
    Cancelled,
    Abandoned,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Accounting {
    CachedReplay,
    InclusiveAttempt,
    Unavailable,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Committed,
    Discarded,
    ReadOnly,
    ExternalEffectsUnknown,
}
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Counters {
    pub database_read_bytes: u64,
    pub database_write_bytes: u64,
    pub database_io_read_bytes: u64,
    pub database_io_write_bytes: u64,
    pub database_read_documents: u64,
    pub database_write_documents: u64,
    pub database_write_index_rows: u64,
    pub storage_read_bytes: u64,
    pub storage_write_bytes: u64,
    pub vector_index_read_bytes: u64,
    pub vector_index_write_bytes: u64,
    pub text_index_query_bytes: u64,
    pub text_index_write_query_bytes: u64,
    pub vector_index_read_query_bytes: u64,
    pub vector_index_write_query_bytes: u64,
    pub network_egress_bytes: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadCounters {
    pub database_read_bytes: u64,
    pub database_io_read_bytes: u64,
    pub database_read_documents: u64,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableReadUsage {
    pub component: String,
    pub table: String,
    pub read_bytes: u64,
    pub io_read_bytes: u64,
    pub read_documents: u64,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableAccess {
    pub table: String,
    pub rows_read: u64,
    pub rows_written: u64,
    pub rows_created: u64,
    pub rows_deleted: u64,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableUsage {
    pub component: String,
    pub table: String,
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub io_read_bytes: u64,
    pub io_write_bytes: u64,
    pub read_documents: u64,
    pub write_documents: u64,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyscallSummary {
    pub operation: String,
    pub invocation_count: u32,
    pub error_count: u32,
    pub duration_ms: f64,
}

/// All fields are code/schema metadata or counters. Never put arguments,
/// exception messages, console text, URLs, credentials or document values here.
#[derive(Clone, Debug, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ExplainEvent {
    Start {
        parent_execution_id: Option<String>,
        is_root: bool,
        parent_scheduled_job: Option<String>,
    },
    Completion {
        outcome: Outcome,
        cache_hit: bool,
        environment: String,
        memory_allocation_mb: u64,
        will_retry: bool,
        occ: bool,
        attempt_number: Option<u64>,
        caller: String,
        query_invocation: Option<String>,
        execution_duration_ms: f64,
        user_execution_duration_ms: Option<f64>,
        mutation_queue_length: Option<usize>,
        accounting: Accounting,
        accounting_unavailable_reason: Option<&'static str>,
        disposition: Disposition,
        counters: Option<Counters>,
        tables: Vec<TableAccess>,
        syscalls: Vec<SyscallSummary>,
        syscall_accounting: &'static str,
    },
    NestedAccounting {
        root_execution_id: String,
        scope: &'static str,
        complete: bool,
        outcome: Option<Outcome>,
        reads: ReadCounters,
        tables: Vec<TableReadUsage>,
        writes: &'static str,
    },
    TransactionAccounting {
        root_execution_id: String,
        scope: &'static str,
        complete: bool,
        outcome: Outcome,
        accounting: Option<Accounting>,
        accounting_unavailable_reason: Option<&'static str>,
        disposition: Option<Disposition>,
        counters: Option<Counters>,
        tables: Vec<TableUsage>,
    },
    End {
        outcome: Outcome,
    },
    Phase {
        phase: Phase,
        start_offset_ms: f64,
        duration_ms: f64,
    },
    Query {
        query_id: String,
        operation: String,
        table: Option<String>,
        index: Option<String>,
        source: Option<SourceLocation>,
        source_unavailable: Option<SourceUnavailable>,
    },
    Syscall {
        operation: String,
        start_offset_ms: f64,
        duration_ms: f64,
        invocation_count: u32,
        error_count: u32,
        query_ids: Vec<String>,
        source: Option<SourceLocation>,
        source_unavailable: Option<SourceUnavailable>,
        timing: SyscallTiming,
        queued_ms: f64,
    },
    CacheStep {
        iteration: u64,
        operation: &'static str,
        outcome: &'static str,
        discarded_cumulative: Option<Counters>,
    },
    TableUsage {
        tables: Vec<TableUsage>,
        counters_reference: &'static str,
    },
    Subscription {
        subscription_id: String,
        invalidated_by: Option<String>,
        causality: String,
    },
    Scheduler {
        scheduled_job_id: String,
        system_retry_count: u32,
        occ_retry_count: u32,
    },
    PersistenceBatch {
        start_offset_ms: f64,
        duration_ms: Option<f64>,
        outcome: Option<Outcome>,
        will_retry: bool,
        batch_id: u64,
        attempt: u32,
        commit_count: usize,
        document_rows: usize,
        index_rows: usize,
        shared: bool,
    },
    Causal {
        relation: String,
        target_execution_id: Option<String>,
        scheduled_job_id: Option<String>,
        causality: String,
    },
}

/// Unknown syscall names are caller-controlled and may contain secrets.
pub fn safe_syscall_operation(operation: &str) -> &str {
    match operation {
        "1.0/actions/action"
        | "1.0/actions/cancel_job"
        | "1.0/actions/mutation"
        | "1.0/actions/query"
        | "1.0/actions/schedule"
        | "1.0/actions/vectorSearch"
        | "1.0/auditLog"
        | "1.0/cancel_job"
        | "1.0/componentArgument"
        | "1.0/count"
        | "1.0/createFunctionHandle"
        | "1.0/createServiceToken"
        | "1.0/db/normalizeId"
        | "1.0/get"
        | "1.0/getDeploymentMetadata"
        | "1.0/getFunctionMetadata"
        | "1.0/getRequestMetadata"
        | "1.0/getSnapshotTs"
        | "1.0/getTransactionMetrics"
        | "1.0/getUserIdentity"
        | "1.0/insert"
        | "1.0/queryCleanup"
        | "1.0/queryPage"
        | "1.0/queryStream"
        | "1.0/queryStreamNext"
        | "1.0/remove"
        | "1.0/replace"
        | "1.0/requireOperation"
        | "1.0/runUdf"
        | "1.0/schedule"
        | "1.0/shallowMerge"
        | "1.0/storageDelete"
        | "1.0/storageGenerateUploadUrl"
        | "1.0/storageGetMetadata"
        | "1.0/storageGetUrl"
        | "1.0/writeDeploymentAuditLog" => operation,
        _ => "unknown_operation",
    }
}
fn scrub_operation(operation: &mut String) -> bool {
    if safe_syscall_operation(operation) == operation {
        false
    } else {
        *operation = "unknown_operation".into();
        true
    }
}

fn cap(s: &mut String) -> bool {
    if s.len() <= MAX_STRING {
        return false;
    }
    s.truncate(s.floor_char_boundary(MAX_STRING));
    s.shrink_to_fit();
    true
}
fn cap_option(s: &mut Option<String>) -> bool {
    s.as_mut().is_some_and(cap)
}
fn cap_vec<T>(items: &mut Vec<T>) -> bool {
    if items.len() <= MAX_ITEMS {
        return false;
    }
    items.truncate(MAX_ITEMS);
    items.shrink_to_fit();
    true
}
impl ExplainEvent {
    fn bound(&mut self) -> bool {
        match self {
            Self::Start {
                parent_execution_id,
                parent_scheduled_job,
                ..
            } => cap_option(parent_execution_id) | cap_option(parent_scheduled_job),
            Self::Completion {
                caller,
                environment,
                query_invocation,
                tables,
                syscalls,
                ..
            } => {
                let mut changed = cap(caller)
                    | cap(environment)
                    | cap_option(query_invocation)
                    | cap_vec(tables)
                    | cap_vec(syscalls);
                for table in tables {
                    changed |= cap(&mut table.table);
                }
                for syscall in syscalls {
                    changed |= scrub_operation(&mut syscall.operation);
                }
                changed
            },
            Self::Query {
                query_id,
                operation,
                table,
                index,
                source,
                ..
            } => {
                cap(query_id)
                    | cap(operation)
                    | cap_option(table)
                    | cap_option(index)
                    | source.as_mut().is_some_and(|s| cap(&mut s.file))
            },
            Self::Syscall {
                operation,
                query_ids,
                source,
                ..
            } => {
                let mut changed = scrub_operation(operation)
                    | cap_vec(query_ids)
                    | source.as_mut().is_some_and(|s| cap(&mut s.file));
                for id in query_ids {
                    changed |= cap(id);
                }
                changed
            },
            Self::Causal {
                relation,
                target_execution_id,
                scheduled_job_id,
                causality,
            } => {
                cap(relation)
                    | cap_option(target_execution_id)
                    | cap_option(scheduled_job_id)
                    | cap(causality)
            },
            Self::NestedAccounting {
                root_execution_id,
                tables,
                ..
            } => {
                let mut changed = cap(root_execution_id) | cap_vec(tables);
                for table in tables {
                    changed |= cap(&mut table.component) | cap(&mut table.table);
                }
                changed
            },
            Self::TransactionAccounting {
                root_execution_id,
                tables,
                ..
            } => {
                let mut changed = cap(root_execution_id) | cap_vec(tables);
                for table in tables {
                    changed |= cap(&mut table.component) | cap(&mut table.table);
                }
                changed
            },
            Self::TableUsage { tables, .. } => {
                let mut changed = cap_vec(tables);
                for table in tables {
                    changed |= cap(&mut table.component) | cap(&mut table.table);
                }
                changed
            },
            Self::Subscription {
                subscription_id,
                invalidated_by,
                causality,
            } => cap(subscription_id) | cap_option(invalidated_by) | cap(causality),
            Self::Scheduler {
                scheduled_job_id, ..
            } => cap(scheduled_job_id),
            Self::End { .. }
            | Self::Phase { .. }
            | Self::PersistenceBatch { .. }
            | Self::CacheStep { .. } => false,
        }
    }
}

#[derive(Clone)]
pub struct ExplainRecorder(Arc<Recorder>);
struct Recorder {
    active: AtomicBool,
    sessions: Mutex<BTreeMap<String, Session>>,
    scheduled: Mutex<BTreeMap<String, ScheduledBinding>>,
}
#[derive(Clone)]
struct ScheduledBinding {
    sessions: Vec<String>,
    parent: ExecutionId,
    expires: Instant,
    source: ScheduledSource,
}
/// Bounded envelope metadata only. No trace/recorder reference cycle.
#[derive(Clone)]
struct ScheduledSource {
    request_id: String,
    logical_call_id: ExecutionId,
    attempt_number: u64,
    execution_identity: &'static str,
    function: String,
    component: String,
    udf_type: String,
}

pub struct ScheduledCancellationBatch {
    recorder: ExplainRecorder,
    jobs: Vec<String>,
    // At most MAX_SESSIONS keys; drops become visible only after commit.
    dropped: BTreeMap<String, u64>,
}
impl ScheduledCancellationBatch {
    pub fn stage(&mut self, id: &str) {
        let scheduled = self.recorder.0.scheduled.lock();
        let Some(binding) = scheduled.get(id).filter(|b| b.expires > Instant::now()) else {
            return;
        };
        if self.jobs.len() < MAX_ITEMS {
            self.jobs.push(id.to_owned());
        } else {
            let sessions = self.recorder.0.sessions.lock();
            // A long transaction can outlive a capture. Keep only live capture
            // IDs so the diagnostic map remains bounded by MAX_SESSIONS.
            self.dropped.retain(|id, _| {
                sessions
                    .get(id)
                    .is_some_and(|s| s.state == CaptureState::Capturing)
            });
            for id in &binding.sessions {
                if sessions
                    .get(id)
                    .is_some_and(|s| s.state == CaptureState::Capturing)
                {
                    *self.dropped.entry(id.clone()).or_default() += 1;
                }
            }
        }
    }

    /// Call only for the successful transaction attempt.
    pub fn commit(self) {
        for job in self.jobs {
            self.recorder.record_scheduled_canceled(&job, None);
        }
        if !self.dropped.is_empty() {
            let mut sessions = self.recorder.0.sessions.lock();
            self.recorder.sweep(&mut sessions, Instant::now());
            for (id, count) in self.dropped {
                if let Some(session) = sessions.get_mut(&id)
                    && session.state == CaptureState::Capturing
                {
                    session.dropped_detail_events += count;
                }
            }
        }
    }
}
impl Recorder {
    fn set_active(&self, active: bool) {
        let previous = self.active.swap(active, Ordering::AcqRel);
        if active && !previous {
            ACTIVE_RECORDERS.fetch_add(1, Ordering::Release);
        }
        if !active && previous {
            ACTIVE_RECORDERS.fetch_sub(1, Ordering::Release);
        }
    }
}
impl Drop for Recorder {
    fn drop(&mut self) {
        self.set_active(false);
    }
}
struct Session {
    started: Instant,
    stopped: Option<Instant>,
    started_at: f64,
    state: CaptureState,
    filter: CaptureFilter,
    limits: CaptureLimits,
    events: VecDeque<(Arc<CapturedEvent>, usize)>,
    bytes: usize,
    cursor: u64,
    dropped: u64,
    truncated: u64,
    lost_bindings: u64,
    dropped_detail_events: u64,
}
impl Session {
    fn capturing(&mut self, now: Instant) -> bool {
        if self.state == CaptureState::Capturing
            && now.duration_since(self.started) >= Duration::from_secs(self.limits.duration_seconds)
        {
            self.state = CaptureState::Expired;
            self.stopped = Some(self.started + Duration::from_secs(self.limits.duration_seconds));
        }
        self.state == CaptureState::Capturing
    }

    fn matches(&self, function: &str, component: &str) -> bool {
        self.filter
            .function
            .as_deref()
            .is_none_or(|f| f == function)
            && self
                .filter
                .component
                .as_deref()
                .is_none_or(|c| c == component)
    }

    fn push(&mut self, mut event: CapturedEvent, truncated: bool) {
        self.cursor += 1;
        event.cursor = self.cursor;
        if truncated {
            self.truncated += 1;
        }
        // Count encoded bytes plus envelope storage. The serialized size also
        // covers strings; this conservative bound includes vector/map overhead.
        let size = serde_json::to_vec(&event).map_or(usize::MAX, |v| {
            v.len()
                .saturating_mul(2)
                .saturating_add(std::mem::size_of::<CapturedEvent>())
        });
        if size > self.limits.max_bytes {
            self.dropped += 1;
            return;
        }
        while self.events.len() >= self.limits.max_events
            || self.bytes + size > self.limits.max_bytes
        {
            if let Some((_, size)) = self.events.pop_front() {
                self.bytes -= size;
                self.dropped += 1;
            } else {
                break;
            }
        }
        self.bytes += size;
        self.events.push_back((Arc::new(event), size));
    }

    fn snapshot(&self, id: &str, cursor: u64) -> CaptureSnapshot {
        let earliest = self
            .events
            .front()
            .map_or(self.cursor + 1, |(event, _)| event.cursor);
        CaptureSnapshot {
            schema_version: 1,
            process_id: PROCESS_ID.clone(),
            session_id: id.to_owned(),
            state: self.state,
            started_at: self.started_at,
            expires_at: self.started_at + self.limits.duration_seconds as f64,
            filter: self.filter.clone(),
            limits: self.limits.clone(),
            coverage: CaptureCoverage {
                dropped_events: self.dropped,
                truncated_events: self.truncated,
                lost_bindings: self.lost_bindings,
                dropped_detail_events: self.dropped_detail_events,
                earliest_cursor: earliest,
                continuity_lost: cursor.saturating_add(1) < earliest || self.dropped > 0,
                retained_bytes: self.bytes,
                matching_events: self.cursor,
                sampling: "all_matching_with_bounded_buffer",
                external_process_spans: "unavailable",
                request_id_encoding: "sha256",
                call_boundary: "event_time_partial_calls",
                system_function_counters: "unavailable_when_system_function_logging_is_suppressed",
                parent_context_provenance: "runtime_transport_context_not_cryptographic_proof",
                http_cancellation_scope: "request_stream_not_udf_abort_or_rollback",
            },
            events: self
                .events
                .iter()
                .filter(|(e, _)| e.cursor > cursor)
                .map(|(e, _)| Arc::clone(e))
                .collect(),
            next_cursor: self.cursor,
        }
    }
}
impl Default for ExplainRecorder {
    fn default() -> Self {
        Self(Arc::new(Recorder {
            active: AtomicBool::new(false),
            sessions: Mutex::new(BTreeMap::new()),
            scheduled: Mutex::new(BTreeMap::new()),
        }))
    }
}
impl ExplainRecorder {
    fn sweep(&self, sessions: &mut BTreeMap<String, Session>, now: Instant) {
        sessions.retain(|_, session| {
            session.capturing(now);
            session
                .stopped
                .is_none_or(|stop| now.duration_since(stop) < GRACE)
        });
        self.0.set_active(
            sessions
                .values()
                .any(|s| s.state == CaptureState::Capturing),
        );
    }

    pub fn start(&self, request: StartCapture) -> anyhow::Result<CaptureSnapshot> {
        let limits = CaptureLimits {
            duration_seconds: request.duration_seconds.unwrap_or(60),
            max_events: request.max_events.unwrap_or(2000),
            max_bytes: request.max_bytes.unwrap_or(2 * 1024 * 1024),
        };
        if !(1..=300).contains(&limits.duration_seconds)
            || !(1..=10_000).contains(&limits.max_events)
            || !(1024..=8 * 1024 * 1024).contains(&limits.max_bytes)
            || request
                .function
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > MAX_STRING)
            || request
                .component
                .as_ref()
                .is_some_and(|s| s.len() > MAX_STRING)
        {
            anyhow::bail!(ErrorMetadata::bad_request(
                "ExplainInvalidLimits",
                "Invalid capture limits or filter"
            ));
        }
        let now = Instant::now();
        let mut sessions = self.0.sessions.lock();
        self.sweep(&mut sessions, now);
        if sessions.len() >= MAX_SESSIONS {
            anyhow::bail!(ErrorMetadata::overloaded(
                "ExplainSessionLimit",
                "At most four capture sessions may be retained per deployment"
            ));
        }
        let id = Uuid::new_v4().to_string();
        let session = Session {
            started: now,
            stopped: None,
            started_at: timestamp(),
            state: CaptureState::Capturing,
            filter: CaptureFilter {
                function: request.function,
                component: request.component,
            },
            limits,
            events: VecDeque::new(),
            bytes: 0,
            cursor: 0,
            dropped: 0,
            truncated: 0,
            lost_bindings: 0,
            dropped_detail_events: 0,
        };
        let snapshot = session.snapshot(&id, 0);
        sessions.insert(id, session);
        self.0.set_active(true);
        Ok(snapshot)
    }

    pub fn poll(
        &self,
        id: &str,
        cursor: u64,
        process_id: Option<&str>,
        stop: bool,
    ) -> anyhow::Result<CaptureSnapshot> {
        if process_id.is_some_and(|id| id != PROCESS_ID.as_str()) {
            anyhow::bail!(ErrorMetadata::bad_request(
                "ExplainProcessChanged",
                "Capture continuity lost because the backend process changed"
            ));
        }
        let now = Instant::now();
        let mut sessions = self.0.sessions.lock();
        self.sweep(&mut sessions, now);
        let Some(session) = sessions.get_mut(id) else {
            anyhow::bail!(ErrorMetadata::not_found(
                "ExplainSessionNotFound",
                "Capture missing, expired, or lost after backend restart"
            ));
        };
        if cursor > session.cursor {
            anyhow::bail!(ErrorMetadata::bad_request(
                "ExplainInvalidCursor",
                "Cursor is ahead of this capture"
            ));
        }
        if stop && session.state == CaptureState::Capturing {
            session.state = CaptureState::Stopped;
            session.stopped = Some(now);
        }
        let result = session.snapshot(id, cursor);
        self.sweep(&mut sessions, now);
        Ok(result)
    }

    /// Reclaims expired buffers even when no executions arrive.
    pub fn cleanup(&self) {
        self.sweep(&mut self.0.sessions.lock(), Instant::now());
        self.0
            .scheduled
            .lock()
            .retain(|_, binding| binding.expires > Instant::now());
        let mut bindings = BINDINGS.lock();
        bindings.retain(|_, trace| trace.upgrade().is_some_and(|t| t.expires > Instant::now()));
        BINDING_COUNT.store(bindings.len(), Ordering::Release);
    }

    pub fn is_capturing(&self) -> bool {
        self.is_active()
    }

    pub fn is_active(&self) -> bool {
        self.0.active.load(Ordering::Acquire)
    }

    pub fn begin(
        &self,
        context: &ExecutionContext,
        function: &str,
        component: &str,
        udf_type: &str,
        parent: Option<ExecutionId>,
    ) -> Option<TraceGuard> {
        self.begin_attempt(
            context,
            function,
            component,
            udf_type,
            parent,
            context.execution_id,
            1,
        )
    }

    pub fn begin_attempt(
        &self,
        context: &ExecutionContext,
        function: &str,
        component: &str,
        udf_type: &str,
        parent: Option<ExecutionId>,
        logical_call_id: ExecutionId,
        attempt_number: u64,
    ) -> Option<TraceGuard> {
        if !self.is_active() {
            return None;
        }
        let invocation = context.request_metadata.explain_invocation.as_ref();
        let (logical_call_id, attempt_number) = invocation
            .map(|invocation| invocation.identity(context.execution_id))
            .unwrap_or((logical_call_id, attempt_number));
        let scheduled = context.parent_scheduled_job.and_then(|(_, id)| {
            self.0
                .scheduled
                .lock()
                .get(&id.to_string())
                .filter(|b| b.expires > Instant::now())
                .cloned()
        });
        let parent = parent.or_else(|| scheduled.as_ref().map(|b| b.parent));
        let inherited = parent
            .and_then(TraceHandle::lookup_id)
            .filter(|h| Arc::ptr_eq(&h.0.recorder.0, &self.0));
        let now = Instant::now();
        let mut sessions = self.0.sessions.lock();
        self.sweep(&mut sessions, now);
        let selected: Vec<String> = sessions
            .iter()
            .filter(|(id, s)| {
                s.state == CaptureState::Capturing
                    && (s.matches(function, component)
                        || inherited
                            .as_ref()
                            .is_some_and(|h| h.0.sessions.contains(id))
                        || scheduled.as_ref().is_some_and(|b| b.sessions.contains(id)))
            })
            .map(|(id, _)| id.clone())
            .collect();
        // Unmatched executions carry only bounded context so a matching nested
        // call can join this recorder. They never record events or source stacks.
        let mut bindings = BINDINGS.lock();
        if bindings.len() >= MAX_BINDINGS {
            bindings.retain(|_, trace| trace.upgrade().is_some_and(|t| t.expires > now));
        }
        if bindings.len() >= MAX_BINDINGS {
            for session in sessions
                .values_mut()
                .filter(|s| s.state == CaptureState::Capturing)
            {
                session.lost_bindings += 1;
            }
            return None;
        }
        let expires = sessions
            .values()
            .filter(|s| s.state == CaptureState::Capturing)
            .map(|s| s.started + Duration::from_secs(s.limits.duration_seconds))
            .max()?;
        let bounded = |value: &str| value[..value.floor_char_boundary(MAX_STRING)].to_owned();
        let transaction = Arc::new(TransactionTrace {
            root_execution_id: context.execution_id,
            root_sessions: selected.clone(),
            descendant_sessions: Mutex::new(Vec::new()),
            tables: Mutex::new(Vec::new()),
            tables_truncated: AtomicBool::new(false),
            projection_state: AtomicU8::new(0),
        });
        let trace = Arc::new(Trace {
            transaction,
            recorder: self.clone(),
            sessions: selected,
            expires,
            execution_id: context.execution_id,
            logical_call_id,
            attempt_number,
            execution_identity: "runtime_execution",
            cache_iteration: AtomicU64::new(0),
            request_id: safe_request_id(context),
            function: bounded(function),
            component: bounded(component),
            udf_type: bounded(udf_type),
            completed: AtomicBool::new(false),
            coordinated: AtomicBool::new(false),
            pending_completion: Mutex::new(None),
            staged_jobs: Arc::new(Mutex::new(Vec::new())),
            scheduled_source: OnceLock::new(),
        });
        bindings.insert(context.execution_id, Arc::downgrade(&trace));
        BINDING_COUNT.store(bindings.len(), Ordering::Release);
        drop(bindings);
        drop(sessions);
        let handle = TraceHandle(trace);
        if let Some(invocation) = invocation {
            handle.0.coordinated.store(true, Ordering::Release);
            invocation.0.lock().trace = Some(handle.clone());
        }
        handle.record(ExplainEvent::Start {
            parent_execution_id: parent.map(|p| p.to_string()),
            is_root: context.is_root(),
            parent_scheduled_job: context.parent_scheduled_job.map(|(_, id)| id.to_string()),
        });
        if attempt_number == 1 && context.is_root() {
            if let Some((start, wait)) = context.request_metadata.explain_admission {
                handle.record(ExplainEvent::Phase {
                    phase: Phase::Admission,
                    start_offset_ms: start.as_secs_f64() * 1000.0,
                    duration_ms: wait.as_secs_f64() * 1000.0,
                });
            }
            if let Some(subscription) = &context.request_metadata.explain_subscription {
                handle.record(ExplainEvent::Subscription {
                    subscription_id: subscription.id.clone(),
                    invalidated_by: subscription.invalidated_by.map(|id| id.to_string()),
                    causality: if subscription.invalidated_by.is_some() {
                        "recorded"
                    } else {
                        "unavailable"
                    }
                    .into(),
                });
            }
        }
        Some(TraceGuard(handle))
    }

    /// Call only after the transaction creating this job committed.
    pub fn remember_scheduled_job(&self, handle: &TraceHandle, job_id: &str) {
        if !self.is_active() || !Arc::ptr_eq(&self.0, &handle.0.recorder.0) {
            return;
        }
        let Ok(job_id) = job_id.parse::<DeveloperDocumentId>() else {
            handle.record_detail_drop();
            return;
        };
        self.publish_scheduled_job(&job_id.to_string(), &handle.scheduled_binding());
    }

    fn publish_scheduled_job(&self, job_id: &str, binding: &ScheduledBinding) {
        if !self.is_active() || binding.sessions.is_empty() || binding.expires <= Instant::now() {
            return;
        }
        let now = Instant::now();
        let mut scheduled = self.0.scheduled.lock();
        if scheduled.len() >= MAX_BINDINGS {
            scheduled.retain(|_, binding| binding.expires > now);
        }
        if scheduled.len() >= MAX_BINDINGS {
            drop(scheduled);
            let mut sessions = self.0.sessions.lock();
            for id in &binding.sessions {
                if let Some(session) = sessions.get_mut(id) {
                    session.dropped_detail_events += 1;
                }
            }
            return;
        }
        scheduled.insert(job_id.to_owned(), binding.clone());
        drop(scheduled);
        self.record(
            binding.source.request_id.clone(),
            binding.parent,
            binding.source.logical_call_id,
            binding.source.attempt_number,
            binding.source.execution_identity,
            None,
            &binding.source.function,
            &binding.source.component,
            &binding.source.udf_type,
            Some(&binding.sessions),
            ExplainEvent::Causal {
                relation: "scheduled_job_committed".into(),
                target_execution_id: None,
                scheduled_job_id: Some(job_id.to_owned()),
                causality: "recorded".into(),
            },
        );
    }

    pub fn cancellation_batch(&self) -> Option<ScheduledCancellationBatch> {
        self.is_capturing().then(|| ScheduledCancellationBatch {
            recorder: self.clone(),
            jobs: Vec::new(),
            dropped: BTreeMap::new(),
        })
    }

    /// A committed job-state transition, not an execution of the canceled UDF.
    /// Removing the binding makes repeated cancellation notification
    /// idempotent.
    pub fn record_scheduled_canceled(&self, job_id: &str, canceled_by: Option<ExecutionId>) {
        if !self.is_capturing() {
            return;
        }
        let Some(binding) = self.0.scheduled.lock().remove(job_id) else {
            return;
        };
        if binding.expires <= Instant::now() {
            return;
        }
        self.record(
            binding.source.request_id,
            binding.parent,
            binding.source.logical_call_id,
            binding.source.attempt_number,
            binding.source.execution_identity,
            None,
            &binding.source.function,
            &binding.source.component,
            &binding.source.udf_type,
            Some(&binding.sessions),
            ExplainEvent::Causal {
                relation: "scheduled_job_canceled".into(),
                target_execution_id: canceled_by.map(|id| id.to_string()),
                scheduled_job_id: Some(job_id.to_owned()),
                causality: "recorded".into(),
            },
        );
    }

    pub fn trace(&self, context: &ExecutionContext) -> Option<TraceHandle> {
        TraceHandle::lookup(context).filter(|h| Arc::ptr_eq(&h.0.recorder.0, &self.0))
    }

    pub fn record_execution(
        &self,
        context: &ExecutionContext,
        function: &str,
        component: &str,
        udf_type: &str,
        mut event: ExplainEvent,
    ) {
        if !self.is_active() {
            return;
        }
        if let Some(handle) = self.trace(context) {
            if handle.0.coordinated.load(Ordering::Acquire)
                && matches!(event, ExplainEvent::Completion { .. })
            {
                if event.bound() {
                    handle.record_detail_drop();
                }
                *handle.0.pending_completion.lock() = Some(event);
                return;
            }
            if matches!(
                event,
                ExplainEvent::Completion {
                    will_retry: true,
                    ..
                }
            ) {
                handle.discard_staged_jobs();
            }
            if matches!(
                event,
                ExplainEvent::Completion {
                    will_retry: false,
                    ..
                }
            ) {
                handle.0.completed.store(true, Ordering::Release);
            }
            if let ExplainEvent::Completion { outcome, .. } = &event {
                handle.project_transaction_accounting(Some(&event), *outcome);
            }
            handle.record(event);
        } else {
            self.record(
                safe_request_id(context),
                context.execution_id,
                context.execution_id,
                1,
                "runtime_execution",
                None,
                function,
                component,
                udf_type,
                None,
                event,
            );
        }
    }

    fn record(
        &self,
        mut request_id: String,
        execution_id: ExecutionId,
        logical_call_id: ExecutionId,
        attempt_number: u64,
        execution_identity: &'static str,
        cache_iteration: Option<u64>,
        function: &str,
        component: &str,
        udf_type: &str,
        selected: Option<&[String]>,
        mut event: ExplainEvent,
    ) {
        if !self.is_active() {
            return;
        }
        let mut function = function.to_owned();
        let mut component = component.to_owned();
        let mut udf_type = udf_type.to_owned();
        let truncated = event.bound()
            | cap(&mut request_id)
            | cap(&mut function)
            | cap(&mut component)
            | cap(&mut udf_type);
        let event = CapturedEvent {
            cursor: 0,
            timestamp: timestamp(),
            monotonic_ms: monotonic_ms(),
            request_id,
            execution_id: execution_id.to_string(),
            logical_call_id: logical_call_id.to_string(),
            attempt_number,
            execution_identity,
            cache_iteration,
            function,
            component,
            udf_type,
            event,
        };
        // ponytail: at most four sessions share this short critical section;
        // use per-session queues only if measured capture contention warrants it.
        let mut sessions = self.0.sessions.lock();
        self.sweep(&mut sessions, Instant::now());
        for (id, session) in sessions.iter_mut() {
            if session.state == CaptureState::Capturing
                && selected.map_or_else(
                    || session.matches(&event.function, &event.component),
                    |ids| ids.contains(id),
                )
            {
                session.push(event.clone(), truncated);
            }
        }
    }
}
struct TransactionTrace {
    root_execution_id: ExecutionId,
    root_sessions: Vec<String>,
    descendant_sessions: Mutex<Vec<String>>,
    tables: Mutex<Vec<TableUsage>>,
    tables_truncated: AtomicBool,
    projection_state: AtomicU8,
}
struct Trace {
    transaction: Arc<TransactionTrace>,
    recorder: ExplainRecorder,
    sessions: Vec<String>,
    expires: Instant,
    execution_id: ExecutionId,
    logical_call_id: ExecutionId,
    attempt_number: u64,
    execution_identity: &'static str,
    cache_iteration: AtomicU64,
    request_id: String,
    function: String,
    component: String,
    udf_type: String,
    completed: AtomicBool,
    coordinated: AtomicBool,
    pending_completion: Mutex<Option<ExplainEvent>>,
    staged_jobs: Arc<Mutex<Vec<ScheduledOperation>>>,
    scheduled_source: OnceLock<Arc<ScheduledBinding>>,
}
enum ScheduledOperation {
    Created {
        id: String,
        source: Arc<ScheduledBinding>,
    },
    Canceled {
        id: String,
        canceled_by: ExecutionId,
    },
}
#[derive(Clone)]
pub struct TraceHandle(Arc<Trace>);
impl TraceHandle {
    pub fn execution_id(&self) -> ExecutionId {
        self.0.execution_id
    }

    pub fn root_execution_id(&self) -> ExecutionId {
        self.0.transaction.root_execution_id
    }

    /// Preserve only a bounded allowlist until final transaction accounting.
    pub fn set_transaction_tables(&self, tables: &[TableUsage]) {
        if self.execution_id() != self.root_execution_id()
            || self.0.transaction.descendant_sessions.lock().is_empty()
        {
            return;
        }
        let mut event = ExplainEvent::TableUsage {
            tables: tables.iter().take(MAX_ITEMS + 1).cloned().collect(),
            counters_reference: "transactionAccounting",
        };
        let truncated = event.bound() || tables.len() > MAX_ITEMS;
        let ExplainEvent::TableUsage { tables, .. } = event else {
            unreachable!()
        };
        *self.0.transaction.tables.lock() = tables;
        self.0
            .transaction
            .tables_truncated
            .store(truncated, Ordering::Release);
    }

    fn project_transaction_accounting(&self, completion: Option<&ExplainEvent>, outcome: Outcome) {
        if self.execution_id() != self.root_execution_id() {
            return;
        }
        let selected: Vec<_> = self
            .0
            .transaction
            .descendant_sessions
            .lock()
            .iter()
            .filter(|id| !self.0.transaction.root_sessions.contains(id))
            .cloned()
            .collect();
        if selected.is_empty() {
            return;
        }
        let final_completion = matches!(completion, Some(ExplainEvent::Completion { .. }));
        let complete = matches!(
            completion,
            Some(ExplainEvent::Completion {
                counters: Some(_),
                ..
            })
        );
        let state = if final_completion { 2 } else { 1 };
        if self
            .0
            .transaction
            .projection_state
            .fetch_max(state, Ordering::AcqRel)
            >= state
        {
            return;
        }
        let (accounting, accounting_unavailable_reason, disposition, counters, outcome) =
            match completion {
                Some(ExplainEvent::Completion {
                    accounting,
                    accounting_unavailable_reason,
                    disposition,
                    counters,
                    outcome,
                    ..
                }) => (
                    Some(accounting.clone()),
                    *accounting_unavailable_reason,
                    Some(disposition.clone()),
                    counters.clone(),
                    *outcome,
                ),
                _ => (None, None, None, None, outcome),
            };
        if self.0.transaction.tables_truncated.load(Ordering::Acquire) {
            let mut sessions = self.0.recorder.0.sessions.lock();
            for id in &selected {
                if let Some(session) = sessions.get_mut(id) {
                    session.truncated += 1;
                }
            }
        }
        self.0.recorder.record(
            self.0.request_id.clone(),
            self.0.execution_id,
            self.0.logical_call_id,
            self.0.attempt_number,
            self.0.execution_identity,
            None,
            &self.0.function,
            &self.0.component,
            &self.0.udf_type,
            Some(&selected),
            ExplainEvent::TransactionAccounting {
                root_execution_id: self.root_execution_id().to_string(),
                scope: "shared_transaction",
                complete,
                outcome,
                accounting,
                accounting_unavailable_reason,
                disposition,
                counters,
                tables: if complete {
                    self.0.transaction.tables.lock().clone()
                } else {
                    Vec::new()
                },
            },
        );
    }

    pub fn lookup(context: &ExecutionContext) -> Option<Self> {
        Self::lookup_id(context.execution_id)
    }

    pub fn lookup_id(id: ExecutionId) -> Option<Self> {
        if ACTIVE_RECORDERS.load(Ordering::Acquire) == 0
            || BINDING_COUNT.load(Ordering::Acquire) == 0
        {
            return None;
        }
        let handle = BINDINGS.lock().get(&id).and_then(Weak::upgrade).map(Self);
        // A carrier may have no selected sessions yet; its nested call can match.
        handle.filter(|h| h.0.expires > Instant::now() && h.0.recorder.is_active())
    }

    pub fn is_recording(&self) -> bool {
        if self.0.sessions.is_empty() || !self.0.recorder.is_active() {
            return false;
        }
        let mut sessions = self.0.recorder.0.sessions.lock();
        self.0.recorder.sweep(&mut sessions, Instant::now());
        self.0.sessions.iter().any(|id| {
            sessions
                .get(id)
                .is_some_and(|s| s.state == CaptureState::Capturing)
        })
    }

    /// Nested component calls share a runtime ExecutionId. Give them a
    /// separate, explicitly diagnostic ID without changing the actual
    /// request context.
    pub fn begin_nested(
        &self,
        function: &str,
        component: &str,
        udf_type: &str,
    ) -> Option<TraceGuard> {
        if !self.0.recorder.is_active() {
            return None;
        }
        let now = Instant::now();
        let mut sessions = self.0.recorder.0.sessions.lock();
        self.0.recorder.sweep(&mut sessions, now);
        let selected: Vec<String> = sessions
            .iter()
            .filter(|(id, s)| {
                s.state == CaptureState::Capturing
                    && (self.0.sessions.contains(id) || s.matches(function, component))
            })
            .map(|(id, _)| id.clone())
            .collect();
        let expires = sessions
            .values()
            .filter(|s| s.state == CaptureState::Capturing)
            .map(|s| s.started + Duration::from_secs(s.limits.duration_seconds))
            .max()?;
        let mut bindings = BINDINGS.lock();
        if bindings.len() >= MAX_BINDINGS {
            bindings.retain(|_, trace| trace.upgrade().is_some_and(|t| t.expires > now));
        }
        if bindings.len() >= MAX_BINDINGS {
            for session in sessions
                .values_mut()
                .filter(|s| s.state == CaptureState::Capturing)
            {
                session.lost_bindings += 1;
            }
            return None;
        }
        let bounded = |value: &str| value[..value.floor_char_boundary(MAX_STRING)].to_owned();
        let execution_id = ExecutionId::new();
        {
            let mut observed = self.0.transaction.descendant_sessions.lock();
            for id in &selected {
                if !observed.contains(id) && observed.len() < MAX_SESSIONS {
                    observed.push(id.clone());
                }
            }
        }
        let trace = Arc::new(Trace {
            transaction: self.0.transaction.clone(),
            recorder: self.0.recorder.clone(),
            sessions: selected,
            expires,
            execution_id,
            logical_call_id: execution_id,
            attempt_number: 1,
            execution_identity: "diagnostic_nested_span",
            cache_iteration: AtomicU64::new(self.0.cache_iteration.load(Ordering::Relaxed)),
            request_id: self.0.request_id.clone(),
            function: bounded(function),
            component: bounded(component),
            udf_type: bounded(udf_type),
            completed: AtomicBool::new(false),
            coordinated: AtomicBool::new(false),
            pending_completion: Mutex::new(None),
            staged_jobs: self.0.staged_jobs.clone(),
            scheduled_source: OnceLock::new(),
        });
        bindings.insert(trace.execution_id, Arc::downgrade(&trace));
        BINDING_COUNT.store(bindings.len(), Ordering::Release);
        drop(bindings);
        drop(sessions);
        let handle = TraceHandle(trace);
        handle.record(ExplainEvent::Start {
            parent_execution_id: Some(self.0.execution_id.to_string()),
            is_root: false,
            parent_scheduled_job: None,
        });
        Some(TraceGuard(handle))
    }

    fn scheduled_binding(&self) -> Arc<ScheduledBinding> {
        self.0
            .scheduled_source
            .get_or_init(|| {
                Arc::new(ScheduledBinding {
                    sessions: self.0.sessions.clone(),
                    parent: self.0.execution_id,
                    expires: self
                        .0
                        .expires
                        .min(Instant::now() + Duration::from_secs(300)),
                    source: ScheduledSource {
                        request_id: self.0.request_id.clone(),
                        logical_call_id: self.0.logical_call_id,
                        attempt_number: self.0.attempt_number,
                        execution_identity: self.0.execution_identity,
                        function: self.0.function.clone(),
                        component: self.0.component.clone(),
                        udf_type: self.0.udf_type.clone(),
                    },
                })
            })
            .clone()
    }

    /// Stage a generated job ID; this is not evidence of a successful commit.
    pub fn stage_scheduled_job(&self, job_id: &str) {
        if !self.is_recording() {
            return;
        }
        let Ok(job_id) = job_id.parse::<DeveloperDocumentId>() else {
            self.record_detail_drop();
            return;
        };
        let mut staged = self.0.staged_jobs.lock();
        let id = job_id.to_string();
        // Model staging can first observe the real root context; the nested
        // provider then replaces only the bounded diagnostic source, not the ID.
        if let Some(ScheduledOperation::Created { source, .. }) = staged.iter_mut().find(
            |op| matches!(op, ScheduledOperation::Created { id: existing, .. } if existing == &id),
        ) {
            *source = self.scheduled_binding();
            return;
        }
        if staged.len() >= MAX_ITEMS {
            drop(staged);
            self.record_detail_drop();
            return;
        }
        staged.push(ScheduledOperation::Created {
            id,
            source: self.scheduled_binding(),
        });
    }

    pub fn stage_canceled_job(&self, job_id: &str) {
        if !self.0.recorder.is_active() {
            return;
        }
        let Ok(id) = job_id.parse::<DeveloperDocumentId>() else {
            self.record_detail_drop();
            return;
        };
        let id = id.to_string();
        let mut staged = self.0.staged_jobs.lock();
        let source = staged
            .iter()
            .find_map(|op| match op {
                ScheduledOperation::Created {
                    id: created,
                    source,
                } if created == &id => Some(source.clone()),
                _ => None,
            })
            .or_else(|| {
                self.0
                    .recorder
                    .0
                    .scheduled
                    .lock()
                    .get(&id)
                    .cloned()
                    .map(Arc::new)
            });
        // An unmatched canceler still observes the lifecycle of a selected job;
        // publication uses that job's sessions and never emits canceler events.
        if self.0.sessions.is_empty() && source.is_none() {
            return;
        }
        if staged.len() >= MAX_ITEMS {
            drop(staged);
            if let Some(source) = source {
                let mut sessions = self.0.recorder.0.sessions.lock();
                for id in &source.sessions {
                    if let Some(session) = sessions.get_mut(id) {
                        session.dropped_detail_events += 1;
                    }
                }
            } else {
                self.record_detail_drop();
            }
            return;
        }
        staged.push(ScheduledOperation::Canceled {
            id,
            canceled_by: self.0.execution_id,
        });
    }

    /// Publish staged parent links only after database commit succeeds.
    pub fn commit_staged_jobs(&self) {
        let jobs = std::mem::take(&mut *self.0.staged_jobs.lock());
        for job in jobs {
            match job {
                ScheduledOperation::Created { id, source } => {
                    self.0.recorder.publish_scheduled_job(&id, &source)
                },
                ScheduledOperation::Canceled { id, canceled_by } => {
                    self.0
                        .recorder
                        .record_scheduled_canceled(&id, Some(canceled_by));
                },
            }
        }
    }

    pub fn staged_jobs_checkpoint(&self) -> usize {
        self.0.staged_jobs.lock().len()
    }

    pub fn rollback_staged_jobs(&self, checkpoint: usize) {
        self.0.staged_jobs.lock().truncate(checkpoint);
    }

    fn finish_coordinated(&self, outcome: Outcome) {
        self.0.coordinated.store(false, Ordering::Release);
        let pending = self.0.pending_completion.lock().take();
        if let Some(mut event) = pending {
            if let ExplainEvent::Completion {
                outcome: completion_outcome,
                will_retry,
                ..
            } = &mut event
            {
                // The coordinator knows about system retries invisible to the
                // function logger. Keep developer failures on a successful API call.
                if !matches!(outcome, Outcome::Success) {
                    *completion_outcome = outcome;
                }
                *will_retry = matches!(outcome, Outcome::Retry);
            }
            self.0.completed.store(true, Ordering::Release);
            self.project_transaction_accounting(Some(&event), outcome);
            self.record(event);
        } else {
            self.finish(outcome);
        }
    }

    pub fn finish(&self, outcome: Outcome) {
        if self.0.coordinated.load(Ordering::Acquire) {
            return;
        }
        if !self.0.completed.swap(true, Ordering::AcqRel) {
            self.project_transaction_accounting(None, outcome);
            self.record(ExplainEvent::End { outcome });
        }
    }

    /// OCC/system retry and failed commits must discard speculative job IDs.
    pub fn discard_staged_jobs(&self) {
        self.0.staged_jobs.lock().clear();
    }

    pub fn record_detail_drop(&self) {
        let mut sessions = self.0.recorder.0.sessions.lock();
        for id in &self.0.sessions {
            if let Some(session) = sessions.get_mut(id) {
                session.dropped_detail_events += 1;
            }
        }
    }

    pub fn elapsed_ms(&self) -> f64 {
        monotonic_ms()
    }

    pub fn record(&self, event: ExplainEvent) {
        if self.0.sessions.is_empty() {
            return;
        }
        self.0.recorder.record(
            self.0.request_id.clone(),
            self.0.execution_id,
            self.0.logical_call_id,
            self.0.attempt_number,
            self.0.execution_identity,
            match self.0.cache_iteration.load(Ordering::Relaxed) {
                0 => None,
                n => Some(n),
            },
            &self.0.function,
            &self.0.component,
            &self.0.udf_type,
            Some(&self.0.sessions),
            event,
        );
    }

    pub fn set_cache_iteration(&self, iteration: u64) {
        self.0.cache_iteration.store(iteration, Ordering::Relaxed);
    }

    pub fn phase(&self, phase: Phase) -> TracePhase {
        TracePhase {
            trace: self.clone(),
            phase,
            start: monotonic_ms(),
        }
    }

    pub fn record_phase(&self, phase: Phase, start_offset_ms: f64) {
        self.record(ExplainEvent::Phase {
            phase,
            start_offset_ms,
            duration_ms: (monotonic_ms() - start_offset_ms).max(0.0),
        });
    }
}
/// A measured interval; Drop does not imply that the operation succeeded.
pub struct TracePhase {
    trace: TraceHandle,
    phase: Phase,
    start: f64,
}
impl Drop for TracePhase {
    fn drop(&mut self) {
        self.trace.record_phase(self.phase, self.start);
    }
}

pub struct TraceGuard(TraceHandle);
impl TraceGuard {
    pub fn handle(&self) -> TraceHandle {
        self.0.clone()
    }

    pub fn finish(self, outcome: Outcome) {
        self.0.finish(outcome);
    }
}
impl Drop for TraceGuard {
    fn drop(&mut self) {
        if !self.0 .0.coordinated.load(Ordering::Acquire)
            && !self.0 .0.completed.swap(true, Ordering::AcqRel)
        {
            self.0
                .project_transaction_accounting(None, Outcome::Abandoned);
            self.0.record(ExplainEvent::End {
                outcome: Outcome::Abandoned,
            });
        }
        let mut bindings = BINDINGS.lock();
        if bindings
            .get(&self.0 .0.execution_id)
            .is_some_and(|binding| binding.ptr_eq(&Arc::downgrade(&self.0 .0)))
        {
            bindings.remove(&self.0 .0.execution_id);
        }
        BINDING_COUNT.store(bindings.len(), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution_context::RequestContext,
        types::FunctionCaller,
    };
    fn context() -> ExecutionContext {
        ExecutionContext::new(
            RequestContext::new_for_system_request(crate::RequestId::new()),
            &FunctionCaller::HttpEndpoint,
        )
    }
    #[test]
    fn filtered_nested_capture_uses_carriers_without_ancestor_events() -> anyhow::Result<()> {
        let recorder = ExplainRecorder::default();
        let capture = recorder.start(StartCapture {
            function: Some("leaf:f".into()),
            component: Some("branch/leaf".into()),
            max_events: Some(2),
            ..Default::default()
        })?;
        let other = ExplainRecorder::default();
        let other_capture = other.start(StartCapture {
            component: Some("elsewhere".into()),
            ..Default::default()
        })?;
        let root_context = context();
        let root = recorder
            .begin(&root_context, "root:f", "", "query", None)
            .unwrap();
        assert!(!root.handle().is_recording());
        assert!(TraceHandle::lookup(&root_context).is_some());
        let middle = root
            .handle()
            .begin_nested("middle:f", "branch", "query")
            .unwrap();
        assert!(!middle.handle().is_recording());
        let leaf = middle
            .handle()
            .begin_nested("leaf:f", "branch/leaf", "query")
            .unwrap();
        assert!(leaf.handle().is_recording());
        leaf.handle().record(ExplainEvent::Phase {
            phase: Phase::Execution,
            start_offset_ms: 0.0,
            duration_ms: 1.0,
        });
        let foreign = other
            .begin(
                &context(),
                "other:f",
                "",
                "query",
                Some(leaf.handle().execution_id()),
            )
            .unwrap();
        assert!(!foreign.handle().is_recording());
        foreign.finish(Outcome::Success);
        leaf.finish(Outcome::Success);
        middle.finish(Outcome::Success);
        root.finish(Outcome::Success);
        assert!(TraceHandle::lookup(&root_context).is_none());
        let snapshot = recorder.poll(&capture.session_id, 0, None, true)?;
        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(snapshot.coverage.dropped_events, 2);
        assert!(snapshot.coverage.continuity_lost);
        assert!(snapshot.events.iter().all(|e| {
            (e.function == "leaf:f" && e.component == "branch/leaf")
                || matches!(e.event, ExplainEvent::TransactionAccounting { .. })
        }));
        assert!(other
            .poll(&other_capture.session_id, 0, None, true)?
            .events
            .is_empty());
        Ok(())
    }

    #[test]
    fn filtered_child_jobs_keep_selected_source_after_root_commit_and_rollback(
    ) -> anyhow::Result<()> {
        let recorder = ExplainRecorder::default();
        let capture = recorder.start(StartCapture {
            function: Some("child:schedule".into()),
            component: Some("child".into()),
            ..Default::default()
        })?;
        let root = recorder
            .begin(&context(), "root:f", "", "mutation", None)
            .unwrap();
        let checkpoint = root.handle().staged_jobs_checkpoint();
        let rolled_back = DeveloperDocumentId::MAX.to_string();
        let child = root
            .handle()
            .begin_nested("child:schedule", "child", "mutation")
            .unwrap();
        child.handle().stage_scheduled_job(&rolled_back);
        child.handle().rollback_staged_jobs(checkpoint);
        child.finish(Outcome::Error);
        root.handle().commit_staged_jobs();
        assert!(recorder.0.scheduled.lock().is_empty());
        let child = root
            .handle()
            .begin_nested("child:schedule", "child", "mutation")
            .unwrap();
        let child_id = child.handle().execution_id();
        let job = DeveloperDocumentId::MIN.to_string();
        child.handle().stage_scheduled_job(&job);
        child.handle().stage_scheduled_job(&job); // one model/provider observation
        assert_eq!(root.handle().staged_jobs_checkpoint(), 1);
        child.finish(Outcome::Success);
        root.handle().commit_staged_jobs();
        root.finish(Outcome::Success);
        let mut dispatched = context();
        dispatched.parent_scheduled_job = Some((
            crate::components::ComponentId::Root,
            DeveloperDocumentId::MIN,
        ));
        let job_trace = recorder
            .begin(&dispatched, "target:f", "", "mutation", None)
            .unwrap();
        assert!(job_trace.handle().is_recording());
        job_trace.finish(Outcome::Success);
        let canceler = recorder
            .begin(&context(), "root:cancel", "", "mutation", None)
            .unwrap();
        assert!(!canceler.handle().is_recording());
        canceler.handle().stage_canceled_job(&job);
        canceler.handle().commit_staged_jobs();
        canceler.finish(Outcome::Success);
        let snapshot = recorder.poll(&capture.session_id, 0, None, true)?;
        assert!(!snapshot
            .events
            .iter()
            .any(|e| e.function.starts_with("root:")
                && !matches!(e.event, ExplainEvent::TransactionAccounting { .. })));

        assert!(snapshot.events.iter().any(|e| matches!(&e.event,
            ExplainEvent::Causal { relation, .. } if relation == "scheduled_job_canceled"
        )));
        let committed: Vec<_> = snapshot
            .events
            .iter()
            .filter(|e| {
                matches!(&e.event, ExplainEvent::Causal {
                relation, scheduled_job_id: Some(id), ..
            } if relation == "scheduled_job_committed" && id == &job)
            })
            .collect();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].execution_id, child_id.to_string());
        assert!(snapshot.events.iter().any(|e| {
            matches!(&e.event, ExplainEvent::Start {
                parent_scheduled_job: Some(id), parent_execution_id: Some(parent), ..
            } if id == &job && parent == &child_id.to_string())
        }));
        assert!(!recorder.0.scheduled.lock().contains_key(&rolled_back));
        Ok(())
    }

    #[test]
    fn descendant_accounting_projects_shared_transaction_once_without_root_events(
    ) -> anyhow::Result<()> {
        for committed in [true, false] {
            let recorder = ExplainRecorder::default();
            let root_capture = recorder.start(StartCapture {
                component: Some("".into()),
                ..Default::default()
            })?;
            let child_capture = recorder.start(StartCapture {
                component: Some("child".into()),
                ..Default::default()
            })?;
            let context = context();
            let root = recorder
                .begin(&context, "root:write", "", "mutation", None)
                .unwrap();
            let child = root
                .handle()
                .begin_nested("child:read", "child", "query")
                .unwrap();
            assert_eq!(child.handle().root_execution_id(), context.execution_id);
            child.handle().record(ExplainEvent::NestedAccounting {
                root_execution_id: child.handle().root_execution_id().to_string(),
                scope: "nested_attempt_inclusive",
                complete: true,
                outcome: Some(Outcome::Success),
                reads: ReadCounters {
                    database_read_documents: 1,
                    database_read_bytes: 40,
                    database_io_read_bytes: 64,
                },
                tables: vec![TableReadUsage {
                    component: "child".into(),
                    table: "items".into(),
                    read_documents: 1,
                    read_bytes: 40,
                    io_read_bytes: 64,
                }],
                writes: "shared_transaction",
            });
            child.finish(Outcome::Success);
            root.handle().set_transaction_tables(&[TableUsage {
                component: "".into(),
                table: "items".into(),
                read_bytes: 40,
                io_read_bytes: 64,
                read_documents: 1,
                write_bytes: if committed { 80 } else { 0 },
                io_write_bytes: 0,
                write_documents: if committed { 1 } else { 0 },
            }]);
            let completion = ExplainEvent::Completion {
                outcome: if committed {
                    Outcome::Success
                } else {
                    Outcome::Error
                },
                cache_hit: false,
                environment: "isolate".into(),
                memory_allocation_mb: 0,
                will_retry: false,
                occ: false,
                attempt_number: None,
                caller: "http".into(),
                query_invocation: None,
                execution_duration_ms: 1.0,
                user_execution_duration_ms: None,
                mutation_queue_length: None,
                accounting: Accounting::InclusiveAttempt,
                accounting_unavailable_reason: None,
                disposition: if committed {
                    Disposition::Committed
                } else {
                    Disposition::Discarded
                },
                counters: Some(Counters {
                    database_read_documents: 1,
                    database_write_documents: if committed { 1 } else { 0 },
                    ..Default::default()
                }),
                tables: vec![],
                syscalls: vec![],
                syscall_accounting: "legacy_async_batches",
            };
            recorder.record_execution(&context, "root:write", "", "mutation", completion.clone());
            root.handle()
                .project_transaction_accounting(Some(&completion), Outcome::Success);
            root.finish(Outcome::Success);
            let child_events = recorder
                .poll(&child_capture.session_id, 0, None, true)?
                .events;
            assert!(child_events.iter().all(|e| e.component == "child"
                || matches!(e.event, ExplainEvent::TransactionAccounting { .. })));
            let projected: Vec<_> = child_events
                .iter()
                .filter_map(|e| match &e.event {
                    ExplainEvent::TransactionAccounting {
                        root_execution_id,
                        complete,
                        counters,
                        disposition,
                        tables,
                        ..
                    } => Some((root_execution_id, complete, counters, disposition, tables)),
                    _ => None,
                })
                .collect();
            assert_eq!(projected.len(), 1);
            let (id, complete, counters, disposition, tables) = projected[0];
            assert_eq!(id, &context.execution_id.to_string());
            assert!(*complete);
            assert_eq!(
                counters.as_ref().unwrap().database_write_documents,
                u64::from(committed)
            );
            assert_eq!(tables[0].write_documents, u64::from(committed));
            assert!(matches!(disposition, Some(Disposition::Committed)) == committed);
            let root_events = recorder
                .poll(&root_capture.session_id, 0, None, true)?
                .events;
            assert_eq!(
                root_events
                    .iter()
                    .filter(|e| matches!(e.event, ExplainEvent::Completion { .. }))
                    .count(),
                1
            );
            assert!(!root_events
                .iter()
                .any(|e| matches!(e.event, ExplainEvent::TransactionAccounting { .. })));
        }
        Ok(())
    }

    #[test]
    fn system_error_projection_keeps_known_outcome_and_unknown_usage() -> anyhow::Result<()> {
        let recorder = ExplainRecorder::default();
        let capture = recorder.start(StartCapture {
            component: Some("child".into()),
            ..Default::default()
        })?;
        let context = context();
        let root = recorder
            .begin(&context, "root:f", "", "mutation", None)
            .unwrap();
        let child = root
            .handle()
            .begin_nested("child:f", "child", "query")
            .unwrap();
        child.finish(Outcome::Success);
        let completion = ExplainEvent::Completion {
            outcome: Outcome::Error,
            cache_hit: false,
            environment: "isolate".into(),
            memory_allocation_mb: 0,
            will_retry: false,
            occ: false,
            attempt_number: None,
            caller: "http".into(),
            query_invocation: None,
            execution_duration_ms: 1.0,
            user_execution_duration_ms: None,
            mutation_queue_length: None,
            accounting: Accounting::Unavailable,
            accounting_unavailable_reason: Some("system_error_usage_not_tracked"),
            disposition: Disposition::Discarded,
            counters: None,
            tables: vec![],
            syscalls: vec![],
            syscall_accounting: "legacy_async_batches",
        };
        let serialized = serde_json::to_value(&completion)?;
        assert_eq!(serialized["accounting"], "unavailable");
        assert!(serialized["counters"].is_null());
        recorder.record_execution(&context, "root:f", "", "mutation", completion);
        root.finish(Outcome::Error);
        let snapshot = recorder.poll(&capture.session_id, 0, None, true)?;
        let projected = snapshot
            .events
            .iter()
            .find(|e| matches!(e.event, ExplainEvent::TransactionAccounting { .. }))
            .unwrap();
        let value = serde_json::to_value(&projected.event)?;
        assert_eq!(value["complete"], false);
        assert_eq!(value["outcome"], "error");
        assert_eq!(value["disposition"], "discarded");
        assert_eq!(value["accounting"], "unavailable");
        assert_eq!(
            value["accountingUnavailableReason"],
            "system_error_usage_not_tracked"
        );
        assert!(value["counters"].is_null());
        Ok(())
    }

    #[test]
    fn descendant_transaction_abandonment_keeps_counters_unknown() -> anyhow::Result<()> {
        let recorder = ExplainRecorder::default();
        let capture = recorder.start(StartCapture {
            component: Some("child".into()),
            ..Default::default()
        })?;
        let root = recorder
            .begin(&context(), "root:f", "", "mutation", None)
            .unwrap();
        let child = root
            .handle()
            .begin_nested("child:f", "child", "mutation")
            .unwrap();
        drop(child);
        drop(root);
        let snapshot = recorder.poll(&capture.session_id, 0, None, true)?;
        assert!(snapshot.events.iter().any(|e| matches!(
            e.event,
            ExplainEvent::TransactionAccounting {
                complete: false,
                outcome: Outcome::Abandoned,
                counters: None,
                disposition: None,
                accounting: None,
                ..
            }
        )));
        Ok(())
    }

    #[test]
    fn coordinated_retry_defers_terminal_completion_and_preserves_logical_identity(
    ) -> anyhow::Result<()> {
        let recorder = ExplainRecorder::default();
        let capture = recorder.start(StartCapture::default())?;
        let invocation = ExplainInvocation::default();
        let mut first = context();
        first.request_metadata.explain_invocation = Some(invocation.clone());
        let first_id = first.execution_id;
        let guard = recorder.begin(&first, "a:f", "", "query", None).unwrap();
        guard.handle().finish(Outcome::Error); // The lower layer cannot decide retry.
        recorder.record_execution(
            &first,
            "a:f",
            "",
            "query",
            ExplainEvent::Completion {
                outcome: Outcome::Error,
                cache_hit: false,
                environment: "isolate".into(),
                memory_allocation_mb: 0,
                will_retry: false,
                occ: false,
                attempt_number: None,
                caller: "syncWorker".into(),
                query_invocation: None,
                execution_duration_ms: 1.0,
                user_execution_duration_ms: None,
                mutation_queue_length: None,
                accounting: Accounting::InclusiveAttempt,
                accounting_unavailable_reason: None,
                disposition: Disposition::ReadOnly,
                counters: Some(Counters::default()),
                tables: vec![],
                syscalls: vec![],
                syscall_accounting: "legacy_async_batches",
            },
        );
        drop(guard);
        assert!(TraceHandle::lookup(&first).is_none());
        assert_eq!(
            recorder
                .poll(&capture.session_id, 0, None, false)?
                .events
                .len(),
            1
        );
        let trace = invocation.finish_attempt(Outcome::Retry).unwrap();
        drop(trace.phase(Phase::RetryBackoff));
        let mut second = context();
        second.request_metadata.explain_invocation = Some(invocation.clone());
        let guard = recorder.begin(&second, "a:f", "", "query", None).unwrap();
        drop(guard);
        invocation.finish_attempt(Outcome::Success);
        let events = recorder.poll(&capture.session_id, 0, None, false)?.events;
        assert!(events
            .iter()
            .all(|e| e.logical_call_id == first_id.to_string()));
        assert!(events
            .iter()
            .any(|e| e.execution_id == second.execution_id.to_string() && e.attempt_number == 2));
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(
                    e.event,
                    ExplainEvent::Completion {
                        outcome: Outcome::Retry,
                        will_retry: true,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert!(!events.iter().any(|e| matches!(
            e.event,
            ExplainEvent::End {
                outcome: Outcome::Abandoned | Outcome::Error
            }
        )));
        let mut cancelled = context();
        cancelled.request_metadata.explain_invocation = Some(ExplainInvocation::default());
        drop(
            recorder
                .begin(&cancelled, "a:f", "", "query", None)
                .unwrap(),
        );
        drop(cancelled);
        let events = recorder.poll(&capture.session_id, 0, None, true)?.events;
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(
                    e.event,
                    ExplainEvent::End {
                        outcome: Outcome::Abandoned
                    }
                ))
                .count(),
            1
        );
        Ok(())
    }

    #[test]
    fn capture_bounds_filters_inheritance_stop_and_restart() -> anyhow::Result<()> {
        let recorder = ExplainRecorder::default();
        assert!(recorder
            .begin(&context(), "a:f", "", "query", None)
            .is_none());
        let capture = recorder.start(StartCapture {
            function: Some("a:f".into()),
            max_events: Some(2),
            ..Default::default()
        })?;
        let carrier = recorder
            .begin(&context(), "other:f", "", "query", None)
            .unwrap();
        assert!(!carrier.handle().is_recording());
        drop(carrier);
        let parent = context();
        let guard = recorder.begin(&parent, "a:f", "", "action", None).unwrap();
        let child = recorder
            .begin(
                &context(),
                "other:f",
                "",
                "query",
                Some(parent.execution_id),
            )
            .unwrap();
        child.finish(Outcome::Success);
        guard.finish(Outcome::Success);
        assert!(TraceHandle::lookup(&parent).is_none());
        let result = recorder.poll(&capture.session_id, 0, Some(&capture.process_id), true)?;
        assert_eq!(result.events.len(), 2);
        assert_eq!(result.coverage.dropped_events, 2);
        assert!(result.coverage.continuity_lost);
        assert_eq!(result.state, CaptureState::Stopped);
        assert!(result.events.iter().any(|e| e.function == "other:f"));
        assert!(!recorder.is_active());
        assert!(recorder
            .poll(&capture.session_id, 0, Some("other-process"), false)
            .is_err());
        assert!(ExplainRecorder::default()
            .poll(&capture.session_id, 0, None, false)
            .is_err());
        Ok(())
    }
    #[test]
    fn capture_rejects_limits_and_preserves_expiry_and_truncation() -> anyhow::Result<()> {
        let recorder = ExplainRecorder::default();
        assert!(recorder
            .start(StartCapture {
                max_events: Some(10001),
                ..Default::default()
            })
            .is_err());
        let capture = recorder.start(StartCapture::default())?;
        recorder.record_execution(
            &context(),
            "a:f",
            "",
            "query",
            ExplainEvent::Query {
                query_id: "q".into(),
                operation: "collect".into(),
                table: Some("t".repeat(2048)),
                index: None,
                source: None,
                source_unavailable: Some(SourceUnavailable::SourceMapMissing),
            },
        );
        let result = recorder.poll(&capture.session_id, 0, None, false)?;
        assert_eq!(result.coverage.truncated_events, 1);
        assert!(result.coverage.retained_bytes <= result.limits.max_bytes);
        {
            let mut sessions = recorder.0.sessions.lock();
            let session = sessions.get_mut(&capture.session_id).unwrap();
            session.started = Instant::now() - Duration::from_secs(61);
        }
        assert_eq!(
            recorder.poll(&capture.session_id, 0, None, false)?.state,
            CaptureState::Expired
        );
        Ok(())
    }
    #[test]
    fn captures_are_isolated_and_terminal_drain_is_stable() -> anyhow::Result<()> {
        let a = ExplainRecorder::default();
        let b = ExplainRecorder::default();
        let capture_a = a.start(StartCapture {
            component: Some("orders".into()),
            ..Default::default()
        })?;
        let capture_b = b.start(StartCapture::default())?;
        let parent = context();
        let guard = a.begin(&parent, "a:f", "orders", "action", None).unwrap();
        let carrier = a.begin(&context(), "a:f", "other", "query", None).unwrap();
        assert!(!carrier.handle().is_recording());
        drop(carrier);
        let other_guard = b
            .begin(&context(), "b:f", "", "query", Some(parent.execution_id))
            .unwrap();
        other_guard.finish(Outcome::Success);
        let before = a.poll(&capture_a.session_id, 0, None, true)?;
        guard.handle().record(ExplainEvent::End {
            outcome: Outcome::Success,
        });
        drop(guard);
        let after = a.poll(&capture_a.session_id, before.next_cursor, None, false)?;
        assert!(after.events.is_empty());
        assert_eq!(after.next_cursor, before.next_cursor);
        assert!(b.poll(&capture_a.session_id, 0, None, false).is_err());
        assert!(b
            .poll(&capture_b.session_id, 0, None, false)?
            .events
            .iter()
            .all(|e| e.function == "b:f"));
        assert!(a
            .poll(&capture_a.session_id, before.next_cursor + 1, None, false)
            .is_err());
        Ok(())
    }

    #[test]
    fn capture_parser_concurrency_and_safe_operations() -> anyhow::Result<()> {
        for invalid in [
            r#"{"durationSeconds":-1}"#,
            r#"{"maxEvents":1.5}"#,
            r#"{"durationSeconds":1e309}"#,
            r#"{"payload":"secret"}"#,
        ] {
            assert!(serde_json::from_str::<StartCapture>(invalid).is_err());
        }
        let recorder = ExplainRecorder::default();
        for _ in 0..MAX_SESSIONS {
            recorder.start(StartCapture::default())?;
        }
        assert!(recorder.start(StartCapture::default()).is_err());
        let capture = ExplainRecorder::default();
        let session = capture.start(StartCapture::default())?;
        let mut execution_context = context();
        execution_context.request_id = "secret request header".parse()?;
        let guard = capture
            .begin(&execution_context, "a:f", "", "query", None)
            .unwrap();
        let handle = guard.handle();
        handle.record_detail_drop();
        handle.record(ExplainEvent::Syscall {
            operation: "secret business payload".into(),
            start_offset_ms: 0.0,
            duration_ms: 1.0,
            invocation_count: 1,
            error_count: 1,
            query_ids: vec![],
            source: None,
            source_unavailable: None,
            timing: SyscallTiming::Synchronous,
            queued_ms: 0.0,
        });
        guard.finish(Outcome::Error);
        let result = capture.poll(&session.session_id, 0, None, false)?;
        assert_eq!(result.coverage.dropped_detail_events, 1);
        let encoded = serde_json::to_string(&result)?;
        assert!(!encoded.contains("secret business payload"));
        assert!(!encoded.contains("secret request header"));
        assert!(encoded.contains("unknown_operation"));
        assert!(encoded.contains("sourceUnavailable"));
        Ok(())
    }
    #[test]
    fn canceled_jobs_publish_after_commit_and_share_nested_rollback() -> anyhow::Result<()> {
        let recorder = ExplainRecorder::default();
        let capture = recorder.start(StartCapture::default())?;
        let ctx = context();
        let guard = recorder
            .begin(&ctx, "root:f", "", "mutation", None)
            .unwrap();
        let trace = guard.handle();
        let existing = DeveloperDocumentId::MIN.to_string();
        let created = DeveloperDocumentId::MAX.to_string();
        trace.stage_scheduled_job(&existing);
        trace.commit_staged_jobs();
        let checkpoint = trace.staged_jobs_checkpoint();
        let nested = trace.begin_nested("nested:f", "child", "mutation").unwrap();
        nested.handle().stage_canceled_job(&existing);
        nested.handle().stage_scheduled_job(&created);
        nested.handle().stage_canceled_job(&created);
        nested.handle().rollback_staged_jobs(checkpoint);
        nested.finish(Outcome::Error);
        trace.commit_staged_jobs();
        assert!(recorder.0.scheduled.lock().contains_key(&existing));
        assert!(!recorder.0.scheduled.lock().contains_key(&created));
        assert!(!recorder.poll(&capture.session_id, 0, None, false)?.events.iter().any(|e| matches!(&e.event, ExplainEvent::Causal { relation, .. } if relation == "scheduled_job_canceled")));
        trace.stage_canceled_job(&existing);
        trace.stage_scheduled_job(&created);
        trace.stage_canceled_job(&created);
        trace.commit_staged_jobs();
        guard.finish(Outcome::Success);
        assert!(recorder.0.scheduled.lock().is_empty());
        recorder.record_scheduled_canceled(&existing, None); // idempotent admin retry
        let snapshot = recorder.poll(&capture.session_id, 0, None, true)?;
        let canceled: Vec<_> = snapshot.events.iter().filter(|e| matches!(&e.event, ExplainEvent::Causal { relation, .. } if relation == "scheduled_job_canceled")).collect();
        assert_eq!(canceled.len(), 2);
        assert!(canceled
            .iter()
            .all(|e| e.execution_id == ctx.execution_id.to_string()));
        for entry in canceled {
            let ExplainEvent::Causal {
                target_execution_id,
                ..
            } = &entry.event
            else {
                unreachable!();
            };
            assert_eq!(
                target_execution_id.as_deref(),
                Some(ctx.execution_id.to_string().as_str())
            );
        }
        Ok(())
    }

    #[test]
    fn cancellation_batch_bounds_and_drop_counts_commit_atomically() -> anyhow::Result<()> {
        let recorder = ExplainRecorder::default();
        let capture = recorder.start(StartCapture::default())?;
        let guard = recorder
            .begin(&context(), "root:f", "", "mutation", None)
            .unwrap();
        let ids: Vec<_> = (0..=MAX_ITEMS)
            .map(|i| {
                DeveloperDocumentId::new(
                    value::TableNumber::MIN,
                    value::InternalId((i as u128).to_be_bytes()),
                )
                .to_string()
            })
            .collect();
        for id in &ids {
            recorder.remember_scheduled_job(&guard.handle(), id);
        }
        let mut rolled_back = recorder.cancellation_batch().unwrap();
        for id in &ids {
            rolled_back.stage(id);
        }
        assert_eq!(rolled_back.jobs.len(), MAX_ITEMS);
        drop(rolled_back);
        assert_eq!(
            recorder
                .poll(&capture.session_id, 0, None, false)?
                .coverage
                .dropped_detail_events,
            0
        );
        assert_eq!(recorder.0.scheduled.lock().len(), MAX_ITEMS + 1);
        let mut committed = recorder.cancellation_batch().unwrap();
        for id in &ids {
            committed.stage(id);
        }
        committed.commit();
        assert_eq!(recorder.0.scheduled.lock().len(), 1);
        guard.finish(Outcome::Success);
        let snapshot = recorder.poll(&capture.session_id, 0, None, true)?;
        assert_eq!(snapshot.coverage.dropped_detail_events, 1);
        assert_eq!(snapshot.events.iter().filter(|e| matches!(&e.event, ExplainEvent::Causal { relation, .. } if relation == "scheduled_job_canceled")).count(), MAX_ITEMS);
        Ok(())
    }

    #[test]
    fn nested_checkpoint_and_committed_jobs_preserve_only_real_parentage() -> anyhow::Result<()> {
        let recorder = ExplainRecorder::default();
        let capture = recorder.start(StartCapture {
            function: Some("root:f".into()),
            ..Default::default()
        })?;
        let ctx = context();
        let guard = recorder
            .begin(&ctx, "root:f", "", "mutation", None)
            .unwrap();
        let parent = guard.handle();
        parent.set_cache_iteration(2);
        let checkpoint = parent.staged_jobs_checkpoint();
        let nested = parent
            .begin_nested("nested:f", "child", "mutation")
            .unwrap();
        nested
            .handle()
            .stage_scheduled_job(&DeveloperDocumentId::MAX.to_string());
        nested.handle().rollback_staged_jobs(checkpoint);
        nested.finish(Outcome::Error);
        parent.commit_staged_jobs();
        assert!(recorder.0.scheduled.lock().is_empty());
        parent.stage_scheduled_job(&DeveloperDocumentId::MIN.to_string());
        parent.commit_staged_jobs();
        guard.finish(Outcome::Success);
        let mut dispatched = context();
        dispatched.parent_scheduled_job = Some((
            crate::components::ComponentId::Root,
            DeveloperDocumentId::MIN,
        ));
        let scheduled = recorder
            .begin(&dispatched, "scheduled:f", "", "mutation", None)
            .unwrap();
        scheduled.finish(Outcome::Success);
        let result = recorder.poll(&capture.session_id, 0, None, false)?;
        assert!(result
            .events
            .iter()
            .any(|e| e.execution_identity == "diagnostic_nested_span"
                && e.execution_id != ctx.execution_id.to_string()));
        assert!(result.events.iter().any(|e| e.function == "scheduled:f" && matches!(&e.event, ExplainEvent::Start { parent_execution_id: Some(parent), .. } if parent == &ctx.execution_id.to_string())));
        assert!(result
            .events
            .iter()
            .any(|e| e.execution_identity == "diagnostic_nested_span"
                && e.cache_iteration == Some(2)
                && e.logical_call_id == e.execution_id));
        let rolled_back = DeveloperDocumentId::MAX.to_string();
        assert!(!result.events.iter().any(|e| {
            if let ExplainEvent::Causal {
                scheduled_job_id: Some(id),
                ..
            } = &e.event
            {
                id == &rolled_back
            } else {
                false
            }
        }));
        Ok(())
    }
}
