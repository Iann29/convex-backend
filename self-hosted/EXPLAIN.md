# C6 EXPLAIN

EXPLAIN observes executions that are already happening. Starting a capture
does not invoke a function, change application data, or enable a different
execution policy. This development branch adds a bounded, volatile recorder;
release and performance acceptance are separate from implementing the API.

## Capture API

All routes use the backend API port, 3210, and require the deployment's existing
`ViewLogs` authorization. A filter narrows observation, not authorization.

| Method and route | Behavior |
|---|---|
| `POST /api/explain/sessions` | Create a session; return its initial snapshot. |
| `GET /api/explain/sessions/{id}?cursor=N&processId=UUID` | Return retained events after the cursor. |
| `DELETE /api/explain/sessions/{id}?cursor=N&processId=UUID` | Stop collection and return the remaining snapshot. |

R7 adds negotiated gzip compression to eligible GET and DELETE snapshots,
including the GET route's HEAD response. POST session creation and neighboring
streams retain their existing behavior. `Accept-Encoding: gzip` can select
gzip; absent/identity and `gzip;q=0` requests retain identity. `Vary` includes
`Accept-Encoding` alongside the existing CORS fields. HEAD has no body.
Compression changes the HTTP representation only: decoded JSON, cursors,
DELETE semantics, authorization, retained-buffer limits and partial-coverage
signals are unchanged. `maxBytes` measures retained allocation, not compressed
transfer bytes.

On the native-backend-to-Synapse hop, the matching Synapse server's default Go
transport negotiates and decodes gzip before applying its 10 MiB response-body
limit and JSON validation. Malformed/truncated gzip or a decoded body above
10 MiB yields `502 invalid_explain_response`. Synapse then emits its validated
JSON envelope. This native compression does not itself promise gzip on the
Synapse-to-CLI hop or a smaller aggregate report.

POST accepts optional `function`, `component`, `durationSeconds`, `maxEvents`
and `maxBytes`. Function names use the public form `module:export`, without
`.js`. The empty component string selects the root component. Both filters
are exact matches. Descendants of a selected execution are included so that
its work can be explained across component and action boundaries. A matching
nested function can be selected even when its ancestors do not match; an
unobserved ancestor is not reported as an observed execution.

| Limit | Default | Accepted range |
|---|---:|---:|
| Duration, seconds | 60 | 1–300 |
| Retained events | 2,000 | 1–10,000 |
| Retained buffer, bytes | 2,097,152 | 1,024–8,388,608 |
| Sessions per application | — | 4, including stopped/expired sessions |

Stopped or expired sessions remain available for 300 seconds. Stopping does
not immediately release a session slot. `ExplainSessionLimit` is an overload
error (HTTP 503 on the native API). Synapse maps that specific error to its
`429 explain_session_limit` contract. Waiting for retention expiry or starting
a different process changes available slots; silently substituting another
session loses continuity and is not a valid recovery strategy.

The event buffer evicts old events when either buffer bound is reached.
`maxBytes` is a conservative retained-allocation estimate, not process RSS.
Independent bounds cover trace bindings (10,000), staged scheduler operations
(128 per shared transaction tree), strings (1,024 bytes), and 1,024 active syscall
and nested-read detail spans shared across the process. Source-position caching
has two layers: up to 512 entries per ModuleMap (L1), plus up to 512 entries
across immutable module sources process-wide (L2). L2 stores only source
positions and fixed unavailable reasons, not decoded source maps. Entries can
outlive a capture and release their slots when their module source is dropped.
At capacity, resolution proceeds uncached: saturation does not discard the
origin or increment capture-drop counters. These caches have independent
bounds; they are not part of the session's `maxBytes` budget.
Coverage reports evictions, truncated details, lost bindings, cursor gaps and
process changes. The recorder never persists captures across backend restarts.
`droppedEvents` counts buffer evictions and events rejected as oversized; an
evicted event may already have reached the collector. It is not the number
of events missing from an aggregated file. `continuityLost` is conservative:
it stays true after any such discard, even if the supplied cursor has no gap.
The CLI preserves that partial classification. Contiguous collected cursors
also cannot prove complete calls at the window boundaries or restore missing
details, bindings or accounting.

## Reading the result

Snapshots have `schemaVersion: 1`, session/process IDs, filters, effective
limits, state, coverage, events and `nextCursor`. Use all three of process ID,
session ID and cursor for resumption. A changed process or missing session is
an explicit error. No events does not establish that a function is fast or
that a queue is empty: it can mean no matching execution occurred.

| Evidence | Interpretation |
|---|---|
| `logicalCallId`, `executionId`, `attemptNumber` | A logical call can contain multiple actual attempts. Nested spans have diagnostic IDs, explicitly distinguished from runtime execution IDs. |
| `completion`, `end` | Success, error, retry, cancellation or abandonment are separate outcomes. An unfinished execution is not silently converted into success. |
| `counters`, `tableUsage` | Accounted document/byte/index work, with a table breakdown referencing the same completion counters. Do not add the breakdown to its total. |
| `accounting`, `disposition` | Inclusive attempt work, cached replay, committed/discarded writes, read-only work and unknown external effects remain distinct. |
| `nestedAccounting` | Observed read deltas for a nested attempt, including its descendants but not its siblings. Incomplete exits are marked; writes belong to the shared transaction. |
| `transactionAccounting` | The root transaction's accounting, projected into a child-only capture when the root was not selected. It can be incomplete (`complete: false`, counters unavailable); a later final projection supersedes an earlier partial one. Join by `rootExecutionId`; never add this shared total to nested read totals. |
| `query`, `syscall` | Actual operation, table/index and available source-map file/line. Failed source resolution has an explicit reason; stream continuations use the origin join described below. |
| `phase` | Monotonic intervals for admission, execution, cache, commit, persistence and retry waits. Concurrent/nested intervals are not an additive wall-clock bill. |
| `persistenceBatch` | Shared physical batch ID, retry attempt, commit/document/index counts and outcome. Deduplicate by process/batch/attempt; a start and finish are one attempt. |
| `scheduler`, `causal` | Recorded scheduling, committed cancellation and persisted retry counters. Rolled-back staging does not establish a scheduled job. |
| `subscription` | Stable subscription identity and the recorded invalidating writer when known. Unknown causality stays unknown. |

A cache algorithm iteration is not necessarily an execution of user code.
Join query origins with execution ID, cache iteration and query ID. Stream
continuation and cleanup syscalls use that join to the stream creator; they do
not repeat its source or unavailable reason. A cached syscall summary can
describe earlier work; it must not be charged again as
current physical work. Likewise, parent and child counters can be inclusive.

Document counters are accounted read/write visits, not distinct business IDs.
For example, a query followed by `db.patch` can account two reads of the same
document because patch reads its previous value. Write counters are recorded
at the validated commit path, after coalescing: a transaction rejected before
that stage can have observed write syscalls and zero accounted write documents.
That zero does not mean no write was attempted.

The native identity reports its actual backend version, database family,
persistence driver tag, available parallelism and application concurrency
knobs. It does not infer a container digest or deployed-code hash from a
version string. A collector may attach independently verified image/code and
resource receipts, while retaining their provenance.

## Privacy, failures and limits of inference

The recorder selects fields before retaining them. It excludes arguments,
results, business document IDs, credentials, arbitrary console/error text and
raw request headers. Request IDs are hashed because clients can choose them.
Scheduled-system IDs are retained only for causal links. Function/component,
table/index and source names are diagnostic metadata visible to the authorized
operator, not a promise of anonymity.

HTTP cancellation establishes request-stream cancellation. It does not prove
the UDF stopped or that external side effects rolled back. Actions retain
`external_effects_unknown` where the backend cannot prove their effects.
Stopping observation does not stop an in-flight function.

The existing Node callback transport authenticates runtime callbacks but does
not cryptographically bind all diagnostic parent headers. The capture states
that provenance; it does not upgrade it into a security guarantee.

A fatal persistence error can terminate the process before a collector polls
the terminal event. The correct result is a retained prefix plus explicit
continuity loss, not a fabricated terminal detail. Some system functions also
suppress normal function accounting; their known outcome is retained without
inventing counters.

System-error log paths do not retain complete usage accounting. Their capture
uses `accounting: unavailable`, `counters: null` and the fixed reason
`system_error_usage_not_tracked`; the known outcome and retry relation remain
visible. Exclude these unavailable counters from measured totals.

These counters are not WAL bytes, fsync counts, CPU time or a monetary bill.
Use matched application results, process CPU/RSS, exporter metrics and an
independent load generator to measure overhead or infer a performance change.

## Synapse CLI

With the matching Synapse server and CLI implementation, observation uses the
existing project/deployment authorization. The CLI does not fetch an admin key
or execute application functions:

```sh
synapse explain --deployment test-backend --function messages:list --duration 30 --output before.json
synapse explain --deployment test-backend --component billing --json
synapse explain report before.json
synapse explain compare before.json after.json --json
```

C5 returns unsupported capture. A comparison reports differences and missing
controls; it cannot establish causality from two uncontrolled captures.
Validate a candidate using equal fixtures, hardware/resources, code, warmup
and arrival patterns, mirrored C5/C6-off/C6-on runs, independent final-state
checks, and bounded overload/failure tests. Preserve failed and inconclusive
runs alongside successful ones.
