//! Opt-in syscall evidence. Never copy argument payloads or index range values.
pub(crate) mod nested;
pub(crate) mod source_cache;
use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};

use common::{
    explain::{
        safe_syscall_operation,
        ExplainEvent,
        SourceLocation,
        SourceUnavailable,
        SyscallTiming,
        TraceHandle,
    },
    runtime::Runtime,
    types::IndexName,
};
use deno_core::{
    v8,
    ModuleSpecifier,
};
use serde_json::Value as JsonValue;
use sourcemap::SourceMap;
use value::{
    heap_size::HeapSize,
    TableName,
};

use crate::{
    environment::V8IsolateEnvironment,
    execution_scope::ExecutionScope,
};

/// A span travels with the existing pending syscall, including across action
/// threads. Batched reads share an execution interval; their durations must not
/// be summed.
const MAX_PENDING_EXPLAIN_SYSCALLS: usize = 1024;
static PENDING_EXPLAIN_SYSCALLS: AtomicUsize = AtomicUsize::new(0);

struct ExplainSlot;
impl Drop for ExplainSlot {
    fn drop(&mut self) {
        PENDING_EXPLAIN_SYSCALLS.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct ExplainSyscall {
    _slot: ExplainSlot,
    trace: TraceHandle,
    operation: String,
    queued_at_ms: f64,
    source: Option<SourceLocation>,
    source_unavailable: Option<SourceUnavailable>,
    query_id: Option<String>,
    query: Option<(&'static str, String, String)>,
}

impl HeapSize for ExplainSyscall {
    fn heap_size(&self) -> usize {
        self.operation.len()
            + self.source.as_ref().map_or(0, |s| s.file.len())
            + self.query_id.as_ref().map_or(0, String::len)
            + self.query.as_ref().map_or(0, |(_, t, i)| t.len() + i.len())
    }
}

impl ExplainSyscall {
    pub fn start_ms(&self) -> f64 {
        self.trace.elapsed_ms()
    }

    pub fn finish(
        mut self,
        start_ms: f64,
        success: bool,
        timing: SyscallTiming,
        result: Option<&JsonValue>,
    ) {
        let finished_ms = self.trace.elapsed_ms();
        if self.operation == "1.0/queryStream" {
            self.query_id = result
                .and_then(|r| r.get("queryId")?.as_u64())
                .map(|id| format!("stream-{id}"));
        }
        if success && let Some((operation, table, index)) = self.query.take() {
            self.trace.record(ExplainEvent::Query {
                query_id: self
                    .query_id
                    .clone()
                    .unwrap_or_else(|| format!("page-{}", self.queued_at_ms)),
                operation: operation.to_owned(),
                table: Some(table),
                index: Some(index),
                source: self.source.clone(),
                source_unavailable: self.source_unavailable.clone(),
            });
        }
        self.trace.record(ExplainEvent::Syscall {
            operation: self.operation,
            start_offset_ms: start_ms,
            duration_ms: (finished_ms - start_ms).max(0.0),
            queued_ms: (start_ms - self.queued_at_ms).max(0.0),
            timing,
            invocation_count: 1,
            error_count: u32::from(!success),
            query_ids: self.query_id.into_iter().collect(),
            source: self.source,
            source_unavailable: self.source_unavailable,
        });
    }
}

impl<RT: Runtime, E: V8IsolateEnvironment<RT>> ExecutionScope<'_, '_, '_, RT, E> {
    fn explain_source_frame(
        &mut self,
        frame: &SourceLocation,
    ) -> (Option<SourceLocation>, Option<SourceUnavailable>) {
        let Ok(specifier) = ModuleSpecifier::parse(&frame.file) else {
            return (None, Some(SourceUnavailable::UserFrameUnavailable));
        };
        let id = self.module_map().get_by_name(&specifier);
        if let Some(id) = id
            && let Some(cached) =
                self.module_map()
                    .explain_sources
                    .get(&(id, frame.line, frame.column))
        {
            return cached.clone();
        }
        let mapped = if let Some(id) = id {
            let source = self.module_map().module_source(id);
            if let Some(mapped) = source.explain_sources.get(frame) {
                mapped
            } else {
                let map = source
                    .source_map()
                    .and_then(|raw| crate::helpers::source_map_from_slice(raw.as_bytes()));
                let mapped = map_frame(frame, map.as_ref());
                source.explain_sources.insert(frame, &mapped);
                mapped
            }
        } else {
            map_frame(frame, None)
        };
        if let Some(id) = id
            && self.module_map().explain_sources.len() < 512
            && mapped
                .0
                .as_ref()
                .is_none_or(|source| source.file.len() <= 1024)
        {
            self.module_map_mut()
                .explain_sources
                .insert((id, frame.line, frame.column), mapped.clone());
        }
        mapped
    }

    pub(crate) fn explain_syscall(
        &mut self,
        trace: TraceHandle,
        operation: &str,
        args: &JsonValue,
    ) -> Option<Box<ExplainSyscall>> {
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
        let slot = ExplainSlot;
        // Continuations refer to the origin captured at stream creation.
        let (source, source_unavailable) =
            if matches!(operation, "1.0/queryStreamNext" | "1.0/queryCleanup") {
                (None, None)
            } else {
                let frames = current_frames(self);
                source_location(frames, |frame| self.explain_source_frame(frame))
            };
        if matches!(
            source_unavailable,
            Some(SourceUnavailable::SourceLimitExceeded)
        ) {
            trace.record_detail_drop();
        }
        let queued_at_ms = trace.elapsed_ms();
        Some(Box::new(ExplainSyscall {
            _slot: slot,
            trace,
            operation: safe_syscall_operation(operation).to_owned(),
            queued_at_ms,
            source,
            source_unavailable,
            query_id: if operation == "1.0/queryPage" {
                Some(format!("page-{queued_at_ms}"))
            } else if matches!(operation, "1.0/queryStreamNext" | "1.0/queryCleanup") {
                args.get("queryId")
                    .and_then(JsonValue::as_u64)
                    .map(|id| format!("stream-{id}"))
            } else {
                None
            },
            query: query_metadata(operation, args),
        }))
    }
}

fn query_metadata(operation: &str, args: &JsonValue) -> Option<(&'static str, String, String)> {
    if !matches!(operation, "1.0/queryStream" | "1.0/queryPage") {
        return None;
    }
    let source = args.get("query")?.get("source")?;
    match source.get("type")?.as_str()? {
        "FullTableScan" => {
            let table: TableName = source.get("tableName")?.as_str()?.parse().ok()?;
            let index = IndexName::by_creation_time(table.clone());
            Some(("full_table_scan", table.to_string(), index.to_string()))
        },
        kind @ ("IndexRange" | "Search") => {
            let index: IndexName = source.get("indexName")?.as_str()?.parse().ok()?;
            Some((
                if kind == "Search" {
                    "search"
                } else {
                    "index_range"
                },
                index.table().to_string(),
                index.to_string(),
            ))
        },
        _ => None,
    }
}

fn current_frames(scope: &mut v8::PinScope<'_, '_>) -> Vec<SourceLocation> {
    let Some(stack) = v8::StackTrace::current_stack_trace(scope, 32) else {
        return Vec::new();
    };
    (0..stack.get_frame_count())
        .filter_map(|i| {
            let frame = stack.get_frame(scope, i)?;
            let file = frame.get_script_name(scope)?.to_rust_string_lossy(scope);
            // Only deployment modules are source evidence; eval/sourceURL strings are not.
            if !file.starts_with("convex:/") || file.starts_with("convex:/_system/") {
                return None;
            }
            Some(SourceLocation {
                file,
                line: u32::try_from(frame.get_line_number()).ok()?,
                column: u32::try_from(frame.get_column()).ok()?,
                mapped: false,
            })
        })
        .collect()
}

fn map_frame(
    frame: &SourceLocation,
    map: Option<&SourceMap>,
) -> (Option<SourceLocation>, Option<SourceUnavailable>) {
    let fallback = |reason| {
        if frame.file.len() > 1024 {
            (None, Some(SourceUnavailable::SourceLimitExceeded))
        } else if frame.file.starts_with("convex:/_deps/") {
            (None, Some(reason))
        } else {
            (Some(frame.clone()), Some(reason))
        }
    };
    let Some(map) = map else {
        return fallback(SourceUnavailable::SourceMapMissing);
    };
    let Some(token) =
        map.lookup_token(frame.line.saturating_sub(1), frame.column.saturating_sub(1))
    else {
        return fallback(SourceUnavailable::TokenMissing);
    };
    let Some(file) = token.get_source() else {
        return fallback(SourceUnavailable::TokenMissing);
    };
    if file.len() > 1024 {
        return (None, Some(SourceUnavailable::SourceLimitExceeded));
    }
    if file.contains("node_modules/")
        || file.contains("node_modules\\")
        || file.contains("convex/src/server/")
        || file.contains("convex\\src\\server\\")
        || file.contains("_generated/")
        || file.contains("_generated\\")
    {
        return (None, None);
    }
    (
        Some(SourceLocation {
            file: file.to_owned(),
            line: token.get_src_line() + 1,
            column: token.get_src_col() + 1,
            mapped: true,
        }),
        None,
    )
}

fn source_location(
    frames: Vec<SourceLocation>,
    mut lookup: impl FnMut(&SourceLocation) -> (Option<SourceLocation>, Option<SourceUnavailable>),
) -> (Option<SourceLocation>, Option<SourceUnavailable>) {
    let mut fallback = (None, Some(SourceUnavailable::UserFrameUnavailable));
    for frame in frames {
        let candidate = lookup(&frame);
        if candidate.0.as_ref().is_some_and(|source| source.mapped) {
            return candidate;
        }
        if fallback.0.is_none()
            && (candidate.0.is_some()
                || matches!(candidate.1, Some(SourceUnavailable::SourceLimitExceeded)))
        {
            fallback = candidate;
        }
    }
    fallback
}

#[cfg(test)]
mod tests {
    use deno_core::v8::scope;
    use serde_json::json;
    use sourcemap::SourceMapBuilder;

    use super::*;

    fn query_callback<'s>(
        scope: &mut v8::PinScope<'s, '_>,
        args: v8::FunctionCallbackArguments<'s>,
        mut rv: v8::ReturnValue,
    ) {
        let frames = current_frames(scope);
        let mut map = SourceMapBuilder::new(None);
        map.add(1, 0, 10, 2, Some("convex/explain_fixture.ts"), None, false);
        map.add(2, 0, 20, 4, Some("convex/explain_fixture.ts"), None, false);
        let map = map.into_sourcemap();
        let (source, reason) = source_location(frames, |frame| map_frame(frame, Some(&map)));
        assert!(reason.is_none());
        let args: JsonValue = deno_core::serde_v8::from_v8(scope, args.get(0)).unwrap();
        let metadata = query_metadata("1.0/queryStream", &args).unwrap();
        let result = json!({"source": source, "table": metadata.1, "index": metadata.2});
        rv.set(deno_core::serde_v8::to_v8(scope, result).unwrap());
    }

    #[test]
    fn v8_query_calls_on_distinct_lines_map_to_distinct_app_origins() {
        crate::client::initialize_v8();
        let mut isolate = v8::Isolate::new(Default::default());
        scope!(let handle_scope, &mut isolate);
        let context = v8::Context::new(handle_scope, Default::default());
        let scope = &mut v8::ContextScope::new(handle_scope, context);
        let callback = v8::Function::new(scope, query_callback).unwrap();
        let key = v8::String::new(scope, "query").unwrap();
        context
            .global(scope)
            .set(scope, key.into(), callback.into());
        let source = v8::String::new(
            scope,
            r#"[
query({query:{source:{type:'FullTableScan',tableName:'messages'}}}),
query({query:{source:{type:'IndexRange',indexName:'messages.by_tenant',range:[{value:'PRIVATE'}]}}})
]"#,
        )
        .unwrap();
        let name = v8::String::new(scope, "convex:/explain_fixture.js").unwrap();
        let origin = v8::ScriptOrigin::new(
            scope,
            name.into(),
            0,
            0,
            false,
            0,
            None,
            true,
            false,
            false,
            None,
        );
        let script = v8::Script::compile(scope, source, Some(&origin)).unwrap();
        let result = script.run(scope).unwrap();
        let result: JsonValue = deno_core::serde_v8::from_v8(scope, result).unwrap();
        assert_eq!(
            result,
            json!([
                {"source":{"file":"convex/explain_fixture.ts","line":11,"column":3,"mapped":true},"table":"messages","index":"messages.by_creation_time"},
                {"source":{"file":"convex/explain_fixture.ts","line":21,"column":5,"mapped":true},"table":"messages","index":"messages.by_tenant"},
            ])
        );
        assert!(!result.to_string().contains("PRIVATE"));
        assert_eq!(safe_syscall_operation("PRIVATE"), "unknown_operation");
    }

    #[test]
    fn missing_map_is_explicit_and_sdk_frames_are_not_app_origins() {
        let frame = SourceLocation {
            file: "convex:/app.js".to_owned(),
            line: 8,
            column: 5,
            mapped: false,
        };
        let (source, unavailable) = map_frame(&frame, None);
        assert_eq!(source.unwrap().line, 8);
        assert!(matches!(
            unavailable,
            Some(SourceUnavailable::SourceMapMissing)
        ));
        let mut map = SourceMapBuilder::new(None);
        map.add(
            7,
            0,
            0,
            0,
            Some("../node_modules/convex/src/server/impl/query_impl.ts"),
            None,
            false,
        );
        assert!(map_frame(&frame, Some(&map.into_sourcemap())).0.is_none());
    }
}
