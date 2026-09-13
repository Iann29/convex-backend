// Bounded source positions tied to one immutable cached module, never payloads.
use std::{
    collections::HashMap,
    sync::atomic::{
        AtomicUsize,
        Ordering,
    },
};

use common::explain::{
    SourceLocation,
    SourceUnavailable,
};
use parking_lot::RwLock;

type Mapping = (Option<SourceLocation>, Option<SourceUnavailable>);
type Positions = HashMap<String, HashMap<(u32, u32), Entry>>;
const LIMIT: usize = 512;
static ENTRIES: AtomicUsize = AtomicUsize::new(0);

struct Entry(Mapping);
impl Drop for Entry {
    fn drop(&mut self) {
        ENTRIES.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub(crate) struct SourceCache(RwLock<Positions>);
impl SourceCache {
    pub(crate) fn get(&self, frame: &SourceLocation) -> Option<Mapping> {
        // Borrow the filename for lookup: do not allocate on a cache hit.
        self.0
            .read()
            .get(&frame.file)?
            .get(&(frame.line, frame.column))
            .map(|entry| entry.0.clone())
    }

    pub(crate) fn insert(&self, frame: &SourceLocation, mapping: &Mapping) {
        if frame.file.len() > 1024 || mapping.0.as_ref().is_some_and(|s| s.file.len() > 1024) {
            return;
        }
        let position = (frame.line, frame.column);
        let mut cache = self.0.write();
        if cache
            .get(&frame.file)
            .is_some_and(|positions| positions.contains_key(&position))
        {
            return;
        }
        if ENTRIES
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < LIMIT).then_some(count + 1)
            })
            .is_err()
        {
            // This is an optimization miss, not missing EXPLAIN evidence.
            return;
        }
        let entry = Entry(mapping.clone());
        cache
            .entry(frame.file.clone())
            .or_default()
            .insert(position, entry);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        Barrier,
    };

    use model::modules::module_versions::FullModuleSource;
    use sourcemap::{
        SourceMap,
        SourceMapBuilder,
    };

    use super::*;
    use crate::{
        explain::map_frame,
        module_cache::V8ModuleSource,
    };

    #[test]
    fn immutable_modules_bound_positions_and_release_slots() {
        fn module(code: &str, original_line: u32) -> V8ModuleSource {
            let mut builder = SourceMapBuilder::new(None);
            builder.add(9, 3, original_line, 2, Some("probes.ts"), None, false);
            builder.add(10, 7, original_line + 1, 4, Some("probes.ts"), None, false);
            let mut bytes = Vec::new();
            builder.into_sourcemap().to_writer(&mut bytes).unwrap();
            V8ModuleSource::new(FullModuleSource {
                source: code.into(),
                source_map: Some(String::from_utf8(bytes).unwrap()),
            })
        }
        fn resolve(source: &V8ModuleSource, frame: &SourceLocation) -> Mapping {
            if let Some(cached) = source.explain_sources.get(frame) {
                return cached;
            }
            let map = source
                .source_map()
                .map(|raw| SourceMap::from_slice(raw.as_bytes()).unwrap());
            let mapped = map_frame(frame, map.as_ref());
            source.explain_sources.insert(frame, &mapped);
            mapped
        }
        fn position(mapping: Mapping) -> (String, u32, u32, bool) {
            let source = mapping.0.unwrap();
            (source.file, source.line, source.column, source.mapped)
        }

        // This focused test owns every admitted entry; no test shim replaces the
        // real SourceCache, V8ModuleSource, SourceLocation, or mapping function.
        assert_eq!(ENTRIES.load(Ordering::Relaxed), 0);
        let frame = SourceLocation {
            file: "convex:/probes.js".into(),
            line: 10,
            column: 4,
            mapped: false,
        };
        {
            let old = module("export const version = 1;", 29);
            assert_eq!(
                position(resolve(&old, &frame)),
                ("probes.ts".into(), 30, 3, true)
            );
            assert_eq!(
                position(resolve(&old, &frame)),
                ("probes.ts".into(), 30, 3, true)
            );
            assert_eq!(ENTRIES.load(Ordering::Relaxed), 1);
            let next = SourceLocation {
                line: 11,
                column: 8,
                ..frame.clone()
            };
            assert!(old.explain_sources.get(&next).is_none());
            assert_eq!(
                position(resolve(&old, &next)),
                ("probes.ts".into(), 31, 5, true)
            );
            let alias = SourceLocation {
                file: "convex:/child/probes.js".into(),
                ..frame.clone()
            };
            assert!(old.explain_sources.get(&alias).is_none());
            old.explain_sources.insert(&alias, &(None, None));
            assert!(matches!(
                old.explain_sources.get(&alias),
                Some((None, None))
            ));
            // Negative entries are immutable, including a reason-bearing miss.
            let missing = SourceLocation {
                line: 1,
                column: 1,
                ..frame.clone()
            };
            assert!(matches!(
                resolve(&old, &missing).1,
                Some(SourceUnavailable::TokenMissing)
            ));
            assert!(matches!(
                old.explain_sources.get(&missing),
                Some((Some(_), Some(SourceUnavailable::TokenMissing)))
            ));
            old.explain_sources
                .insert(&alias, &(Some(frame.clone()), None));
            assert!(matches!(
                old.explain_sources.get(&alias),
                Some((None, None))
            ));

            let changed_map = module("export const version = 1;", 98);
            let changed_code = module("export const version = 2;", 29);
            assert!(changed_map.explain_sources.get(&frame).is_none());
            assert!(changed_code.explain_sources.get(&frame).is_none());
            assert_eq!(position(resolve(&changed_map, &frame)).1, 99);
            assert_eq!(position(resolve(&old, &frame)).1, 30);
            assert_eq!(position(resolve(&changed_code, &frame)).1, 30);
            // These are new immutable objects, not a claim about repairing the
            // existing source||map hash's historical delimiter limitation.
        }
        assert_eq!(ENTRIES.load(Ordering::Relaxed), 0);

        let modules: Vec<_> = (0..16)
            .map(|_| Arc::new(module("export {};", 29)))
            .collect();
        let barrier = Arc::new(Barrier::new(modules.len()));
        std::thread::scope(|scope| {
            for source in &modules {
                let barrier = barrier.clone();
                let frame = frame.clone();
                scope.spawn(move || {
                    barrier.wait();
                    for line in 0..1024 {
                        let frame = SourceLocation {
                            line,
                            ..frame.clone()
                        };
                        source
                            .explain_sources
                            .insert(&frame, &(Some(frame.clone()), None));
                        assert!(ENTRIES.load(Ordering::Relaxed) <= LIMIT);
                    }
                });
            }
        });
        assert_eq!(ENTRIES.load(Ordering::Relaxed), LIMIT);
        let saturated = module("export const version = 3;", 98);
        // Saturation skips caching, but resolution must still return real data.
        assert_eq!(position(resolve(&saturated, &frame)).1, 99);
        assert!(saturated.explain_sources.get(&frame).is_none());
        drop(modules);
        assert_eq!(ENTRIES.load(Ordering::Relaxed), 0);
        assert_eq!(position(resolve(&saturated, &frame)).1, 99);
        assert!(saturated.explain_sources.get(&frame).is_some());
        drop(saturated);
        assert_eq!(ENTRIES.load(Ordering::Relaxed), 0);
    }
}
