use std::{
    collections::HashMap,
    sync::Arc,
};

use async_lru::async_lru::SizedValue;
use deno_core::{
    v8,
    ModuleSpecifier,
};
use derive_more::Sub;
use model::modules::module_versions::SourceMap;

use crate::module_cache::V8ModuleSource;

pub type ModuleId = usize;

/// A snapshot of a `ModuleMap`'s contents, so that two snapshots can be
/// subtracted to get what a single request registered.
#[derive(Clone, Copy, Sub)]
pub struct ModulesRegistered {
    pub module_count: usize,
    pub source_size: usize,
}

/// All of the modules currently loaded, indexed by name and by V8 handle.
pub struct ModuleMap {
    modules: Vec<ModuleInfo>,
    by_name: HashMap<ModuleSpecifier, ModuleId>,
    by_handle: HashMap<v8::Global<v8::Module>, ModuleId>,
    total_source_size: usize,
    // Filled only by opt-in EXPLAIN. At most 512 callsites, with bounded file names.
    pub(crate) explain_sources: HashMap<
        (ModuleId, u32, u32),
        (
            Option<common::explain::SourceLocation>,
            Option<common::explain::SourceUnavailable>,
        ),
    >,
}

struct ModuleInfo {
    pub name: ModuleSpecifier,
    pub handle: v8::Global<v8::Module>,
    pub module_source: Arc<V8ModuleSource>,
}

impl ModuleMap {
    pub fn new() -> Self {
        Self {
            modules: vec![],
            by_name: HashMap::new(),
            by_handle: HashMap::new(),
            total_source_size: 0,
            explain_sources: HashMap::new(),
        }
    }

    /// The number of modules registered and the sum of their source sizes,
    /// for measuring how much of a request's import closure was already loaded.
    pub fn registered(&self) -> ModulesRegistered {
        ModulesRegistered {
            module_count: self.modules.len(),
            source_size: self.total_source_size,
        }
    }

    /// Every registered module with its source, in registration order: for a
    /// fresh import, the entry module first.
    pub fn modules(&self) -> impl Iterator<Item = (&ModuleSpecifier, &Arc<V8ModuleSource>)> {
        self.modules.iter().map(|m| (&m.name, &m.module_source))
    }

    pub fn name_by_handle(&self, handle: &v8::Global<v8::Module>) -> Option<&ModuleSpecifier> {
        let id = self.by_handle.get(handle)?;
        let info = &self.modules[*id];
        Some(&info.name)
    }

    pub fn handle_by_id(&self, id: ModuleId) -> Option<v8::Global<v8::Module>> {
        self.modules.get(id).map(|m| m.handle.clone())
    }

    pub fn get_by_name(&self, specifier: &ModuleSpecifier) -> Option<ModuleId> {
        self.by_name.get(specifier).cloned()
    }

    pub(crate) fn module_source(&self, id: ModuleId) -> &Arc<V8ModuleSource> {
        &self.modules[id].module_source
    }

    pub fn source_map(&self, id: ModuleId) -> Option<&SourceMap> {
        self.modules[id].module_source.source_map()
    }

    pub fn register(
        &mut self,
        name: &ModuleSpecifier,
        handle: v8::Global<v8::Module>,
        module_source: Arc<V8ModuleSource>,
    ) -> ModuleId {
        let id = self.modules.len();

        self.total_source_size += module_source.size() as usize;
        let info = ModuleInfo {
            name: name.to_owned(),
            handle: handle.clone(),
            module_source,
        };
        self.modules.push(info);
        self.by_name.insert(name.to_owned(), id);
        self.by_handle.insert(handle, id);

        id
    }
}
