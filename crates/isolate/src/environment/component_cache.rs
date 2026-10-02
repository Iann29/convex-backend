//! Reads every function run used to repeat although they depend only on a
//! component's modules: which source package is the latest, and what each
//! child component exports. With hundreds of modules, both walk them all.
//!
//! `_modules` and `_source_packages` are in-memory indexes, and a snapshot
//! knows when each was last written. Two snapshots that see the same
//! last-modified timestamps see the same documents, so the answer computed
//! for one serves the other. A transaction that wrote either index, or a
//! snapshot without the timestamps, reads as before.

use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    sync::{
        Arc,
        LazyLock,
    },
};

use common::{
    components::{
        ComponentId,
        ComponentPath,
        Resource,
    },
    document::ParsedDocument,
    runtime::Runtime,
    types::{
        IndexId,
        Timestamp,
    },
};
use database::{
    BootstrapComponentsModel,
    Transaction,
};
use model::{
    components::ComponentsModel,
    modules::MODULES_TABLE,
    source_packages::{
        types::SourcePackage,
        SourcePackageModel,
        SOURCE_PACKAGES_TABLE,
    },
};
use parking_lot::Mutex;
use sync_types::path::PathComponent;
use value::{
    TableName,
    TableNamespace,
};

type Version = (IndexId, Timestamp);

/// The `by_id` index of `table` in `namespace`, and when it was last written
/// as of `tx`'s snapshot.
fn version<RT: Runtime>(
    tx: &mut Transaction<RT>,
    namespace: TableNamespace,
    table: &TableName,
) -> Option<Version> {
    let tablet = tx
        .table_mapping()
        .namespace(namespace)
        .id_if_exists(table)?;
    let index_id = tx.index_registry().must_get_by_id(tablet).ok()?.id();
    Some((index_id, tx.in_memory_index_last_modified(index_id)?))
}

struct Latest {
    modules: Version,
    packages: Version,
    package: Option<Arc<ParsedDocument<SourcePackage>>>,
}

static LATEST: LazyLock<Mutex<HashMap<ComponentId, Latest>>> = LazyLock::new(Default::default);

/// [`SourcePackageModel::get_latest`], for a transaction whose read set does
/// not matter (a snapshot query, or an action's read-only one).
pub async fn latest_source_package<RT: Runtime>(
    tx: &mut Transaction<RT>,
    component: ComponentId,
) -> anyhow::Result<Option<Arc<ParsedDocument<SourcePackage>>>> {
    let namespace = component.into();
    let versions =
        version(tx, namespace, &MODULES_TABLE).zip(version(tx, namespace, &SOURCE_PACKAGES_TABLE));
    if let Some((modules, packages)) = versions
        && let Some(latest) = LATEST.lock().get(&component)
        && latest.modules == modules
        && latest.packages == packages
    {
        return Ok(latest.package.clone());
    }
    let package = SourcePackageModel::new(tx, namespace).get_latest().await?;
    if let Some((modules, packages)) = versions {
        LATEST.lock().insert(
            component,
            Latest {
                modules,
                packages,
                package: package.clone(),
            },
        );
    }
    Ok(package)
}

pub type Exports = Arc<BTreeMap<Vec<PathComponent>, Resource>>;

struct ChildExports {
    modules: Version,
    path: ComponentPath,
    exports: Exports,
}

static CHILD_EXPORTS: LazyLock<Mutex<HashMap<ComponentId, ChildExports>>> =
    LazyLock::new(Default::default);

/// [`ComponentsModel::preload_exported_resources`] for `child`. When every
/// export is one of the child's own functions, the result depends only on
/// its modules and its path, and is kept for the next action.
pub async fn child_exports<RT: Runtime>(
    tx: &mut Transaction<RT>,
    child: ComponentId,
) -> anyhow::Result<Exports> {
    let modules = version(tx, child.into(), &MODULES_TABLE);
    let path = BootstrapComponentsModel::new(tx).must_component_path(child)?;
    if let Some(modules) = modules
        && let Some(cached) = CHILD_EXPORTS.lock().get(&child)
        && cached.modules == modules
        && cached.path == path
    {
        return Ok(cached.exports.clone());
    }
    let exports = Arc::new(
        ComponentsModel::new(tx)
            .preload_exported_resources(child)
            .await?,
    );
    let own_functions = exports
        .values()
        .all(|resource| matches!(resource, Resource::Function(f) if f.component == path));
    if let Some(modules) = modules
        && own_functions
    {
        CHILD_EXPORTS.lock().insert(
            child,
            ChildExports {
                modules,
                path,
                exports: exports.clone(),
            },
        );
    }
    Ok(exports)
}
