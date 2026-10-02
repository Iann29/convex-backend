//! Ready contexts for queries and mutations.
//!
//! Every query or mutation runs in a fresh V8 context, and the app's module
//! graph is compiled, instantiated and evaluated again inside it. With real
//! app code that costs 4–20 ms per call, before the function does anything.
//! Import is deterministic here: it cannot reach the database; `Math.random`
//! and `Date.now` run on a seed and a time fixed per push; and environment
//! variables are read from the deployment. So the context right after the
//! import is the same for every request on the same source package while
//! the variables it read hold the same values ([`ImportInputs`]). It is
//! built once into a V8 snapshot, and a request deserializes it instead of
//! evaluating the graph. Each request still gets its own copy, so module
//! state starts fresh every time, as before.
//!
//! 1. After a fresh import succeeds, [`observe`] keeps the module names and
//!    sources of the graph under (component, module, source package), with the
//!    import's inputs.
//! 2. A background thread builds one blob with every kept graph, in a child
//!    process: the backend binary started with [`BUILDER_ENV`], fed through
//!    pipes so the variables never touch the disk. The import runs under a
//!    probe that answers the inputs and the pure ops, and refuses anything
//!    else. V8 aborts the process on objects it cannot serialize (an `Intl`
//!    formatter created at import is one), so the child tries each new graph on
//!    its own first and reports progress; a graph that kills it is left out.
//! 3. The blob becomes the next generation. Isolate workers recreate their
//!    isolate from it, and [`crate::context_cache::ContextCache`] answers which
//!    graphs the isolate's blob holds.

use std::{
    borrow::Cow,
    cell::{
        Cell,
        RefCell,
    },
    collections::{
        BTreeMap,
        HashMap,
        HashSet,
    },
    io::{
        BufRead,
        BufReader,
        Read,
        Write,
    },
    process::{
        Command,
        Stdio,
    },
    sync::{
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
        mpsc,
        Arc,
        LazyLock,
        OnceLock,
    },
    time::{
        Duration,
        Instant,
    },
};

use anyhow::Context as _;
use common::{
    components::{
        CanonicalizedComponentModulePath,
        ComponentId,
    },
    knobs::{
        AMAGE_WARM_SNAPSHOT,
        AMAGE_WARM_SNAPSHOT_DELAY,
        AMAGE_WARM_SNAPSHOT_MAX_MODULES,
        AMAGE_WARM_SNAPSHOT_TIMEOUT,
    },
    runtime::UnixTimestamp,
    types::{
        EnvVarName,
        EnvVarValue,
    },
};
use deno_core::{
    v8::{
        self,
        scope,
        tc_scope,
    },
    ModuleSpecifier,
};
use encoding_rs::Encoding;
use parking_lot::{
    Mutex,
    RwLock,
};
use rand::{
    Rng,
    SeedableRng,
};
use rand_chacha::ChaCha12Rng;
use serde::{
    Deserialize,
    Serialize,
};
use value::ResolvedDocumentId;

use crate::{
    helpers,
    metrics,
    module_cache::V8ModuleSource,
    module_map::ModuleMap,
    udf_runtime,
};

/// Set on the child process. It reads the runtime snapshot and the graphs on
/// stdin, reports progress on stderr and writes the blob on stdout.
pub const BUILDER_ENV: &str = "AMAGE_WARM_SNAPSHOT_BUILDER";

/// Prefix of the child's progress lines on stderr.
const PROGRESS: &str = "warm-snapshot:";

/// Starts the child's output on stdout: a JSON length, the JSON, the blob.
const OUTPUT_MARKER: &[u8] = b"\0amage-warm-snapshot\0";

/// Properties `RequestScope::new` installs on `Convex`, in that order.
const CONVEX_HOOKS: [&str; 4] = ["syscall", "op", "asyncSyscall", "asyncOp"];

/// What a ready context is filed under. The source package fixes every
/// module of the graph: a push writes a new one.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WarmKey {
    pub module: CanonicalizedComponentModulePath,
    pub source_package: ResolvedDocumentId,
}

/// What an import read from outside its source. A ready context built from
/// an import is valid while these hold.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ImportInputs {
    /// Environment variables and the values they held, sorted by name.
    pub env: Vec<(EnvVarName, Option<EnvVarValue>)>,
    /// The per-push seed of `Math.random` and time of `Date.now` during
    /// import.
    pub seed_and_time: Option<([u8; 32], UnixTimestamp)>,
}

impl ImportInputs {
    /// `env` as read, possibly with repeats, in the import's order.
    pub fn new(
        env: &[(EnvVarName, Option<EnvVarValue>)],
        seed_and_time: Option<([u8; 32], UnixTimestamp)>,
    ) -> Self {
        let env = env
            .iter()
            .cloned()
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .collect();
        Self { env, seed_and_time }
    }
}

/// A ready context in an isolate's blob.
#[derive(Clone, Debug)]
pub struct WarmEntry {
    /// Index for `v8::Context::from_snapshot`.
    pub index: usize,
    /// The graph, in the order its modules were attached to the context as
    /// snapshot data (entry first).
    pub modules: Arc<[ModuleSpecifier]>,
    /// The inputs the import ran with; the request must see the same.
    pub inputs: Arc<ImportInputs>,
}

/// One generation: a blob and the ready contexts in it.
pub struct WarmSnapshot {
    generation: u64,
    blob: Box<[u8]>,
    entries: HashMap<WarmKey, WarmEntry>,
}

impl WarmSnapshot {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The startup blob: the runtime's default context plus the ready ones.
    pub fn blob(&self) -> &[u8] {
        &self.blob
    }

    pub fn entry(&self, key: &WarmKey) -> Option<&WarmEntry> {
        self.entries.get(key)
    }
}

static CURRENT: LazyLock<RwLock<Option<Arc<WarmSnapshot>>>> = LazyLock::new(|| RwLock::new(None));
static GENERATION: AtomicU64 = AtomicU64::new(0);

/// The newest generation, for a new isolate.
pub fn current() -> Option<Arc<WarmSnapshot>> {
    CURRENT.read().clone()
}

/// The newest generation number (0 = none yet).
pub fn generation() -> u64 {
    GENERATION.load(Ordering::Acquire)
}

#[derive(Clone)]
struct Graph {
    /// Entry first, then the modules it imports, as the request registered
    /// them.
    modules: Vec<(ModuleSpecifier, Arc<V8ModuleSource>)>,
    inputs: ImportInputs,
}

enum State {
    /// Kept after a fresh import, not in a blob yet.
    Waiting(Graph),
    /// In the current blob.
    Ready(Graph),
    /// The builder refused it, or the child died on it, with these inputs.
    Refused(ImportInputs),
}

#[derive(Default)]
struct Registry {
    keys: HashMap<WarmKey, State>,
    /// The newest source package seen per component. Requests load the latest
    /// one, so keys for older packages are dropped.
    package: HashMap<ComponentId, ResolvedDocumentId>,
}

static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(|| Mutex::new(Registry::default()));
static WAKE: OnceLock<Mutex<mpsc::Sender<()>>> = OnceLock::new();
static BUILDER_ENABLED: AtomicBool = AtomicBool::new(false);

/// Lets this process build ready contexts. Only the backend binary calls it,
/// after `run_builder_if_requested`: the builder is the current executable
/// started again, which in a test binary would run the tests instead.
pub fn enable_builder() {
    BUILDER_ENABLED.store(true, Ordering::Release);
}

/// Keeps the graph of a module that was just imported in a fresh context, so
/// the next build can include it. Cheap when the key is already known with
/// the same inputs; with new inputs (a variable changed), it is built again.
pub fn observe(key: WarmKey, module_map: &ModuleMap, inputs: ImportInputs) {
    if !*AMAGE_WARM_SNAPSHOT || !BUILDER_ENABLED.load(Ordering::Acquire) {
        return;
    }
    {
        let mut registry = REGISTRY.lock();
        match registry.keys.get(&key) {
            Some(State::Waiting(graph) | State::Ready(graph)) if graph.inputs == inputs => return,
            Some(State::Refused(refused)) if *refused == inputs => return,
            _ => (),
        }
        let component = key.module.component;
        if registry.package.get(&component) != Some(&key.source_package) {
            registry.package.insert(component, key.source_package);
            registry.keys.retain(|k, _| {
                k.module.component != component || k.source_package == key.source_package
            });
        }
        if !registry.keys.contains_key(&key)
            && registry.keys.len() >= *AMAGE_WARM_SNAPSHOT_MAX_MODULES
        {
            return;
        }
        let modules = module_map
            .modules()
            .map(|(name, source)| (name.clone(), source.clone()))
            .collect::<Vec<_>>();
        if modules.is_empty() {
            return;
        }
        registry
            .keys
            .insert(key, State::Waiting(Graph { modules, inputs }));
    }
    let wake = WAKE.get_or_init(|| {
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("warm-snapshot".to_string())
            .spawn(move || builder_loop(rx))
            .expect("Failed to start the warm snapshot thread");
        Mutex::new(tx)
    });
    let _ = wake.lock().send(());
}

fn builder_loop(rx: mpsc::Receiver<()>) {
    while rx.recv().is_ok() {
        // Let the first requests after a start or a push register their graphs,
        // then build once for all of them.
        std::thread::sleep(*AMAGE_WARM_SNAPSHOT_DELAY);
        while rx.try_recv().is_ok() {}
        let Some(plan) = plan() else {
            continue;
        };
        let start = Instant::now();
        match build_in_child(&plan) {
            Ok(built) => {
                let contexts = built.contexts.len();
                publish(&plan, built);
                metrics::log_warm_snapshot_build(start.elapsed(), true);
                tracing::info!(
                    "Warm snapshot generation {}: {contexts} ready contexts, built in {:?}",
                    generation(),
                    start.elapsed()
                );
            },
            Err(e) => {
                metrics::log_warm_snapshot_build(start.elapsed(), false);
                tracing::warn!("Warm snapshot build failed: {e:#}");
            },
        }
    }
}

struct PlanEntry {
    id: u32,
    key: WarmKey,
    graph: Graph,
    /// Not in a blob before, so the child tries it alone first.
    trial: bool,
}

/// Everything waiting or ready, if anything is waiting.
fn plan() -> Option<Vec<PlanEntry>> {
    let registry = REGISTRY.lock();
    let mut waiting = false;
    let mut plan = vec![];
    for (key, state) in &registry.keys {
        let (graph, trial) = match state {
            State::Waiting(graph) => {
                waiting = true;
                (graph, true)
            },
            State::Ready(graph) => (graph, false),
            State::Refused(_) => continue,
        };
        plan.push(PlanEntry {
            id: plan.len() as u32,
            key: key.clone(),
            graph: graph.clone(),
            trial,
        });
    }
    waiting.then_some(plan)
}

fn publish(plan: &[PlanEntry], built: Built) {
    let by_id: HashMap<u32, &PlanEntry> = plan.iter().map(|e| (e.id, e)).collect();
    let generation = GENERATION.load(Ordering::Acquire) + 1;
    let mut entries = HashMap::new();
    let mut registry = REGISTRY.lock();
    for (id, index) in &built.contexts {
        let Some(entry) = by_id.get(id) else {
            continue;
        };
        entries.insert(
            entry.key.clone(),
            WarmEntry {
                index: *index,
                modules: entry
                    .graph
                    .modules
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect(),
                inputs: Arc::new(entry.graph.inputs.clone()),
            },
        );
        // Unless a newer import replaced it meanwhile.
        if let Some(state) = registry.keys.get_mut(&entry.key)
            && matches!(state, State::Waiting(graph) if graph.inputs == entry.graph.inputs)
        {
            *state = State::Ready(entry.graph.clone());
        }
    }
    for (id, reason) in &built.refused {
        let Some(entry) = by_id.get(id) else {
            continue;
        };
        tracing::info!(
            "Warm snapshot leaves {:?} out: {reason}",
            entry.key.module.module_path
        );
        registry.keys.insert(
            entry.key.clone(),
            State::Refused(entry.graph.inputs.clone()),
        );
    }
    drop(registry);
    metrics::log_warm_snapshot_contexts(entries.len(), built.blob.len());
    *CURRENT.write() = Some(Arc::new(WarmSnapshot {
        generation,
        blob: built.blob.into_boxed_slice(),
        entries,
    }));
    GENERATION.store(generation, Ordering::Release);
}

#[derive(Serialize, Deserialize)]
struct BuildInput {
    groups: Vec<GroupInput>,
}

/// Graphs of one component on one source package: module names are only
/// unique within it.
#[derive(Serialize, Deserialize)]
struct GroupInput {
    modules: BTreeMap<String, String>,
    entries: Vec<EntryInput>,
}

#[derive(Serialize, Deserialize)]
struct EntryInput {
    id: u32,
    /// Entry first.
    modules: Vec<String>,
    trial: bool,
    /// Environment variables the import read, and their values.
    env: BTreeMap<String, Option<String>>,
    /// The import's `Math.random` seed and `Date.now` in milliseconds.
    seed: Option<[u8; 32]>,
    now_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct BuildOutput {
    contexts: Vec<(u32, usize)>,
    refused: Vec<(u32, String)>,
}

struct Built {
    blob: Vec<u8>,
    contexts: Vec<(u32, usize)>,
    refused: Vec<(u32, String)>,
}

fn build_in_child(plan: &[PlanEntry]) -> anyhow::Result<Built> {
    let mut groups: BTreeMap<(ComponentId, ResolvedDocumentId), GroupInput> = BTreeMap::new();
    let mut refused = vec![];
    'entries: for entry in plan {
        let mut texts = vec![];
        for (name, source) in &entry.graph.modules {
            // The child compiles text; a source that does not survive the trip
            // exactly would compile into a different module.
            let Some(text) = source.source().to_exact_text() else {
                refused.push((entry.id, format!("{name} is not valid text")));
                continue 'entries;
            };
            texts.push((name.to_string(), text));
        }
        let (seed, now_ms) = match entry.graph.inputs.seed_and_time {
            Some((seed, time)) => (Some(seed), Some(time.as_ms_since_epoch()?)),
            None => (None, None),
        };
        let group = groups
            .entry((entry.key.module.component, entry.key.source_package))
            .or_insert_with(|| GroupInput {
                modules: BTreeMap::new(),
                entries: vec![],
            });
        for (name, text) in texts {
            group.modules.entry(name).or_insert(text);
        }
        group.entries.push(EntryInput {
            id: entry.id,
            modules: entry
                .graph
                .modules
                .iter()
                .map(|(name, _)| name.to_string())
                .collect(),
            trial: entry.trial,
            env: entry
                .graph
                .inputs
                .env
                .iter()
                .map(|(name, value)| (name.to_string(), value.as_ref().map(|v| v.to_string())))
                .collect(),
            seed,
            now_ms,
        });
    }
    let mut input = BuildInput {
        groups: groups.into_values().collect(),
    };
    // A graph that kills the child is left out and the child runs again,
    // a bounded number of times.
    for _ in 0..4 {
        match run_child(&input)? {
            ChildOutcome::Done(output, blob) => {
                refused.extend(output.refused);
                return Ok(Built {
                    blob,
                    contexts: output.contexts,
                    refused,
                });
            },
            ChildOutcome::DiedOn(id, why) => {
                refused.push((id, format!("the builder died on it ({why})")));
                for group in &mut input.groups {
                    group.entries.retain(|e| e.id != id);
                }
            },
            ChildOutcome::Failed(why) => anyhow::bail!("the builder failed: {why}"),
        }
    }
    anyhow::bail!("the builder kept dying")
}

enum ChildOutcome {
    Done(BuildOutput, Vec<u8>),
    DiedOn(u32, String),
    Failed(String),
}

/// What the child reads on stdin: the runtime snapshot, then the graphs.
fn child_input(input: &BuildInput) -> anyhow::Result<Vec<u8>> {
    // Every blob in this process must carry the same read-only part as the
    // runtime snapshot (see `run_builder`), so the child starts from ours.
    let base = udf_runtime::base_snapshot();
    let mut bytes = (base.len() as u64).to_le_bytes().to_vec();
    bytes.extend_from_slice(base);
    serde_json::to_writer(&mut bytes, input)?;
    Ok(bytes)
}

fn parse_output(output: &[u8]) -> anyhow::Result<(BuildOutput, Vec<u8>)> {
    let start = output
        .windows(OUTPUT_MARKER.len())
        .position(|w| w == OUTPUT_MARKER)
        .context("the builder wrote no output")?
        + OUTPUT_MARKER.len();
    let output = &output[start..];
    anyhow::ensure!(output.len() >= 8, "the builder output is cut");
    let json_len = u64::from_le_bytes(output[..8].try_into()?) as usize;
    anyhow::ensure!(output.len() >= 8 + json_len, "the builder output is cut");
    let built = serde_json::from_slice(&output[8..8 + json_len])?;
    Ok((built, output[8 + json_len..].to_vec()))
}

fn run_child(input: &BuildInput) -> anyhow::Result<ChildOutcome> {
    let exe = std::env::current_exe().context("current_exe")?;
    let mut child = Command::new(exe)
        .env(BUILDER_ENV, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("Failed to start the warm snapshot builder")?;
    let bytes = child_input(input)?;
    let mut stdin = child.stdin.take().context("child stdin")?;
    let writer = std::thread::spawn(move || stdin.write_all(&bytes));
    let mut stdout = child.stdout.take().context("child stdout")?;
    let output = std::thread::spawn(move || {
        let mut bytes = vec![];
        stdout.read_to_end(&mut bytes).map(|_| bytes)
    });
    let stderr = child.stderr.take().context("child stderr")?;
    let progress = std::thread::spawn(move || {
        let (mut lines, mut last) = (vec![], String::new());
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            match line.strip_prefix(PROGRESS) {
                Some(progress) => lines.push(progress.trim().to_string()),
                None => last = line,
            }
        }
        (lines, last)
    });
    let deadline = Instant::now() + *AMAGE_WARM_SNAPSHOT_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break Some(status);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    // A child that died early leaves the writer with a broken pipe.
    let _ = writer.join();
    let output = output.join().unwrap_or_else(|_| Ok(vec![]))?;
    let (lines, last_error) = progress.join().unwrap_or_default();
    if status.is_some_and(|s| s.success()) {
        let (built, blob) = parse_output(&output)?;
        return Ok(ChildOutcome::Done(built, blob));
    }
    // The child's own last words go to the log only as their kind: what V8
    // prints when it aborts can quote heap contents.
    let why = match status {
        Some(status) if last_error.starts_with('#') => format!("{status}, V8 aborted"),
        Some(status) => status.to_string(),
        None => "timed out".to_string(),
    };
    // The graph it was trying when it died.
    let mut trying = None;
    for line in &lines {
        let mut words = line.split(' ');
        match (
            words.next(),
            words.next().and_then(|id| id.parse::<u32>().ok()),
        ) {
            (Some("try"), Some(id)) => trying = Some(id),
            (Some("ok" | "no"), Some(id)) if trying == Some(id) => trying = None,
            _ => (),
        }
    }
    Ok(match trying {
        Some(id) => ChildOutcome::DiedOn(id, why),
        None => ChildOutcome::Failed(why),
    })
}

/// Runs the builder if this process was started as one; returns its exit
/// code. Called before anything else in `main`.
pub fn run_builder_if_requested() -> Option<i32> {
    std::env::var_os(BUILDER_ENV)?;
    Some(match run_builder() {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("warm snapshot builder: {e:#}");
            1
        },
    })
}

fn run_builder() -> anyhow::Result<()> {
    // V8 shares one read-only heap among all isolates of a process, set up
    // from the first snapshot, so every blob the parent loads must lay out
    // its read-only part exactly as the parent's runtime snapshot does. A
    // snapshot creator normally keeps read-only space open and promotes
    // objects into it when it serializes; sealed, the blob keeps the base's
    // read-only part. Must precede V8's initialization.
    v8::V8::set_flags_from_string("--no-extensible-ro-snapshot");
    crate::client::initialize_v8();
    let mut bytes = vec![];
    std::io::stdin().read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() >= 8, "no input");
    let base_len = u64::from_le_bytes(bytes[..8].try_into()?) as usize;
    anyhow::ensure!(bytes.len() >= 8 + base_len, "cut input");
    let base: &'static [u8] = Box::leak(bytes[8..8 + base_len].to_vec().into_boxed_slice());
    let input: BuildInput = serde_json::from_slice(&bytes[8 + base_len..])?;
    drop(bytes);
    let mut out = std::io::stderr().lock();
    let mut accepted = vec![];
    let mut refused = vec![];
    for (g, group) in input.groups.iter().enumerate() {
        for entry in &group.entries {
            if !entry.trial {
                accepted.push((g, entry));
                continue;
            }
            writeln!(out, "{PROGRESS} try {}", entry.id)?;
            out.flush()?;
            let (_, contexts, mut no) = build_blob(base, &input.groups, &[(g, entry)])?;
            if contexts.is_empty() {
                let reason = no.pop().map(|(_, r)| r).unwrap_or_default();
                writeln!(
                    out,
                    "{PROGRESS} no {} {}",
                    entry.id,
                    reason.replace('\n', " ")
                )?;
                refused.push((entry.id, reason));
            } else {
                writeln!(out, "{PROGRESS} ok {}", entry.id)?;
                accepted.push((g, entry));
            }
            out.flush()?;
        }
    }
    let (blob, contexts, no) = build_blob(base, &input.groups, &accepted)?;
    refused.extend(no);
    let json = serde_json::to_vec(&BuildOutput { contexts, refused })?;
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(OUTPUT_MARKER)?;
    stdout.write_all(&(json.len() as u64).to_le_bytes())?;
    stdout.write_all(&json)?;
    stdout.write_all(&blob)?;
    stdout.flush()?;
    Ok(())
}

thread_local! {
    /// The builder's view of the modules compiled in the current context.
    static COMPILED: RefCell<HashMap<String, v8::Global<v8::Module>>> =
        RefCell::new(HashMap::new());
    static NAMES: RefCell<HashMap<v8::Global<v8::Module>, String>> = RefCell::new(HashMap::new());
    /// Ops, syscalls and unhandled rejections seen during an import.
    static VIOLATIONS: RefCell<Vec<String>> = const { RefCell::new(vec![]) };
    /// Set while the builder imports; off at request time.
    static PROBING: Cell<bool> = const { Cell::new(false) };
    static REACTED: Cell<bool> = const { Cell::new(false) };
    /// The inputs of the import running in the builder.
    static IMPORT: RefCell<Option<ImportState>> = const { RefCell::new(None) };
}

struct ImportState {
    env: BTreeMap<String, Option<String>>,
    rng: Option<ChaCha12Rng>,
    now_ms: Option<u64>,
}

/// One blob with the runtime's default context and a ready context per
/// accepted graph. A graph whose import breaks a rule is reported instead.
fn build_blob(
    base: &'static [u8],
    groups: &[GroupInput],
    entries: &[(usize, &EntryInput)],
) -> anyhow::Result<(Vec<u8>, Vec<(u32, usize)>, Vec<(u32, String)>)> {
    let mut creator = v8::Isolate::snapshot_creator_from_existing_snapshot(
        Cow::Borrowed(base).into(),
        Some(udf_runtime::external_references().into()),
        None,
    );
    // As on the isolates that run requests (`Isolate::new`).
    creator.set_capture_stack_trace_for_uncaught_exceptions(true, 10);
    creator.set_host_initialize_import_meta_object_callback(import_meta_unsupported);
    creator.set_host_import_module_dynamically_callback(dynamic_import_unsupported);
    creator.set_allow_atomics_wait(false);
    creator.set_microtasks_policy(v8::MicrotasksPolicy::Explicit);
    creator.set_promise_reject_callback(record_rejection);
    let mut contexts = vec![];
    let mut refused = vec![];
    {
        scope!(let scope, &mut creator);
        let default_context = v8::Context::new(scope, v8::ContextOptions::default());
        scope.set_default_context(default_context);
        for (g, entry) in entries {
            let context = v8::Context::new(scope, v8::ContextOptions::default());
            let result = {
                let scope = &mut v8::ContextScope::new(scope, context);
                prepare_context(scope, context, &groups[*g], entry)
            };
            COMPILED.with(|c| c.borrow_mut().clear());
            NAMES.with(|n| n.borrow_mut().clear());
            match result {
                Ok(()) => contexts.push((entry.id, scope.add_context(context))),
                Err(e) => refused.push((entry.id, format!("{e:#}"))),
            }
        }
    }
    let blob = creator
        .create_blob(v8::FunctionCodeHandling::Keep)
        .context("V8 could not create the snapshot")?;
    Ok((blob.to_vec(), contexts, refused))
}

/// Imports the graph like a request does, checks the import broke no rule,
/// and attaches the graph's modules to the context.
fn prepare_context(
    scope: &mut v8::PinScope<'_, '_>,
    context: v8::Local<'_, v8::Context>,
    group: &GroupInput,
    entry: &EntryInput,
) -> anyhow::Result<()> {
    VIOLATIONS.with(|v| v.borrow_mut().clear());
    REACTED.set(false);
    IMPORT.set(Some(ImportState {
        env: entry.env.clone(),
        rng: entry.seed.map(ChaCha12Rng::from_seed),
        now_ms: entry.now_ms,
    }));
    PROBING.set(true);
    let result = import(scope, group, entry);
    PROBING.set(false);
    IMPORT.set(None);
    // Reasons name ops and syscalls only: an exception's text may hold the
    // value of a variable, and reasons end up in the backend's log.
    const REJECTION: &str = "an unhandled rejection";
    let violations = VIOLATIONS.with(|v| std::mem::take(&mut *v.borrow_mut()));
    let ops = violations
        .iter()
        .filter(|v| *v != REJECTION)
        .cloned()
        .collect::<Vec<_>>();
    anyhow::ensure!(ops.is_empty(), "import used {}", ops.join(", "));
    // An import that throws also leaves its own promise rejected.
    result?;
    anyhow::ensure!(violations.is_empty(), "import left {REJECTION}");
    anyhow::ensure!(
        !REACTED.get(),
        "import left promise reactions for the request to run"
    );
    remove_probe(scope)?;

    let compiled = COMPILED.with(|c| c.borrow().keys().cloned().collect::<HashSet<_>>());
    let captured = entry.modules.iter().cloned().collect::<HashSet<_>>();
    anyhow::ensure!(
        compiled == captured,
        "the graph has {} modules, the request had {}",
        compiled.len(),
        captured.len()
    );
    for (i, name) in entry.modules.iter().enumerate() {
        let module = COMPILED
            .with(|c| c.borrow().get(name).cloned())
            .context("module disappeared")?;
        let module = v8::Local::new(scope, module);
        anyhow::ensure!(scope.add_context_data(context, module) == i);
    }
    Ok(())
}

fn import(
    scope: &mut v8::PinScope<'_, '_>,
    group: &GroupInput,
    entry: &EntryInput,
) -> anyhow::Result<()> {
    install_probe(scope)?;
    let name = entry.modules.first().context("empty graph")?;
    let module = compile(scope, group, name)?;
    {
        tc_scope!(let tc, scope);
        let instantiated = module.instantiate_module(tc, resolve);
        if tc.exception().is_some() {
            anyhow::bail!("instantiate threw");
        }
        anyhow::ensure!(instantiated == Some(true), "instantiate failed");
        let value = module.evaluate(tc);
        if tc.exception().is_some() {
            anyhow::bail!("import threw");
        }
        let promise: v8::Local<v8::Promise> = value
            .context("evaluate returned nothing")?
            .try_into()
            .context("evaluate did not return a promise")?;
        match promise.state() {
            v8::PromiseState::Fulfilled => (),
            v8::PromiseState::Pending => anyhow::bail!("top-level await"),
            v8::PromiseState::Rejected => anyhow::bail!("import threw"),
        }
    }
    anyhow::ensure!(module.get_status() == v8::ModuleStatus::Evaluated);
    // A request drains microtasks only after calling the function, so any
    // reaction queued by the import must not run here.
    scope.set_promise_hook(record_reaction);
    scope.perform_microtask_checkpoint();
    scope.set_promise_hook(ignore_promise);
    Ok(())
}

fn compile<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    group: &GroupInput,
    name: &str,
) -> anyhow::Result<v8::Local<'s, v8::Module>> {
    if let Some(module) = COMPILED.with(|c| c.borrow().get(name).cloned()) {
        return Ok(v8::Local::new(scope, module));
    }
    let source = group
        .modules
        .get(name)
        .with_context(|| format!("{name} is not in the source package"))?;
    let name_str = v8::String::new(scope, name).context("name")?;
    let source_str = v8::String::new(scope, source).context("source")?;
    let origin = helpers::module_origin(scope, name_str);
    let mut source = v8::script_compiler::Source::new(source_str, Some(&origin));
    let module = {
        tc_scope!(let tc, scope);
        let module = v8::script_compiler::compile_module(tc, &mut source);
        if tc.exception().is_some() {
            anyhow::bail!("{name} does not compile");
        }
        module.context("compile failed")?
    };
    let module = v8::Local::new(scope, module);
    let global = v8::Global::new(scope, module);
    COMPILED.with(|c| c.borrow_mut().insert(name.to_string(), global.clone()));
    NAMES.with(|n| n.borrow_mut().insert(global, name.to_string()));
    let requests = module.get_module_requests();
    for i in 0..requests.length() {
        let request: v8::Local<v8::ModuleRequest> = requests
            .get(scope, i)
            .context("module request")?
            .try_into()?;
        let specifier = request.get_specifier().to_rust_string_lossy(scope);
        let resolved = deno_core::resolve_import(&specifier, name)?;
        compile(scope, group, resolved.as_str())?;
    }
    Ok(module)
}

fn resolve<'c>(
    context: v8::Local<'c, v8::Context>,
    specifier: v8::Local<'c, v8::String>,
    _import_attributes: v8::Local<'c, v8::FixedArray>,
    referrer: v8::Local<'c, v8::Module>,
) -> Option<v8::Local<'c, v8::Module>> {
    v8::callback_scope!(unsafe let scope, context);
    let referrer = v8::Global::new(scope, referrer);
    let referrer_name = NAMES.with(|n| n.borrow().get(&referrer).cloned())?;
    let specifier = specifier.to_rust_string_lossy(scope);
    let resolved = deno_core::resolve_import(&specifier, &referrer_name).ok()?;
    let module = COMPILED.with(|c| c.borrow().get(resolved.as_str()).cloned())?;
    Some(v8::Local::new(scope, module))
}

/// Installs the probe where `RequestScope::new` puts the real syscall and op
/// functions.
fn install_probe(scope: &mut v8::PinScope<'_, '_>) -> anyhow::Result<()> {
    let convex = convex_object(scope)?;
    for name in CONVEX_HOOKS {
        // Each callback must stay a distinct fn item for V8's binding.
        let template = if name == "op" {
            v8::FunctionTemplate::new(scope, probe_op)
        } else {
            v8::FunctionTemplate::new(scope, probe_other)
        };
        let function = template.get_function(scope).context("probe function")?;
        let key = v8::String::new(scope, name).context("key")?;
        convex.set(scope, key.into(), function.into());
    }
    Ok(())
}

/// Deletes the probe in reverse order, so `Convex` looks as it does in a
/// fresh context before `RequestScope::new`.
fn remove_probe(scope: &mut v8::PinScope<'_, '_>) -> anyhow::Result<()> {
    let convex = convex_object(scope)?;
    for name in CONVEX_HOOKS.iter().rev() {
        let key = v8::String::new(scope, name).context("key")?;
        anyhow::ensure!(convex.delete(scope, key.into()) == Some(true));
    }
    Ok(())
}

fn convex_object<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> anyhow::Result<v8::Local<'s, v8::Object>> {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, "Convex").context("key")?;
    Ok(global
        .get(scope, key.into())
        .context("missing global.Convex")?
        .try_into()?)
}

fn violation(scope: &mut v8::PinScope<'_, '_>, what: String) {
    let message = if PROBING.get() {
        let message = format!("{what} during a ready-context import");
        VIOLATIONS.with(|v| v.borrow_mut().push(what));
        message
    } else {
        // Only reachable if an import kept a reference to the probe.
        format!("Internal error: ready-context probe called for {what}")
    };
    let message = v8::String::new(scope, &message).unwrap();
    let exception = v8::Exception::error(scope, message);
    scope.throw_exception(exception);
}

/// `Convex.op` during a ready-context import: answers the import's inputs
/// and the pure ops with what the real ops return
/// (`ops::environment_variables`, `ops::random`, `ops::time`, `ops::text`), and
/// refuses the rest.
pub(crate) fn probe_op<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: v8::FunctionCallbackArguments<'s>,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let name = args.get(0).to_rust_string_lossy(scope);
    if PROBING.get() && matches!(&name[..], "environmentVariables/get" | "random" | "now") {
        let arg = args.get(1).to_rust_string_lossy(scope);
        enum Answer {
            Text(Option<String>),
            Number(f64),
            Unknown,
        }
        let answer = IMPORT.with(|import| {
            let mut import = import.borrow_mut();
            let Some(import) = import.as_mut() else {
                return Answer::Unknown;
            };
            match &name[..] {
                "environmentVariables/get" => match import.env.get(&arg) {
                    Some(value) => Answer::Text(value.clone()),
                    None => Answer::Unknown,
                },
                "random" => match import.rng.as_mut() {
                    Some(rng) => Answer::Number(rng.random()),
                    None => Answer::Unknown,
                },
                _ => match import.now_ms {
                    Some(ms) => Answer::Number(ms as f64),
                    None => Answer::Unknown,
                },
            }
        });
        match answer {
            Answer::Text(Some(value)) => {
                let value = v8::String::new(scope, &value).unwrap();
                rv.set(value.into());
            },
            Answer::Text(None) => rv.set_null(),
            Answer::Number(n) => {
                let n = v8::Number::new(scope, n);
                rv.set(n.into());
            },
            Answer::Unknown => violation(scope, format!("op {name} without its input")),
        }
        return;
    }
    if PROBING.get() && name == "textEncoder/normalizeLabel" {
        let label = args.get(1).to_rust_string_lossy(scope);
        let (key, value) = match Encoding::for_label_no_replacement(label.as_bytes()) {
            Some(encoding) => ("label", encoding.name().to_lowercase()),
            None => (
                "error",
                format!("The encoding label provided ('{label}') is invalid."),
            ),
        };
        let object = v8::Object::new(scope);
        let key = v8::String::new(scope, key).unwrap();
        let value = v8::String::new(scope, &value).unwrap();
        object.set(scope, key.into(), value.into());
        rv.set(object.into());
        return;
    }
    violation(scope, format!("op {name}"));
}

pub(crate) fn probe_other<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: v8::FunctionCallbackArguments<'s>,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let name = args.get(0).to_rust_string_lossy(scope);
    violation(scope, format!("syscall {name}"));
}

extern "C" fn record_rejection(message: v8::PromiseRejectMessage) {
    if message.get_event() == v8::PromiseRejectEvent::PromiseRejectWithNoHandler && PROBING.get() {
        VIOLATIONS.with(|v| v.borrow_mut().push("an unhandled rejection".to_string()));
    }
}

unsafe extern "C" fn record_reaction(
    kind: v8::PromiseHookType,
    _promise: v8::Local<v8::Promise>,
    _parent: v8::Local<v8::Value>,
) {
    if matches!(kind, v8::PromiseHookType::Before) {
        REACTED.set(true);
    }
}

unsafe extern "C" fn ignore_promise(
    _kind: v8::PromiseHookType,
    _promise: v8::Local<v8::Promise>,
    _parent: v8::Local<v8::Value>,
) {
}

extern "C" fn import_meta_unsupported(
    context: v8::Local<v8::Context>,
    _module: v8::Local<v8::Module>,
    _meta: v8::Local<v8::Object>,
) {
    v8::callback_scope!(unsafe let scope, context);
    let message = v8::String::new(scope, "import.meta is unsupported").unwrap();
    let exception = v8::Exception::type_error(scope, message);
    scope.throw_exception(exception);
}

fn dynamic_import_unsupported<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _host_defined_options: v8::Local<'s, v8::Data>,
    _resource_name: v8::Local<'s, v8::Value>,
    _specifier: v8::Local<'s, v8::String>,
    _import_assertions: v8::Local<'s, v8::FixedArray>,
) -> Option<v8::Local<'s, v8::Promise>> {
    let message = v8::String::new(scope, "dynamic import during a ready-context import").unwrap();
    let exception = v8::Exception::type_error(scope, message);
    let resolver = v8::PromiseResolver::new(scope)?;
    let promise = resolver.get_promise(scope);
    resolver.reject(scope, exception);
    Some(promise)
}

#[cfg(test)]
mod tests {
    //! Blobs are built by this test binary started again as the builder
    //! child, as the backend does: a blob built in-process would not share
    //! the read-only layout of this process's runtime snapshot, and isolates
    //! of other tests run concurrently.

    use std::{
        collections::HashMap,
        sync::Arc,
    };

    use deno_core::v8::{
        self,
        scope,
    };
    use rand::{
        Rng,
        SeedableRng,
    };
    use rand_chacha::ChaCha12Rng;

    use super::{
        BuildInput,
        BuildOutput,
        EntryInput,
        GroupInput,
        WarmSnapshot,
        BUILDER_ENV,
    };
    use crate::udf_runtime;

    /// The child half: this binary started again as the builder.
    #[test]
    fn builder_child() {
        if std::env::var_os(BUILDER_ENV).is_some() {
            std::process::exit(super::run_builder_if_requested().unwrap());
        }
    }

    fn entry(id: u32, modules: &[&str]) -> EntryInput {
        EntryInput {
            id,
            modules: modules.iter().map(|m| format!("convex:/{m}")).collect(),
            trial: true,
            env: Default::default(),
            seed: None,
            now_ms: None,
        }
    }

    /// Builds a blob in the child, which is this test binary started again.
    fn child_build_entries(
        modules: &[(&str, &str)],
        entries: Vec<EntryInput>,
    ) -> (Vec<u8>, BuildOutput) {
        use std::io::{
            Read,
            Write,
        };
        crate::client::initialize_v8();
        let input = BuildInput {
            groups: vec![GroupInput {
                modules: modules
                    .iter()
                    .map(|(name, source)| (format!("convex:/{name}"), source.to_string()))
                    .collect(),
                entries,
            }],
        };
        let bytes = super::child_input(&input).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "warm_snapshot::tests::builder_child",
                "--test-threads=1",
            ])
            .env(BUILDER_ENV, "1")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let writer = std::thread::spawn(move || stdin.write_all(&bytes));
        let mut output = vec![];
        child
            .stdout
            .take()
            .unwrap()
            .read_to_end(&mut output)
            .unwrap();
        writer.join().unwrap().unwrap();
        let status = child.wait().unwrap();
        assert!(status.success(), "builder child failed: {status}");
        let (built, blob) = super::parse_output(&output).unwrap();
        (blob, built)
    }

    /// Builds a blob in the child with one entry per graph, no inputs.
    fn child_build(modules: &[(&str, &str)], graphs: &[&[&str]]) -> (Vec<u8>, BuildOutput) {
        let entries = graphs
            .iter()
            .enumerate()
            .map(|(id, graph)| entry(id as u32, graph))
            .collect();
        child_build_entries(modules, entries)
    }

    /// Builds one ready context: its blob and index, or why it was refused.
    fn build_one(modules: &[(&str, &str)], graph: &[&str]) -> Result<(Vec<u8>, usize), String> {
        let (blob, output) = child_build(modules, &[graph]);
        match (output.contexts.as_slice(), output.refused.as_slice()) {
            ([(0, index)], []) => Ok((blob, *index)),
            ([], [(0, reason)]) => Err(reason.clone()),
            other => panic!("unexpected build result {other:?}"),
        }
    }

    fn warm(blob: Vec<u8>) -> WarmSnapshot {
        WarmSnapshot {
            generation: 1,
            blob: blob.into_boxed_slice(),
            entries: HashMap::new(),
        }
    }

    /// Runs `check` in `rounds` contexts deserialized from the blob, each
    /// with the graph's modules taken out of the snapshot data.
    fn with_ready_contexts(
        blob: Vec<u8>,
        index: usize,
        modules: usize,
        rounds: usize,
        mut check: impl FnMut(&mut v8::PinScope<'_, '_>, Vec<v8::Local<'_, v8::Module>>),
    ) {
        let snapshot = warm(blob);
        // SAFETY: `snapshot` outlives `isolate`, which is dropped first.
        let mut isolate = unsafe {
            udf_runtime::create_isolate_with_udf_runtime(
                v8::CreateParams::default(),
                64 << 20,
                Some(&snapshot),
            )
        };
        for _ in 0..rounds {
            scope!(let scope, &mut isolate);
            let context =
                v8::Context::from_snapshot(scope, index, v8::ContextOptions::default()).unwrap();
            let scope = &mut v8::ContextScope::new(scope, context);
            let graph = (0..modules)
                .map(|i| {
                    scope
                        .get_context_data_from_snapshot_once::<v8::Module>(i)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            check(scope, graph);
        }
        drop(isolate);
    }

    fn call_export(
        scope: &mut v8::PinScope<'_, '_>,
        module: v8::Local<'_, v8::Module>,
        name: &str,
        args: &[v8::Local<'_, v8::Value>],
    ) -> String {
        let namespace = module.get_module_namespace().to_object(scope).unwrap();
        let key = v8::String::new(scope, name).unwrap();
        let function: v8::Local<v8::Function> = namespace
            .get(scope, key.into())
            .unwrap()
            .try_into()
            .unwrap();
        let undefined = v8::undefined(scope);
        function
            .call(scope, undefined.into(), args)
            .unwrap()
            .to_rust_string_lossy(scope)
    }

    fn eval(scope: &mut v8::PinScope<'_, '_>, code: &str) -> String {
        let code = v8::String::new(scope, code).unwrap();
        let script = v8::Script::compile(scope, code, None).unwrap();
        script.run(scope).unwrap().to_rust_string_lossy(scope)
    }

    #[test]
    fn every_request_gets_fresh_module_state() {
        let (blob, index) = build_one(
            &[
                (
                    "counter.js",
                    "import { label } from './_deps/label.js';\nlet calls = 0;\nconst seen = \
                     [];\nexport function bump() { calls += 1; seen.push(calls); return \
                     `${label}:${calls}:${seen.length}`; }",
                ),
                (
                    "_deps/label.js",
                    "export const label = ['a', 'b'].join('-');",
                ),
            ],
            &["counter.js", "_deps/label.js"],
        )
        .unwrap();
        let mut rounds = 0;
        with_ready_contexts(blob, index, 2, 3, |scope, graph| {
            rounds += 1;
            assert_eq!(graph[0].get_status(), v8::ModuleStatus::Evaluated);
            assert_eq!(graph[1].get_status(), v8::ModuleStatus::Evaluated);
            assert_eq!(call_export(scope, graph[0], "bump", &[]), "a-b:1:1");
            assert_eq!(call_export(scope, graph[0], "bump", &[]), "a-b:2:2");
            // The probe is gone: `Convex` looks as in a fresh context, where
            // `RequestScope::new` adds the real hooks.
            assert_eq!(
                eval(
                    scope,
                    "['syscall', 'op', 'asyncSyscall', 'asyncOp'].filter(k => k in Convex).length"
                ),
                "0"
            );
        });
        assert_eq!(rounds, 3);
    }

    #[test]
    fn a_ready_context_looks_like_a_fresh_one() {
        const SHAPE: &str = "JSON.stringify([Object.getOwnPropertyNames(globalThis).sort(), \
                             Object.getOwnPropertyNames(Convex).sort()])";
        let (blob, index) =
            build_one(&[("plain.js", "export const x = 1;")], &["plain.js"]).unwrap();
        let fresh = {
            // SAFETY: the base snapshot is static.
            let mut isolate = unsafe {
                udf_runtime::create_isolate_with_udf_runtime(
                    v8::CreateParams::default(),
                    64 << 20,
                    None,
                )
            };
            scope!(let scope, &mut isolate);
            let context = v8::Context::new(scope, v8::ContextOptions::default());
            let scope = &mut v8::ContextScope::new(scope, context);
            eval(scope, SHAPE)
        };
        with_ready_contexts(blob, index, 1, 1, |scope, _| {
            assert_eq!(eval(scope, SHAPE), fresh);
        });
    }

    #[test]
    fn pure_text_decoding_is_allowed_at_import() {
        let (blob, index) = build_one(
            &[(
                "decoder.js",
                "const decoder = new TextDecoder('UTF-8');\nexport function encoding() { return \
                 decoder.encoding; }",
            )],
            &["decoder.js"],
        )
        .unwrap();
        with_ready_contexts(blob, index, 1, 2, |scope, graph| {
            assert_eq!(call_export(scope, graph[0], "encoding", &[]), "utf-8");
        });
    }

    #[test]
    fn imports_that_reach_the_request_are_refused() {
        let sources = [
            ("export const r = Math.random();", "op random"),
            ("export const t = Date.now();", "op now"),
            (
                "export const e = process.env.SECRET;",
                "op environmentVariables/get",
            ),
            ("console.log('hello');", "op console/message"),
            ("Promise.resolve().then(() => {});", "promise reactions"),
            (
                "Promise.reject(new Error('late'));",
                "an unhandled rejection",
            ),
            ("throw new Error('secret-value');", "import threw"),
            ("await Promise.resolve();", "top-level await"),
        ];
        let modules = sources
            .iter()
            .enumerate()
            .map(|(i, (source, _))| (format!("m{i}.js"), source.to_string()))
            .collect::<Vec<_>>();
        let module_refs = modules
            .iter()
            .map(|(name, source)| (name.as_str(), source.as_str()))
            .collect::<Vec<_>>();
        let graphs = modules
            .iter()
            .map(|(name, _)| vec![name.as_str()])
            .collect::<Vec<_>>();
        let graph_refs = graphs.iter().map(|g| g.as_slice()).collect::<Vec<_>>();
        let (_, output) = child_build(&module_refs, &graph_refs);
        assert_eq!(output.contexts, vec![]);
        let reasons = output.refused.into_iter().collect::<HashMap<_, _>>();
        for (i, (source, why)) in sources.iter().enumerate() {
            let reason = &reasons[&(i as u32)];
            assert!(reason.contains(why), "{source}: {reason}");
            // An exception's text never reaches a reason.
            assert!(!reason.contains("secret-value"), "{source}: {reason}");
        }
    }

    #[test]
    fn a_graph_that_differs_from_the_request_is_refused() {
        let reason = build_one(
            &[
                ("a.js", "import './_deps/b.js'; export const a = 1;"),
                ("_deps/b.js", "export const b = 2;"),
            ],
            &["a.js"],
        )
        .unwrap_err();
        assert!(reason.contains("the graph has 2 modules"), "{reason}");
    }

    /// `Math.random`, `Date.now` and environment variables read at import get
    /// what the real ops give the request: the per-push seed and time, and
    /// the variables the observed import read.
    #[test]
    fn import_inputs_are_answered_like_the_ops() {
        let seed = [7u8; 32];
        let mut with_inputs = entry(0, &["inputs.js"]);
        with_inputs.seed = Some(seed);
        with_inputs.now_ms = Some(1_790_000_000_123);
        with_inputs.env = [
            ("FLAG".to_string(), Some("on".to_string())),
            ("MISSING".to_string(), None),
        ]
        .into_iter()
        .collect();
        let (blob, output) = child_build_entries(
            &[(
                "inputs.js",
                "const r = [Math.random(), Math.random()];\nconst t = Date.now();\nconst e = \
                 [process.env.FLAG, process.env.MISSING ?? 'none'];\nexport function check(r0, \
                 r1) {\nreturn [r[0] === r0, r[1] === r1, t, e.join(',')].join('|');\n}",
            )],
            vec![with_inputs],
        );
        assert_eq!(output.refused, vec![]);
        let index = output.contexts[0].1;
        let mut rng = ChaCha12Rng::from_seed(seed);
        let (r0, r1): (f64, f64) = (rng.random(), rng.random());
        with_ready_contexts(blob, index, 1, 2, |scope, graph| {
            let r0 = v8::Number::new(scope, r0);
            let r1 = v8::Number::new(scope, r1);
            assert_eq!(
                call_export(scope, graph[0], "check", &[r0.into(), r1.into()]),
                "true|true|1790000000123|on,none"
            );
        });
        // A variable the observed import did not read has no value to give.
        let mut other = entry(0, &["other.js"]);
        other.env = [("FLAG".to_string(), Some("on".to_string()))]
            .into_iter()
            .collect();
        let (_, output) = child_build_entries(
            &[("other.js", "export const x = process.env.OTHER;")],
            vec![other],
        );
        assert!(
            output.refused[0].1.contains("without its input"),
            "{output:?}"
        );
    }

    const WORK: &str =
        "export function work(seed) {\nconst m = new Map();\nfor (let i = 0; i < 300; i++) \
         m.set('k' + ((i * 7 + seed) % 97), i);\nconst o = {};\nfor (let i = 0; i < 120; i++) \
         o['p' + i] = i;\nfor (let i = 0; i < 60; i++) delete o['p' + i * 2];\nconst words = 'the \
         quick brown fox jumps over the lazy dog'.split(' ');\nreturn \
         [JSON.stringify([...m.entries()].sort().slice(0, 4)), \
         Object.keys(o).join(),\n/a(b+)c/.exec('xabbbcx')[1], 'u\\u0308'.normalize('NFC'), \
         Symbol.for('sym' + seed).description,\nwords.sort().join('-'), (2n ** 70n + \
         BigInt(seed)).toString(36), JSON.parse('{\"z\":[1,{\"y\":2}]}').z[1].y,\nnew \
         Set(words).size, Array.from({ length: 5 }, (_, i) => i * seed).reduce((a, b) => a + \
         b)].join('|');\n}";

    /// V8 shares one read-only heap among the isolates of a process. Blobs
    /// built by the child keep the runtime snapshot's read-only layout, so
    /// they run next to isolates on the runtime snapshot and on another
    /// generation, on many threads, through full GCs.
    #[test]
    fn blobs_from_the_builder_mix_with_the_runtime_snapshot() {
        let ro_size = |b: &[u8]| {
            let at = |i: usize| u32::from_le_bytes(b[80 + i * 4..84 + i * 4].try_into().unwrap());
            at(1) - at(0)
        };
        let mut generations = vec![];
        for (name, tag) in [("a.js", "gen-a"), ("b.js", "gen-b")] {
            let source = format!("{WORK}\nexport const tag = '{tag}';");
            let (blob, index) = build_one(&[(name, &source)], &[name]).unwrap();
            assert_eq!(ro_size(&blob), ro_size(udf_runtime::base_snapshot()));
            generations.push(Arc::new((warm(blob), index, tag)));
        }
        let script = WORK.replace("export function work", "globalThis.work = function");
        let expected = {
            // SAFETY: the base snapshot is static.
            let mut isolate = unsafe {
                udf_runtime::create_isolate_with_udf_runtime(
                    v8::CreateParams::default(),
                    64 << 20,
                    None,
                )
            };
            scope!(let scope, &mut isolate);
            let context = v8::Context::new(scope, v8::ContextOptions::default());
            let scope = &mut v8::ContextScope::new(scope, context);
            eval(scope, &script);
            (0..4)
                .map(|seed| eval(scope, &format!("work({seed})")))
                .collect::<Vec<_>>()
        };
        let threads = (0..8)
            .map(|t| {
                let generations = generations.clone();
                let expected = expected.clone();
                let script = script.clone();
                std::thread::spawn(move || {
                    for round in 0..6 {
                        let which = (t + round) % 3;
                        let warm = (which > 0).then(|| generations[which - 1].clone());
                        // SAFETY: `warm` outlives the isolate.
                        let mut isolate = unsafe {
                            udf_runtime::create_isolate_with_udf_runtime(
                                v8::CreateParams::default(),
                                16 << 20,
                                warm.as_deref().map(|(snapshot, ..)| snapshot),
                            )
                        };
                        for i in 0..30 {
                            {
                                scope!(let scope, &mut isolate);
                                let seed = i % 4;
                                let result = match &warm {
                                    Some(warm) => {
                                        let context = v8::Context::from_snapshot(
                                            scope,
                                            warm.1,
                                            v8::ContextOptions::default(),
                                        )
                                        .unwrap();
                                        let scope = &mut v8::ContextScope::new(scope, context);
                                        let module: v8::Local<v8::Module> =
                                            scope.get_context_data_from_snapshot_once(0).unwrap();
                                        let arg = v8::Integer::new(scope, seed as i32);
                                        let tag = call_export(scope, module, "work", &[arg.into()]);
                                        let namespace =
                                            module.get_module_namespace().to_object(scope).unwrap();
                                        let key = v8::String::new(scope, "tag").unwrap();
                                        let value = namespace
                                            .get(scope, key.into())
                                            .unwrap()
                                            .to_rust_string_lossy(scope);
                                        assert_eq!(value, warm.2);
                                        tag
                                    },
                                    None => {
                                        let context =
                                            v8::Context::new(scope, v8::ContextOptions::default());
                                        let scope = &mut v8::ContextScope::new(scope, context);
                                        eval(scope, &script);
                                        eval(scope, &format!("work({seed})"))
                                    },
                                };
                                assert_eq!(result, expected[seed], "thread {t} round {round}");
                            }
                            if i % 10 == 9 {
                                isolate.low_memory_notification();
                            }
                        }
                        drop(isolate);
                    }
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
    }
}
