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
        BTreeSet,
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
        AMAGE_WARM_SNAPSHOT_MAX_BYTES,
        AMAGE_WARM_SNAPSHOT_MAX_CONTEXT_BYTES,
        AMAGE_WARM_SNAPSHOT_MAX_MODULES,
        AMAGE_WARM_SNAPSHOT_MAX_SHARED_BYTES,
        AMAGE_WARM_SNAPSHOT_TIMEOUT,
        ISOLATE_MAX_USER_HEAP_SIZE,
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

/// Starts the child's output on stdout: the JSON length, the JSON, the blob
/// length, the blob.
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
    /// The stand-ins its import took, bound by [`bind_stand_ins`] before the
    /// request runs.
    pub stand_ins: Arc<[String]>,
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
    /// What its ready context adds to the blob, once built.
    size: Option<Cost>,
}

/// What a ready context adds to a blob: in all, and in the startup part that
/// every isolate made from the blob loads when it is created. V8 serializes
/// strings, scripts and compiled functions there, not with the context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Cost {
    total: usize,
    shared: usize,
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
        registry.keys.insert(
            key,
            State::Waiting(Graph {
                modules,
                inputs,
                size: None,
            }),
        );
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
    let mut last: Option<(Instant, Duration)> = None;
    let mut failures = 0;
    while rx.recv().is_ok() {
        // Let the first requests after a start or a push register their graphs,
        // then build once for all of them.
        std::thread::sleep(*AMAGE_WARM_SNAPSHOT_DELAY);
        // Every build imports every graph again, and new graphs show up one
        // at a time as traffic reaches them after a push: resting twice as
        // long as the last build took keeps the builder under a third of a
        // core.
        if let Some((ended, took)) = last
            && let Some(rest) = (ended + took * 2).checked_duration_since(Instant::now())
        {
            std::thread::sleep(rest);
        }
        while rx.try_recv().is_ok() {}
        let Some(plan) = plan() else {
            continue;
        };
        let start = Instant::now();
        let result = build_in_child(&plan);
        last = Some((Instant::now(), start.elapsed()));
        match result {
            Ok(built) => {
                let contexts = built.contexts.len();
                let (blob, startup) = (built.blob.len(), startup_part(&built.blob).unwrap_or(0));
                publish(&plan, built);
                failures = 0;
                metrics::log_warm_snapshot_build(start.elapsed(), true);
                tracing::info!(
                    "Warm snapshot generation {}: {contexts} ready contexts ({} MiB, {} MiB \
                     loaded by every isolate), built in {:?}",
                    generation(),
                    blob >> 20,
                    startup >> 20,
                    start.elapsed()
                );
            },
            Err(e) => {
                metrics::log_warm_snapshot_build(start.elapsed(), false);
                tracing::warn!("Warm snapshot build failed: {e:#}");
                // Each new graph would start a build that fails again: after
                // two in a row, the new graphs wait for new inputs, as refused
                // ones do.
                failures += 1;
                if failures >= 2 {
                    refuse_new(&plan, "two builds failed in a row");
                    failures = 0;
                }
            },
        }
    }
}

fn refuse_new(plan: &[PlanEntry], reason: &str) {
    let mut registry = REGISTRY.lock();
    for entry in plan.iter().filter(|entry| entry.trial) {
        if let Some(state) = registry.keys.get_mut(&entry.key)
            && matches!(state, State::Waiting(graph) if graph.inputs == entry.graph.inputs)
        {
            tracing::info!(
                "Warm snapshot leaves {:?} out: {reason}",
                entry.key.module.module_path
            );
            *state = State::Refused(entry.graph.inputs.clone());
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
                stand_ins: built.stand_ins.get(id).cloned().unwrap_or_default().into(),
            },
        );
        // Unless a newer import replaced it meanwhile.
        if let Some(state) = registry.keys.get_mut(&entry.key)
            && matches!(state, State::Waiting(graph) if graph.inputs == entry.graph.inputs)
        {
            *state = State::Ready(Graph {
                size: built.costs.get(id).copied(),
                ..entry.graph.clone()
            });
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
    /// What its ready context added to the last blob, if it was in one.
    cost: Option<Cost>,
    /// In the last blob: left out last when the blob is too big.
    #[serde(default)]
    ready: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct BuildOutput {
    contexts: Vec<(u32, usize)>,
    refused: Vec<(u32, String)>,
    /// What each ready context adds to the blob.
    costs: HashMap<u32, Cost>,
    /// The stand-ins each ready context's import took (see [`bind_stand_ins`]).
    stand_ins: HashMap<u32, Vec<String>>,
}

struct Built {
    blob: Vec<u8>,
    contexts: Vec<(u32, usize)>,
    refused: Vec<(u32, String)>,
    costs: HashMap<u32, Cost>,
    stand_ins: HashMap<u32, Vec<String>>,
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
            cost: entry.graph.size,
            ready: !entry.trial,
        });
    }
    let mut input = BuildInput {
        groups: groups.into_values().collect(),
    };
    // A graph that kills the child is left out and the child runs again,
    // keeping what it decided about the others before it died; so does a
    // child that died starting. A kill from outside (out of time, the
    // kernel's OOM killer) need not be the graph's fault: it is left out
    // only if it was in progress at two of them.
    let trials = input
        .groups
        .iter()
        .flat_map(|group| &group.entries)
        .filter(|entry| entry.trial)
        .count();
    let mut killed = HashMap::<u32, u32>::new();
    for _ in 0..2 * trials + 3 {
        match run_child(&input, builder_command()?)? {
            ChildOutcome::Done(output, blob) => {
                refused.extend(output.refused);
                return Ok(Built {
                    blob,
                    contexts: output.contexts,
                    refused,
                    costs: output.costs,
                    stand_ins: output.stand_ins,
                });
            },
            ChildOutcome::DiedOn {
                id,
                why,
                ok,
                no,
                killed: from_outside,
            } => {
                let mut gone = no.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
                let times = killed.entry(id).or_default();
                *times += 1;
                if !from_outside || *times >= 2 {
                    refused.push((id, format!("the builder died on it ({why})")));
                    gone.insert(id);
                }
                refused.extend(no);
                for group in &mut input.groups {
                    group.entries.retain(|e| !gone.contains(&e.id));
                    for entry in &mut group.entries {
                        if let Some(cost) = ok.get(&entry.id) {
                            entry.trial = false;
                            entry.cost = Some(*cost);
                        }
                    }
                }
            },
            ChildOutcome::DiedStarting(why) => {
                tracing::warn!("Warm snapshot builder died starting ({why}); running it again");
            },
            ChildOutcome::Failed(why) => anyhow::bail!("the builder failed: {why}"),
        }
    }
    anyhow::bail!("the builder kept dying")
}

enum ChildOutcome {
    Done(BuildOutput, Vec<u8>),
    /// On the graph `id`, after accepting the graphs in `ok` and refusing
    /// those in `no`; `killed` from outside (out of time, out of memory).
    DiedOn {
        id: u32,
        why: String,
        ok: HashMap<u32, Cost>,
        no: Vec<(u32, String)>,
        killed: bool,
    },
    /// Before it had its input: nothing about the graphs. A process can
    /// abort that early on its own, for instance in `fastant`'s clock
    /// calibration when the cores' TSCs disagree.
    DiedStarting(String),
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

/// Reads the child's output (after whatever a test harness prints first),
/// with the blob read into a buffer of its size: the backend holds the
/// current blob while it reads the next, and a copy would make three.
fn read_output(mut stdout: impl Read) -> anyhow::Result<(BuildOutput, Vec<u8>)> {
    let word = |bytes: &[u8], at: usize| -> Option<usize> {
        Some(u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?) as usize)
    };
    let mut head = vec![];
    let mut chunk = vec![0u8; 64 << 10];
    let (json, blob_len) = loop {
        if let Some(at) = head
            .windows(OUTPUT_MARKER.len())
            .position(|w| w == OUTPUT_MARKER)
        {
            let start = at + OUTPUT_MARKER.len();
            if let Some(json_len) = word(&head, start)
                && let Some(blob_len) = word(&head, start + 8 + json_len)
            {
                break (start + 8..start + 8 + json_len, blob_len);
            }
        }
        let read = stdout.read(&mut chunk)?;
        anyhow::ensure!(read > 0, "the builder output is cut");
        head.extend_from_slice(&chunk[..read]);
    };
    let built = serde_json::from_slice(&head[json.clone()])?;
    let mut blob = Vec::with_capacity(blob_len);
    blob.extend_from_slice(&head[json.end + 8..]);
    drop(head);
    let rest = blob_len
        .checked_sub(blob.len())
        .context("the builder wrote too much")?;
    stdout.take(rest as u64).read_to_end(&mut blob)?;
    anyhow::ensure!(blob.len() == blob_len, "the builder output is cut");
    Ok((built, blob))
}

/// The current executable, which `main` turns into the builder.
fn builder_command() -> anyhow::Result<Command> {
    Ok(Command::new(
        std::env::current_exe().context("current_exe")?,
    ))
}

fn run_child(input: &BuildInput, mut command: Command) -> anyhow::Result<ChildOutcome> {
    let mut child = command
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
        let output = read_output(&mut stdout);
        // Whatever is left, so the child never blocks writing it.
        let _ = std::io::copy(&mut stdout, &mut std::io::sink());
        output
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
    // The child has a budget of CPU time (`run_builder`); this is the backstop,
    // generous because at nice 10 it may get little of a busy host.
    let deadline = Instant::now() + *AMAGE_WARM_SNAPSHOT_TIMEOUT * 4;
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
    let output = output
        .join()
        .unwrap_or_else(|_| Err(anyhow::anyhow!("the output reader panicked")));
    let (lines, last_error) = progress.join().unwrap_or_default();
    if status.is_some_and(|s| s.success()) {
        let (built, blob) = output?;
        return Ok(ChildOutcome::Done(built, blob));
    }
    // The child's own last words go to the log only as their kind: what V8
    // prints when it aborts can quote heap contents.
    let why = match status {
        Some(status) if last_error.starts_with('#') => format!("{status}, V8 aborted"),
        Some(status) => status.to_string(),
        None => "timed out".to_string(),
    };
    if status.is_some() && !lines.iter().any(|line| line == "start") {
        return Ok(ChildOutcome::DiedStarting(why));
    }
    // The graph it was trying when it died, and what it decided before.
    let (mut trying, mut ok, mut no) = (None, HashMap::new(), vec![]);
    for line in &lines {
        let mut words = line.splitn(3, ' ');
        let (word, id, rest) = (
            words.next(),
            words.next().and_then(|id| id.parse::<u32>().ok()),
            words.next().unwrap_or_default(),
        );
        match (word, id) {
            (Some("try"), Some(id)) => trying = Some(id),
            (Some("ok"), Some(id)) => {
                let mut costs = rest.split(' ').map(|n| n.parse::<usize>().ok());
                if let (Some(Some(total)), Some(Some(shared))) = (costs.next(), costs.next()) {
                    ok.insert(id, Cost { total, shared });
                }
                trying = None;
            },
            (Some("no"), Some(id)) => {
                no.push((id, rest.to_string()));
                trying = None;
            },
            _ => (),
        }
    }
    // Out of wall-clock time, out of CPU time (SIGXCPU), or SIGKILL.
    let killed = status.is_none_or(|status| {
        use std::os::unix::process::ExitStatusExt;
        matches!(status.signal(), Some(9 | 24))
    });
    Ok(match trying {
        Some(id) => ChildOutcome::DiedOn {
            id,
            why,
            ok,
            no,
            killed,
        },
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
    // The builder yields to the backend it runs next to: it gives way for
    // CPU, and it is the first process the kernel kills when the container
    // runs out of memory. Before V8 starts its threads, which inherit the
    // priority.
    #[cfg(target_os = "linux")]
    {
        // SAFETY: lowers this thread's own priority, and limits this
        // process's CPU time (SIGXCPU past it).
        unsafe {
            libc::setpriority(libc::PRIO_PROCESS, 0, 10);
            let seconds = AMAGE_WARM_SNAPSHOT_TIMEOUT.as_secs() as libc::rlim_t;
            libc::setrlimit(
                libc::RLIMIT_CPU,
                &libc::rlimit {
                    rlim_cur: seconds,
                    rlim_max: seconds + 10,
                },
            );
        }
        let _ = std::fs::write("/proc/self/oom_score_adj", "1000");
    }
    // V8 shares one read-only heap among all isolates of a process, set up
    // from the first snapshot, so every blob the parent loads must lay out
    // its read-only part exactly as the parent's runtime snapshot does. A
    // snapshot creator normally keeps read-only space open and promotes
    // objects into it when it serializes; sealed, the blob keeps the base's
    // read-only part. Natives syntax lets the builder make the holders of
    // V8's additions fast again (`unguard_additions`); it changes nothing
    // for code that compiles without it, and V8 checks flags only against
    // code caches, not startup blobs. Must precede V8's initialization.
    v8::V8::set_flags_from_string("--no-extensible-ro-snapshot --allow-natives-syntax");
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
    writeln!(out, "{PROGRESS} start")?;
    out.flush()?;
    let request = request_built_ins(base)?;
    // A blob with no ready context, to see what each one adds.
    let empty = build_blob(base, &request, &input.groups, &[])?.bytes;
    let empty = Cost {
        total: empty.len(),
        shared: startup_part(&empty)?,
    };
    let mut accepted = vec![];
    let mut refused = vec![];
    let mut costs = HashMap::new();
    for entry in input.groups.iter().flat_map(|group| &group.entries) {
        if !entry.trial
            && let Some(cost) = entry.cost
        {
            costs.insert(entry.id, cost);
        }
    }
    for (g, group) in input.groups.iter().enumerate() {
        for entry in &group.entries {
            if !entry.trial {
                accepted.push((g, entry));
                continue;
            }
            writeln!(out, "{PROGRESS} try {}", entry.id)?;
            out.flush()?;
            // A test kills the child here, as the OOM killer would.
            #[cfg(test)]
            if std::env::var_os(tests::KILL_ON_TRIAL).is_some() {
                // SAFETY: signals this process.
                unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
            }
            let Blob {
                bytes: blob,
                contexts,
                refused: mut no,
                ..
            } = build_blob(base, &request, &input.groups, &[(g, entry)])?;
            let opened = if contexts.is_empty() {
                let reason = no.pop().map(|(_, r)| r).unwrap_or_default();
                Err(anyhow::anyhow!(reason))
            } else {
                open_contexts(&blob, &contexts, |_| entry.modules.len())
                    .context("its ready context did not open")
                    .and_then(|()| {
                        let cost = Cost {
                            total: blob.len().saturating_sub(empty.total),
                            shared: startup_part(&blob)?.saturating_sub(empty.shared),
                        };
                        fits_alone(cost)?;
                        Ok(cost)
                    })
            };
            match opened {
                Ok(cost) => {
                    costs.insert(entry.id, cost);
                    writeln!(
                        out,
                        "{PROGRESS} ok {} {} {}",
                        entry.id, cost.total, cost.shared
                    )?;
                    accepted.push((g, entry));
                },
                Err(reason) => {
                    let reason = format!("{reason:#}");
                    writeln!(
                        out,
                        "{PROGRESS} no {} {}",
                        entry.id,
                        reason.replace('\n', " ")
                    )?;
                    refused.push((entry.id, reason));
                },
            }
            out.flush()?;
        }
    }
    // Graphs share their modules' code in one blob, so the limits on all of
    // them are checked on the blob itself. Over one, the new graph that
    // added the most to it alone is left out, and the blob built again.
    let Blob {
        bytes: blob,
        contexts,
        refused: no,
        stand_ins,
    } = loop {
        let blob = build_blob(base, &request, &input.groups, &accepted)?;
        let size = Cost {
            total: blob.bytes.len().saturating_sub(empty.total),
            shared: startup_part(&blob.bytes)?.saturating_sub(empty.shared),
        };
        let Err(reason) = fits_together(size) else {
            break blob;
        };
        // New graphs go first, heaviest first, until what they added on
        // their own covers the excess; then one build checks.
        let over_shared = size.shared > *AMAGE_WARM_SNAPSHOT_MAX_SHARED_BYTES;
        let excess = if over_shared {
            size.shared - *AMAGE_WARM_SNAPSHOT_MAX_SHARED_BYTES
        } else {
            size.total.saturating_sub(*AMAGE_WARM_SNAPSHOT_MAX_BYTES)
        };
        let weight = |id: u32| {
            let cost = costs.get(&id).copied().unwrap_or_default();
            if over_shared {
                cost.shared
            } else {
                cost.total
            }
        };
        let mut order = (0..accepted.len()).collect::<Vec<_>>();
        order.sort_by_key(|&i| {
            let entry = accepted[i].1;
            (entry.ready, std::cmp::Reverse(weight(entry.id)))
        });
        let mut out = vec![];
        let mut freed = 0;
        for i in order {
            if freed >= excess && !out.is_empty() {
                break;
            }
            freed += weight(accepted[i].1.id);
            out.push(i);
        }
        if out.is_empty() {
            break blob;
        }
        out.sort_unstable_by(|a, b| b.cmp(a));
        for i in out {
            let (_, entry) = accepted.remove(i);
            refused.push((entry.id, format!("{reason:#}")));
        }
    };
    refused.extend(no);
    // The whole blob too, before the backend loads it.
    writeln!(out, "{PROGRESS} check")?;
    out.flush()?;
    let modules = accepted
        .iter()
        .map(|(_, entry)| (entry.id, entry.modules.len()))
        .collect::<HashMap<_, _>>();
    open_contexts(&blob, &contexts, |id| {
        modules.get(&id).copied().unwrap_or(0)
    })?;
    let costs = contexts
        .iter()
        .map(|(id, _)| (*id, costs.get(id).copied().unwrap_or_default()))
        .collect();
    let json = serde_json::to_vec(&BuildOutput {
        contexts,
        refused,
        costs,
        stand_ins,
    })?;
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(OUTPUT_MARKER)?;
    stdout.write_all(&(json.len() as u64).to_le_bytes())?;
    stdout.write_all(&json)?;
    stdout.write_all(&(blob.len() as u64).to_le_bytes())?;
    stdout.write_all(&blob)?;
    stdout.flush()?;
    Ok(())
}

/// The startup part of a V8 startup blob, which every isolate made from the
/// blob loads when it is created. `SnapshotImpl` in `snapshot.cc` writes the
/// number of contexts at byte 0 and the offset of the read-only part at
/// byte 80; the startup part starts after the context offsets (from byte 88
/// on), aligned to 8 bytes, and runs up to the read-only part.
fn startup_part(blob: &[u8]) -> anyhow::Result<usize> {
    let word = |at: usize| -> anyhow::Result<usize> {
        let bytes = blob.get(at..at + 4).context("blob header cut")?;
        Ok(u32::from_le_bytes(bytes.try_into()?) as usize)
    };
    let start = (88 + 4 * word(0)?).next_multiple_of(8);
    let read_only = word(80)?;
    anyhow::ensure!(
        start <= read_only && read_only <= blob.len(),
        "bad blob offsets"
    );
    Ok(read_only - start)
}

fn mib(bytes: usize) -> usize {
    bytes.div_ceil(1 << 20)
}

/// Whether a ready context that adds `cost` to a blob on its own could fit.
fn fits_alone(cost: Cost) -> anyhow::Result<()> {
    anyhow::ensure!(
        cost.total <= *AMAGE_WARM_SNAPSHOT_MAX_CONTEXT_BYTES,
        "its ready context takes {} MiB, over the limit of {} MiB",
        mib(cost.total),
        mib(*AMAGE_WARM_SNAPSHOT_MAX_CONTEXT_BYTES)
    );
    anyhow::ensure!(
        cost.shared <= *AMAGE_WARM_SNAPSHOT_MAX_SHARED_BYTES,
        "its code takes {} MiB of the part every isolate loads, over the limit of {} MiB",
        mib(cost.shared),
        mib(*AMAGE_WARM_SNAPSHOT_MAX_SHARED_BYTES)
    );
    Ok(())
}

/// Whether ready contexts that add `size` to a blob together fit.
fn fits_together(size: Cost) -> anyhow::Result<()> {
    anyhow::ensure!(
        size.total <= *AMAGE_WARM_SNAPSHOT_MAX_BYTES,
        "no room left: ready contexts would take {} of {} MiB",
        mib(size.total),
        mib(*AMAGE_WARM_SNAPSHOT_MAX_BYTES)
    );
    anyhow::ensure!(
        size.shared <= *AMAGE_WARM_SNAPSHOT_MAX_SHARED_BYTES,
        "no room left in the part every isolate loads: it would take {} of {} MiB",
        mib(size.shared),
        mib(*AMAGE_WARM_SNAPSHOT_MAX_SHARED_BYTES)
    );
    Ok(())
}

/// Opens every ready context of `blob` in a plain isolate, as a request
/// does. V8 sets a context up again when it deserializes one (see
/// [`Additions`]) and aborts the process if it cannot: that must happen to
/// this child, never to the backend.
fn open_contexts(
    blob: &[u8],
    contexts: &[(u32, usize)],
    modules: impl Fn(u32) -> usize,
) -> anyhow::Result<()> {
    let warm = WarmSnapshot {
        generation: 0,
        blob: blob.into(),
        entries: HashMap::new(),
    };
    // With the heap limit of the backend's isolates, which load the blob's
    // startup part when they are created.
    // SAFETY: `warm` outlives the isolate, dropped first.
    let mut isolate = unsafe {
        udf_runtime::create_isolate_with_udf_runtime(
            v8::CreateParams::default(),
            *ISOLATE_MAX_USER_HEAP_SIZE,
            Some(&warm),
        )
    };
    let result = (|| {
        // One at a time, as requests do.
        for (id, index) in contexts {
            // What the previous one held is garbage by now.
            isolate.low_memory_notification();
            scope!(let scope, &mut isolate);
            let context = v8::Context::from_snapshot(scope, *index, v8::ContextOptions::default())
                .with_context(|| format!("context {index}"))?;
            let scope = &mut v8::ContextScope::new(scope, context);
            for i in 0..modules(*id) {
                let module = scope
                    .get_context_data_from_snapshot_once::<v8::Module>(i)
                    .with_context(|| format!("module {i} of context {index}"))?;
                anyhow::ensure!(
                    module.get_status() == v8::ModuleStatus::Evaluated,
                    "module {i} of context {index} is not evaluated"
                );
            }
            // The backend makes an isolate again when less than this is left
            // after a request (`Isolate::check_isolate_clean`): with the blob's
            // startup part and this context, it would on every request.
            let available = scope.get_heap_statistics().total_available_size();
            anyhow::ensure!(
                available >= *ISOLATE_MAX_USER_HEAP_SIZE,
                "with its ready context open, an isolate has {} MiB of heap left, less than {} MiB",
                available >> 20,
                *ISOLATE_MAX_USER_HEAP_SIZE >> 20
            );
        }
        Ok(())
    })();
    drop(isolate);
    result
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
    /// Set while the builder checks its guards are still in place.
    static CHECKING: Cell<bool> = const { Cell::new(false) };
    static GUARD_HIT: Cell<bool> = const { Cell::new(false) };
    /// What a request's context has that the builder's lacks; the same for
    /// every creator on the runtime snapshot.
    static ADDITIONS: RefCell<Option<Arc<Additions>>> = const { RefCell::new(None) };
    /// The stand-ins of the context importing now, by name. Held here, not
    /// in the guards: V8 keeps a template's instances in the context, so
    /// template and accessor data must not point into one.
    static STAND_INS: RefCell<HashMap<String, StandIn>> = RefCell::new(HashMap::new());
    /// The paths of the stand-ins the import running now took.
    static TOOK: RefCell<BTreeSet<String>> = const { RefCell::new(BTreeSet::new()) };
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
    request: &[BuiltIn],
    groups: &[GroupInput],
    entries: &[(usize, &EntryInput)],
) -> anyhow::Result<Blob> {
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
    let mut stand_ins = HashMap::new();
    {
        scope!(let scope, &mut creator);
        let additions = match ADDITIONS.with(|a| a.borrow().clone()) {
            Some(additions) => additions,
            None => {
                // In a context of its own, left out of the blob.
                let scratch = v8::Context::new(scope, v8::ContextOptions::default());
                let scope = &mut v8::ContextScope::new(scope, scratch);
                let additions = Arc::new(find_additions(scope, request)?);
                ADDITIONS.with(|a| *a.borrow_mut() = Some(additions.clone()));
                additions
            },
        };
        let default_context = v8::Context::new(scope, v8::ContextOptions::default());
        scope.set_default_context(default_context);
        for (g, entry) in entries {
            let context = v8::Context::new(scope, v8::ContextOptions::default());
            let result = {
                let scope = &mut v8::ContextScope::new(scope, context);
                prepare_context(scope, context, &groups[*g], entry, &additions)
            };
            COMPILED.with(|c| c.borrow_mut().clear());
            NAMES.with(|n| n.borrow_mut().clear());
            STAND_INS.with(|s| s.borrow_mut().clear());
            match result {
                Ok(took) => {
                    contexts.push((entry.id, scope.add_context(context)));
                    if !took.is_empty() {
                        stand_ins.insert(entry.id, took);
                    }
                },
                Err(e) => refused.push((entry.id, format!("{e:#}"))),
            }
        }
    }
    let blob = creator
        .create_blob(v8::FunctionCodeHandling::Keep)
        .context("V8 could not create the snapshot")?;
    Ok(Blob {
        bytes: blob.to_vec(),
        contexts,
        refused,
        stand_ins,
    })
}

/// What [`build_blob`] made.
struct Blob {
    bytes: Vec<u8>,
    contexts: Vec<(u32, usize)>,
    refused: Vec<(u32, String)>,
    /// The paths of the stand-ins each accepted graph's import took.
    stand_ins: HashMap<u32, Vec<String>>,
}

/// Imports the graph like a request does, checks the import broke no rule,
/// and attaches the graph's modules to the context.
fn prepare_context(
    scope: &mut v8::PinScope<'_, '_>,
    context: v8::Local<'_, v8::Context>,
    group: &GroupInput,
    entry: &EntryInput,
    additions: &Additions,
) -> anyhow::Result<Vec<String>> {
    VIOLATIONS.with(|v| v.borrow_mut().clear());
    TOOK.with(|took| took.borrow_mut().clear());
    REACTED.set(false);
    IMPORT.set(Some(ImportState {
        env: entry.env.clone(),
        rng: entry.seed.map(ChaCha12Rng::from_seed),
        now_ms: entry.now_ms,
    }));
    let guarded = guard_additions(scope, additions)?;
    PROBING.set(true);
    let result = import(scope, group, entry);
    PROBING.set(false);
    IMPORT.set(None);
    // Reasons name ops, syscalls and built-ins only: an exception's text may
    // hold the value of a variable, and reasons end up in the backend's log.
    const REJECTION: &str = "an unhandled rejection";
    let violations = VIOLATIONS.with(|v| std::mem::take(&mut *v.borrow_mut()));
    let mut used = vec![];
    for violation in &violations {
        if violation != REJECTION && !used.contains(violation) {
            used.push(violation.clone());
        }
    }
    anyhow::ensure!(used.is_empty(), "import used {}", used.join(", "));
    // An import that throws also leaves its own promise rejected.
    result?;
    anyhow::ensure!(violations.is_empty(), "import left {REJECTION}");
    anyhow::ensure!(
        !REACTED.get(),
        "import left promise reactions for the request to run"
    );
    unguard_additions(scope, guarded)?;
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
    Ok(TOOK
        .with(|took| std::mem::take(&mut *took.borrow_mut()))
        .into_iter()
        .collect())
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

/// What V8 adds to every context it creates outside a snapshot creator,
/// after the snapshot's part: shipping language features (`Promise.try`,
/// `Uint8Array.fromBase64`, `Iterator.prototype[Symbol.dispose]`, ...),
/// `SharedArrayBuffer`, `WebAssembly` and `Error.stackTraceLimit`. A
/// creator's contexts lack them, and V8 adds them again to each context it
/// deserializes, aborting the process if one is already there or if its
/// holder no longer takes properties: an import that ran
/// `Object.freeze(Math)` would bring the backend down. So while a graph
/// imports, each one is in place (see [`Kind`]), and afterwards it is taken
/// out.
#[derive(Default)]
struct Additions {
    holders: Vec<Holder>,
}

/// An object V8 adds properties to, by its path from `globalThis`.
struct Holder {
    path: Vec<Step>,
    added: Vec<Added>,
}

/// One step of a path from `globalThis`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Step {
    Property(String),
    Prototype,
    /// A new `async function* () {}`, first step of the way to
    /// `%AsyncIteratorPrototype%`, which no global names.
    AsyncGenerator,
}

/// A key that names the same property in every context: a string, or a
/// well-known symbol by its name (`Symbol.dispose` is `Symbol("dispose")`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum Key {
    String(String),
    Symbol(String),
}

struct Added {
    key: Key,
    attributes: Attributes,
    kind: Kind,
}

/// How an addition stands in while the builder imports.
#[derive(Clone, Debug, PartialEq)]
enum Kind {
    /// With its value, so the import runs as in a request:
    /// `Error.stackTraceLimit` sets how much stack an error made at import
    /// captures.
    Primitive(Primitive),
    /// A function that is not a constructor. Libraries test at import
    /// whether one exists (`if (Uint8Array.prototype.toBase64)`) and call it
    /// later, so the import sees a stand-in with its name and length. An
    /// import that calls it is left out; a stand-in the module keeps calls
    /// V8's own function at request time.
    Method { name: String, length: i32 },
    /// Anything else: the graph whose import touches it is left out.
    Other,
}

#[derive(Clone, Debug, PartialEq)]
enum Primitive {
    Number(f64),
    String(String),
    Boolean(bool),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Attributes {
    read_only: bool,
    dont_enum: bool,
    dont_delete: bool,
}

impl Attributes {
    fn of(attributes: &v8::PropertyAttribute) -> Self {
        Self {
            read_only: attributes.is_read_only(),
            dont_enum: attributes.is_dont_enum(),
            dont_delete: attributes.is_dont_delete(),
        }
    }

    fn to_v8(self) -> v8::PropertyAttribute {
        let mut attributes = v8::PropertyAttribute::NONE;
        if self.read_only {
            attributes = attributes | v8::PropertyAttribute::READ_ONLY;
        }
        if self.dont_enum {
            attributes = attributes | v8::PropertyAttribute::DONT_ENUM;
        }
        if self.dont_delete {
            attributes = attributes | v8::PropertyAttribute::DONT_DELETE;
        }
        attributes
    }
}

/// How far from `globalThis` the builder looks for built-ins, and how many
/// it looks at.
const BUILT_IN_DEPTH: usize = 4;
const MAX_BUILT_INS: usize = 50_000;

/// A built-in object with its own keys.
struct BuiltIn {
    path: Vec<Step>,
    keys: Vec<(Key, Attributes, Kind)>,
}

fn path_name(path: &[Step], key: Option<&Key>) -> String {
    let parts = path
        .iter()
        .map(|step| match step {
            Step::Property(part) => part.clone(),
            Step::Prototype => "__proto__".to_string(),
            Step::AsyncGenerator => "(async function* () {})".to_string(),
        })
        .collect::<Vec<_>>();
    let holder = parts.join(".");
    match key {
        None if holder.is_empty() => "globalThis".to_string(),
        None => holder,
        Some(Key::String(key)) if holder.is_empty() => key.clone(),
        Some(Key::String(key)) => format!("{holder}.{key}"),
        Some(Key::Symbol(key)) if holder.is_empty() => format!("globalThis[Symbol.{key}]"),
        Some(Key::Symbol(key)) => format!("{holder}[Symbol.{key}]"),
    }
}

/// The built-ins of a context as a request gets it: an isolate on the same
/// runtime snapshot, outside a snapshot creator.
fn request_built_ins(base: &'static [u8]) -> anyhow::Result<Vec<BuiltIn>> {
    let mut isolate = v8::Isolate::new(
        v8::CreateParams::default()
            .snapshot_blob(Cow::Borrowed(base).into())
            .external_references(udf_runtime::external_references().into()),
    );
    scope!(let scope, &mut isolate);
    let context = v8::Context::new(scope, v8::ContextOptions::default());
    let scope = &mut v8::ContextScope::new(scope, context);
    built_ins(scope)
}

/// Every object reachable from `globalThis` (and from a new async generator
/// function) through own data properties and prototypes, nearest first,
/// each once.
fn built_ins<'s>(scope: &mut v8::PinScope<'s, '_>) -> anyhow::Result<Vec<BuiltIn>> {
    let global: v8::Local<'s, v8::Object> = scope.get_current_context().global(scope);
    let async_generator = new_async_generator(scope).context("async generator")?;
    let mut seen = HashMap::<std::num::NonZeroI32, Vec<v8::Local<'s, v8::Object>>>::new();
    let mut first_visit = |object: v8::Local<'s, v8::Object>| {
        let same = seen.entry(object.get_identity_hash()).or_default();
        if same.iter().any(|o| o.strict_equals(object.into())) {
            return false;
        }
        same.push(object);
        true
    };
    first_visit(global);
    first_visit(async_generator);
    let mut queue = std::collections::VecDeque::from([
        (vec![], global),
        (vec![Step::AsyncGenerator], async_generator),
    ]);
    let mut found = vec![];
    while let Some((path, object)) = queue.pop_front() {
        anyhow::ensure!(found.len() < MAX_BUILT_INS, "too many built-ins");
        let mut keys = vec![];
        // An object V8 will not list is left out of the walk.
        for (key, name) in own_named_keys(scope, object).unwrap_or_default() {
            let Some(attributes) = object.get_real_named_property_attributes(scope, name) else {
                continue;
            };
            let value = own_value(scope, object, name);
            if path.len() < BUILT_IN_DEPTH
                && let Key::String(property) = &key
                && let Some(value) = value
                && let Ok(child) = v8::Local::<v8::Object>::try_from(value)
                && first_visit(child)
            {
                let mut child_path = path.clone();
                child_path.push(Step::Property(property.clone()));
                queue.push_back((child_path, child));
            }
            keys.push((
                key,
                Attributes::of(&attributes),
                value.map_or(Kind::Other, |value| kind(scope, value)),
            ));
        }
        if path.len() < BUILT_IN_DEPTH
            && let Some(prototype) = object.get_prototype(scope)
            && let Ok(child) = v8::Local::<v8::Object>::try_from(prototype)
            && first_visit(child)
        {
            let mut child_path = path.clone();
            child_path.push(Step::Prototype);
            queue.push_back((child_path, child));
        }
        found.push(BuiltIn { path, keys });
    }
    Ok(found)
}

/// What a request's context has that the creator's lacks.
fn find_additions(
    scope: &mut v8::PinScope<'_, '_>,
    request: &[BuiltIn],
) -> anyhow::Result<Additions> {
    let global: v8::Local<v8::Object> = scope.get_current_context().global(scope);
    let mut holders = vec![];
    for built_in in request {
        // Under an addition itself.
        let Some(object) = follow(scope, global, &built_in.path) else {
            continue;
        };
        let Ok(own) = own_named_keys(scope, object) else {
            continue;
        };
        let own = own.into_iter().map(|(key, _)| key).collect::<HashSet<_>>();
        let added = built_in
            .keys
            .iter()
            .filter(|(key, ..)| !own.contains(key))
            // A symbol this context cannot name stays unguarded; the child
            // still opens every context it built before handing it over.
            .filter(|(key, ..)| key_name(scope, key).is_some())
            .map(|(key, attributes, kind)| Added {
                key: key.clone(),
                attributes: *attributes,
                // One the builder could not take out again is guarded instead.
                kind: match kind {
                    Kind::Primitive(_) if attributes.dont_delete => Kind::Other,
                    kind => kind.clone(),
                },
            })
            .collect::<Vec<_>>();
        if !added.is_empty() {
            holders.push(Holder {
                path: built_in.path.clone(),
                added,
            });
        }
    }
    Ok(Additions { holders })
}

/// Own keys that name the same property in every context: strings and
/// well-known symbols, whose description is `Symbol.<name>`.
fn own_named_keys<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
) -> anyhow::Result<Vec<(Key, v8::Local<'s, v8::Name>)>> {
    let mut keys = vec![];
    for name in own_keys(scope, object)? {
        if let Ok(string) = v8::Local::<v8::String>::try_from(name) {
            keys.push((
                Key::String(string.to_rust_string_lossy(scope)),
                string.into(),
            ));
        } else if let Ok(symbol) = v8::Local::<v8::Symbol>::try_from(name) {
            let description = symbol.description(scope).to_rust_string_lossy(scope);
            if let Some(name) = description.strip_prefix("Symbol.") {
                keys.push((Key::Symbol(name.to_string()), symbol.into()));
            }
        }
    }
    Ok(keys)
}

/// Every own key, symbols included, in order.
fn own_keys<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
) -> anyhow::Result<Vec<v8::Local<'s, v8::Value>>> {
    let names = object
        .get_own_property_names(
            scope,
            v8::GetPropertyNamesArgs {
                property_filter: v8::PropertyFilter::ALL_PROPERTIES,
                ..Default::default()
            },
        )
        .context("own property names")?;
    (0..names.length())
        .map(|i| names.get_index(scope, i).context("own key"))
        .collect()
}

/// `key` in the current context. A well-known symbol is a constant of the
/// `Symbol` constructor, read before any import code runs.
fn key_name<'s>(scope: &mut v8::PinScope<'s, '_>, key: &Key) -> Option<v8::Local<'s, v8::Name>> {
    match key {
        Key::String(key) => Some(v8::String::new(scope, key)?.into()),
        Key::Symbol(key) => {
            let global = scope.get_current_context().global(scope);
            let symbol = follow(scope, global, &[Step::Property("Symbol".to_string())])?;
            let key = v8::String::new(scope, key)?;
            let value = own_value(scope, symbol, key.into())?;
            Some(v8::Local::<v8::Symbol>::try_from(value).ok()?.into())
        },
    }
}

/// The value of an own data property. Never runs a getter written in
/// JavaScript; V8's native accessors report their value, as in
/// `Object.getOwnPropertyDescriptor`.
fn own_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    key: v8::Local<'s, v8::Name>,
) -> Option<v8::Local<'s, v8::Value>> {
    tc_scope!(let tc, scope);
    let descriptor = object.get_own_property_descriptor(tc, key);
    if tc.has_caught() {
        return None;
    }
    let descriptor = v8::Local::<v8::Object>::try_from(descriptor?).ok()?;
    let value = v8::String::new(tc, "value")?;
    if descriptor.has_own_property(tc, value.into())? {
        descriptor.get(tc, value.into())
    } else {
        None
    }
}

fn primitive(
    scope: &mut v8::PinScope<'_, '_>,
    value: v8::Local<'_, v8::Value>,
) -> Option<Primitive> {
    if value.is_number() {
        Some(Primitive::Number(value.number_value(scope)?))
    } else if value.is_string() {
        Some(Primitive::String(value.to_rust_string_lossy(scope)))
    } else if value.is_boolean() {
        Some(Primitive::Boolean(value.is_true()))
    } else {
        None
    }
}

fn kind<'s>(scope: &mut v8::PinScope<'s, '_>, value: v8::Local<'s, v8::Value>) -> Kind {
    if let Some(primitive) = primitive(scope, value) {
        return Kind::Primitive(primitive);
    }
    let Ok(function) = v8::Local::<v8::Function>::try_from(value) else {
        return Kind::Other;
    };
    let Some(prototype) = v8::String::new(scope, "prototype") else {
        return Kind::Other;
    };
    if function.has_own_property(scope, prototype.into()) != Some(false) {
        return Kind::Other;
    }
    let length = v8::String::new(scope, "length")
        .and_then(|key| own_value(scope, function.into(), key.into()))
        .and_then(|length| length.int32_value(scope));
    match length {
        Some(length) => Kind::Method {
            name: function.get_name(scope).to_rust_string_lossy(scope),
            length,
        },
        None => Kind::Other,
    }
}

fn primitive_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    primitive: &Primitive,
) -> Option<v8::Local<'s, v8::Value>> {
    Some(match primitive {
        Primitive::Number(n) => v8::Number::new(scope, *n).into(),
        Primitive::String(s) => v8::String::new(scope, s)?.into(),
        Primitive::Boolean(b) => v8::Boolean::new(scope, *b).into(),
    })
}

/// A new `async function* () {}`. The literal reaches V8's intrinsics
/// without looking anything up that an import could have changed.
fn new_async_generator<'s>(scope: &mut v8::PinScope<'s, '_>) -> Option<v8::Local<'s, v8::Object>> {
    let code = v8::String::new(scope, "(async function* () {})")?;
    let script = v8::Script::compile(scope, code, None)?;
    script.run(scope)?.try_into().ok()
}

/// Follows `path` from `object` through own data properties and prototypes.
fn follow<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    mut object: v8::Local<'s, v8::Object>,
    path: &[Step],
) -> Option<v8::Local<'s, v8::Object>> {
    for step in path {
        let next = match step {
            Step::Property(name) => {
                let key = v8::String::new(scope, name)?;
                own_value(scope, object, key.into())?
            },
            Step::Prototype => object.get_prototype(scope)?,
            Step::AsyncGenerator => new_async_generator(scope)?.into(),
        };
        object = next.try_into().ok()?;
    }
    Some(object)
}

/// What `Object.getOwnPropertyDescriptor` gives, as handles.
struct Descriptor<'s> {
    value: Option<v8::Local<'s, v8::Value>>,
    get: Option<v8::Local<'s, v8::Value>>,
    set: Option<v8::Local<'s, v8::Value>>,
    attributes: Option<Attributes>,
}

impl Descriptor<'_> {
    fn same(&self, other: &Descriptor<'_>) -> bool {
        let same = |a: Option<v8::Local<v8::Value>>, b: Option<v8::Local<v8::Value>>| match (a, b) {
            (None, None) => true,
            (Some(a), Some(b)) => a.same_value(b),
            _ => false,
        };
        same(self.value, other.value)
            && same(self.get, other.get)
            && same(self.set, other.set)
            && self.attributes == other.attributes
    }
}

fn descriptor<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    key: v8::Local<'s, v8::Name>,
) -> Option<Descriptor<'s>> {
    let attributes = object
        .get_real_named_property_attributes(scope, key)
        .map(|attributes| Attributes::of(&attributes));
    tc_scope!(let tc, scope);
    let descriptor = object.get_own_property_descriptor(tc, key);
    if tc.has_caught() {
        return None;
    }
    let descriptor = v8::Local::<v8::Object>::try_from(descriptor?).ok()?;
    let field = |name: &str| {
        let name = v8::String::new(tc, name)?;
        if descriptor.has_own_property(tc, name.into())? {
            descriptor.get(tc, name.into())
        } else {
            None
        }
    };
    Some(Descriptor {
        value: field("value"),
        get: field("get"),
        set: field("set"),
        attributes,
    })
}

/// One holder while a graph imports.
struct GuardedHolder<'s, 'a> {
    object: v8::Local<'s, v8::Object>,
    holder: &'a Holder,
    /// Its own keys before the guards went in.
    keys: Vec<v8::Local<'s, v8::Value>>,
    /// Its own properties before the guards went in. Adding the guards and
    /// taking them out again rebuilds a holder's layout, which loses what
    /// V8 knows about the import's own writes to it (whether
    /// `Map.prototype.set` is still the original, for one), so the import
    /// must leave them as they were. Not kept for `globalThis`.
    properties: Vec<(v8::Local<'s, v8::Name>, Descriptor<'s>)>,
    /// The key of each addition, in this context.
    names: Vec<v8::Local<'s, v8::Name>>,
}

/// The additions of one context, in place while its graph imports.
struct Guarded<'s, 'a> {
    holders: Vec<GuardedHolder<'s, 'a>>,
    is_extensible: v8::Local<'s, v8::Function>,
}

fn guard_additions<'s, 'a>(
    scope: &mut v8::PinScope<'s, '_>,
    additions: &'a Additions,
) -> anyhow::Result<Guarded<'s, 'a>> {
    let global: v8::Local<v8::Object> = scope.get_current_context().global(scope);
    // Taken before any import code runs.
    let is_extensible = follow(
        scope,
        global,
        &[
            Step::Property("Object".to_string()),
            Step::Property("isExtensible".to_string()),
        ],
    )
    .and_then(|f| v8::Local::<v8::Function>::try_from(v8::Local::<v8::Value>::from(f)).ok())
    .context("Object.isExtensible")?;
    let mut holders = vec![];
    for holder in &additions.holders {
        let object = follow(scope, global, &holder.path)
            .with_context(|| format!("no {}", path_name(&holder.path, None)))?;
        let keys = own_keys(scope, object)?;
        let mut properties = vec![];
        if !holder.path.is_empty() {
            for key in &keys {
                let Ok(name) = v8::Local::<v8::Name>::try_from(*key) else {
                    continue;
                };
                let descriptor = descriptor(scope, object, name).with_context(|| {
                    format!("no descriptor in {}", path_name(&holder.path, None))
                })?;
                properties.push((name, descriptor));
            }
        }
        let mut names = vec![];
        for added in &holder.added {
            let what = path_name(&holder.path, Some(&added.key));
            let key = key_name(scope, &added.key).with_context(|| format!("no key {what}"))?;
            names.push(key);
            let done = match &added.kind {
                Kind::Primitive(primitive) => {
                    let value = primitive_value(scope, primitive).context("value")?;
                    object.define_own_property(scope, key, value, added.attributes.to_v8())
                },
                kind => {
                    if let Kind::Method { name, length } = kind {
                        let path = StandInPath {
                            what: what.clone(),
                            steps: holder.path.clone(),
                            key: added.key.clone(),
                        };
                        let stand_in = StandIn {
                            function: None,
                            path: serde_json::to_string(&path)?,
                            name: name.clone(),
                            length: *length,
                        };
                        STAND_INS.with(|s| s.borrow_mut().insert(what.clone(), stand_in));
                    }
                    // Configurable, so it can be taken out again.
                    let attributes = Attributes {
                        read_only: false,
                        dont_enum: added.attributes.dont_enum,
                        dont_delete: false,
                    };
                    let data = v8::String::new(scope, &what).context("name")?;
                    object.set_accessor_with_configuration(
                        scope,
                        key,
                        v8::AccessorConfiguration::new(guard_get)
                            .setter(guard_set)
                            .property_attribute(attributes.to_v8())
                            .data(data.into()),
                    )
                },
            };
            anyhow::ensure!(done == Some(true), "could not add {what}");
        }
        holders.push(GuardedHolder {
            object,
            holder,
            keys,
            properties,
            names,
        });
    }
    Ok(Guarded {
        holders,
        is_extensible,
    })
}

/// Takes the additions out again, after checking the import left them and
/// their holders as they were, so V8 can add them to the deserialized
/// context.
fn unguard_additions<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    guarded: Guarded<'s, '_>,
) -> anyhow::Result<()> {
    let global: v8::Local<v8::Object> = scope.get_current_context().global(scope);
    for guarded_holder in guarded.holders.iter().rev() {
        let GuardedHolder {
            object,
            holder,
            names,
            ..
        } = guarded_holder;
        for (added, key) in holder.added.iter().zip(names).rev() {
            let name = path_name(&holder.path, Some(&added.key));
            let intact = match &added.kind {
                Kind::Primitive(primitive) => {
                    own_value(scope, *object, *key).and_then(|v| self::primitive(scope, v))
                        == Some(primitive.clone())
                        && object
                            .get_real_named_property_attributes(scope, *key)
                            .map(|a| Attributes::of(&a))
                            == Some(added.attributes)
                },
                Kind::Method { .. } | Kind::Other => {
                    // Only the guard's own getter sets this.
                    GUARD_HIT.set(false);
                    CHECKING.set(true);
                    let _ = own_value(scope, *object, *key);
                    CHECKING.set(false);
                    GUARD_HIT.get()
                },
            };
            anyhow::ensure!(intact, "import changed {name}");
            anyhow::ensure!(
                object.delete(scope, (*key).into()) == Some(true),
                "import froze {}",
                path_name(&holder.path, None)
            );
        }
    }
    let undefined = v8::undefined(scope);
    for GuardedHolder {
        object,
        holder,
        keys,
        properties,
        ..
    } in &guarded.holders
    {
        let name = path_name(&holder.path, None);
        let same =
            follow(scope, global, &holder.path).is_some_and(|o| o.strict_equals((*object).into()));
        anyhow::ensure!(same, "import replaced {name}");
        let extensible = guarded
            .is_extensible
            .call(scope, undefined.into(), &[(*object).into()])
            .is_some_and(|v| v.is_true());
        // A test lets one through, to see V8 fail on it in the child.
        #[cfg(test)]
        let extensible = extensible || std::env::var_os(tests::LET_SEALED_THROUGH).is_some();
        anyhow::ensure!(extensible, "import froze {name}");
        // V8 appends what it adds, so other keys must be as a request finds
        // them. `globalThis` may gain some: the order of its names is all
        // that differs.
        if !holder.path.is_empty() {
            let now = own_keys(scope, *object)?;
            let unchanged =
                now.len() == keys.len() && now.iter().zip(keys).all(|(a, b)| a.strict_equals(*b));
            anyhow::ensure!(unchanged, "import changed {name}");
            for (key, before) in properties {
                let now = descriptor(scope, *object, *key);
                if !now.is_some_and(|now| now.same(before)) {
                    let key = key.to_rust_string_lossy(scope);
                    anyhow::bail!("import changed {name}.{key}");
                }
            }
        }
    }
    // A guard is an API accessor, and V8 turns the object that gets one into
    // a dictionary of properties. Prototypes become fast again on their own
    // when used; `Math`, `Promise`, `Error` and the rest would stay slow in
    // every ready context (3% in a hot loop over them).
    let fast = fast_properties(scope).context("fast properties")?;
    for GuardedHolder { object, holder, .. } in &guarded.holders {
        if !holder.path.is_empty() {
            fast.call(scope, undefined.into(), &[(*object).into()])
                .context("fast properties")?;
        }
    }
    Ok(())
}

/// `%ToFastProperties`, callable thanks to the builder's natives syntax.
fn fast_properties<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> anyhow::Result<v8::Local<'s, v8::Function>> {
    let code =
        v8::String::new(scope, "(function (o) { %ToFastProperties(o); })").context("code")?;
    let script = v8::Script::compile(scope, code, None).context("compile")?;
    Ok(script.run(scope).context("run")?.try_into()?)
}

/// Getter of a guarded addition. While the builder imports, it gives a
/// method's stand-in, and reading anything else leaves the graph out; when
/// checking, it only answers that it is still there. Unreachable at request
/// time: the guards are taken out before serializing.
pub(crate) fn guard_get<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _key: v8::Local<'s, v8::Name>,
    args: v8::PropertyCallbackArguments<'s>,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if CHECKING.get() {
        GUARD_HIT.set(true);
        return;
    }
    let what = args.data().to_rust_string_lossy(scope);
    if let Some(stand_in) = STAND_INS.with(|s| s.borrow().get(&what).cloned()) {
        // Made the first time an import reads it.
        let function = match stand_in.function {
            Some(function) => v8::Local::new(scope, function),
            None => {
                let Ok(function) =
                    self::stand_in(scope, &stand_in.path, &stand_in.name, stand_in.length)
                else {
                    return;
                };
                let global = v8::Global::new(scope, function);
                STAND_INS.with(|s| {
                    if let Some(entry) = s.borrow_mut().get_mut(&what) {
                        entry.function = Some(global);
                    }
                });
                function
            },
        };
        if PROBING.get() {
            TOOK.with(|took| took.borrow_mut().insert(stand_in.path));
        }
        rv.set(function.into());
    } else if PROBING.get() {
        VIOLATIONS.with(|v| v.borrow_mut().push(format!("built-in {what}")));
    }
}

pub(crate) fn guard_set<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _key: v8::Local<'s, v8::Name>,
    _value: v8::Local<'s, v8::Value>,
    args: v8::PropertyCallbackArguments<'s>,
    _rv: v8::ReturnValue<()>,
) {
    if PROBING.get() {
        let what = args.data().to_rust_string_lossy(scope);
        VIOLATIONS.with(|v| v.borrow_mut().push(format!("built-in {what}")));
    }
}

/// A method's stand-in in the context importing now.
#[derive(Clone)]
struct StandIn {
    function: Option<v8::Global<v8::Function>>,
    /// Its [`StandInPath`], as JSON.
    path: String,
    name: String,
    length: i32,
}

/// Where a stand-in finds V8's own function.
#[derive(Serialize, Deserialize)]
struct StandInPath {
    what: String,
    steps: Vec<Step>,
    key: Key,
}

impl StandInPath {
    fn resolve<'s>(&self, scope: &mut v8::PinScope<'s, '_>) -> Option<v8::Local<'s, v8::Function>> {
        let global = scope.get_current_context().global(scope);
        let holder = follow(scope, global, &self.steps)?;
        let key = key_name(scope, &self.key)?;
        own_value(scope, holder, key)?.try_into().ok()
    }
}

/// A method's stand-in: a function with its name and length that is not a
/// constructor, as V8's own, holding its [`StandInPath`] as JSON.
fn stand_in<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    path: &str,
    name: &str,
    length: i32,
) -> anyhow::Result<v8::Local<'s, v8::Function>> {
    let path = v8::String::new(scope, path).context("path")?;
    let function = v8::FunctionTemplate::builder(call_stand_in)
        .data(path.into())
        .length(length)
        .constructor_behavior(v8::ConstructorBehavior::Throw)
        .build(scope)
        .get_function(scope)
        .context("stand-in")?;
    function.set_name(v8::String::new(scope, name).context("name")?);
    Ok(function)
}

/// Where a ready context keeps V8's own functions for its stand-ins, by name.
fn bound_stand_ins<'s>(scope: &mut v8::PinScope<'s, '_>) -> Option<v8::Local<'s, v8::Private>> {
    let name = v8::String::new(scope, "amage.warmSnapshot.standIns")?;
    Some(v8::Private::for_api(scope, Some(name)))
}

/// Before a request runs in a ready context whose import took stand-ins:
/// finds V8's own functions where V8 just put them, and keeps them where
/// the stand-ins look first, so a request that changes those properties
/// does not change what a stand-in calls.
pub fn bind_stand_ins(scope: &mut v8::PinScope<'_, '_>, paths: &[String]) {
    if paths.is_empty() {
        return;
    }
    let null = v8::null(scope);
    let store = v8::Object::with_prototype_and_properties(scope, null.into(), &[], &[]);
    for path in paths {
        let Ok(path) = serde_json::from_str::<StandInPath>(path) else {
            continue;
        };
        if let Some(function) = path.resolve(scope)
            && let Some(what) = v8::String::new(scope, &path.what)
        {
            store.create_data_property(scope, what.into(), function.into());
        }
    }
    let global = scope.get_current_context().global(scope);
    if let Some(key) = bound_stand_ins(scope) {
        global.set_private(scope, key, store.into());
    }
}

/// A stand-in called. During a ready-context import the graph is left out;
/// at request time, from a module that kept it, the call goes to V8's own
/// function, the one bound when the context was made.
pub(crate) fn call_stand_in<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: v8::FunctionCallbackArguments<'s>,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.data().to_rust_string_lossy(scope);
    let Ok(path) = serde_json::from_str::<StandInPath>(&path) else {
        return;
    };
    if PROBING.get() {
        violation(scope, format!("built-in {} called", path.what));
        return;
    }
    let bound = (|| {
        let global = scope.get_current_context().global(scope);
        let key = bound_stand_ins(scope)?;
        let store = v8::Local::<v8::Object>::try_from(global.get_private(scope, key)?).ok()?;
        let what = v8::String::new(scope, &path.what)?;
        v8::Local::<v8::Function>::try_from(store.get(scope, what.into())?).ok()
    })();
    let Some(function) = bound.or_else(|| path.resolve(scope)) else {
        let message = v8::String::new(scope, &format!("{} is not available", path.what)).unwrap();
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
        return;
    };
    let arguments = (0..args.length()).map(|i| args.get(i)).collect::<Vec<_>>();
    if let Some(value) = function.call(scope, args.this().into(), &arguments) {
        rv.set(value);
    }
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
            cost: None,
            ready: false,
        }
    }

    /// Builds a blob in the child, which is this test binary started again.
    fn child_build_entries(
        modules: &[(&str, &str)],
        entries: Vec<EntryInput>,
    ) -> (Vec<u8>, BuildOutput) {
        child_build_with_env(modules, entries, &[])
    }

    fn child_build_with_env(
        modules: &[(&str, &str)],
        entries: Vec<EntryInput>,
        env: &[(&str, &str)],
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
        // A process can abort before `main` (see `ChildOutcome::DiedStarting`);
        // the backend runs the child again, and so does this.
        for _ in 0..4 {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "warm_snapshot::tests::builder_child",
                    "--test-threads=1",
                ])
                .env(BUILDER_ENV, "1")
                .envs(env.iter().copied())
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let mut stdin = child.stdin.take().unwrap();
            let bytes = bytes.clone();
            let writer = std::thread::spawn(move || stdin.write_all(&bytes));
            let mut output = vec![];
            child
                .stdout
                .take()
                .unwrap()
                .read_to_end(&mut output)
                .unwrap();
            let written = writer.join().unwrap();
            let status = child.wait().unwrap();
            if !status.success() && output.is_empty() {
                continue;
            }
            written.unwrap();
            assert!(status.success(), "builder child failed: {status}");
            let (built, blob) = super::read_output(&output[..]).unwrap();
            return (blob, built);
        }
        panic!("the builder child kept dying");
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
        // What V8 adds to a context it creates is there, in V8's order.
        const SHAPE: &str =
            "JSON.stringify([Object.getOwnPropertyNames(globalThis).sort(), \
             Object.getOwnPropertyNames(Convex).sort(), Object.getOwnPropertyNames(Error), \
             Object.getOwnPropertyNames(Uint8Array), \
             Object.getOwnPropertyNames(Uint8Array.prototype), \
             Object.getOwnPropertyNames(Promise), Object.getOwnPropertyNames(Math), \
             Error.stackTraceLimit, typeof WebAssembly, typeof SharedArrayBuffer])";
        let (blob, index) = build_one(
            &[(
                "plain.js",
                "export const x = 1;\nglobalThis.addedAtImport = new Uint8Array(2);",
            )],
            &["plain.js"],
        )
        .unwrap();
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
            assert_eq!(eval(scope, "addedAtImport.length"), "2");
            assert_eq!(eval(scope, "delete globalThis.addedAtImport; 1"), "1");
            assert_eq!(eval(scope, SHAPE), fresh);
        });
    }

    /// The builder's contexts lack what V8 adds to the contexts it creates
    /// for requests, and V8 adds it again when it deserializes one. An
    /// import that would see the difference, or keep V8 from adding it, is
    /// left out; one that froze `Math` used to abort the backend.
    #[test]
    fn imports_that_touch_what_v8_adds_are_refused() {
        let sources = [
            (
                "export const t = typeof WebAssembly;",
                "built-in WebAssembly",
            ),
            (
                "export const t = typeof SharedArrayBuffer;",
                "built-in SharedArrayBuffer",
            ),
            (
                "export const f = new Float16Array(2);",
                "built-in Float16Array",
            ),
            ("Promise.try(() => 1);", "built-in Promise.try called"),
            (
                "Uint8Array.fromBase64('aGk=');",
                "built-in Uint8Array.fromBase64 called",
            ),
            (
                "new Uint8Array(2).toBase64();",
                "built-in Uint8Array.prototype.toBase64 called",
            ),
            ("Promise.try = () => 1;", "built-in Promise.try"),
            (
                "Error.stackTraceLimit = 50;",
                "import changed Error.stackTraceLimit",
            ),
            ("Object.freeze(Math);", "import froze Math"),
            ("Object.preventExtensions(Error);", "import froze Error"),
            (
                "Object.preventExtensions(globalThis);",
                "import froze globalThis",
            ),
            (
                "delete globalThis.Uint8Array;",
                "import replaced Uint8Array",
            ),
            ("Uint8Array.extra = 1;", "import changed Uint8Array"),
            // V8 writes through the guard's setter.
            (
                "Object.defineProperty(Promise, 'try', { value: 1, configurable: true });",
                "built-in Promise.try",
            ),
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
        }
    }

    /// Libraries pick an implementation at import by testing whether a
    /// method exists (`oauth4webapi` does for `Uint8Array.prototype.toBase64`)
    /// and call it later: the import sees it, and calls from a ready context
    /// reach V8's own function. A stand-in the module keeps is not V8's
    /// function object, the one difference left.
    #[test]
    fn feature_detection_at_import_sees_what_a_request_sees() {
        let (blob, index) = build_one(
            &[(
                "detect.js",
                "const enc = Uint8Array.prototype.toBase64 ? (a) => a.toBase64() : () => \
                 'fallback';\nconst dec = Uint8Array.fromBase64 ?? null;\nconst bound = \
                 Uint8Array.fromBase64?.bind(Uint8Array);\nconst toB64 = \
                 Uint8Array.prototype.toBase64;\nconst tryKind = typeof Promise.try;\nexport \
                 function check() {\nconst a = new Uint8Array([104, 105]);\nlet error;\ntry { \
                 dec('!'); } catch (e) { error = e.constructor.name; }\nreturn [enc(a), \
                 dec('aGk=').join(','), bound('aGk=').length, toB64.call(a), tryKind, 'prototype' \
                 in dec, dec.name, dec.length, error, dec === Uint8Array.fromBase64].join('|');\n}",
            )],
            &["detect.js"],
        )
        .unwrap();
        with_ready_contexts(blob, index, 1, 2, |scope, graph| {
            assert_eq!(
                call_export(scope, graph[0], "check", &[]),
                "aGk=|104,105|2|aGk=|function|false|fromBase64|1|SyntaxError|false"
            );
        });
    }

    /// Data built at import goes into every ready context whose graph
    /// imports it, so one context has a size limit, and all of them another.
    #[test]
    fn ready_contexts_stay_within_their_memory_limits() {
        // About 8 MiB of doubles.
        let modules = [
            (
                "big.js",
                "export const table = Array.from({ length: 1000000 }, (_, i) => i + 0.5);",
            ),
            (
                "a.js",
                "import { table } from './big.js'; export const a = table[1];",
            ),
            (
                "b.js",
                "import { table } from './big.js'; export const b = table[2];",
            ),
            (
                "c.js",
                "import { table } from './big.js'; export const c = table[3];",
            ),
            ("small.js", "export const s = 1;"),
        ];
        let entries = || {
            vec![
                entry(0, &["a.js", "big.js"]),
                entry(1, &["b.js", "big.js"]),
                entry(2, &["c.js", "big.js"]),
                entry(3, &["small.js"]),
            ]
        };
        let refused = |output: &BuildOutput| {
            output
                .refused
                .iter()
                .map(|(id, reason)| (*id, reason.clone()))
                .collect::<HashMap<_, _>>()
        };
        let (_, output) = child_build_with_env(
            &modules,
            entries(),
            &[("AMAGE_WARM_SNAPSHOT_MAX_CONTEXT_MIB", "4")],
        );
        let reasons = refused(&output);
        assert_eq!(output.contexts.len(), 1, "{output:?}");
        for id in 0..3 {
            assert!(
                reasons[&id].contains("over the limit of 4 MiB"),
                "{reasons:?}"
            );
        }
        assert!(output.costs[&3].total < 1 << 20, "{output:?}");
        let (_, output) = child_build_with_env(
            &modules,
            entries(),
            &[("AMAGE_WARM_SNAPSHOT_MAX_MIB", "20")],
        );
        // Over the limit together, one of the three is left out.
        let reasons = refused(&output);
        let ids = output
            .contexts
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 3, "{output:?}");
        assert!(ids.contains(&3), "{output:?}");
        assert_eq!(reasons.len(), 1, "{reasons:?}");
        assert!(
            reasons.values().all(|r| r.contains("no room left")),
            "{reasons:?}"
        );
        assert!(output.costs[&ids[0]].total > 7 << 20, "{output:?}");
    }

    /// Strings, scripts and compiled code go into the blob's startup part,
    /// which every isolate loads when it is created, whatever context it
    /// runs: that part has a limit of its own, and the child checks an
    /// isolate made from the blob still has the heap a request needs.
    #[test]
    fn what_every_isolate_loads_has_its_own_limit() {
        let modules = [(
            "words.js",
            "export const words = Array.from({ length: 200000 }, (_, i) => 'word-' + i);",
        )];
        let (_, output) = child_build_with_env(&modules, vec![entry(0, &["words.js"])], &[]);
        let cost = output.costs[&0];
        assert!(cost.shared > 4 << 20, "{output:?}");
        let (_, output) = child_build_with_env(
            &modules,
            vec![entry(0, &["words.js"])],
            &[("AMAGE_WARM_SNAPSHOT_MAX_SHARED_MIB", "2")],
        );
        assert!(
            output.refused[0]
                .1
                .contains("of the part every isolate loads"),
            "{output:?}"
        );
        // An isolate of 32 + 1 MiB would start with too little heap left.
        let (_, output) = child_build_with_env(
            &modules,
            vec![entry(0, &["words.js"])],
            &[
                ("AMAGE_WARM_SNAPSHOT_MAX_SHARED_MIB", "64"),
                ("ISOLATE_MAX_USER_HEAP_SIZE", "33554432"),
                ("ISOLATE_MAX_HEAP_EXTRA_SIZE", "1048576"),
            ],
        );
        assert!(output.refused[0].1.contains("of heap left"), "{output:?}");
    }

    /// Putting guards on a holder and taking them out again rebuilds its
    /// layout, which loses what V8 knows about the import's own writes to it
    /// (`new Map(entries)` would skip an overridden `Map.prototype.set`), so
    /// an import that changes a holder is left out. Well-known symbols and
    /// `%AsyncIteratorPrototype%`, which no global names, are guarded too.
    #[test]
    fn holders_stay_as_v8_made_them() {
        let sources = [
            (
                "const o = Map.prototype.set; Map.prototype.set = function (k, v) { return \
                 o.call(this, k, v); };",
                "import changed Map.prototype.set",
            ),
            (
                "const o = WeakMap.prototype.set; WeakMap.prototype.set = function (k, v) { \
                 return o.call(this, k, v); };",
                "import changed WeakMap.prototype.set",
            ),
            (
                "Iterator.prototype[Symbol.dispose] = function () {};",
                "built-in Iterator.prototype[Symbol.dispose]",
            ),
            (
                "Object.freeze(Iterator.prototype);",
                "import froze Iterator.prototype",
            ),
            (
                "Object.freeze(Object.getPrototypeOf(Object.getPrototypeOf((async function* () \
                 {}).prototype)));",
                "import froze (async function* () {})",
            ),
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
        }
        // Reading one is fine, and it is there at request time.
        let (blob, index) = build_one(
            &[(
                "dispose.js",
                "const d = Iterator.prototype[Symbol.dispose];\nexport function check() { return \
                 [typeof d, typeof Iterator.prototype[Symbol.dispose]].join(); }",
            )],
            &["dispose.js"],
        )
        .unwrap();
        with_ready_contexts(blob, index, 1, 1, |scope, graph| {
            assert_eq!(
                call_export(scope, graph[0], "check", &[]),
                "function,function"
            );
        });
    }

    /// A stand-in the import kept calls the function V8 put in place when
    /// the context was made, even after the request replaces that property
    /// with code that calls the stand-in.
    #[test]
    fn kept_stand_ins_are_bound_before_the_request_runs() {
        let (blob, output) = child_build_entries(
            &[(
                "kept.js",
                "const orig = Uint8Array.fromBase64;\nexport function check() \
                 {\nUint8Array.fromBase64 = (s) => orig.call(Uint8Array, s.trim());\nreturn \
                 Uint8Array.fromBase64(' aGk= ').join(',');\n}",
            )],
            vec![entry(0, &["kept.js"])],
        );
        let index = output.contexts[0].1;
        let stand_ins = output.stand_ins[&0].clone();
        assert_eq!(stand_ins.len(), 1, "{output:?}");
        with_ready_contexts(blob, index, 1, 2, |scope, graph| {
            super::bind_stand_ins(scope, &stand_ins);
            assert_eq!(call_export(scope, graph[0], "check", &[]), "104,105");
        });
    }

    /// Known limit, ignored until V8 changes: a number field of an object
    /// made at import, stored as a double, is a box that V8 puts in the
    /// blob's startup part, which every context of an isolate shares, and V8
    /// writes the box in place. A request that changes such a field changes
    /// it for the next request on the same isolate. The fix is in V8: keep
    /// HeapNumbers out of `ContextSerializer::ShouldBeInTheStartupObjectCache`.
    #[test]
    #[ignore]
    fn double_fields_made_at_import_start_fresh_in_every_request() {
        let (blob, index) = build_one(
            &[(
                "stats.js",
                "const stats = { at: 1790000000123.5 };\nclass P { #p = 0.75; bump() { return \
                 (this.#p += 1); } }\nconst p = new P();\nexport function bump() { stats.at += 1; \
                 return [stats.at, p.bump()].join(','); }",
            )],
            &["stats.js"],
        )
        .unwrap();
        with_ready_contexts(blob, index, 1, 3, |scope, graph| {
            assert_eq!(
                call_export(scope, graph[0], "bump", &[]),
                "1790000000124.5,1.75"
            );
        });
    }

    pub(super) const KILL_ON_TRIAL: &str = "AMAGE_WARM_SNAPSHOT_TEST_KILL_ON_TRIAL";

    /// A child killed from outside (out of memory, out of time) need not be
    /// the fault of the graph it was trying: the parent sees it as killed,
    /// and leaves the graph out only after a second time.
    #[test]
    fn a_child_killed_from_outside_is_told_apart() {
        crate::client::initialize_v8();
        let input = BuildInput {
            groups: vec![GroupInput {
                modules: [(
                    "convex:/plain.js".to_string(),
                    "export const x = 1;".to_string(),
                )]
                .into_iter()
                .collect(),
                entries: vec![entry(3, &["plain.js"])],
            }],
        };
        for _ in 0..4 {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "warm_snapshot::tests::builder_child",
                    "--test-threads=1",
                ])
                .env(KILL_ON_TRIAL, "1");
            match super::run_child(&input, command).unwrap() {
                super::ChildOutcome::DiedOn { id, killed, .. } => {
                    assert_eq!(id, 3);
                    assert!(killed);
                    return;
                },
                super::ChildOutcome::DiedStarting(_) => continue,
                super::ChildOutcome::Done(output, _) => panic!("built: {output:?}"),
                super::ChildOutcome::Failed(why) => panic!("failed: {why}"),
            }
        }
        panic!("the builder child kept dying starting");
    }

    pub(super) const LET_SEALED_THROUGH: &str = "AMAGE_WARM_SNAPSHOT_TEST_LET_SEALED_THROUGH";

    /// A ready context V8 cannot set up again aborts the child that opens it
    /// to check, and is left out; the backend never loads it.
    #[test]
    fn a_context_v8_cannot_open_kills_the_child_not_the_backend() {
        crate::client::initialize_v8();
        let mut sealed = entry(7, &["sealed.js"]);
        sealed.trial = true;
        let input = BuildInput {
            groups: vec![GroupInput {
                modules: [(
                    "convex:/sealed.js".to_string(),
                    "Object.preventExtensions(Math);\nexport const x = 1;".to_string(),
                )]
                .into_iter()
                .collect(),
                entries: vec![sealed],
            }],
        };
        for _ in 0..4 {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "warm_snapshot::tests::builder_child",
                    "--test-threads=1",
                ])
                .env(LET_SEALED_THROUGH, "1");
            match super::run_child(&input, command).unwrap() {
                super::ChildOutcome::DiedOn { id, killed, .. } => {
                    assert_eq!(id, 7);
                    // V8 aborting is the graph's fault.
                    assert!(!killed);
                    return;
                },
                super::ChildOutcome::DiedStarting(_) => continue,
                super::ChildOutcome::Done(output, _) => panic!("built: {output:?}"),
                super::ChildOutcome::Failed(why) => panic!("failed: {why}"),
            }
        }
        panic!("the builder child kept dying starting");
    }

    /// An error made at import captures its stack as in a request, and V8
    /// formats it when the request reads it.
    #[test]
    fn errors_made_at_import_keep_their_stack() {
        let (blob, index) = build_one(
            &[(
                "stack.js",
                "const e = new Error('made at import');\nlet c;\ntry { JSON.parse('{'); } catch \
                 (x) { c = x; }\nexport function stacks() { return [e.stack, c.stack, \
                 Error.stackTraceLimit].join('|'); }",
            )],
            &["stack.js"],
        )
        .unwrap();
        with_ready_contexts(blob, index, 1, 2, |scope, graph| {
            // The runtime's formatter needs a request's ops; V8's own one is
            // enough to see the frames.
            eval(scope, "delete Error.prepareStackTrace; 1");
            let stacks = call_export(scope, graph[0], "stacks", &[]);
            assert!(
                stacks.starts_with("Error: made at import\n    at convex:/stack.js:1:11|"),
                "{stacks}"
            );
            assert!(stacks.contains("SyntaxError"), "{stacks}");
            assert!(stacks.contains("convex:/stack.js:3:"), "{stacks}");
            assert!(stacks.ends_with("|10"), "{stacks}");
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
