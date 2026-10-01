//! Convex persistence on Turso, the Rust rewrite of SQLite.
//!
//! The database file, schema and SQL are the ones of the `sqlite` crate, so a
//! deployment can move between the two engines on the same `db.sqlite3`.
//! What changes is the execution model: reads use a pool of connections that
//! each see a WAL snapshot while the single writer commits, commits go ahead
//! of retention deletes for that writer, and the file is always in WAL mode.
#![feature(coroutines)]

mod engine;

use std::{
    cmp,
    collections::{
        BTreeMap,
        BTreeSet,
        HashSet,
    },
    future::Future,
    ops::{
        Deref,
        DerefMut,
    },
    path::{
        Path,
        PathBuf,
    },
    sync::{
        atomic::{
            AtomicBool,
            AtomicUsize,
            Ordering,
        },
        Arc,
        LazyLock,
        Weak,
    },
    time::{
        Duration,
        Instant,
    },
};

use anyhow::Context as _;
use async_trait::async_trait;
use cmd_util::env::env_config;
use common::{
    document::{
        InternalId,
        ResolvedDocument,
    },
    index::{
        IndexEntry,
        IndexKeyBytes,
    },
    interval::{
        End,
        Interval,
        StartIncluded,
    },
    persistence::{
        row_index_retention::{
            delete_expired_entries,
            IndexRowPersistence,
        },
        ConflictStrategy,
        DocumentLogEntry,
        DocumentPrevTsQuery,
        DocumentStream,
        IndexRetentionProgress,
        IndexRetentionRequest,
        IndexStream,
        LatestDocument,
        Persistence,
        PersistenceGlobalKey,
        PersistenceIndexEntry,
        PersistenceReader,
        RetentionValidator,
        TimestampRange,
    },
    query::Order,
    runtime::CoopStreamExt as _,
    types::{
        IndexId,
        PersistenceVersion,
        Timestamp,
    },
    value::{
        InternalDocumentId,
        TabletId,
    },
};
use engine::{
    blob,
    blob_param,
    int_param,
    integer,
    optional,
    text,
    text_param,
    Conn,
    Row,
    Value,
};
use futures::{
    future::BoxFuture,
    FutureExt as _,
    StreamExt,
};
use futures_async_stream::try_stream;
use metrics::{
    log_counter_with_labels,
    log_distribution,
    log_gauge,
    register_convex_counter,
    register_convex_gauge,
    register_convex_histogram,
    StaticMetricLabel,
    Timer,
};
use serde::Deserialize as _;
use serde_json::Value as JsonValue;
use tokio::{
    runtime::{
        Handle,
        RuntimeFlavor,
    },
    sync::{
        Mutex,
        Notify,
        Semaphore,
        SemaphorePermit,
    },
    task::block_in_place,
    time::MissedTickBehavior,
};
use turso_core::Database;

/// Connections that serve reads concurrently with the writer. One CPU is
/// left to the writer and the isolates: readers beyond the free CPUs only
/// compete with them (on 2 CPUs, 4 readers halved commit throughput). With
/// two or more, scans use all but one, which stays free for point lookups.
/// `available_parallelism` honors CPU affinity and cgroup quotas.
static TURSO_READERS: LazyLock<usize> = LazyLock::new(|| {
    let cpus = std::thread::available_parallelism().map_or(1, usize::from);
    env_config("CONVEX_TURSO_READERS", cpus.saturating_sub(1).clamp(1, 4)).max(1)
});

/// How a [`TursoPersistence`] is opened; [`TursoOptions::from_knobs`] reads
/// the `CONVEX_TURSO_*` environment.
#[derive(Clone, Debug)]
pub struct TursoOptions {
    pub readers: usize,
    /// Page cache per connection, in KiB.
    pub cache_kib: i64,
    pub wal: WalPolicy,
}

impl TursoOptions {
    pub fn from_knobs() -> Self {
        Self {
            readers: *TURSO_READERS,
            cache_kib: *TURSO_CACHE_KIB,
            wal: WalPolicy::default(),
        }
    }
}

/// How the WAL is kept small. Turso restarts the WAL only at an instant with
/// no reader in it and never waits for one, so under steady reads the file
/// grows without bound and a TRUNCATE checkpoint reports busy every time. The
/// reader pool is the only reader of the file, so that instant can be made:
/// hold the writer and every reader permit, then truncate.
#[derive(Clone, Debug)]
pub struct WalPolicy {
    /// How often the size of the WAL is checked.
    pub period: Duration,
    /// Past this size the WAL is truncated if the writer and every reader
    /// happen to be free.
    pub soft_bytes: u64,
    /// Past this size the writer is taken and the reader pool is closed to
    /// truncate the WAL, giving up after `gate_deadline` behind a long read.
    pub hard_bytes: u64,
    pub gate_deadline: Duration,
}

impl Default for WalPolicy {
    fn default() -> Self {
        Self {
            period: Duration::from_millis(250),
            soft_bytes: 16 << 20,
            hard_bytes: 64 << 20,
            gate_deadline: Duration::from_millis(250),
        }
    }
}

/// Page cache per connection, in KiB (SQLite's `cache_size` default is 2000).
static TURSO_CACHE_KIB: LazyLock<i64> =
    LazyLock::new(|| env_config("CONVEX_TURSO_CACHE_KIB", 16384i64).max(64));

/// The longest a background write stands aside for commits before it queues
/// for the writer anyway, so retention keeps up under a steady commit stream.
const BACKGROUND_MAX_YIELD: Duration = Duration::from_millis(100);

register_convex_histogram!(
    TURSO_WRITER_WAIT_SECONDS,
    "Time waiting for the Turso writer connection",
    &["op"]
);
register_convex_histogram!(
    TURSO_WRITER_HOLD_SECONDS,
    "Time holding the Turso writer connection",
    &["op"]
);
register_convex_histogram!(
    TURSO_READER_HOLD_SECONDS,
    "Time holding a Turso reader connection",
    &["op"]
);
register_convex_histogram!(
    TURSO_READER_WAIT_SECONDS,
    "Time waiting for a Turso reader connection",
    &["op"]
);
register_convex_gauge!(TURSO_WAL_BYTES, "Size of the Turso WAL file");
register_convex_counter!(
    TURSO_WAL_TRUNCATIONS_TOTAL,
    "Turso WAL truncation attempts by outcome",
    &["outcome"]
);
register_convex_histogram!(
    TURSO_WAL_GATE_SECONDS,
    "Time to take the writer and close the reader pool for a WAL truncation"
);

#[derive(Clone)]
pub struct TursoPersistence {
    inner: Arc<Inner>,
    newly_created: bool,
}

/// Who is asking for the writer. Convex splits each retention chunk into
/// parallel deletes; on a plain FIFO all of them queued in front of the next
/// commit, which then waited for every one of their fsyncs.
#[derive(Clone, Copy)]
enum Lane {
    /// Commits and globals, served in arrival order.
    Foreground,
    /// Retention deletes: one at a time, and only while no commit is pending.
    Background,
}

struct Inner {
    writer: Mutex<Conn>,
    /// Foreground writes waiting for or holding the writer.
    foreground: AtomicUsize,
    /// Signalled when `foreground` drops to zero.
    foreground_idle: Notify,
    /// Background writes take turns here, so at most one of them is ever
    /// queued for the writer ahead of a commit.
    background_turn: Mutex<()>,
    readers: ReaderPool,
    /// Syscall I/O reads pages and fsyncs inside `poll`: a slow disk would
    /// hold the Tokio worker for the whole stall.
    blocking_io: bool,
    /// `{path}-wal`, for file-backed databases.
    wal_path: Option<PathBuf>,
    /// The WAL's directory entry has been fsynced since the WAL existed.
    wal_dir_synced: AtomicBool,
    wal_policy: WalPolicy,
    /// Every permit of `readers`: holding them all closes the pool.
    readers_total: u32,
    /// Declared last: the file is released once the connections are gone.
    _open_file: Option<OpenFile>,
}

/// The result row of `PRAGMA wal_checkpoint`. Turso reports a checkpoint it
/// could not finish as a row with busy set, not as an error.
struct Checkpoint {
    busy: bool,
    log: i64,
    checkpointed: i64,
}

async fn wal_checkpoint(conn: &mut Conn, sql: &str) -> anyhow::Result<Checkpoint> {
    let rows = conn
        .query(sql, vec![], Some(1), |row| {
            Ok([0, 1, 2].map(|i| integer(row.get_value(i)).unwrap_or(-1)))
        })
        .await?;
    let [busy, log, checkpointed] = rows
        .first()
        .copied()
        .context("wal_checkpoint returned no row")?;
    Ok(Checkpoint {
        busy: busy != 0,
        log,
        checkpointed,
    })
}

/// Database files open through a live [`TursoPersistence`] in this process.
/// turso_core hands a second open of a file the database already open, so a
/// second persistence would bring a second writer, whose commits fail as busy,
/// and run its startup checkpoint under the first one's readers.
static OPEN_FILES: LazyLock<parking_lot::Mutex<HashSet<PathBuf>>> = LazyLock::new(Default::default);

/// Holds a file in [`OPEN_FILES`] while its persistence lives.
struct OpenFile(PathBuf);

impl OpenFile {
    fn claim(path: &str) -> anyhow::Result<Self> {
        // The file may not exist yet: resolve its directory, so any spelling
        // of the same path names the same file.
        let path = Path::new(path);
        let dir = path
            .parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let name = path.file_name().context("database path has no file name")?;
        let file = std::fs::canonicalize(dir)?.join(name);
        anyhow::ensure!(
            OPEN_FILES.lock().insert(file.clone()),
            "Turso database {} is already open in this process",
            file.display()
        );
        Ok(Self(file))
    }
}

impl Drop for OpenFile {
    fn drop(&mut self) {
        OPEN_FILES.lock().remove(&self.0);
    }
}

impl Inner {
    /// After a commit, fsync the directory the first time the WAL exists.
    fn after_commit(&self) -> anyhow::Result<()> {
        if let Some(wal_path) = &self.wal_path
            && !self.wal_dir_synced.load(Ordering::Relaxed)
            && wal_path.exists()
        {
            sync_directory(wal_path)?;
            self.wal_dir_synced.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    fn wal_bytes(&self) -> u64 {
        self.wal_path
            .as_ref()
            .and_then(|path| std::fs::metadata(path).ok())
            .map_or(0, |meta| meta.len())
    }

    /// Truncate the WAL once it passes the soft size, if the writer and every
    /// reader are free right now; past the hard size, make them free.
    async fn maintain_wal(&self) -> anyhow::Result<()> {
        let bytes = self.wal_bytes();
        let policy = &self.wal_policy;
        if bytes <= policy.soft_bytes {
            return Ok(());
        }
        let outcome = |outcome: &'static str| {
            log_counter_with_labels(
                &TURSO_WAL_TRUNCATIONS_TOTAL,
                1,
                vec![StaticMetricLabel::new("outcome", outcome)],
            )
        };
        let hard = bytes > policy.hard_bytes;
        // The writer first: reads then wait only for the checkpoint itself.
        let writer = if hard {
            Some(self.writer.lock().await)
        } else {
            self.writer.try_lock().ok()
        };
        let Some(mut writer) = writer else {
            outcome("writer_busy");
            return Ok(());
        };
        let started = Instant::now();
        let pool = if hard {
            match tokio::time::timeout(
                policy.gate_deadline,
                self.readers.permits.acquire_many(self.readers_total),
            )
            .await
            {
                Ok(permits) => Some(permits.context("Turso reader pool closed")?),
                Err(_) => None,
            }
        } else {
            self.readers
                .permits
                .try_acquire_many(self.readers_total)
                .ok()
        };
        let Some(_pool) = pool else {
            outcome("readers_busy");
            return Ok(());
        };
        log_distribution(&TURSO_WAL_GATE_SECONDS, started.elapsed().as_secs_f64());
        let checkpoint = self
            .drive(wal_checkpoint(
                &mut writer,
                "PRAGMA wal_checkpoint(TRUNCATE)",
            ))
            .await?;
        if checkpoint.busy {
            // Nothing in this process reads the WAL now, and Turso does not
            // see readers in other processes: busy is an I/O error the
            // PRAGMA swallowed. By then turso_core 0.8.1 has reset the WAL in
            // memory, ahead of the database fsync, and the next commit would
            // cut the file with the only durable copy of those frames. Stop
            // before any commit; on restart SQLite recovers them from the file.
            panic!(
                "Turso could not checkpoint its WAL with no reader or writer in it ({} of {} \
                 frames): stopping before a commit can truncate it",
                checkpoint.checkpointed, checkpoint.log
            );
        }
        outcome("truncated");
        Ok(())
    }

    /// Drive one bounded operation — a page or a write transaction — to
    /// completion. Syscall I/O reads and fsyncs inside `poll`, so the
    /// operation hands its worker off like the `sqlite` crate does: readers
    /// still run in parallel and a stalled fsync cannot freeze the runtime.
    async fn drive<R>(&self, operation: impl Future<Output = R>) -> R {
        if self.blocking_io
            && let Ok(handle) = Handle::try_current()
            && handle.runtime_flavor() == RuntimeFlavor::MultiThread
        {
            return block_in_place(|| handle.block_on(operation));
        }
        operation.await
    }

    /// Wait until no foreground write is pending, for at most
    /// [`BACKGROUND_MAX_YIELD`].
    async fn yield_to_foreground(&self) {
        let idle = async {
            loop {
                // Registered before the check: a `notify_waiters` in between
                // still wakes it.
                let notified = self.foreground_idle.notified();
                if self.foreground.load(Ordering::Acquire) == 0 {
                    return;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(BACKGROUND_MAX_YIELD, idle).await;
    }
}

/// Counts a foreground write as pending from before it queues for the writer
/// until it is done with it.
struct ForegroundGuard<'a>(&'a Inner);

impl<'a> ForegroundGuard<'a> {
    fn new(inner: &'a Inner) -> Self {
        inner.foreground.fetch_add(1, Ordering::AcqRel);
        Self(inner)
    }
}

impl Drop for ForegroundGuard<'_> {
    fn drop(&mut self) {
        if self.0.foreground.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.foreground_idle.notify_waiters();
        }
    }
}

struct ReaderPool {
    db: Arc<Database>,
    idle: parking_lot::Mutex<Vec<Conn>>,
    permits: Semaphore,
    /// Scans hold a reader for a whole page or batch. At most `readers - 1`
    /// of them run at once, so a point lookup (above all the commit's check
    /// that new IDs were never used) always finds a reader instead of
    /// queueing behind them.
    scans: Semaphore,
    cache_kib: i64,
}

/// A reader connection on loan. A connection whose read was interrupted — its
/// future dropped mid-statement — would keep that statement's snapshot, so
/// it is discarded instead of pooled.
struct ReaderGuard<'a> {
    pool: &'a ReaderPool,
    connection: Option<Conn>,
    _permit: SemaphorePermit<'a>,
    _scan_permit: Option<SemaphorePermit<'a>>,
    _hold_timer: Timer<metrics::prometheus::VMHistogramVec>,
}

impl Deref for ReaderGuard<'_> {
    type Target = Conn;

    fn deref(&self) -> &Conn {
        self.connection
            .as_ref()
            .expect("connection taken before drop")
    }
}

impl DerefMut for ReaderGuard<'_> {
    fn deref_mut(&mut self) -> &mut Conn {
        self.connection
            .as_mut()
            .expect("connection taken before drop")
    }
}

impl Drop for ReaderGuard<'_> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take()
            && connection.is_clean()
        {
            self.pool.idle.lock().push(connection);
        }
    }
}

impl ReaderPool {
    /// A reader for a point lookup.
    async fn get(&self, operation: &'static str) -> anyhow::Result<ReaderGuard<'_>> {
        self.acquire(operation, false).await
    }

    /// A reader for a scan, which keeps one reader free for point lookups.
    async fn get_for_scan(&self, operation: &'static str) -> anyhow::Result<ReaderGuard<'_>> {
        self.acquire(operation, true).await
    }

    async fn acquire(
        &self,
        operation: &'static str,
        scan: bool,
    ) -> anyhow::Result<ReaderGuard<'_>> {
        let mut wait_timer = Timer::new_with_labels(&TURSO_READER_WAIT_SECONDS);
        wait_timer.add_label(StaticMetricLabel::new("op", operation));
        let scan_permit = if scan {
            Some(
                self.scans
                    .acquire()
                    .await
                    .context("Turso reader pool closed")?,
            )
        } else {
            None
        };
        let permit = self
            .permits
            .acquire()
            .await
            .context("Turso reader pool closed")?;
        drop(wait_timer);
        let pooled = self.idle.lock().pop();
        let connection = match pooled {
            Some(connection) => connection,
            None => Conn::new(&self.db, self.cache_kib)?,
        };
        let mut hold_timer = Timer::new_with_labels(&TURSO_READER_HOLD_SECONDS);
        hold_timer.add_label(StaticMetricLabel::new("op", operation));
        Ok(ReaderGuard {
            pool: self,
            connection: Some(connection),
            _permit: permit,
            _scan_permit: scan_permit,
            _hold_timer: hold_timer,
        })
    }
}

/// A WAL of this size or less holds no frame.
const WAL_HEADER_BYTES: u64 = 32;

/// Let SQLite finish what a crash left in `path` before Turso reads it.
///
/// A hot rollback journal is SQLite's record of a transaction interrupted in
/// the rollback-journal mode older images use: its pages must be written back
/// before the database is read. Turso does not read `-journal` files, so it
/// would see the torn pages, and a later SQLite open would replay the journal
/// over what Turso wrote. SQLite rolls a hot journal back on its first read.
///
/// A WAL with frames is checkpointed into the database by SQLite, so Turso
/// always opens with nothing to recover: turso_core 0.8.1 treats a WAL whose
/// header read came back short as empty (tursodatabase/turso#8593), and the
/// startup checkpoint would then drop every committed frame in it for good.
fn recover_with_sqlite(path: &str) -> anyhow::Result<()> {
    let hot_journal = std::fs::metadata(format!("{path}-journal")).is_ok_and(|meta| meta.len() > 0);
    let wal_frames =
        std::fs::metadata(format!("{path}-wal")).is_ok_and(|meta| meta.len() > WAL_HEADER_BYTES);
    if !hot_journal && !wal_frames {
        return Ok(());
    }
    let connection = rusqlite::Connection::open(path)?;
    connection.query_row("PRAGMA schema_version", [], |_| Ok(()))?;
    if hot_journal {
        tracing::warn!(
            "SQLite rolled back an interrupted transaction at {path} before Turso opened it"
        );
    }
    if wal_frames {
        let (busy, log, checkpointed): (i64, i64, i64) =
            connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        anyhow::ensure!(
            busy == 0 && log == checkpointed,
            "SQLite could not checkpoint the WAL of {path} before Turso opened it ({checkpointed} \
             of {log} frames, busy {busy}): is another process using the file?"
        );
    }
    Ok(())
}

/// Make a new `-wal` survive a power loss: Turso does not fsync the directory
/// after creating it, and on some filesystems the file and every commit in it
/// could vanish.
fn sync_directory(path: &Path) -> anyhow::Result<()> {
    let dir = path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Check the WAL every `period` for as long as the persistence lives. The
/// persistence is borrowed only while the WAL needs work, so a tick does not
/// hold up dropping it.
async fn maintain_wal_until_dropped(inner: Weak<Inner>, wal_path: PathBuf) {
    let Some(policy) = inner.upgrade().map(|inner| inner.wal_policy.clone()) else {
        return;
    };
    let mut tick = tokio::time::interval(policy.period);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if inner.strong_count() == 0 {
            return;
        }
        let bytes = std::fs::metadata(&wal_path).map_or(0, |meta| meta.len());
        log_gauge(&TURSO_WAL_BYTES, bytes as f64);
        if bytes <= policy.soft_bytes {
            continue;
        }
        let Some(inner) = inner.upgrade() else {
            return;
        };
        if let Err(e) = inner.maintain_wal().await {
            tracing::warn!("Turso WAL maintenance: {e:#}");
        }
    }
}

/// The body of a write transaction, borrowing the writer connection.
type WriteFuture<'c, R> = BoxFuture<'c, anyhow::Result<R>>;

/// Run a write to completion even if the caller stops polling, as the
/// `sqlite` crate does once an operation is admitted: a dropped commit must
/// not leave the outcome to whoever takes the writer next.
async fn run_to_completion<R: Send + 'static>(
    future: BoxFuture<'static, anyhow::Result<R>>,
) -> anyhow::Result<R> {
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::spawn(future).await.context("Turso write task")?
    } else {
        future.await
    }
}

impl TursoPersistence {
    pub async fn new(path: &str) -> anyhow::Result<Self> {
        Self::with_options(path, TursoOptions::from_knobs()).await
    }

    pub async fn with_options(path: &str, options: TursoOptions) -> anyhow::Result<Self> {
        anyhow::ensure!(options.readers > 0, "Turso needs at least one reader");
        let in_memory = path == ":memory:";
        let open_file = (!in_memory).then(|| OpenFile::claim(path)).transpose()?;
        let newly_created = in_memory || !Path::new(path).exists();
        if !in_memory {
            recover_with_sqlite(path)?;
        }
        let db = engine::open(path)?;
        let mut writer = Conn::new(&db, options.cache_kib)?;
        // Execute create tables unconditionally since they are idempotent.
        for statement in SCHEMA {
            writer.execute(statement, vec![]).await?;
        }
        let wal_path = (!in_memory).then(|| PathBuf::from(format!("{path}-wal")));
        let mut wal_dir_synced = false;
        if let Some(wal_path) = &wal_path {
            // Start from an empty WAL. SQLite has already moved a crash's
            // frames into the file, and nothing else holds it yet, so a busy
            // checkpoint means another process has it open.
            match wal_checkpoint(&mut writer, "PRAGMA wal_checkpoint(TRUNCATE)").await {
                Ok(Checkpoint {
                    busy,
                    log,
                    checkpointed,
                }) if busy || log != checkpointed => tracing::warn!(
                    "Turso at {path}: startup checkpoint left the WAL at {log} frames \
                     ({checkpointed} checkpointed, busy {busy})"
                ),
                Ok(_) => {},
                Err(e) => tracing::warn!("Turso at {path}: startup checkpoint failed: {e:#}"),
            }
            if wal_path.exists() {
                sync_directory(wal_path)?;
                wal_dir_synced = true;
            }
        }
        tracing::info!(
            "Turso persistence at {path}: {} readers, {} KiB page cache each",
            options.readers,
            options.cache_kib
        );
        let inner = Arc::new(Inner {
            writer: Mutex::new(writer),
            foreground: AtomicUsize::new(0),
            foreground_idle: Notify::new(),
            background_turn: Mutex::new(()),
            blocking_io: !in_memory,
            wal_path,
            wal_dir_synced: AtomicBool::new(wal_dir_synced),
            wal_policy: options.wal,
            readers_total: u32::try_from(options.readers)?,
            readers: ReaderPool {
                db,
                idle: parking_lot::Mutex::new(vec![]),
                permits: Semaphore::new(options.readers),
                scans: Semaphore::new(options.readers.saturating_sub(1).max(1)),
                cache_kib: options.cache_kib,
            },
            _open_file: open_file,
        });
        if let Some(wal_path) = inner.wal_path.clone()
            && let Ok(handle) = Handle::try_current()
        {
            handle.spawn(maintain_wal_until_dropped(Arc::downgrade(&inner), wal_path));
        }
        Ok(Self {
            inner,
            newly_created,
        })
    }

    /// Run `body` in one write transaction on the single writer connection.
    async fn write_transaction<R: Send + 'static>(
        &self,
        operation: &'static str,
        lane: Lane,
        body: impl for<'c> FnOnce(&'c mut Conn) -> WriteFuture<'c, R> + Send + 'static,
    ) -> anyhow::Result<R> {
        let inner = self.inner.clone();
        run_to_completion(
            async move {
                let this: &Inner = &inner;
                let mut wait_timer = Timer::new_with_labels(&TURSO_WRITER_WAIT_SECONDS);
                wait_timer.add_label(StaticMetricLabel::new("op", operation));
                // Both kept until the writer is released, which is dropped
                // first: a commit stays pending until it is done, and the
                // next background write waits for this one to finish.
                let (_foreground, _background_turn) = match lane {
                    Lane::Foreground => (Some(ForegroundGuard::new(this)), None),
                    Lane::Background => {
                        let turn = this.background_turn.lock().await;
                        this.yield_to_foreground().await;
                        (None, Some(turn))
                    },
                };
                let mut writer = this.writer.lock().await;
                drop(wait_timer);
                let mut hold_timer = Timer::new_with_labels(&TURSO_WRITER_HOLD_SECONDS);
                hold_timer.add_label(StaticMetricLabel::new("op", operation));
                let writer = &mut *writer;
                this.drive(async move {
                    // A body that panicked can leave a statement mid-step
                    // or a transaction open.
                    if !writer.is_clean() {
                        writer.recover();
                        if !writer.is_autocommit() {
                            writer.execute("ROLLBACK", vec![]).await?;
                        }
                    }
                    writer.execute("BEGIN", vec![]).await?;
                    match body(&mut *writer).await {
                        Ok(result) => {
                            writer.execute("COMMIT", vec![]).await?;
                            this.after_commit()?;
                            Ok(result)
                        },
                        Err(e) => {
                            if let Err(rollback) = writer.execute("ROLLBACK", vec![]).await {
                                return Err(e.context(format!(
                                    "rollback after the failed write also failed: {rollback:#}"
                                )));
                            }
                            Err(e)
                        },
                    }
                })
                .await
            }
            .boxed(),
        )
        .await
    }

    /// Read one page of the documents log, at most `page_size` rows, resuming
    /// strictly after `cursor` — the `(ts, table_id, id)` primary key of the
    /// last row the previous page read, in scan order. A table filter is
    /// applied in SQL before the page is materialized.
    async fn _load_documents_page(
        &self,
        tablet_id: Option<TabletId>,
        range: TimestampRange,
        order: Order,
        page_size: usize,
        cursor: Option<&(u64, Vec<u8>, Vec<u8>)>,
    ) -> anyhow::Result<Vec<DocumentLogEntry>> {
        let (query, params) = load_documents_query(order, cursor, tablet_id, range, page_size)?;
        let mut reader = self.inner.readers.get_for_scan("load_documents").await?;
        self.inner
            .drive(reader.query(&query, params, None, |row| {
                let (document_id, ts, document, prev_ts) = document_row(row)?;
                Ok(DocumentLogEntry {
                    ts,
                    id: document_id,
                    value: document,
                    prev_ts,
                })
            }))
            .await
    }

    /// Stream the documents log one page at a time. As in the Postgres
    /// reader, the snapshot is validated against retention after each page is
    /// read and before any of its rows are yielded: pages are read at
    /// different times, and each read is only valid while the range's minimum
    /// timestamp is still within retention. Nothing is filtered out — deletes
    /// are part of the log — so a short page always means the range is
    /// exhausted.
    #[try_stream(ok = DocumentLogEntry, error = anyhow::Error)]
    async fn _load_documents_paginated(
        &self,
        tablet_id: Option<TabletId>,
        range: TimestampRange,
        order: Order,
        page_size: u32,
        retention_validator: Arc<dyn RetentionValidator>,
    ) {
        // A zero-sized page would never advance the cursor or end the stream.
        let page_size = page_size.max(1) as usize;
        let mut cursor: Option<(u64, Vec<u8>, Vec<u8>)> = None;
        loop {
            let page = self
                ._load_documents_page(tablet_id, range, order, page_size, cursor.as_ref())
                .await?;
            let page_len = page.len();
            retention_validator
                .validate_document_snapshot(range.min_timestamp_inclusive())
                .await?;
            for entry in page {
                cursor = Some((
                    u64::from(entry.ts),
                    entry.id.table().0[..].to_vec(),
                    entry.id.internal_id()[..].to_vec(),
                ));
                yield entry;
            }
            if page_len < page_size {
                break;
            }
        }
    }

    /// Read one page of an index scan: at most `batch_size` index rows,
    /// resuming after `cursor` (the last key the previous page returned, in
    /// scan order). Returns the latest visible revision of each key the page
    /// completed — tombstones included, which advance the cursor but are not
    /// emitted — and whether the interval is exhausted. Revisions are decoded
    /// by the caller after it validated the snapshot, so a read that fell out
    /// of retention reports that rather than a dangling row.
    ///
    /// Rows come back in primary-key order, `(key, ts)` ascending or both
    /// descending, so no sort is ever materialized. Picking the latest
    /// revision per key here, instead of a correlated `MAX(ts)` subquery per
    /// row, halves the work of the scan. Descending, the first row of a key is
    /// its latest revision. Ascending it is the last, so a full page's last
    /// key may be cut short and is left for the next page, unless it is the
    /// only key on the page.
    async fn _index_scan_page(
        &self,
        index_id: IndexId,
        read_timestamp: Timestamp,
        interval: &Interval,
        order: Order,
        batch_size: usize,
        cursor: Option<&IndexKeyBytes>,
    ) -> anyhow::Result<(Vec<IndexRevision>, bool)> {
        if let Some(key) = interval.is_singleton() {
            // A point read — every `db.get` — is one primary-key lookup of the
            // key's latest revision, whatever the page size or order.
            let params = vec![
                blob_param(&index_id.0[..]),
                blob_param(&key[..]),
                ts_param(read_timestamp.into())?,
            ];
            let mut reader = self.inner.readers.get("index_scan").await?;
            let revision = self
                .inner
                .drive(reader.query(LATEST_REVISION_QUERY, params, Some(1), index_revision))
                .await?;
            return Ok((revision, true));
        }
        let mut params = vec![
            blob_param(&index_id.0[..]),
            ts_param(read_timestamp.into())?,
        ];

        let StartIncluded(ref start) = interval.start;
        let start_bytes = &start[..];
        let cursor_bytes = cursor.map(|key| &key.0[..]);
        // Use one bound on each side so the PK seek starts at this page's cursor.
        let (lower_bytes, lower_op) = match (order, cursor_bytes) {
            (Order::Asc, Some(cursor)) if cursor >= start_bytes => (cursor, ">"),
            _ => (start_bytes, ">="),
        };
        params.push(blob_param(lower_bytes));
        let lower = format!(" AND B.key {lower_op} ?{}", params.len());
        let end_bytes = match interval.end {
            End::Excluded(ref end) => Some(&end[..]),
            End::Unbounded => None,
        };
        let end_bytes = match (order, cursor_bytes, end_bytes) {
            (Order::Desc, Some(cursor), Some(end)) => Some(cursor.min(end)),
            (Order::Desc, Some(cursor), None) => Some(cursor),
            (_, _, end) => end,
        };
        let upper = match end_bytes {
            Some(end) => {
                params.push(blob_param(end));
                format!(" AND B.key < ?{}", params.len())
            },
            None => String::new(),
        };
        params.push(int_param(i64::try_from(batch_size)?));
        let limit = params.len();
        let direction = match order {
            Order::Asc => "ASC",
            Order::Desc => "DESC",
        };
        let query = format!(
            r#"
SELECT B.key, B.ts, B.document_id, C.table_id, C.json_value, C.prev_ts, B.deleted
FROM indexes B
LEFT JOIN documents C
ON B.ts = C.ts AND B.table_id = C.table_id AND B.document_id = C.id
WHERE B.index_id = ?1 AND B.ts <= ?2{lower}{upper}
ORDER BY B.key {direction}, B.ts {direction}
LIMIT ?{limit}
"#,
        );

        let cut_ts = ts_param(read_timestamp.into())?;
        let mut reader = self.inner.readers.get_for_scan("index_scan").await?;
        let reader = &mut *reader;
        self.inner
            .drive(async move {
                let rows = reader.query(&query, params, None, index_revision).await?;
                let exhausted = rows.len() < batch_size;
                let mut latest: Vec<IndexRevision> = Vec::new();
                for row in rows {
                    match latest.last_mut() {
                        Some(last) if last.key == row.key => {
                            if order == Order::Asc {
                                *last = row;
                            }
                        },
                        _ => latest.push(row),
                    }
                }
                if order == Order::Asc && !exhausted {
                    // A full page may end inside the last key's revisions, so
                    // its latest one is not known yet. Settle it with a point
                    // lookup rather than leave it to the next page: `take(n)`
                    // reads pages of n rows, and would read a whole second
                    // page for that one key.
                    let cut = latest.pop().expect("a full page has rows");
                    let params = vec![blob_param(&index_id.0[..]), blob_param(&cut.key), cut_ts];
                    let revision = reader
                        .query(LATEST_REVISION_QUERY, params, Some(1), index_revision)
                        .await?
                        .pop();
                    // The key was on the page a statement ago; only retention
                    // can have removed it since, and the caller's snapshot
                    // validation reports that. Skip it as a tombstone so the
                    // cursor advances.
                    latest.push(revision.unwrap_or(IndexRevision {
                        deleted: true,
                        json_value: None,
                        ..cut
                    }));
                }
                anyhow::Ok((latest, exhausted))
            })
            .await
    }

    /// Stream an index scan one page at a time. The snapshot is validated
    /// against retention after each page is read and before any of its rows
    /// are yielded, mirroring the Postgres reader: pages are read at
    /// different times, and each read is only valid if the snapshot is still
    /// within retention.
    #[try_stream(ok = (IndexKeyBytes, LatestDocument), error = anyhow::Error)]
    async fn _index_scan_paginated(
        &self,
        index_id: IndexId,
        tablet_id: TabletId,
        read_timestamp: Timestamp,
        interval: Interval,
        order: Order,
        batch_size: usize,
        retention_validator: Arc<dyn RetentionValidator>,
    ) {
        let mut cursor: Option<IndexKeyBytes> = None;
        loop {
            let page = self
                ._index_scan_page(
                    index_id,
                    read_timestamp,
                    &interval,
                    order,
                    batch_size,
                    cursor.as_ref(),
                )
                .await?;
            let (page, exhausted) = page;
            retention_validator
                .validate_snapshot(read_timestamp)
                .await?;
            for revision in page {
                let (key, doc) = revision.into_latest(tablet_id)?;
                cursor = Some(key.clone());
                if let Some(doc) = doc {
                    yield (key, doc);
                }
            }
            if exhausted {
                break;
            }
        }
    }

    /// Look up one revision per `(id, ts)` with `query` on a single reader
    /// connection; `emit` receives the query's input and the matching row.
    async fn revisions<K>(
        &self,
        query: &'static str,
        lookups: impl IntoIterator<Item = (InternalDocumentId, Timestamp, K)>,
        mut emit: impl FnMut(K, DocumentLogEntry),
    ) -> anyhow::Result<()> {
        let mut reader = self.inner.readers.get("previous_revisions").await?;
        let reader = &mut *reader;
        self.inner
            .drive(async move {
                for (id, ts, key) in lookups {
                    let params = vec![
                        blob_param(&id.table().0[..]),
                        blob_param(&id.internal_id()[..]),
                        ts_param(ts.into())?,
                    ];
                    let row = reader
                        .query(query, params, Some(1), document_row)
                        .await?
                        .pop();
                    if let Some((document_id, prev_ts, document, prev_prev_ts)) = row {
                        emit(
                            key,
                            DocumentLogEntry {
                                ts: prev_ts,
                                id: document_id,
                                value: document,
                                prev_ts: prev_prev_ts,
                            },
                        );
                    }
                }
                Ok(())
            })
            .await
    }
}

#[async_trait]
impl IndexRowPersistence for TursoPersistence {
    async fn delete_index_rows(&self, expired_rows: Vec<IndexEntry>) -> anyhow::Result<usize> {
        let rows = expired_rows
            .into_iter()
            .map(
                |IndexEntry {
                     index_id,
                     key_prefix,
                     ts,
                     ..
                 }| {
                    Ok(vec![
                        blob_param(&index_id.0[..]),
                        ts_param(ts.into())?,
                        Value::Blob(key_prefix),
                    ])
                },
            )
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.write_transaction("delete", Lane::Background, move |tx| {
            async move { tx.execute_each(DELETE_INDEX, rows).await }.boxed()
        })
        .await
    }
}

#[async_trait]
impl Persistence for TursoPersistence {
    fn is_fresh(&self) -> bool {
        self.newly_created
    }

    fn reader(&self) -> Arc<dyn PersistenceReader> {
        Arc::new(self.clone())
    }

    async fn write<'a>(
        &self,
        documents: &'a [DocumentLogEntry],
        indexes: &'a [PersistenceIndexEntry],
        conflict_strategy: ConflictStrategy,
    ) -> anyhow::Result<()> {
        let document_rows = documents
            .iter()
            .map(|update| {
                let (json_value, deleted) = match &update.value {
                    Some(document) => {
                        assert_eq!(update.id, document.id_with_table_id());
                        (text_param(document.value().json_serialize()?), 0)
                    },
                    None => (Value::Null, 1),
                };
                Ok(vec![
                    blob_param(&update.id.internal_id()[..]),
                    ts_param(update.ts.into())?,
                    blob_param(&update.id.table().0[..]),
                    json_value,
                    int_param(deleted),
                    match update.prev_ts {
                        Some(prev_ts) => ts_param(prev_ts.into())?,
                        None => Value::Null,
                    },
                ])
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let index_rows = indexes
            .iter()
            .map(|update| {
                let (deleted, table_id, document_id) = match update.value {
                    None => (1, Value::Null, Value::Null),
                    Some(doc_id) => (
                        0,
                        blob_param(&doc_id.table().0[..]),
                        blob_param(&doc_id.internal_id()[..]),
                    ),
                };
                Ok(vec![
                    blob_param(&update.index_id.0[..]),
                    ts_param(update.ts.into())?,
                    blob_param(&update.key.0),
                    int_param(deleted),
                    table_id,
                    document_id,
                ])
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let (insert_document, insert_index) = match conflict_strategy {
            ConflictStrategy::Error => (INSERT_DOCUMENT, INSERT_INDEX),
            ConflictStrategy::Overwrite => (INSERT_OVERWRITE_DOCUMENT, INSERT_OVERWRITE_INDEX),
        };
        self.write_transaction("write", Lane::Foreground, move |tx| {
            async move {
                tx.execute_each(insert_document, document_rows).await?;
                tx.execute_each(insert_index, index_rows).await?;
                Ok(())
            }
            .boxed()
        })
        .await
    }

    async fn write_persistence_global(
        &self,
        key: PersistenceGlobalKey,
        value: JsonValue,
    ) -> anyhow::Result<()> {
        let row = vec![
            text_param(String::from(key)),
            text_param(serde_json::to_string(&value)?),
        ];
        self.write_transaction("globals", Lane::Foreground, move |tx| {
            async move {
                tx.execute_each(WRITE_PERSISTENCE_GLOBAL, vec![row]).await?;
                Ok(())
            }
            .boxed()
        })
        .await
    }

    async fn has_index_entries(&self) -> anyhow::Result<bool> {
        let mut reader = self.inner.readers.get("has_index_entries").await?;
        let rows = self
            .inner
            .drive(reader.query(HAS_INDEX_ENTRIES, vec![], Some(1), |_| Ok(())))
            .await?;
        Ok(!rows.is_empty())
    }

    async fn reclaim_index_history(
        &self,
        request: IndexRetentionRequest<'_>,
    ) -> anyhow::Result<IndexRetentionProgress> {
        delete_expired_entries(self, request).await
    }

    async fn delete(
        &self,
        documents: Vec<(Timestamp, InternalDocumentId)>,
    ) -> anyhow::Result<usize> {
        let rows = documents
            .into_iter()
            .map(|(ts, internal_id)| {
                Ok(vec![
                    blob_param(&internal_id.table().0[..]),
                    blob_param(&internal_id.internal_id()[..]),
                    ts_param(ts.into())?,
                ])
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.write_transaction("delete", Lane::Background, move |tx| {
            async move { tx.execute_each(DELETE_DOCUMENT, rows).await }.boxed()
        })
        .await
    }

    async fn delete_tablet_documents(
        &self,
        tablet_id: TabletId,
        chunk_size: usize,
    ) -> anyhow::Result<usize> {
        let row = vec![
            blob_param(&tablet_id.0[..]),
            blob_param(&tablet_id.0[..]),
            int_param(i64::try_from(chunk_size)?),
        ];
        self.write_transaction("delete", Lane::Background, move |tx| {
            async move { tx.execute_each(DELETE_TABLE_DOCUMENTS, vec![row]).await }.boxed()
        })
        .await
    }
}

#[async_trait]
impl PersistenceReader for TursoPersistence {
    fn load_documents(
        &self,
        range: TimestampRange,
        order: Order,
        page_size: u32,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> DocumentStream<'_> {
        self._load_documents_paginated(None, range, order, page_size, retention_validator)
            .cooperative()
            .boxed()
    }

    fn load_documents_from_table(
        &self,
        tablet_id: TabletId,
        range: TimestampRange,
        order: Order,
        page_size: u32,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> DocumentStream<'_> {
        self._load_documents_paginated(
            Some(tablet_id),
            range,
            order,
            page_size,
            retention_validator,
        )
        .cooperative()
        .boxed()
    }

    async fn previous_revisions(
        &self,
        ids: BTreeSet<(InternalDocumentId, Timestamp)>,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> anyhow::Result<BTreeMap<(InternalDocumentId, Timestamp), DocumentLogEntry>> {
        let min_ts = ids.iter().map(|(_, ts)| *ts).fold(Timestamp::MAX, cmp::min);
        let mut out = BTreeMap::new();
        self.revisions(
            PREV_REV_QUERY,
            ids.into_iter().map(|(id, ts)| (id, ts, ts)),
            |ts, entry| {
                out.insert((entry.id, ts), entry);
            },
        )
        .await?;
        retention_validator
            .validate_document_snapshot(min_ts)
            .await?;
        Ok(out)
    }

    async fn previous_revisions_of_documents(
        &self,
        ids: BTreeSet<DocumentPrevTsQuery>,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> anyhow::Result<BTreeMap<DocumentPrevTsQuery, DocumentLogEntry>> {
        // Validate retention for all queried timestamps first
        let min_ts = ids.iter().map(|DocumentPrevTsQuery { ts, .. }| *ts).min();

        let mut out = BTreeMap::new();
        self.revisions(
            EXACT_REV_QUERY,
            ids.into_iter()
                .map(|DocumentPrevTsQuery { id, ts, prev_ts }| (id, prev_ts, ts)),
            |ts, entry| {
                out.insert(
                    DocumentPrevTsQuery {
                        id: entry.id,
                        ts,
                        prev_ts: entry.ts,
                    },
                    entry,
                );
            },
        )
        .await?;
        if let Some(min_ts) = min_ts {
            retention_validator
                .validate_document_snapshot(min_ts)
                .await?;
        }
        Ok(out)
    }

    fn index_scan(
        &self,
        index_id: IndexId,
        tablet_id: TabletId,
        read_timestamp: Timestamp,
        interval: &Interval,
        order: Order,
        size_hint: usize,
        retention_validator: Arc<dyn RetentionValidator>,
    ) -> IndexStream<'_> {
        // Mirror the Postgres reader: use the caller's size_hint to bound how
        // much of the interval each query materializes, so a small take()
        // over a large table no longer loads the entire range into memory.
        let batch_size = size_hint.clamp(1, 5000);
        self._index_scan_paginated(
            index_id,
            tablet_id,
            read_timestamp,
            interval.clone(),
            order,
            batch_size,
            retention_validator,
        )
        .boxed()
    }

    async fn get_persistence_global(
        &self,
        key: PersistenceGlobalKey,
    ) -> anyhow::Result<Option<JsonValue>> {
        let key = String::from(key);
        let mut reader = self.inner.readers.get("globals").await?;
        let row = self
            .inner
            .drive(reader.query(
                GET_PERSISTENCE_GLOBAL,
                vec![text_param(key.clone())],
                Some(1),
                |row| Ok(text(row.get_value(0))?.to_owned()),
            ))
            .await?
            .pop();
        let Some(json_value_str) = row else {
            return Ok(None);
        };
        let mut json_deserializer = serde_json::Deserializer::from_str(&json_value_str);
        // XXX: this is bad, but shapes can get much more nested than convex values
        json_deserializer.disable_recursion_limit();
        let json_value = JsonValue::deserialize(&mut json_deserializer)
            .with_context(|| format!("Invalid JSON at persistence key {key:?}"))?;
        json_deserializer.end()?;
        Ok(Some(json_value))
    }

    fn version(&self) -> PersistenceVersion {
        PersistenceVersion::V5
    }
}

/// The documents-log page query and its parameters. Each side of the `ts`
/// range has exactly one bound, so the planner seeks where the page starts:
/// with a cursor, the cursor replaces the range bound on its side (it is
/// always inside the range) and a tiebreak on `(table_id, id)` resumes a
/// run of equal timestamps. The table filter is written `+table_id` so the
/// planner keeps the primary-key scan, already in `(ts, table_id, id)` order,
/// instead of reading `documents_by_table_and_id` and sorting every revision
/// of the table on each page.
fn load_documents_query(
    order: Order,
    cursor: Option<&(u64, Vec<u8>, Vec<u8>)>,
    tablet_id: Option<TabletId>,
    range: TimestampRange,
    page_size: usize,
) -> anyhow::Result<(String, Vec<Value>)> {
    let mut params: Vec<Value> = vec![];
    let mut filters = vec![];
    let min = ts_param(range.min_timestamp_inclusive().into())?;
    let max = ts_param(range.max_timestamp_exclusive().into())?;
    match cursor {
        None => {
            params.push(min);
            params.push(max);
            filters.push("ts >= ?1 AND ts < ?2".to_owned());
        },
        Some((ts, table_id, id)) => {
            params.push(ts_param(*ts)?);
            params.push(blob_param(table_id));
            params.push(blob_param(id));
            let (seek, bound, other, before) = match order {
                Order::Asc => ("ts >= ?1", "ts < ?4", max, ">"),
                Order::Desc => ("ts <= ?1", "ts >= ?4", min, "<"),
            };
            params.push(other);
            filters.push(format!(
                "{seek} AND {bound} AND (ts {before} ?1 OR (ts = ?1 AND (table_id {before} ?2 OR \
                 (table_id = ?2 AND id {before} ?3))))"
            ));
        },
    }
    if let Some(tablet_id) = tablet_id {
        params.push(blob_param(&tablet_id.0[..]));
        filters.push(format!("+table_id = ?{}", params.len()));
    }
    params.push(int_param(i64::try_from(page_size)?));
    let limit = params.len();
    let direction = match order {
        Order::Asc => "ASC",
        Order::Desc => "DESC",
    };
    let query = format!(
        "SELECT id, ts, table_id, json_value, deleted, prev_ts FROM documents WHERE {} ORDER BY \
         ts {direction}, table_id {direction}, id {direction} LIMIT ?{limit}",
        filters.join(" AND ")
    );
    Ok((query, params))
}

/// One index row with its document, as the scan query returns it.
struct IndexRevision {
    key: Vec<u8>,
    ts: Timestamp,
    document_id: Option<Vec<u8>>,
    table: Option<Vec<u8>>,
    json_value: Option<String>,
    prev_ts: Option<Timestamp>,
    deleted: bool,
}

fn index_revision(row: &Row) -> anyhow::Result<IndexRevision> {
    let deleted = integer(row.get_value(6))? != 0;
    Ok(IndexRevision {
        key: blob(row.get_value(0))?.to_vec(),
        ts: timestamp(row.get_value(1))?,
        document_id: optional(row.get_value(2), blob)?.map(<[u8]>::to_vec),
        table: optional(row.get_value(3), blob)?.map(<[u8]>::to_vec),
        // A tombstone's document is never decoded; skip copying it.
        json_value: if deleted {
            None
        } else {
            optional(row.get_value(4), text)?.map(str::to_owned)
        },
        prev_ts: optional(row.get_value(5), timestamp)?,
        deleted,
    })
}

impl IndexRevision {
    /// The key and, unless it is a tombstone, the document it points to.
    fn into_latest(
        self,
        tablet_id: TabletId,
    ) -> anyhow::Result<(IndexKeyBytes, Option<LatestDocument>)> {
        let key = IndexKeyBytes(self.key);
        let ts = self.ts;
        if self.deleted {
            return Ok((key, None));
        }
        let table = self
            .table
            .ok_or_else(|| anyhow::anyhow!("Dangling index reference for {:?} {:?}", key, ts))?;
        let table = TabletId(table.try_into()?);
        let document_id = self
            .document_id
            .ok_or_else(|| anyhow::anyhow!("Dangling index reference for {:?} {:?}", key, ts))?;
        let _document_id = InternalDocumentId::new(table, InternalId::try_from(document_id)?);
        let json_value = self.json_value.ok_or_else(|| {
            anyhow::anyhow!("Index reference to deleted document {:?} {:?}", key, ts)
        })?;
        let value = common::value::json_deserialize(&json_value)?;
        let document = ResolvedDocument::from_database(tablet_id, value)?;
        Ok((
            key,
            Some(LatestDocument {
                ts,
                value: document,
                prev_ts: self.prev_ts,
            }),
        ))
    }
}

fn ts_param(value: u64) -> anyhow::Result<Value> {
    Ok(int_param(i64::try_from(value)?))
}

fn timestamp(value: &Value) -> anyhow::Result<Timestamp> {
    Timestamp::try_from(u64::try_from(integer(value)?)?)
}

/// Decode a `(id, ts, table_id, json_value, deleted, prev_ts)` documents row.
fn document_row(
    row: &Row,
) -> anyhow::Result<(
    InternalDocumentId,
    Timestamp,
    Option<ResolvedDocument>,
    Option<Timestamp>,
)> {
    let id = InternalId::try_from(blob(row.get_value(0))?)?;
    let prev_ts = timestamp(row.get_value(1))?;
    let table = TabletId(blob(row.get_value(2))?.try_into()?);
    let json_value = optional(row.get_value(3), text)?;
    let deleted = integer(row.get_value(4))? != 0;
    let prev_prev_ts = optional(row.get_value(5), timestamp)?;
    let document_id = InternalDocumentId::new(table, id);
    let document = if !deleted {
        let json_value = json_value
            .ok_or_else(|| anyhow::anyhow!("Unexpected NULL json_value at {} {}", id, prev_ts))?;
        let value = common::value::json_deserialize(json_value)?;
        Some(ResolvedDocument::from_database(table, value)?)
    } else {
        None
    };
    Ok((document_id, prev_ts, document, prev_prev_ts))
}

const DOCUMENTS_INIT: &str = r#"
CREATE TABLE IF NOT EXISTS documents (
    id BLOB NOT NULL,
    ts INTEGER NOT NULL,

    table_id BLOB NOT NULL,

    json_value TEXT NULL,
    deleted INTEGER NOT NULL,

    prev_ts INTEGER,

    PRIMARY KEY (ts, table_id, id)
);
"#;

const DOCUMENTS_BY_TABLE_AND_ID_INIT: &str =
    "CREATE INDEX IF NOT EXISTS documents_by_table_and_id ON documents (table_id, id, ts)";

const INDEXES_INIT: &str = r#"
CREATE TABLE IF NOT EXISTS indexes (
    index_id BLOB NOT NULL,
    ts INTEGER NOT NULL,

    key BLOB NOT NULL,

    deleted INTEGER NOT NULL,

    table_id BLOB NULL,
    document_id BLOB NULL,

    PRIMARY KEY (index_id, key, ts)
);
"#;

const PERSISTENCE_GLOBALS_INIT: &str = r#"
CREATE TABLE IF NOT EXISTS persistence_globals (
    key TEXT NOT NULL,
    json_value TEXT NOT NULL,

    PRIMARY KEY (key)
);
"#;

/// The `sqlite` crate's schema, one statement at a time.
const SCHEMA: [&str; 4] = [
    DOCUMENTS_INIT,
    DOCUMENTS_BY_TABLE_AND_ID_INIT,
    INDEXES_INIT,
    PERSISTENCE_GLOBALS_INIT,
];

const GET_PERSISTENCE_GLOBAL: &str = "SELECT json_value FROM persistence_globals WHERE key = ?1";

const INSERT_DOCUMENT: &str = "INSERT INTO documents (id, ts, table_id, json_value, deleted, \
                               prev_ts) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
const INSERT_OVERWRITE_DOCUMENT: &str = "INSERT OR REPLACE INTO documents (id, ts, table_id, \
                                         json_value, deleted, prev_ts) VALUES (?1, ?2, ?3, ?4, \
                                         ?5, ?6)";
const INSERT_INDEX: &str = "INSERT INTO indexes VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
const INSERT_OVERWRITE_INDEX: &str =
    "INSERT OR REPLACE INTO indexes VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
const WRITE_PERSISTENCE_GLOBAL: &str = "INSERT OR REPLACE INTO persistence_globals VALUES (?1, ?2)";

const HAS_INDEX_ENTRIES: &str = "SELECT 1 FROM indexes LIMIT 1";

const DELETE_INDEX: &str = "DELETE FROM indexes WHERE index_id = ?1 AND ts <= ?2 AND key = ?3";

const DELETE_DOCUMENT: &str = "DELETE FROM documents WHERE table_id = ?1 AND id = ?2 AND ts <= ?3";

const DELETE_TABLE_DOCUMENTS: &str = "DELETE FROM documents WHERE table_id = ?1 AND id IN (SELECT \
                                      id FROM documents WHERE table_id = ?2 LIMIT ?3)";

/// The latest revision of one index key at a read timestamp.
const LATEST_REVISION_QUERY: &str = r#"
SELECT B.key, B.ts, B.document_id, C.table_id, C.json_value, C.prev_ts, B.deleted
FROM indexes B
LEFT JOIN documents C
ON B.ts = C.ts AND B.table_id = C.table_id AND B.document_id = C.id
WHERE B.index_id = ?1 AND B.key = ?2 AND B.ts <= ?3
ORDER BY B.ts DESC
LIMIT 1
"#;

const PREV_REV_QUERY: &str = r#"
SELECT id, ts, table_id, json_value, deleted, prev_ts
FROM documents
WHERE
    table_id = ?1 AND
    id = ?2 AND
    ts < ?3
ORDER BY ts desc
LIMIT 1
"#;

const EXACT_REV_QUERY: &str = r#"
SELECT id, ts, table_id, json_value, deleted, prev_ts
FROM documents
WHERE
    table_id = ?1 AND
    id = ?2 AND
    ts = ?3
ORDER BY ts ASC, table_id ASC, id ASC
"#;

#[cfg(test)]
mod tests;
