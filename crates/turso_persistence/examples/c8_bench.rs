//! C8 persistence bench: the same Convex-shaped workload on both engines.
//!
//! Arms (one database file each, under `--dir`, on the disk being measured):
//!   sqlite-delete  the `sqlite` crate as shipped (rollback journal)
//!   sqlite-wal     the `sqlite` crate on a file switched to WAL beforehand
//!   turso          `turso_persistence`
//!
//! Phases per arm:
//!   commit  sequential committer-shaped writes (docs + index rows each),
//!           latency per commit
//!   mixed   `--readers` tasks doing index scans while one task keeps
//!           committing; read latency, read throughput and commit latency
//!
//! Usage: c8_bench --dir /path/on/disk [--arms a,b] [--commits N] [--docs N]
//!        [--seconds N] [--readers N] [--scan N]
#![feature(coroutines)]

use std::{
    sync::{
        atomic::{
            AtomicBool,
            AtomicU64,
            Ordering,
        },
        Arc,
    },
    time::{
        Duration,
        Instant,
    },
};

use common::{
    document::{
        CreationTime,
        ResolvedDocument,
    },
    index::IndexKeyBytes,
    interval::{
        BinaryKey,
        End,
        Interval,
        StartIncluded,
    },
    obj,
    persistence::{
        ConflictStrategy,
        DocumentLogEntry,
        NoopRetentionValidator,
        Persistence,
        PersistenceIndexEntry,
    },
    query::Order,
    types::{
        IndexId,
        Timestamp,
    },
    value::{
        DeveloperDocumentId,
        InternalDocumentId,
        InternalId,
        ResolvedDocumentId,
        TableNumber,
        TabletId,
    },
};
use futures::{
    StreamExt,
    TryStreamExt,
};

struct Args {
    dir: String,
    arms: Vec<String>,
    commits: usize,
    docs: usize,
    seconds: u64,
    readers: usize,
    scan: usize,
}

fn args() -> Args {
    let mut a = Args {
        dir: String::new(),
        arms: ["sqlite-delete", "sqlite-wal", "turso"]
            .map(String::from)
            .to_vec(),
        commits: 500,
        docs: 8,
        seconds: 10,
        readers: 8,
        scan: 100,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it.next().expect("flag value");
        match flag.as_str() {
            "--dir" => a.dir = value,
            "--arms" => a.arms = value.split(',').map(String::from).collect(),
            "--commits" => a.commits = value.parse().unwrap(),
            "--docs" => a.docs = value.parse().unwrap(),
            "--seconds" => a.seconds = value.parse().unwrap(),
            "--readers" => a.readers = value.parse().unwrap(),
            "--scan" => a.scan = value.parse().unwrap(),
            other => panic!("unknown flag {other}"),
        }
    }
    assert!(!a.dir.is_empty(), "--dir is required");
    a
}

const TABLET: u8 = 7;
const INDEX: u8 = 9;

fn id(n: u64) -> InternalId {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&n.to_be_bytes());
    InternalId::from(bytes)
}

/// One committer batch: `docs` documents with ~600 bytes of JSON each and
/// one index row per document, keyed so scans walk a realistic range.
fn batch(
    ts: u64,
    first_doc: u64,
    docs: usize,
) -> anyhow::Result<(Vec<DocumentLogEntry>, Vec<PersistenceIndexEntry>)> {
    let tablet_id = TabletId(InternalId::from([TABLET; 16]));
    let table_number = TableNumber::try_from(10001).unwrap();
    let index_id = IndexId(InternalId::from([INDEX; 16]));
    let payload = "x".repeat(512);
    let mut documents = vec![];
    let mut indexes = vec![];
    for i in 0..docs as u64 {
        let n = first_doc + i;
        let doc_id =
            ResolvedDocumentId::new(tablet_id, DeveloperDocumentId::new(table_number, id(n)));
        let value = obj!("n" => n as i64, "payload" => payload.clone())?;
        let document =
            ResolvedDocument::new(doc_id, CreationTime::try_from(1.0).unwrap(), value).unwrap();
        let internal = InternalDocumentId::new(tablet_id, id(n));
        documents.push(DocumentLogEntry {
            ts: Timestamp::try_from(ts).unwrap(),
            id: internal,
            value: Some(document),
            prev_ts: None,
        });
        indexes.push(PersistenceIndexEntry {
            ts: Timestamp::try_from(ts).unwrap(),
            index_id,
            key: IndexKeyBytes(n.to_be_bytes().to_vec()),
            value: Some(internal),
        });
    }
    Ok((documents, indexes))
}

async fn open(arm: &str, path: &str) -> anyhow::Result<Arc<dyn Persistence>> {
    // Journals are explicit: `SqlitePersistence::new` follows the
    // CONVEX_SQLITE_JOURNAL_MODE default, which is WAL since C8.
    Ok(match arm {
        "sqlite-delete" => Arc::new(sqlite::SqlitePersistence::with_journal(
            path, "delete", "full",
        )?),
        "sqlite-wal" => Arc::new(sqlite::SqlitePersistence::with_journal(
            path, "wal", "full",
        )?),
        "turso" | "turso-syscall" => {
            Arc::new(turso_persistence::TursoPersistence::new(path).await?)
        },
        other => anyhow::bail!("unknown arm {other}"),
    })
}

fn percentiles(mut v: Vec<Duration>) -> String {
    if v.is_empty() {
        return "n=0".into();
    }
    v.sort();
    let p = |q: f64| v[((v.len() - 1) as f64 * q).round() as usize].as_secs_f64() * 1e3;
    format!(
        "n={} p50={:.3}ms p95={:.3}ms p99={:.3}ms max={:.3}ms",
        v.len(),
        p(0.5),
        p(0.95),
        p(0.99),
        p(1.0)
    )
}

fn cpu_secs() -> anyhow::Result<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    Ok((fields[11].parse::<f64>()? + fields[12].parse::<f64>()?) / 100.0)
}

fn fsyncs() -> u64 {
    // syscw counts write syscalls; fsync is not in /proc/self/io, so report
    // bytes actually sent to the block layer instead.
    std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("write_bytes:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

async fn run_arm(a: &Args, arm: &str) -> anyhow::Result<()> {
    let dir = std::path::Path::new(&a.dir).join(arm);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("db.sqlite3");
    let path = path.to_str().unwrap();
    // Force the turso io variant per arm before the LazyLock is read: run
    // each arm in its own process (the driver script does) for turso arms.
    let p = open(arm, path).await?;

    // Phase 1: sequential commits.
    let bytes0 = fsyncs();
    let mut lat = vec![];
    let t0 = Instant::now();
    let mut ts = 1u64;
    let mut next_doc = 0u64;
    for _ in 0..a.commits {
        let (docs, idx) = batch(ts, next_doc, a.docs)?;
        let t = Instant::now();
        p.write(&docs, &idx, ConflictStrategy::Error).await?;
        lat.push(t.elapsed());
        ts += 1;
        next_doc += a.docs as u64;
    }
    let wall = t0.elapsed();
    println!(
        "{arm} commit: {} | {:.0} commits/s | block-layer writes {:.1} MB",
        percentiles(lat),
        a.commits as f64 / wall.as_secs_f64(),
        (fsyncs() - bytes0) as f64 / 1e6
    );

    // Phase 1b: one reader alone, for CPU cost per scan.
    let solo = p.reader();
    let cpu0 = cpu_secs()?;
    let t0 = Instant::now();
    let mut lat = vec![];
    let mut k = 1u64;
    for _ in 0..3000 {
        k = k
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407)
            % next_doc.max(1);
        let interval = Interval {
            start: StartIncluded(BinaryKey::from(k.to_be_bytes().to_vec())),
            end: End::Unbounded,
        };
        let t = Instant::now();
        let rows: Vec<(IndexKeyBytes, _)> = solo
            .index_scan(
                IndexId(InternalId::from([INDEX; 16])),
                TabletId(InternalId::from([TABLET; 16])),
                Timestamp::try_from(u64::MAX >> 2).unwrap(),
                &interval,
                Order::Asc,
                a.scan,
                Arc::new(NoopRetentionValidator),
            )
            .take(a.scan)
            .try_collect()
            .await?;
        assert!(!rows.is_empty());
        lat.push(t.elapsed());
    }
    let wall = t0.elapsed();
    let cpu = cpu_secs()? - cpu0;
    println!(
        "{arm} solo read: {} | {:.0} scans/s | cpu {:.3} ms/scan",
        percentiles(lat),
        3000.0 / wall.as_secs_f64(),
        cpu * 1e3 / 3000.0
    );

    // Phase 2: readers scanning while one writer keeps committing.
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let total_docs = next_doc;
    let reader_p = p.reader();
    let mut readers = vec![];
    for r in 0..a.readers {
        let reader = reader_p.clone();
        let stop = stop.clone();
        let reads = reads.clone();
        let scan = a.scan;
        readers.push(tokio::spawn(async move {
            let mut lat = vec![];
            let mut k = r as u64 * 7919;
            while !stop.load(Ordering::Relaxed) {
                k = k
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407)
                    % total_docs.max(1);
                let start = k.to_be_bytes().to_vec();
                let interval = Interval {
                    start: StartIncluded(BinaryKey::from(start)),
                    end: End::Unbounded,
                };
                let t = Instant::now();
                let rows: Vec<(IndexKeyBytes, _)> = reader
                    .index_scan(
                        IndexId(InternalId::from([INDEX; 16])),
                        TabletId(InternalId::from([TABLET; 16])),
                        Timestamp::try_from(u64::MAX >> 2).unwrap(),
                        &interval,
                        Order::Asc,
                        scan,
                        Arc::new(NoopRetentionValidator),
                    )
                    .take(scan)
                    .try_collect()
                    .await
                    .unwrap();
                assert!(!rows.is_empty());
                lat.push(t.elapsed());
                reads.fetch_add(1, Ordering::Relaxed);
            }
            lat
        }));
    }
    let writer_p = p.clone();
    let writer_stop = stop.clone();
    let docs = a.docs;
    let writer = tokio::spawn(async move {
        let mut lat = vec![];
        let mut ts = ts;
        let mut next_doc = next_doc;
        while !writer_stop.load(Ordering::Relaxed) {
            let (d, i) = batch(ts, next_doc, docs).unwrap();
            let t = Instant::now();
            writer_p
                .write(&d, &i, ConflictStrategy::Error)
                .await
                .unwrap();
            lat.push(t.elapsed());
            ts += 1;
            next_doc += docs as u64;
        }
        lat
    });
    tokio::time::sleep(Duration::from_secs(a.seconds)).await;
    stop.store(true, Ordering::Relaxed);
    let mut read_lat = vec![];
    for r in readers {
        read_lat.extend(r.await?);
    }
    let write_lat = writer.await?;
    println!(
        "{arm} mixed read : {} | {:.0} scans/s",
        percentiles(read_lat),
        reads.load(Ordering::Relaxed) as f64 / a.seconds as f64
    );
    println!("{arm} mixed write: {}", percentiles(write_lat));
    let stat = std::fs::read_to_string("/proc/self/stat")?;
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .collect();
    let ticks: f64 = fields[11].parse::<f64>()? + fields[12].parse::<f64>()?;
    println!("{arm} process cpu: {:.2}s", ticks / 100.0);
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let a = args();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    rt.block_on(async {
        for arm in a.arms.clone() {
            run_arm(&a, &arm).await?;
        }
        Ok(())
    })
}
