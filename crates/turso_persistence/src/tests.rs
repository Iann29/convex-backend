use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    sync::Arc,
    time::Duration,
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
        DocumentPrevTsQuery,
        LatestDocument,
        NoopRetentionValidator,
        Persistence,
        PersistenceIndexEntry,
        PersistenceReader,
        TimestampRange,
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
use futures::TryStreamExt;
use sqlite::SqlitePersistence;

use super::{
    TursoOptions,
    TursoPersistence,
};

/// One indexed key and its history: `(ts, Some(v))` is a live write of
/// value `v`, `(ts, None)` is a tombstone.
struct KeySpec {
    key: Vec<u8>,
    doc_n: u8,
    versions: Vec<(u64, Option<i64>)>,
}

struct Fixture {
    persistence: TursoPersistence,
    index_id: IndexId,
    tablet_id: TabletId,
    table_number: TableNumber,
    // Kept alive so the database file outlives the test body.
    _dir: tempfile::TempDir,
}

fn internal_id(n: u8) -> InternalId {
    InternalId::from([n; 16])
}

fn make_doc(
    tablet_id: TabletId,
    table_number: TableNumber,
    doc_n: u8,
    v: i64,
) -> anyhow::Result<ResolvedDocument> {
    let id = ResolvedDocumentId::new(
        tablet_id,
        DeveloperDocumentId::new(table_number, internal_id(doc_n)),
    );
    ResolvedDocument::new(id, CreationTime::try_from(1234.5)?, obj!("v" => v)?)
}

fn fixture_rows(
    specs: &[KeySpec],
    index_id: IndexId,
    tablet_id: TabletId,
    table_number: TableNumber,
) -> anyhow::Result<(Vec<DocumentLogEntry>, Vec<PersistenceIndexEntry>)> {
    let mut documents = vec![];
    let mut indexes = vec![];
    for spec in specs {
        let doc_id = InternalDocumentId::new(tablet_id, internal_id(spec.doc_n));
        for &(ts, v) in &spec.versions {
            let ts = Timestamp::try_from(ts)?;
            let value = v
                .map(|v| make_doc(tablet_id, table_number, spec.doc_n, v))
                .transpose()?;
            documents.push(DocumentLogEntry {
                ts,
                id: doc_id,
                value,
                prev_ts: None,
            });
            indexes.push(PersistenceIndexEntry {
                ts,
                index_id,
                key: IndexKeyBytes(spec.key.clone()),
                value: v.map(|_| doc_id),
            });
        }
    }
    Ok((documents, indexes))
}

async fn make_fixture(specs: &[KeySpec]) -> anyhow::Result<Fixture> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("test.sqlite3");
    let persistence = TursoPersistence::new(path.to_str().unwrap()).await?;
    let index_id = IndexId(internal_id(255));
    let tablet_id = TabletId(internal_id(254));
    let table_number = TableNumber::try_from(1)?;
    let (documents, indexes) = fixture_rows(specs, index_id, tablet_id, table_number)?;
    persistence
        .write(&documents, &indexes, ConflictStrategy::Error)
        .await?;
    Ok(Fixture {
        persistence,
        index_id,
        tablet_id,
        table_number,
        _dir: dir,
    })
}

fn make_interval(start: Vec<u8>, end: Option<Vec<u8>>) -> Interval {
    Interval {
        start: StartIncluded(BinaryKey::from(start)),
        end: match end {
            Some(end) => End::Excluded(BinaryKey::from(end)),
            None => End::Unbounded,
        },
    }
}

/// The reference implementation: latest visible version per key at
/// `read_ts`, tombstones dropped, interval applied, in scan order.
fn expected_scan(
    tablet_id: TabletId,
    table_number: TableNumber,
    specs: &[KeySpec],
    read_ts: u64,
    interval: &Interval,
    order: Order,
) -> anyhow::Result<Vec<(IndexKeyBytes, LatestDocument)>> {
    let mut rows = vec![];
    for spec in specs {
        if !interval.contains(&spec.key) {
            continue;
        }
        let visible = spec
            .versions
            .iter()
            .filter(|(ts, _)| *ts <= read_ts)
            .max_by_key(|(ts, _)| *ts);
        let Some(&(ts, Some(v))) = visible else {
            continue;
        };
        rows.push((
            IndexKeyBytes(spec.key.clone()),
            LatestDocument {
                ts: Timestamp::try_from(ts)?,
                value: make_doc(tablet_id, table_number, spec.doc_n, v)?,
                prev_ts: None,
            },
        ));
    }
    rows.sort_by(|(a, _), (b, _)| a.cmp(b));
    if order == Order::Desc {
        rows.reverse();
    }
    Ok(rows)
}

async fn scan(
    persistence: &dyn PersistenceReader,
    index_id: IndexId,
    tablet_id: TabletId,
    read_ts: u64,
    interval: &Interval,
    order: Order,
    size_hint: usize,
) -> anyhow::Result<Vec<(IndexKeyBytes, LatestDocument)>> {
    persistence
        .index_scan(
            index_id,
            tablet_id,
            Timestamp::try_from(read_ts)?,
            interval,
            order,
            size_hint,
            Arc::new(NoopRetentionValidator),
        )
        .try_collect()
        .await
}

async fn run_scan(
    fixture: &Fixture,
    read_ts: u64,
    interval: &Interval,
    order: Order,
    size_hint: usize,
) -> anyhow::Result<Vec<(IndexKeyBytes, LatestDocument)>> {
    scan(
        &fixture.persistence,
        fixture.index_id,
        fixture.tablet_id,
        read_ts,
        interval,
        order,
        size_hint,
    )
    .await
}

fn scan_specs() -> Vec<KeySpec> {
    (0u8..25)
        .map(|k| {
            let mut versions = vec![(10, Some(1))];
            if k % 3 == 0 {
                versions.push((20, Some(2)));
            }
            if k % 5 == 0 {
                versions.push((30, None));
            }
            if k % 2 == 0 {
                versions.push((40, Some(3)));
            }
            KeySpec {
                key: vec![k],
                doc_n: k,
                versions,
            }
        })
        .collect()
}

/// 25 keys with updates, tombstones on page boundaries, and writes past
/// the read snapshot, scanned under every pagination regime: the
/// paginated scan must be indistinguishable from the one-shot scan.
#[tokio::test]
async fn paginated_scan_matches_reference() -> anyhow::Result<()> {
    let specs = scan_specs();
    let fixture = make_fixture(&specs).await?;

    let intervals = [
        make_interval(vec![], None),
        make_interval(vec![3], Some(vec![20])),
    ];
    // At ts 35 the tombstones (ts 30) are the latest visible versions;
    // at ts 45 the ts-40 writes resurrect the even keys among them.
    for read_ts in [35, 45] {
        for interval in &intervals {
            for order in [Order::Asc, Order::Desc] {
                let expected = expected_scan(
                    fixture.tablet_id,
                    fixture.table_number,
                    &specs,
                    read_ts,
                    interval,
                    order,
                )?;
                for size_hint in [0, 1, 2, 3, 10_000] {
                    let got = run_scan(&fixture, read_ts, interval, order, size_hint).await?;
                    assert_eq!(
                        got, expected,
                        "scan mismatch at read_ts={read_ts} order={order:?} size_hint={size_hint}"
                    );
                }
            }
        }
    }
    Ok(())
}

/// Ten consecutive tombstoned keys form entire pages with no live rows.
/// A scan that terminates on "page yielded fewer live rows than
/// requested" would stop in the middle of the interval.
#[tokio::test]
async fn page_of_tombstones_does_not_terminate_scan() -> anyhow::Result<()> {
    let specs: Vec<KeySpec> = (0u8..30)
        .map(|k| {
            let mut versions = vec![(10, Some(1))];
            if (10..20).contains(&k) {
                versions.push((20, None));
            }
            KeySpec {
                key: vec![k],
                doc_n: k,
                versions,
            }
        })
        .collect();
    let fixture = make_fixture(&specs).await?;

    let interval = make_interval(vec![], None);
    for order in [Order::Asc, Order::Desc] {
        let expected = expected_scan(
            fixture.tablet_id,
            fixture.table_number,
            &specs,
            25,
            &interval,
            order,
        )?;
        assert_eq!(expected.len(), 20);
        let got = run_scan(&fixture, 25, &interval, order, 5).await?;
        assert_eq!(got, expected, "tombstone run must not end the scan early");
    }
    Ok(())
}

#[tokio::test]
async fn empty_table_and_empty_range() -> anyhow::Result<()> {
    let empty = make_fixture(&[]).await?;
    let all = make_interval(vec![], None);
    assert!(run_scan(&empty, 100, &all, Order::Asc, 3).await?.is_empty());

    let specs: Vec<KeySpec> = (0u8..5)
        .map(|k| KeySpec {
            key: vec![k],
            doc_n: k,
            versions: vec![(10, Some(1))],
        })
        .collect();
    let fixture = make_fixture(&specs).await?;
    let out_of_range = make_interval(vec![200], Some(vec![201]));
    assert!(run_scan(&fixture, 100, &out_of_range, Order::Asc, 3)
        .await?
        .is_empty());
    Ok(())
}

/// One row of the documents log for the `load_documents` tests:
/// `value: None` is a delete entry, which the log emits as-is.
struct LogEntrySpec {
    ts: u64,
    tablet_n: u8,
    doc_n: u8,
    value: Option<i64>,
}

async fn make_log_fixture(
    specs: &[LogEntrySpec],
) -> anyhow::Result<(TursoPersistence, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("test.sqlite3");
    let persistence = TursoPersistence::new(path.to_str().unwrap()).await?;
    let documents = specs
        .iter()
        .map(log_entry)
        .collect::<anyhow::Result<Vec<_>>>()?;
    persistence
        .write(&documents, &[], ConflictStrategy::Error)
        .await?;
    Ok((persistence, dir))
}

fn log_entry(spec: &LogEntrySpec) -> anyhow::Result<DocumentLogEntry> {
    let tablet_id = TabletId(internal_id(spec.tablet_n));
    let table_number = TableNumber::try_from(1)?;
    let value = spec
        .value
        .map(|v| make_doc(tablet_id, table_number, spec.doc_n, v))
        .transpose()?;
    Ok(DocumentLogEntry {
        ts: Timestamp::try_from(spec.ts)?,
        id: InternalDocumentId::new(tablet_id, internal_id(spec.doc_n)),
        value,
        prev_ts: None,
    })
}

/// The reference implementation: every log row in the timestamp range,
/// deletes included, in `(ts, table_id, id)` scan order.
fn expected_log(
    specs: &[LogEntrySpec],
    range: &TimestampRange,
    order: Order,
) -> anyhow::Result<Vec<DocumentLogEntry>> {
    let mut rows = vec![];
    for spec in specs {
        let ts = Timestamp::try_from(spec.ts)?;
        if ts < range.min_timestamp_inclusive() || ts >= range.max_timestamp_exclusive() {
            continue;
        }
        let entry = log_entry(spec)?;
        let key = (
            spec.ts,
            entry.id.table().0[..].to_vec(),
            entry.id.internal_id()[..].to_vec(),
        );
        rows.push((key, entry));
    }
    rows.sort_by(|(a, _), (b, _)| a.cmp(b));
    if order == Order::Desc {
        rows.reverse();
    }
    Ok(rows.into_iter().map(|(_, entry)| entry).collect())
}

async fn run_load(
    persistence: &dyn PersistenceReader,
    range: TimestampRange,
    order: Order,
    page_size: u32,
) -> anyhow::Result<Vec<DocumentLogEntry>> {
    persistence
        .load_documents(range, order, page_size, Arc::new(NoopRetentionValidator))
        .try_collect()
        .await
}

fn log_specs() -> Vec<LogEntrySpec> {
    let mut specs = vec![];
    for tablet_n in [1u8, 2] {
        for doc_n in 0u8..6 {
            specs.push(LogEntrySpec {
                ts: 10,
                tablet_n,
                doc_n,
                value: Some(1),
            });
            if doc_n % 2 == 0 {
                specs.push(LogEntrySpec {
                    ts: 20,
                    tablet_n,
                    doc_n,
                    value: Some(2),
                });
            }
            if doc_n % 3 == 0 {
                specs.push(LogEntrySpec {
                    ts: 30,
                    tablet_n,
                    doc_n,
                    value: None,
                });
            }
            if doc_n % 2 == 1 {
                specs.push(LogEntrySpec {
                    ts: 40,
                    tablet_n,
                    doc_n,
                    value: Some(3),
                });
            }
        }
    }
    specs
}

/// Two tablets writing interleaved updates and deletes, with several rows
/// sharing the same timestamp: the compound cursor's `(table_id, id)`
/// tiebreak keeps pagination correct inside a same-`ts` run.
#[tokio::test]
async fn paginated_load_matches_reference() -> anyhow::Result<()> {
    let specs = log_specs();
    let (persistence, _dir) = make_log_fixture(&specs).await?;

    let ranges = [
        TimestampRange::all(),
        TimestampRange::new(Timestamp::try_from(15u64)?..Timestamp::try_from(35u64)?),
    ];
    for range in &ranges {
        for order in [Order::Asc, Order::Desc] {
            let expected = expected_log(&specs, range, order)?;
            for page_size in [0, 1, 2, 3, 10_000] {
                let got = run_load(&persistence, *range, order, page_size).await?;
                assert_eq!(
                    got, expected,
                    "load mismatch at range={range:?} order={order:?} page_size={page_size}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn same_ts_run_spans_pages() -> anyhow::Result<()> {
    let specs: Vec<LogEntrySpec> = (0u8..10)
        .map(|doc_n| LogEntrySpec {
            ts: 17,
            tablet_n: 1,
            doc_n,
            value: Some(i64::from(doc_n)),
        })
        .collect();
    let (persistence, _dir) = make_log_fixture(&specs).await?;

    let range = TimestampRange::all();
    for order in [Order::Asc, Order::Desc] {
        let expected = expected_log(&specs, &range, order)?;
        assert_eq!(expected.len(), 10);
        let got = run_load(&persistence, range, order, 3).await?;
        assert_eq!(got, expected, "same-ts run must paginate by (table_id, id)");
    }
    Ok(())
}

#[tokio::test]
async fn paginated_table_load_matches_reference() -> anyhow::Result<()> {
    let mut specs = vec![];
    for tablet_n in [1u8, 2, 3] {
        for doc_n in 0u8..6 {
            for ts in [10, 20, 30] {
                specs.push(LogEntrySpec {
                    ts,
                    tablet_n,
                    doc_n,
                    value: (ts != 30).then_some(i64::from(doc_n)),
                });
            }
        }
    }
    let (persistence, _dir) = make_log_fixture(&specs).await?;
    for range in [
        TimestampRange::all(),
        TimestampRange::new(Timestamp::try_from(15u64)?..Timestamp::try_from(30u64)?),
    ] {
        for order in [Order::Asc, Order::Desc] {
            for tablet_n in [1, 2, 3, 4] {
                let tablet_id = TabletId(internal_id(tablet_n));
                let expected: Vec<_> = expected_log(&specs, &range, order)?
                    .into_iter()
                    .filter(|entry| entry.id.table() == tablet_id)
                    .collect();
                for page_size in [0, 1, 2, 3, 10_000] {
                    let got: Vec<_> = persistence
                        .load_documents_from_table(
                            tablet_id,
                            range,
                            order,
                            page_size,
                            Arc::new(NoopRetentionValidator),
                        )
                        .try_collect()
                        .await?;
                    assert_eq!(
                        got, expected,
                        "table={tablet_n} range={range:?} order={order:?} page_size={page_size}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn table_load_does_not_decode_other_tables() -> anyhow::Result<()> {
    let specs: Vec<_> = [1u8, 2]
        .into_iter()
        .map(|tablet_n| LogEntrySpec {
            ts: 10,
            tablet_n,
            doc_n: 1,
            value: Some(1),
        })
        .collect();
    let (persistence, _dir) = make_log_fixture(&specs).await?;
    let other_tablet = internal_id(2);
    persistence
        .inner
        .writer
        .lock()
        .await
        .execute(
            "UPDATE documents SET json_value = 'invalid json' WHERE table_id = ?1",
            vec![crate::engine::blob_param(&other_tablet[..])],
        )
        .await?;

    let got: Vec<_> = persistence
        .load_documents_from_table(
            TabletId(internal_id(1)),
            TimestampRange::all(),
            Order::Asc,
            1,
            Arc::new(NoopRetentionValidator),
        )
        .try_collect()
        .await?;
    assert_eq!(got, vec![log_entry(&specs[0])?]);
    assert!(run_load(&persistence, TimestampRange::all(), Order::Asc, 1)
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
async fn load_empty_and_out_of_range() -> anyhow::Result<()> {
    let (empty, _dir) = make_log_fixture(&[]).await?;
    assert!(run_load(&empty, TimestampRange::all(), Order::Asc, 2)
        .await?
        .is_empty());

    let specs: Vec<LogEntrySpec> = (0u8..5)
        .map(|doc_n| LogEntrySpec {
            ts: 10,
            tablet_n: 1,
            doc_n,
            value: Some(1),
        })
        .collect();
    let (persistence, _dir) = make_log_fixture(&specs).await?;
    let out_of_range =
        TimestampRange::new(Timestamp::try_from(100u64)?..Timestamp::try_from(200u64)?);
    assert!(run_load(&persistence, out_of_range, Order::Asc, 2)
        .await?
        .is_empty());
    Ok(())
}

#[tokio::test]
async fn revision_batches_rebind_ids_timestamps_and_tombstones() -> anyhow::Result<()> {
    let mut specs = vec![];
    for tablet_n in [1, 2] {
        for doc_n in 1..=4 {
            for ts in [10, 20, 30] {
                specs.push(LogEntrySpec {
                    ts,
                    tablet_n,
                    doc_n,
                    value: (ts != 20).then_some(i64::from(doc_n)),
                });
            }
        }
    }
    let (persistence, _dir) = make_log_fixture(&specs).await?;
    let mut previous = BTreeSet::new();
    let mut exact = BTreeSet::new();
    let mut expected_previous = BTreeMap::new();
    let mut expected_exact = BTreeMap::new();
    for spec in &specs {
        let entry = log_entry(spec)?;
        let id = entry.id;
        let query_ts = Timestamp::try_from(spec.ts + 1)?;
        let key = (entry.id, query_ts);
        previous.insert(key);
        expected_previous.insert(key, entry.clone());
        let key = DocumentPrevTsQuery {
            id: entry.id,
            ts: query_ts,
            prev_ts: entry.ts,
        };
        exact.insert(key);
        expected_exact.insert(key, entry);
        // Misses interleaved with hits must not reuse a preceding row.
        let missing = Timestamp::try_from(1u64)?;
        previous.insert((id, missing));
        exact.insert(DocumentPrevTsQuery {
            id,
            ts: query_ts,
            prev_ts: missing,
        });
    }
    let validator = Arc::new(NoopRetentionValidator);
    assert_eq!(
        persistence
            .previous_revisions(previous, validator.clone())
            .await?,
        expected_previous
    );
    assert_eq!(
        persistence
            .previous_revisions_of_documents(exact, validator)
            .await?,
        expected_exact
    );
    assert!(persistence
        .previous_revisions(BTreeSet::new(), Arc::new(NoopRetentionValidator))
        .await?
        .is_empty());
    assert!(persistence
        .previous_revisions_of_documents(BTreeSet::new(), Arc::new(NoopRetentionValidator))
        .await?
        .is_empty());
    Ok(())
}

/// A write that fails midway — the second document collides with the first
/// under `ConflictStrategy::Error` — commits nothing, leaves the writer
/// usable, and nothing of it survives a reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn failed_write_rolls_back_before_reuse_and_reopen() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("rollback.sqlite3");
    let path = path.to_str().unwrap();
    let persistence = TursoPersistence::new(path).await?;
    let entry = log_entry(&LogEntrySpec {
        ts: 10,
        tablet_n: 1,
        doc_n: 1,
        value: Some(1),
    })?;
    let result = persistence
        .write(
            &[entry.clone(), entry.clone()],
            &[],
            ConflictStrategy::Error,
        )
        .await;
    assert!(result.is_err(), "duplicate primary key must fail the write");
    assert!(persistence.inner.writer.lock().await.is_autocommit());
    assert!(
        run_load(&persistence, TimestampRange::all(), Order::Asc, 10)
            .await?
            .is_empty()
    );
    persistence
        .write(std::slice::from_ref(&entry), &[], ConflictStrategy::Error)
        .await?;
    drop(persistence);
    let reopened = TursoPersistence::new(path).await?;
    assert!(!reopened.is_fresh());
    assert_eq!(
        run_load(&reopened, TimestampRange::all(), Order::Asc, 10).await?,
        vec![entry]
    );
    Ok(())
}

/// A caller that stops polling a write does not decide its outcome: the
/// write runs to completion, and the next write finds the writer idle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_write_runs_to_completion() -> anyhow::Result<()> {
    let (persistence, _dir) = make_log_fixture(&[]).await?;
    let entries = (0u8..200)
        .map(|doc_n| {
            log_entry(&LogEntrySpec {
                ts: 10,
                tablet_n: 1,
                doc_n,
                value: Some(1),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let writer = persistence.clone();
    let batch = entries.clone();
    let task =
        tokio::spawn(async move { writer.write(&batch, &[], ConflictStrategy::Error).await });
    tokio::task::yield_now().await;
    task.abort();
    let _ = task.await;
    // Taking the writer waits for the detached write to finish.
    drop(persistence.inner.writer.lock().await);
    let got = run_load(&persistence, TimestampRange::all(), Order::Asc, 1000).await?;
    assert!(
        got.is_empty() || got == entries,
        "a write is all or nothing, got {} rows",
        got.len()
    );
    let next = log_entry(&LogEntrySpec {
        ts: 20,
        tablet_n: 1,
        doc_n: 1,
        value: Some(2),
    })?;
    persistence
        .write(std::slice::from_ref(&next), &[], ConflictStrategy::Error)
        .await?;
    Ok(())
}

/// Reads are served while the writer sits inside an open transaction, and
/// they see the last committed state, not the uncommitted rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_proceed_while_writer_is_in_a_transaction() -> anyhow::Result<()> {
    let specs = log_specs();
    let (persistence, _dir) = make_log_fixture(&specs).await?;
    let mut writer = persistence.inner.writer.lock().await;
    writer.execute("BEGIN", vec![]).await?;
    writer
        .execute(
            "INSERT INTO documents VALUES (?1, 99, ?2, NULL, 1, NULL)",
            vec![
                crate::engine::blob_param(&internal_id(9)[..]),
                crate::engine::blob_param(&internal_id(1)[..]),
            ],
        )
        .await?;
    let read = tokio::time::timeout(
        Duration::from_secs(5),
        run_load(&persistence, TimestampRange::all(), Order::Asc, 7),
    )
    .await
    .expect("read must not wait for the writer")?;
    assert_eq!(
        read,
        expected_log(&specs, &TimestampRange::all(), Order::Asc)?
    );
    writer.execute("ROLLBACK", vec![]).await?;
    Ok(())
}

/// The engine swap in both directions on the same file: a database written
/// by the `sqlite` crate opens under Turso with identical reads, and what
/// Turso writes on top is read back by the `sqlite` crate. This is both the
/// migration and the rollback path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_file_round_trips_between_engines() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let path = path.to_str().unwrap();
    let index_id = IndexId(internal_id(255));
    let tablet_id = TabletId(internal_id(254));
    let table_number = TableNumber::try_from(1)?;
    let specs = scan_specs();
    let (documents, indexes) = fixture_rows(&specs, index_id, tablet_id, table_number)?;
    let (before, after) = documents.split_at(documents.len() / 2);
    let (index_before, index_after) = indexes.split_at(indexes.len() / 2);

    let legacy = SqlitePersistence::new(path)?;
    legacy
        .write(before, index_before, ConflictStrategy::Error)
        .await?;
    let legacy_log = run_load(&legacy, TimestampRange::all(), Order::Asc, 1000).await?;
    drop(legacy);

    let turso = TursoPersistence::new(path).await?;
    assert!(!turso.is_fresh());
    assert_eq!(
        run_load(&turso, TimestampRange::all(), Order::Asc, 1000).await?,
        legacy_log
    );
    turso
        .write(after, index_after, ConflictStrategy::Error)
        .await?;
    let all = make_interval(vec![], None);
    let turso_scan = scan(&turso, index_id, tablet_id, 45, &all, Order::Asc, 7).await?;
    let turso_log = run_load(&turso, TimestampRange::all(), Order::Asc, 1000).await?;
    assert_eq!(turso_log.len(), documents.len());
    drop(turso);

    let back = SqlitePersistence::new(path)?;
    assert_eq!(
        scan(&back, index_id, tablet_id, 45, &all, Order::Asc, 7).await?,
        turso_scan
    );
    assert_eq!(
        turso_scan,
        expected_scan(tablet_id, table_number, &specs, 45, &all, Order::Asc)?
    );
    assert_eq!(
        run_load(&back, TimestampRange::all(), Order::Asc, 1000).await?,
        turso_log
    );
    Ok(())
}

/// Keys with many more revisions than a page holds, interleaved with
/// single-revision keys and tombstones: every page boundary lands inside a
/// key's history, in both orders, at read timestamps that cut the histories
/// at different points.
#[tokio::test]
async fn revision_runs_longer_than_a_page() -> anyhow::Result<()> {
    let specs: Vec<KeySpec> = (0u8..12)
        .map(|k| {
            let versions = if k % 3 == 0 {
                (1..=11u64)
                    .map(|v| (v * 10, (v % 4 != 0).then_some(v as i64)))
                    .collect()
            } else {
                vec![(5 + u64::from(k), Some(i64::from(k)))]
            };
            KeySpec {
                key: vec![k],
                doc_n: k,
                versions,
            }
        })
        .collect();
    let fixture = make_fixture(&specs).await?;
    let intervals = [
        make_interval(vec![], None),
        make_interval(vec![2], Some(vec![10])),
    ];
    for read_ts in [9, 35, 40, 75, 200] {
        for interval in &intervals {
            for order in [Order::Asc, Order::Desc] {
                let expected = expected_scan(
                    fixture.tablet_id,
                    fixture.table_number,
                    &specs,
                    read_ts,
                    interval,
                    order,
                )?;
                for size_hint in [1, 2, 3, 4, 7, 100] {
                    let got = run_scan(&fixture, read_ts, interval, order, size_hint).await?;
                    assert_eq!(
                        got, expected,
                        "read_ts={read_ts} order={order:?} size_hint={size_hint}"
                    );
                }
            }
        }
    }
    Ok(())
}

fn one_reader() -> TursoOptions {
    TursoOptions {
        readers: 1,
        cache_kib: 2048,
        wal: super::WalPolicy::default(),
    }
}

fn entry_at(ts: u64, doc_n: u8) -> anyhow::Result<DocumentLogEntry> {
    log_entry(&LogEntrySpec {
        ts,
        tablet_n: 1,
        doc_n,
        value: Some(i64::from(doc_n)),
    })
}

/// A read abandoned mid-statement — its future dropped between two steps —
/// must not hand its connection back to the pool: the unfinished statement
/// keeps a read snapshot, and every later read on that connection would miss
/// newer commits.
#[tokio::test]
async fn interrupted_read_does_not_leave_a_stale_snapshot() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), one_reader()).await?;
    let first = entry_at(10, 1)?;
    persistence
        .write(std::slice::from_ref(&first), &[], ConflictStrategy::Error)
        .await?;
    {
        let mut reader = persistence.inner.readers.get("test").await?;
        assert!(reader.start_and_abandon("SELECT id FROM documents").await?);
    }
    let second = entry_at(20, 2)?;
    persistence
        .write(std::slice::from_ref(&second), &[], ConflictStrategy::Error)
        .await?;
    assert_eq!(
        run_load(&persistence, TimestampRange::all(), Order::Asc, 10).await?,
        vec![first, second]
    );
    Ok(())
}

/// The same on the writer: a statement left mid-step on it is reset before
/// the next write transaction, which commits and is visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_writer_statement_is_recovered() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), one_reader()).await?;
    let first = entry_at(10, 1)?;
    persistence
        .write(std::slice::from_ref(&first), &[], ConflictStrategy::Error)
        .await?;
    {
        let mut writer = persistence.inner.writer.lock().await;
        assert!(writer.start_and_abandon("SELECT id FROM documents").await?);
        assert!(!writer.is_clean());
    }
    let second = entry_at(20, 2)?;
    persistence
        .write(std::slice::from_ref(&second), &[], ConflictStrategy::Error)
        .await?;
    assert!(persistence.inner.writer.lock().await.is_clean());
    assert_eq!(
        run_load(&persistence, TimestampRange::all(), Order::Asc, 10).await?,
        vec![first, second]
    );
    Ok(())
}

/// Reopening checkpoints the WAL into the database file: the file is whole
/// on its own again, and nothing committed is lost.
#[tokio::test]
async fn reopen_checkpoints_the_wal() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let wal = dir.path().join("db.sqlite3-wal");
    let path = path.to_str().unwrap();
    let entries = (0u8..50)
        .map(|n| entry_at(10 + u64::from(n), n))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let persistence = TursoPersistence::with_options(path, one_reader()).await?;
    persistence
        .write(&entries, &[], ConflictStrategy::Error)
        .await?;
    drop(persistence);
    assert!(std::fs::metadata(&wal).map_or(0, |m| m.len()) > 0);
    let reopened = TursoPersistence::with_options(path, one_reader()).await?;
    assert_eq!(std::fs::metadata(&wal).map_or(0, |m| m.len()), 0);
    assert_eq!(
        run_load(&reopened, TimestampRange::all(), Order::Asc, 100).await?,
        entries
    );
    Ok(())
}

#[tokio::test]
async fn zero_readers_is_refused() {
    let options = TursoOptions {
        readers: 0,
        ..one_reader()
    };
    assert!(TursoPersistence::with_options(":memory:", options)
        .await
        .is_err());
}

/// A C7-era file (rollback journal) whose process died mid-transaction has a
/// hot `-journal`: the database file holds some of the transaction's pages
/// and the journal holds the originals. Turso must see only committed data,
/// and the journal must be gone before Turso writes anything.
#[tokio::test]
async fn hot_rollback_journal_is_rolled_back_before_turso_opens() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let crashed = dir.path().join("crashed.sqlite3");
    let path_str = path.to_str().unwrap();
    let entries = (0u8..200)
        .map(|n| entry_at(10 + u64::from(n), n))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let legacy = SqlitePersistence::with_journal(path_str, "delete", "full")?;
    legacy.write(&entries, &[], ConflictStrategy::Error).await?;
    drop(legacy);

    // A tiny cache makes SQLite spill the half-done UPDATE into the file,
    // after the journal holding the original pages was synced.
    let connection = rusqlite::Connection::open(&path)?;
    connection.pragma_update(None, "cache_size", 2)?;
    connection
        .execute_batch("BEGIN; UPDATE documents SET json_value = '{\"torn\":' || ts || '}';")?;
    let journal = dir.path().join("db.sqlite3-journal");
    assert!(
        std::fs::metadata(&journal)?.len() > 0,
        "the transaction must be journaled"
    );
    // Copy both files as the crash leaves them.
    std::fs::copy(&path, &crashed)?;
    std::fs::copy(&journal, dir.path().join("crashed.sqlite3-journal"))?;
    connection.execute_batch("ROLLBACK")?;
    drop(connection);

    let turso = TursoPersistence::with_options(crashed.to_str().unwrap(), one_reader()).await?;
    assert_eq!(
        run_load(&turso, TimestampRange::all(), Order::Asc, 1000).await?,
        entries
    );
    assert!(!dir.path().join("crashed.sqlite3-journal").exists());
    Ok(())
}

/// Point reads (a singleton interval, as `db.get` issues) return each key's
/// latest revision at the read timestamp, including across long histories
/// and tombstones, whatever the order and page size.
#[tokio::test]
async fn point_reads_take_the_latest_revision() -> anyhow::Result<()> {
    let specs: Vec<KeySpec> = (0u8..6)
        .map(|k| KeySpec {
            key: vec![k],
            doc_n: k,
            versions: (1..=9u64)
                .map(|v| (v * 10, (v % 3 != 0).then_some(v as i64)))
                .collect(),
        })
        .collect();
    let fixture = make_fixture(&specs).await?;
    for k in [0u8, 3, 5, 9] {
        let interval = Interval::singleton(BinaryKey::from(vec![k]));
        assert_eq!(
            interval.is_singleton().map(|key| key.to_vec()),
            Some(vec![k])
        );
        for read_ts in [5, 10, 25, 30, 35, 90, 200] {
            for order in [Order::Asc, Order::Desc] {
                let expected = expected_scan(
                    fixture.tablet_id,
                    fixture.table_number,
                    &specs,
                    read_ts,
                    &interval,
                    order,
                )?;
                for size_hint in [1, 2, 100] {
                    let got = run_scan(&fixture, read_ts, &interval, order, size_hint).await?;
                    assert_eq!(got, expected, "key={k} read_ts={read_ts} order={order:?}");
                }
            }
        }
    }
    Ok(())
}

/// No page query may sort: every shape of the documents-log query and both
/// index-scan orders must stream from an index. A sorter here sorts the
/// whole range (every revision of a table) on every page.
#[tokio::test]
async fn page_queries_never_sort() -> anyhow::Result<()> {
    // The planner weighs table and index sizes: with small or few documents
    // the primary key looks cheapest either way and hides the bad plan.
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), one_reader()).await?;
    let table_number = TableNumber::try_from(1)?;
    let payload = "x".repeat(600);
    let entries = (0u64..5000)
        .map(|n| {
            let tablet_id = TabletId(internal_id((n % 5) as u8 + 1));
            let id = InternalId::from(u128::from(n).to_be_bytes());
            let document = ResolvedDocument::new(
                ResolvedDocumentId::new(tablet_id, DeveloperDocumentId::new(table_number, id)),
                CreationTime::try_from(1234.5)?,
                obj!("n" => n as i64, "payload" => payload.clone())?,
            )?;
            Ok(DocumentLogEntry {
                ts: Timestamp::try_from(10 + n)?,
                id: InternalDocumentId::new(tablet_id, id),
                value: Some(document),
                prev_ts: None,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    persistence
        .write(&entries, &[], ConflictStrategy::Error)
        .await?;
    // Plan against the schema as read back from the file, as production does.
    drop(persistence);
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), one_reader()).await?;
    let cursor = (5u64, vec![1u8; 16], vec![2u8; 16]);
    let mut shapes = vec![];
    for order in [Order::Asc, Order::Desc] {
        for with_cursor in [false, true] {
            for tablet in [None, Some(TabletId(internal_id(3)))] {
                let (sql, _) = super::load_documents_query(
                    order,
                    with_cursor.then_some(&cursor),
                    tablet,
                    TimestampRange::all(),
                    100,
                )?;
                shapes.push((
                    format!(
                        "load {order:?} cursor={with_cursor} table={}",
                        tablet.is_some()
                    ),
                    sql,
                ));
            }
        }
        let direction = if order == Order::Asc { "ASC" } else { "DESC" };
        shapes.push((
            format!("index scan {order:?}"),
            format!(
                "SELECT B.key, B.ts, B.document_id, C.table_id, C.json_value, C.prev_ts, \
                 B.deleted FROM indexes B LEFT JOIN documents C ON B.ts = C.ts AND B.table_id = \
                 C.table_id AND B.document_id = C.id WHERE B.index_id = ?1 AND B.ts <= ?2 AND \
                 B.key > ?3 AND B.key < ?4 ORDER BY B.key {direction}, B.ts {direction} LIMIT ?5"
            ),
        ));
    }
    let mut reader = persistence.inner.readers.get("test").await?;
    for (label, sql) in shapes {
        let opcodes = reader
            .query(&format!("EXPLAIN {sql}"), vec![], None, |row| {
                Ok(crate::engine::text(row.get_value(1))?.to_owned())
            })
            .await?;
        assert!(!opcodes.is_empty(), "{label}: no bytecode");
        assert!(
            opcodes.iter().all(|op| !op.contains("Sorter")),
            "{label} sorts: {opcodes:?}"
        );
    }
    Ok(())
}

/// Convex splits each retention chunk into parallel deletes. A commit that
/// arrives behind them must run right after the delete already waiting for
/// the writer, not after all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commits_go_ahead_of_queued_retention_deletes() -> anyhow::Result<()> {
    use futures::FutureExt as _;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), one_reader()).await?;
    let order = Arc::new(parking_lot::Mutex::new(vec![]));
    let request = |label: &'static str, lane: super::Lane| {
        let persistence = persistence.clone();
        let order = order.clone();
        tokio::spawn(async move {
            persistence
                .write_transaction("test", lane, move |_| {
                    async move {
                        order.lock().push(label);
                        Ok(())
                    }
                    .boxed()
                })
                .await
        })
    };
    // Hold the writer so that every request queues.
    let held = persistence.inner.writer.lock().await;
    let deletes: Vec<_> = ["delete 1", "delete 2", "delete 3"]
        .into_iter()
        .map(|label| request(label, super::Lane::Background))
        .collect();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let commit = request("commit", super::Lane::Foreground);
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(held);
    commit.await??;
    for delete in deletes {
        delete.await??;
    }
    // Which delete took the first turn depends on task scheduling; the
    // commit must come right after it.
    let order = order.lock().clone();
    assert_eq!(order.len(), 4, "{order:?}");
    assert_eq!(order[1], "commit", "{order:?}");
    Ok(())
}

/// Commits that never stop coming cannot starve retention: a background
/// write stands aside for at most `BACKGROUND_MAX_YIELD`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_deletes_still_run_under_steady_commits() -> anyhow::Result<()> {
    use futures::FutureExt as _;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), one_reader()).await?;
    let pending_commit = super::ForegroundGuard::new(&persistence.inner);
    let started = std::time::Instant::now();
    persistence
        .write_transaction("test", super::Lane::Background, |_| {
            async { Ok(()) }.boxed()
        })
        .await?;
    assert!(started.elapsed() >= super::BACKGROUND_MAX_YIELD);
    drop(pending_commit);
    Ok(())
}

/// turso_core shares one database per file within a process, so a second
/// persistence on an open file — under any spelling of its path — is
/// refused until the first one is gone.
#[tokio::test]
async fn one_persistence_per_file_per_process() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let first = TursoPersistence::with_options(path.to_str().unwrap(), one_reader()).await?;
    let same_file = dir.path().join(".").join("db.sqlite3");
    let Err(e) = TursoPersistence::with_options(same_file.to_str().unwrap(), one_reader()).await
    else {
        panic!("a second persistence opened the same file");
    };
    assert!(format!("{e:#}").contains("already open"), "{e:#}");
    drop(first);
    TursoPersistence::with_options(path.to_str().unwrap(), one_reader()).await?;
    Ok(())
}

/// The WAL a crash leaves behind is checkpointed into the database by SQLite
/// before Turso opens the file, so no misread of the WAL by Turso can lose
/// the commits in it.
#[tokio::test]
async fn crash_wal_is_checkpointed_by_sqlite_before_turso_opens() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let crashed = dir.path().join("crashed.sqlite3");
    let crashed_wal = dir.path().join("crashed.sqlite3-wal");
    let entries = (0u8..50)
        .map(|n| entry_at(10 + u64::from(n), n))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), one_reader()).await?;
    persistence
        .write(&entries, &[], ConflictStrategy::Error)
        .await?;
    // The crash image: the database file and its WAL as the process left them.
    std::fs::copy(&path, &crashed)?;
    std::fs::copy(dir.path().join("db.sqlite3-wal"), &crashed_wal)?;
    drop(persistence);
    assert!(std::fs::metadata(&crashed_wal)?.len() > super::WAL_HEADER_BYTES);

    super::recover_with_sqlite(crashed.to_str().unwrap())?;
    assert!(
        std::fs::metadata(&crashed_wal).map_or(0, |meta| meta.len()) <= super::WAL_HEADER_BYTES,
        "the commits must be in the database file, not left in the WAL"
    );
    let reopened = TursoPersistence::with_options(crashed.to_str().unwrap(), one_reader()).await?;
    assert_eq!(
        run_load(&reopened, TimestampRange::all(), Order::Asc, 100).await?,
        entries
    );
    Ok(())
}

/// `take(n)` reads index pages of n rows. A full ascending page settles the
/// key it was cut on with a point lookup, so the n keys come from one page
/// and nothing after it is read: the document right after the page cannot
/// even be decoded, and reading a second page would fail on it.
#[tokio::test]
async fn take_of_a_full_page_reads_one_page() -> anyhow::Result<()> {
    use futures::StreamExt as _;

    const N: u8 = 10;
    let specs: Vec<KeySpec> = (0..N + 5)
        .map(|k| KeySpec {
            key: vec![k],
            doc_n: k,
            versions: vec![(10 + u64::from(k), Some(i64::from(k)))],
        })
        .collect();
    let fixture = make_fixture(&specs).await?;
    fixture
        .persistence
        .inner
        .writer
        .lock()
        .await
        .execute(
            "UPDATE documents SET json_value = ?1 WHERE id = ?2",
            vec![
                crate::engine::blob_param(b"not text"),
                crate::engine::blob_param(&internal_id(N)[..]),
            ],
        )
        .await?;
    let taken: Vec<_> = fixture
        .persistence
        .index_scan(
            fixture.index_id,
            fixture.tablet_id,
            Timestamp::try_from(100u64)?,
            &Interval::all(),
            Order::Asc,
            usize::from(N),
            Arc::new(NoopRetentionValidator),
        )
        .take(usize::from(N))
        .try_collect()
        .await?;
    let keys: Vec<_> = taken.into_iter().map(|(key, _)| key.0).collect();
    assert_eq!(keys, (0..N).map(|k| vec![k]).collect::<Vec<_>>());
    Ok(())
}

/// Under reads that never stop Turso finds no instant to restart the WAL, and
/// the file grew without bound. The maintenance makes that instant: the WAL
/// stays near the hard size, and every commit is still there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_stays_bounded_under_continuous_reads() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let wal = dir.path().join("db.sqlite3-wal");
    let options = TursoOptions {
        readers: 2,
        wal: super::WalPolicy {
            period: Duration::from_millis(5),
            soft_bytes: 256 << 10,
            hard_bytes: 1 << 20,
            gate_deadline: Duration::from_millis(100),
        },
        ..one_reader()
    };
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), options).await?;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let persistence = persistence.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    run_load(&persistence, TimestampRange::all(), Order::Asc, 50).await?;
                }
                anyhow::Ok(())
            })
        })
        .collect();
    let mut written = vec![];
    let mut max_wal = 0;
    for batch in 0u64..600 {
        let entries = (0u8..20)
            .map(|n| entry_at(10 + batch * 20 + u64::from(n), n))
            .collect::<anyhow::Result<Vec<_>>>()?;
        persistence
            .write(&entries, &[], ConflictStrategy::Error)
            .await?;
        written.extend(entries);
        max_wal = max_wal.max(std::fs::metadata(&wal).map_or(0, |meta| meta.len()));
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for reader in readers {
        reader.await??;
    }
    assert!(max_wal < 4 << 20, "the WAL reached {max_wal} bytes");
    assert_eq!(
        run_load(&persistence, TimestampRange::all(), Order::Asc, 500).await?,
        written
    );
    Ok(())
}

/// Past the hard size the maintenance takes the writer and waits for the
/// reader pool, but only for a while: a long read makes it give up, not
/// stall the writer, and the next attempt truncates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_gate_gives_up_behind_a_long_read() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let wal = dir.path().join("db.sqlite3-wal");
    let options = TursoOptions {
        wal: super::WalPolicy {
            period: Duration::from_secs(3600),
            soft_bytes: 1024,
            hard_bytes: 2048,
            gate_deadline: Duration::from_millis(50),
        },
        ..one_reader()
    };
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), options).await?;
    let entries = (0u8..50)
        .map(|n| entry_at(10 + u64::from(n), n))
        .collect::<anyhow::Result<Vec<_>>>()?;
    persistence
        .write(&entries, &[], ConflictStrategy::Error)
        .await?;
    let before = std::fs::metadata(&wal)?.len();
    assert!(before > 2048);

    let long_read = persistence.inner.readers.get("test").await?;
    let started = std::time::Instant::now();
    persistence.inner.maintain_wal().await?;
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(50) && waited < Duration::from_secs(5),
        "{waited:?}"
    );
    assert_eq!(std::fs::metadata(&wal)?.len(), before);

    drop(long_read);
    persistence.inner.maintain_wal().await?;
    assert!(std::fs::metadata(&wal).map_or(0, |meta| meta.len()) < 2048);
    assert_eq!(
        run_load(&persistence, TimestampRange::all(), Order::Asc, 100).await?,
        entries
    );
    Ok(())
}

/// Turso answers a checkpoint that a reader stands in the way of with a row
/// saying busy, not with an error; it must be read as such.
#[tokio::test]
async fn busy_checkpoint_is_reported_not_ignored() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), one_reader()).await?;
    let entries = (0u8..50)
        .map(|n| entry_at(10 + u64::from(n), n))
        .collect::<anyhow::Result<Vec<_>>>()?;
    persistence
        .write(&entries, &[], ConflictStrategy::Error)
        .await?;
    let mut reader = persistence.inner.readers.get("test").await?;
    assert!(reader.start_and_abandon("SELECT id FROM documents").await?);
    let checkpoint = super::wal_checkpoint(
        &mut *persistence.inner.writer.lock().await,
        "PRAGMA wal_checkpoint(TRUNCATE)",
    )
    .await?;
    assert!(checkpoint.busy);
    reader.recover();
    Ok(())
}

/// Scans leave one reader free: a commit's ID check is a point lookup and
/// must not queue behind the page reads of every running query.
#[tokio::test]
async fn point_lookups_skip_queued_scans() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite3");
    let options = TursoOptions {
        readers: 3,
        ..one_reader()
    };
    let persistence = TursoPersistence::with_options(path.to_str().unwrap(), options).await?;
    let entry = entry_at(10, 1)?;
    persistence
        .write(std::slice::from_ref(&entry), &[], ConflictStrategy::Error)
        .await?;

    let _scans = (
        persistence.inner.readers.get_for_scan("test").await?,
        persistence.inner.readers.get_for_scan("test").await?,
    );
    let third_scan = tokio::time::timeout(
        Duration::from_millis(50),
        persistence.inner.readers.get_for_scan("test"),
    )
    .await;
    assert!(
        third_scan.is_err(),
        "a third scan must wait for a free scan reader"
    );

    let query_ts = Timestamp::try_from(11u64)?;
    let lookup = tokio::time::timeout(
        Duration::from_secs(5),
        persistence.previous_revisions(
            BTreeSet::from([(entry.id, query_ts)]),
            Arc::new(NoopRetentionValidator),
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the ID check queued behind the scans"))??;
    assert_eq!(lookup.get(&(entry.id, query_ts)), Some(&entry));
    Ok(())
}

/// A reader keeps its page cache across commits and drops only the pages a
/// commit wrote. Under a writer that moves amounts between rows, resizes them
/// (splitting and merging pages) and stamps each change with a version, every
/// read must see one committed state no older than the last commit before
/// it started: the total and the row count never change, the newest stamp
/// equals the version, and the version is at least that commit's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readers_never_see_a_stale_page() -> anyhow::Result<()> {
    use std::sync::atomic::{
        AtomicBool,
        AtomicI64,
        Ordering,
    };

    use super::engine::{
        blob_param,
        int_param,
        integer,
        Conn,
    };

    const ROWS: i64 = 2000;
    const START: i64 = 100;
    const CHECK: &str = "SELECT (SELECT version FROM meta WHERE id = 1), (SELECT MAX(ver) FROM \
                         accounts), (SELECT SUM(balance) FROM accounts), (SELECT COUNT(*) FROM \
                         accounts)";
    let dir = tempfile::tempdir()?;
    let db = super::engine::open(dir.path().join("db.sqlite3").to_str().unwrap())?;
    let mut writer = Conn::new(&db, 2048)?;
    writer
        .execute(
            "CREATE TABLE meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL)",
            vec![],
        )
        .await?;
    writer
        .execute(
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL, ver INTEGER \
             NOT NULL, pad BLOB NOT NULL)",
            vec![],
        )
        .await?;
    writer.execute("BEGIN", vec![]).await?;
    writer
        .execute("INSERT INTO meta VALUES (1, 0)", vec![])
        .await?;
    for id in 0..ROWS {
        let pad = vec![0u8; 100 + (id as usize % 200)];
        writer
            .execute(
                "INSERT INTO accounts VALUES (?1, ?2, 0, ?3)",
                vec![int_param(id), int_param(START), blob_param(&pad)],
            )
            .await?;
    }
    writer.execute("COMMIT", vec![]).await?;

    // A fixed LCG keeps the run reproducible.
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move |bound: u64| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) % bound
    };
    let mut version = 0i64;
    // Move a random amount between two random rows, resizing both, and stamp
    // the change with the next version.
    async fn transfer(
        writer: &mut Conn,
        version: i64,
        next: &mut impl FnMut(u64) -> u64,
    ) -> anyhow::Result<()> {
        let (from, to) = (next(ROWS as u64) as i64, next(ROWS as u64) as i64);
        let amount = next(50) as i64;
        writer.execute("BEGIN", vec![]).await?;
        for (id, delta) in [(from, -amount), (to, amount)] {
            let pad = vec![0u8; 50 + next(400) as usize];
            writer
                .execute(
                    "UPDATE accounts SET balance = balance + ?2, ver = ?3, pad = ?4 WHERE id = ?1",
                    vec![
                        int_param(id),
                        int_param(delta),
                        int_param(version),
                        blob_param(&pad),
                    ],
                )
                .await?;
        }
        writer
            .execute(
                "UPDATE meta SET version = ?1 WHERE id = 1",
                vec![int_param(version)],
            )
            .await?;
        writer.execute("COMMIT", vec![]).await?;
        Ok(())
    }
    async fn check(reader: &mut Conn) -> anyhow::Result<i64> {
        let (version, newest, total, count) = reader
            .query(CHECK, vec![], Some(1), |row| {
                Ok((
                    integer(row.get_value(0))?,
                    integer(row.get_value(1))?,
                    integer(row.get_value(2))?,
                    integer(row.get_value(3))?,
                ))
            })
            .await?
            .pop()
            .expect("one row");
        anyhow::ensure!(
            total == ROWS * START && count == ROWS && newest == version,
            "read a state no commit wrote: version {version}, newest stamp {newest}, total \
             {total}, rows {count}"
        );
        Ok(version)
    }

    // Lockstep: exactly one commit between two reads of each warm reader.
    let mut readers = (0..3)
        .map(|_| Conn::new(&db, 2048))
        .collect::<anyhow::Result<Vec<_>>>()?;
    for reader in &mut readers {
        check(reader).await?;
    }
    for _ in 0..100 {
        version += 1;
        transfer(&mut writer, version, &mut next).await?;
        for reader in &mut readers {
            let read = check(reader).await?;
            anyhow::ensure!(read == version, "read version {read} after version {version} committed");
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let pause = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicI64::new(0));
    let readers = readers
        .into_iter()
        .map(|mut reader| {
            let stop = stop.clone();
            let pause = pause.clone();
            let committed = committed.clone();
            tokio::spawn(async move {
                let mut reads = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if pause.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        continue;
                    }
                    let floor = committed.load(Ordering::Acquire);
                    let version = check(&mut reader).await?;
                    anyhow::ensure!(
                        version >= floor,
                        "read version {version} after version {floor} had committed"
                    );
                    reads += 1;
                }
                anyhow::Ok(reads)
            })
        })
        .collect::<Vec<_>>();

    // Concurrent: readers race the writer and an occasional WAL restart.
    let mut restarts = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    while std::time::Instant::now() < deadline {
        version += 1;
        transfer(&mut writer, version, &mut next).await?;
        committed.store(version, Ordering::Release);
        // Now and then pause the readers and restart the WAL, so the next
        // reads start from caches of the previous WAL generation.
        if version % 300 == 0 {
            pause.store(true, Ordering::Release);
            tokio::time::sleep(Duration::from_millis(5)).await;
            if !super::wal_checkpoint(&mut writer, "PRAGMA wal_checkpoint(TRUNCATE)")
                .await?
                .busy
            {
                restarts += 1;
            }
            pause.store(false, Ordering::Release);
        }
        tokio::task::yield_now().await;
    }
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        let reads = reader.await??;
        assert!(reads > 50, "a reader only read {reads} times");
    }
    assert!(version > 400, "the writer only committed {version} times");
    assert!(restarts > 0, "the WAL never restarted");
    println!("{version} commits, {restarts} WAL restarts");
    Ok(())
}
