#![feature(try_blocks_heterogeneous)]
#![feature(coroutines)]

use std::{
    cmp,
    collections::{
        BTreeMap,
        BTreeSet,
    },
    path::Path,
    sync::Arc,
};

use anyhow::Context as _;
use async_trait::async_trait;
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
    runtime::{
        block_in_place,
        CoopStreamExt as _,
    },
    types::{
        IndexId,
        PersistenceVersion,
        Timestamp,
    },
    value::{
        ConvexValue,
        InternalDocumentId,
        TabletId,
    },
};
use futures::StreamExt;
use futures_async_stream::try_stream;
use metrics::{
    register_convex_histogram,
    StaticMetricLabel,
    Timer,
};
use parking_lot::Mutex;
use rusqlite::{
    params,
    types::Null,
    Connection,
    Row,
    ToSql,
};
use serde::Deserialize as _;
use serde_json::Value as JsonValue;
use tokio::{
    runtime::{
        Handle,
        RuntimeFlavor,
    },
    sync::Semaphore,
};

// ponytail: one connection serializes persistence; evaluate WAL readers when
// measured connection waits justify parallel reads.
#[derive(Clone)]
pub struct SqlitePersistence {
    inner: Arc<Mutex<Inner>>,
    access: Arc<Semaphore>,
    newly_created: bool,
}

struct Inner {
    connection: Connection,
}

register_convex_histogram!(
    SQLITE_LOCK_WAIT_SECONDS,
    "Time waiting for the SQLite connection lock",
    &["op"]
);
register_convex_histogram!(
    SQLITE_LOCK_HOLD_SECONDS,
    "Time holding the SQLite connection lock",
    &["op"]
);
register_convex_histogram!(
    SQLITE_PERMIT_WAIT_SECONDS,
    "Time waiting for admission to the SQLite connection",
    &["op", "flavor"]
);

impl SqlitePersistence {
    async fn run<R: Send>(
        &self,
        operation: &'static str,
        f: impl FnOnce() -> anyhow::Result<R> + Send,
    ) -> anyhow::Result<R> {
        let runtime = Handle::try_current();
        let flavor = match runtime.as_ref().map(Handle::runtime_flavor) {
            Ok(RuntimeFlavor::MultiThread) => "multi",
            Ok(RuntimeFlavor::CurrentThread) => "current",
            _ => "other",
        };
        let mut wait_timer = Timer::new_with_labels(&SQLITE_PERMIT_WAIT_SECONDS);
        wait_timer.add_label(StaticMetricLabel::new("op", operation));
        wait_timer.add_label(StaticMetricLabel::new("flavor", flavor));
        let _permit = self
            .access
            .acquire()
            .await
            .context("SQLite admission closed")?;
        drop(wait_timer);

        // One page or a whole write/revision batch is synchronous. Waiting for
        // its turn is async; only the admitted operation hands off a Tokio
        // worker. Any transaction opened inside f is committed or dropped
        // before this poll returns, preserving the synchronous operation's
        // cancellation boundary and borrowed data.
        if runtime.is_ok() {
            block_in_place(f)
        } else {
            // Preserve callers that drive persistence without a Tokio runtime.
            f()
        }
    }

    fn with_connection<R>(
        &self,
        operation: &'static str,
        f: impl FnOnce(&mut Connection) -> anyhow::Result<R>,
    ) -> anyhow::Result<R> {
        let mut wait_timer = Timer::new_with_labels(&SQLITE_LOCK_WAIT_SECONDS);
        wait_timer.add_label(StaticMetricLabel::new("op", operation));
        let mut inner = self.inner.lock();
        drop(wait_timer);
        let mut hold_timer = Timer::new_with_labels(&SQLITE_LOCK_HOLD_SECONDS);
        hold_timer.add_label(StaticMetricLabel::new("op", operation));
        let result = f(&mut inner.connection);
        drop(inner);
        result
    }

    pub fn new(path: &str) -> anyhow::Result<Self> {
        let newly_created = !Path::new(path).exists();
        let connection = Connection::open(path)?;
        // Execute create tables unconditionally since they are idempotent.
        connection.execute_batch(DOCUMENTS_INIT)?;
        connection.execute_batch(INDEXES_INIT)?;
        connection.execute_batch(PERSISTENCE_GLOBALS_INIT)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner { connection })),
            access: Arc::new(Semaphore::new(1)),
            newly_created,
        })
    }

    /// Read one page of the documents log, at most `page_size` rows, resuming
    /// strictly after `cursor` — the `(ts, table_id, id)` primary key of the
    /// last row the previous page read, in scan order. A table filter is
    /// applied in SQL before the page is materialized.
    fn _load_documents_page(
        &self,
        tablet_id: Option<TabletId>,
        range: TimestampRange,
        order: Order,
        page_size: usize,
        cursor: Option<&(u64, Vec<u8>, Vec<u8>)>,
    ) -> anyhow::Result<Vec<DocumentLogEntry>> {
        let mut params: Vec<&dyn ToSql> = vec![];
        let cursor_bound = match cursor {
            Some((ts, table_id, id)) => {
                params.push(ts);
                params.push(table_id);
                params.push(id);
                match order {
                    Order::Asc => {
                        " AND (ts > $1 OR (ts = $1 AND (table_id > $2 OR (table_id = $2 AND id > \
                         $3))))"
                    },
                    Order::Desc => {
                        " AND (ts < $1 OR (ts = $1 AND (table_id < $2 OR (table_id = $2 AND id < \
                         $3))))"
                    },
                }
            },
            None => "",
        };
        let tablet_id_bytes = tablet_id.map(|tablet_id| tablet_id.0);
        let tablet_id_slice = tablet_id_bytes.as_ref().map(|tablet_id| &tablet_id[..]);
        let tablet_filter = match tablet_id_slice {
            Some(ref tablet_id) => {
                params.push(tablet_id);
                format!(" AND table_id = ${}", params.len())
            },
            None => String::new(),
        };
        let order_str = match order {
            Order::Asc => "ASC",
            Order::Desc => "DESC",
        };
        let query = format!(
            r#"
SELECT id, ts, table_id, json_value, deleted, prev_ts
FROM documents
WHERE ts >= {min} AND ts < {max}{cursor_bound}{tablet_filter}
ORDER BY ts {order_str}, table_id {order_str}, id {order_str}
LIMIT {page_size}
"#,
            min = range.min_timestamp_inclusive(),
            max = range.max_timestamp_exclusive(),
        );
        self.with_connection("load_documents", |connection| {
            let mut stmt = connection.prepare(&query)?;
            let mut page = vec![];
            for row in stmt.query_map(&params[..], load_document_row)? {
                let (document_id, ts, document, prev_ts) = row_to_document(row)?;
                page.push(DocumentLogEntry {
                    ts,
                    id: document_id,
                    value: document,
                    prev_ts,
                });
            }
            Ok(page)
        })
    }

    /// Stream the documents log one page at a time. As in the Postgres
    /// reader, the snapshot is validated against retention after each page is
    /// read and before any of its rows are yielded: pages are read at
    /// different times, and each read is only valid while the range's minimum
    /// timestamp is still within retention. Unlike `_index_scan_paginated`,
    /// nothing is filtered out — deletes are part of the log — so a short
    /// page always means the range is exhausted.
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
                .run("load_documents", || {
                    self._load_documents_page(tablet_id, range, order, page_size, cursor.as_ref())
                })
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

    /// Read one page of an index scan, at most `batch_size` keys, resuming
    /// after `cursor` (the last key the previous page scanned, in scan
    /// order). Tombstoned keys are returned as `(key, None)`: they must
    /// still advance the cursor, but must not be emitted to the caller.
    fn _index_scan_page(
        &self,
        index_id: IndexId,
        tablet_id: TabletId,
        read_timestamp: Timestamp,
        interval: &Interval,
        order: Order,
        batch_size: usize,
        cursor: Option<&IndexKeyBytes>,
    ) -> anyhow::Result<Vec<(IndexKeyBytes, Option<LatestDocument>)>> {
        let interval = interval.clone();
        let index_id = &index_id.0[..];
        let read_timestamp: u64 = read_timestamp.into();

        let mut params = params![index_id, read_timestamp].to_vec();

        let StartIncluded(ref start) = interval.start;
        let start_bytes = &start[..];
        let cursor_bytes = cursor.map(|key| &key.0[..]);
        // Use one bound on each side so the PK seek starts at this page's cursor.
        let (lower_bytes, lower_op) = match (order, cursor_bytes) {
            (Order::Asc, Some(cursor)) if cursor >= start_bytes => (cursor, ">"),
            _ => (start_bytes, ">="),
        };
        params.push(&lower_bytes);
        let lower = format!(" AND B.key {lower_op} ${}", params.len());
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
            Some(ref end) => {
                params.push(end);
                format!(" AND B.key < ${}", params.len())
            },
            None => String::new(),
        };
        let order = match order {
            Order::Asc => "ASC",
            Order::Desc => "DESC",
        };
        // Walk keys in PK order and select the last visible revision per key.
        // Keep tombstones in the page so a deleted page still advances the cursor.
        let query = format!(
            r#"
SELECT B.key, B.ts, B.document_id, C.table_id, C.json_value, C.prev_ts, B.deleted
FROM indexes B
LEFT JOIN documents C
ON B.ts = C.ts AND B.table_id = C.table_id AND B.document_id = C.id
WHERE B.index_id = $1
AND B.ts = (
    SELECT MAX(X.ts) FROM indexes X
    WHERE X.index_id = B.index_id AND X.key = B.key AND X.ts <= $2
){lower}{upper}
ORDER BY B.key {order}
LIMIT {batch_size}
"#,
        );

        self.with_connection("index_scan", |connection| {
            let mut stmt = connection.prepare(&query)?;
            let row_iter = stmt.query_map(&params[..], |row| {
                let key = IndexKeyBytes(row.get::<_, Vec<u8>>(0)?);
                let ts =
                    Timestamp::try_from(row.get::<_, u64>(1)?).expect("timestamp out of bounds");
                let document_id: Option<Vec<u8>> = row.get(2)?;
                let table: Option<Vec<u8>> = row.get(3)?;
                let json_value: Option<String> = row.get(4)?;
                let prev_ts: Option<Timestamp> = row
                    .get::<_, Option<u64>>(5)?
                    .map(|ts| Timestamp::try_from(ts).expect("prev_ts out of bounds"));
                let deleted = row.get::<_, u32>(6)? != 0;

                Ok((key, ts, document_id, table, json_value, prev_ts, deleted))
            })?;
            let mut page = vec![];
            for row in row_iter {
                let (key, ts, document_id, table, json_value, prev_ts, deleted) = row?;
                if deleted {
                    page.push((key, None));
                    continue;
                }
                let table = table.ok_or_else(|| {
                    anyhow::anyhow!("Dangling index reference for {:?} {:?}", key, ts)
                })?;
                let table = TabletId(table.try_into()?);
                let document_id = document_id.ok_or_else(|| {
                    anyhow::anyhow!("Dangling index reference for {:?} {:?}", key, ts)
                })?;
                let _document_id =
                    InternalDocumentId::new(table, InternalId::try_from(document_id)?);
                let json_value = json_value.ok_or_else(|| {
                    anyhow::anyhow!("Index reference to deleted document {:?} {:?}", key, ts)
                })?;
                let json_value: serde_json::Value = serde_json::from_str(&json_value)?;
                let value: ConvexValue = json_value.try_into()?;
                let document = ResolvedDocument::from_database(tablet_id, value)?;
                page.push((
                    key,
                    Some(LatestDocument {
                        ts,
                        value: document,
                        prev_ts,
                    }),
                ));
            }
            Ok(page)
        })
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
                .run("index_scan", || {
                    self._index_scan_page(
                        index_id,
                        tablet_id,
                        read_timestamp,
                        &interval,
                        order,
                        batch_size,
                        cursor.as_ref(),
                    )
                })
                .await?;
            let page_len = page.len();
            retention_validator
                .validate_snapshot(read_timestamp)
                .await?;
            for (key, doc) in page {
                cursor = Some(key.clone());
                if let Some(doc) = doc {
                    yield (key, doc);
                }
            }
            if page_len < batch_size {
                break;
            }
        }
    }

    fn _get_persistence_global(
        &self,
        key: PersistenceGlobalKey,
    ) -> anyhow::Result<Option<JsonValue>> {
        self.with_connection("globals", |connection| {
            let mut stmt = connection.prepare(GET_PERSISTENCE_GLOBAL)?;
            let key = String::from(key);
            let params: Vec<&dyn ToSql> = vec![&key];
            let mut row_iter = stmt.query_map(&params[..], |row| {
                let json_value_str: String = row.get(0)?;
                Ok(json_value_str)
            })?;
            row_iter
                .next()
                .map(|json_value_str| {
                    let json_value_str = json_value_str?;
                    let mut json_deserializer = serde_json::Deserializer::from_str(&json_value_str);
                    // XXX: this is bad, but shapes can get much more nested than convex values
                    json_deserializer.disable_recursion_limit();
                    let json_value = JsonValue::deserialize(&mut json_deserializer)
                        .with_context(|| format!("Invalid JSON at persistence key {key:?}"))?;
                    json_deserializer.end()?;
                    Ok(json_value)
                })
                .transpose()
        })
    }
}

#[async_trait]
impl IndexRowPersistence for SqlitePersistence {
    async fn delete_index_rows(&self, expired_rows: Vec<IndexEntry>) -> anyhow::Result<usize> {
        self.run("delete", || {
            self.with_connection("delete", |connection| {
                let tx = connection.transaction()?;
                let mut delete_index_query = tx.prepare_cached(DELETE_INDEX)?;
                let mut count_deleted = 0;

                for IndexEntry {
                    index_id,
                    key_prefix,
                    ts,
                    ..
                } in expired_rows
                {
                    count_deleted += delete_index_query.execute(params![
                        &index_id.0[..],
                        &u64::from(ts),
                        key_prefix,
                    ])?;
                }
                drop(delete_index_query);
                tx.commit()?;
                Ok(count_deleted)
            })
        })
        .await
    }
}

#[async_trait]
impl Persistence for SqlitePersistence {
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
        self.run("write", || {
            self.with_connection("write", |connection| {
                let tx = connection.transaction()?;
                let mut insert_document_query = match conflict_strategy {
                    ConflictStrategy::Error => tx.prepare_cached(INSERT_DOCUMENT)?,
                    ConflictStrategy::Overwrite => tx.prepare_cached(INSERT_OVERWRITE_DOCUMENT)?,
                };

                for update in documents {
                    let (json_value, deleted) = if let Some(document) = &update.value {
                        assert_eq!(update.id, document.id_with_table_id());
                        let json_value = document.value().json_serialize()?;
                        (Some(json_value), 0)
                    } else {
                        (None, 1)
                    };
                    insert_document_query.execute(params![
                        &update.id.internal_id()[..],
                        &u64::from(update.ts),
                        &update.id.table().0[..],
                        &json_value,
                        &deleted,
                        &update.prev_ts.map(u64::from),
                    ])?;
                }
                drop(insert_document_query);

                let mut insert_index_query = if conflict_strategy == ConflictStrategy::Overwrite {
                    tx.prepare_cached(INSERT_OVERWRITE_INDEX)?
                } else {
                    tx.prepare_cached(INSERT_INDEX)?
                };
                for update in indexes {
                    let index_id = update.index_id;
                    let key: &[u8] = &update.key.0;
                    match update.value {
                        None => {
                            insert_index_query.execute(params![
                                &index_id.0[..],
                                &u64::from(update.ts),
                                key,
                                &1,
                                &Null,
                                &Null,
                            ])?;
                        },
                        Some(doc_id) => {
                            insert_index_query.execute(params![
                                &index_id.0[..],
                                &u64::from(update.ts),
                                key,
                                &0,
                                &doc_id.table().0[..],
                                &doc_id.internal_id()[..],
                            ])?;
                        },
                    };
                }
                drop(insert_index_query);

                tx.commit()?;
                Ok(())
            })
        })
        .await
    }

    async fn write_persistence_global(
        &self,
        key: PersistenceGlobalKey,
        value: JsonValue,
    ) -> anyhow::Result<()> {
        self.run("globals", || {
            self.with_connection("globals", |connection| {
                let tx = connection.transaction()?;
                let mut write_query = tx.prepare_cached(WRITE_PERSISTENCE_GLOBAL)?;
                let json_value = serde_json::to_string(&value)?;
                write_query.execute(params![&String::from(key), &json_value])?;
                drop(write_query);
                tx.commit()?;
                Ok(())
            })
        })
        .await
    }

    async fn has_index_entries(&self) -> anyhow::Result<bool> {
        self.run("has_index_entries", || {
            self.with_connection("has_index_entries", |connection| {
                Ok(connection.prepare(HAS_INDEX_ENTRIES)?.exists([])?)
            })
        })
        .await
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
        self.run("delete", || {
            self.with_connection("delete", |connection| {
                let tx = connection.transaction()?;
                let mut delete_document_query = tx.prepare_cached(DELETE_DOCUMENT)?;
                let mut count_deleted = 0;

                for (ts, internal_id) in documents {
                    let tablet_id: TabletId = internal_id.table();
                    let id = internal_id.internal_id();
                    count_deleted += delete_document_query.execute(params![
                        &tablet_id.0[..],
                        &id[..],
                        &u64::from(ts),
                    ])?;
                }
                drop(delete_document_query);
                tx.commit()?;
                Ok(count_deleted)
            })
        })
        .await
    }

    async fn delete_tablet_documents(
        &self,
        tablet_id: TabletId,
        chunk_size: usize,
    ) -> anyhow::Result<usize> {
        self.run("delete", || {
            self.with_connection("delete", |connection| {
                let tx = connection.transaction()?;
                let mut delete_table_documents_query = tx.prepare_cached(DELETE_TABLE_DOCUMENTS)?;
                let count_deleted = delete_table_documents_query.execute(params![
                    &tablet_id.0[..],
                    &tablet_id.0[..],
                    chunk_size,
                ])?;
                drop(delete_table_documents_query);
                tx.commit()?;
                Ok(count_deleted)
            })
        })
        .await
    }
}

#[async_trait]
impl PersistenceReader for SqlitePersistence {
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
        let mut out = BTreeMap::new();
        let mut min_ts = Timestamp::MAX;
        self.run("previous_revisions", || {
            self.with_connection("previous_revisions", |connection| {
                for (id, ts) in ids {
                    min_ts = cmp::min(ts, min_ts);
                    let mut stmt = connection.prepare(PREV_REV_QUERY)?;
                    let internal_id = id.internal_id();
                    let params = params![&id.table().0[..], &internal_id[..], &u64::from(ts)];
                    let mut row_iter = stmt.query_map(params, load_document_row)?;
                    if let Some(row) = row_iter.next() {
                        let (document_id, prev_ts, document, prev_prev_ts) = row_to_document(row)?;
                        out.insert(
                            (document_id, ts),
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
        })
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
        self.run("previous_revisions", || {
            self.with_connection("previous_revisions", |connection| {
                for DocumentPrevTsQuery { id, ts, prev_ts } in ids {
                    let mut stmt = connection.prepare(EXACT_REV_QUERY)?;
                    let internal_id = id.internal_id();
                    let params = params![&id.table().0[..], &internal_id[..], &u64::from(prev_ts)];
                    let mut row_iter = stmt.query_map(params, load_document_row)?;
                    if let Some(row) = row_iter.next() {
                        let (document_id, prev_ts, document, prev_prev_ts) = row_to_document(row)?;
                        out.insert(
                            DocumentPrevTsQuery {
                                id: document_id,
                                ts,
                                prev_ts,
                            },
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
        })
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
        self.run("globals", || self._get_persistence_global(key))
            .await
    }

    fn version(&self) -> PersistenceVersion {
        PersistenceVersion::V5
    }
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
CREATE INDEX IF NOT EXISTS documents_by_table_and_id ON documents (table_id, id, ts);
"#;

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

fn row_to_document(
    row: rusqlite::Result<(Vec<u8>, u64, Vec<u8>, Option<String>, bool, Option<u64>)>,
) -> anyhow::Result<(
    InternalDocumentId,
    Timestamp,
    Option<ResolvedDocument>,
    Option<Timestamp>,
)> {
    let (id, prev_ts, table, json_value, deleted, prev_prev_ts) = row?;
    let id = InternalId::try_from(id)?;
    let prev_ts = Timestamp::try_from(prev_ts)?;
    let table = TabletId(table.try_into()?);
    let document_id = InternalDocumentId::new(table, id);
    let document = if !deleted {
        let json_value = json_value
            .ok_or_else(|| anyhow::anyhow!("Unexpected NULL json_value at {} {}", id, prev_ts))?;
        let json_value: serde_json::Value = serde_json::from_str(&json_value)?;
        let value: ConvexValue = json_value.try_into()?;
        Some(ResolvedDocument::from_database(table, value)?)
    } else {
        None
    };
    let prev_prev_ts = prev_prev_ts.map(Timestamp::try_from).transpose()?;
    Ok((document_id, prev_ts, document, prev_prev_ts))
}

fn load_document_row(
    row: &Row<'_>,
) -> rusqlite::Result<(Vec<u8>, u64, Vec<u8>, Option<String>, bool, Option<u64>)> {
    let id = row.get::<_, Vec<u8>>(0)?;
    let ts = row.get::<_, u64>(1)?;
    let table: Vec<u8> = row.get(2)?;
    let json_value: Option<String> = row.get(3)?;
    let deleted = row.get::<_, u32>(4)? != 0;
    let prev_ts: Option<u64> = row.get(5)?;
    Ok((id, ts, table, json_value, deleted, prev_ts))
}

const GET_PERSISTENCE_GLOBAL: &str = "SELECT json_value FROM persistence_globals WHERE key = ?";

const INSERT_DOCUMENT: &str = "INSERT INTO documents (id, ts, table_id, json_value, deleted, \
                               prev_ts) VALUES (?, ?, ?, ?, ?, ?)";
const INSERT_OVERWRITE_DOCUMENT: &str = "INSERT OR REPLACE INTO documents (id, ts, table_id, \
                                         json_value, deleted, prev_ts) VALUES (?, ?, ?, ?, ?, ?)";
const INSERT_INDEX: &str = "INSERT INTO indexes VALUES (?, ?, ?, ?, ?, ?)";
const INSERT_OVERWRITE_INDEX: &str = "INSERT OR REPLACE INTO indexes VALUES (?, ?, ?, ?, ?, ?)";
const WRITE_PERSISTENCE_GLOBAL: &str = "INSERT OR REPLACE INTO persistence_globals VALUES (?, ?)";

const HAS_INDEX_ENTRIES: &str = "SELECT 1 FROM indexes LIMIT 1";

const DELETE_INDEX: &str = "DELETE FROM indexes WHERE index_id = ? AND ts <= ? AND key = ?";

const DELETE_DOCUMENT: &str = "DELETE FROM documents WHERE table_id = ? AND id = ? AND ts <= ?";

const DELETE_TABLE_DOCUMENTS: &str = "DELETE FROM documents WHERE table_id = ? AND id IN (SELECT \
                                      id FROM documents WHERE table_id = ? LIMIT ?)";

const PREV_REV_QUERY: &str = r#"
SELECT id, ts, table_id, json_value, deleted, prev_ts
FROM documents
WHERE
    table_id = $1 AND
    id = $2 AND
    ts < $3
ORDER BY ts desc
LIMIT 1
"#;

const EXACT_REV_QUERY: &str = r#"
SELECT id, ts, table_id, json_value, deleted, prev_ts
FROM documents
WHERE
    table_id = $1 AND
    id = $2 AND
    ts = $3
ORDER BY ts ASC, table_id ASC, id ASC
"#;

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{
                AtomicUsize,
                Ordering,
            },
            mpsc,
            Arc,
        },
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

    use super::SqlitePersistence;

    #[test]
    fn blocking_sql_releases_the_only_runtime_worker() -> anyhow::Result<()> {
        fn heartbeat_progresses(offload: bool) -> anyhow::Result<bool> {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()?;
            let persistence = SqlitePersistence::new(":memory:")?;
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let blocker = runtime.spawn(async move {
                let connection_owner = persistence.clone();
                let operation = move || {
                    connection_owner.with_connection("globals", |connection| {
                        connection.query_row("SELECT 1", [], |row| row.get::<_, i64>(0))?;
                        entered_tx.send(())?;
                        release_rx.recv_timeout(Duration::from_secs(10))?;
                        Ok(())
                    })
                };
                if offload {
                    persistence.run("globals", operation).await
                } else {
                    // Negative control: the same SQL holds the only worker,
                    // as synchronous persistence did before the handoff.
                    operation()
                }
            });
            entered_rx.recv_timeout(Duration::from_secs(5))?;
            let (heartbeat_tx, heartbeat_rx) = mpsc::channel();
            let heartbeat = runtime.spawn(async move { heartbeat_tx.send(()) });
            let progressed = heartbeat_rx.recv_timeout(Duration::from_secs(2)).is_ok();
            // Always release the SQL task before checking the result.
            release_tx.send(())?;
            runtime.block_on(blocker)??;
            runtime.block_on(heartbeat)??;
            Ok(progressed)
        }
        assert!(!heartbeat_progresses(false)?, "inline control must block");
        assert!(heartbeat_progresses(true)?, "admitted SQL must hand off");
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_while_waiting_executes_no_sql() -> anyhow::Result<()> {
        let persistence = SqlitePersistence::new(":memory:")?;
        let permit = persistence.access.acquire().await?;
        let statements = Arc::new(AtomicUsize::new(0));
        let count = statements.clone();
        let pending = persistence.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            entered_tx.send(()).unwrap();
            pending
                .run("globals", || {
                    pending.with_connection("globals", |connection| {
                        count.fetch_add(1, Ordering::SeqCst);
                        connection.execute_batch("CREATE TABLE canceled_probe (id INTEGER)")?;
                        Ok(())
                    })
                })
                .await
        });
        entered_rx.await?;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(permit);
        persistence
            .run("globals", || {
                persistence.with_connection("globals", |connection| {
                    let tables: i64 = connection.query_row(
                        "SELECT count(*) FROM sqlite_master WHERE name = 'canceled_probe'",
                        [],
                        |row| row.get(0),
                    )?;
                    assert_eq!(tables, 0);
                    Ok(())
                })
            })
            .await?;
        assert_eq!(statements.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn failed_transaction_rolls_back_before_reuse_and_reopen() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("rollback.sqlite");
        let path = path.to_str().unwrap();
        let persistence = SqlitePersistence::new(path)?;
        let result: anyhow::Result<()> = persistence
            .run("write", || {
                persistence.with_connection("write", |connection| {
                    connection.execute_batch("CREATE TABLE rollback_probe (id INTEGER)")?;
                    let tx = connection.transaction()?;
                    tx.execute("INSERT INTO rollback_probe VALUES (1)", [])?;
                    anyhow::bail!("injected error before commit")
                })
            })
            .await;
        assert!(result.is_err());
        persistence
            .run("globals", || {
                persistence.with_connection("globals", |connection| {
                    assert!(connection.is_autocommit());
                    Ok(())
                })
            })
            .await?;
        drop(persistence);
        let reopened = SqlitePersistence::new(path)?;
        reopened
            .run("globals", || {
                reopened.with_connection("globals", |connection| {
                    let rows: i64 =
                        connection.query_row("SELECT count(*) FROM rollback_probe", [], |row| {
                            row.get(0)
                        })?;
                    assert_eq!(rows, 0);
                    Ok(())
                })
            })
            .await?;
        Ok(())
    }

    /// One indexed key and its history: `(ts, Some(v))` is a live write of
    /// value `v`, `(ts, None)` is a tombstone.
    struct KeySpec {
        key: Vec<u8>,
        doc_n: u8,
        versions: Vec<(u64, Option<i64>)>,
    }

    struct Fixture {
        persistence: SqlitePersistence,
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

    async fn make_fixture(specs: &[KeySpec]) -> anyhow::Result<Fixture> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.sqlite3");
        let persistence = SqlitePersistence::new(path.to_str().unwrap())?;
        let index_id = IndexId(internal_id(255));
        let tablet_id = TabletId(internal_id(254));
        let table_number = TableNumber::try_from(1)?;

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
        fixture: &Fixture,
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
                    value: make_doc(fixture.tablet_id, fixture.table_number, spec.doc_n, v)?,
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

    async fn run_scan(
        fixture: &Fixture,
        read_ts: u64,
        interval: &Interval,
        order: Order,
        size_hint: usize,
    ) -> anyhow::Result<Vec<(IndexKeyBytes, LatestDocument)>> {
        fixture
            .persistence
            .index_scan(
                fixture.index_id,
                fixture.tablet_id,
                Timestamp::try_from(read_ts)?,
                interval,
                order,
                size_hint,
                Arc::new(NoopRetentionValidator),
            )
            .try_collect()
            .await
    }

    /// 25 keys with updates, tombstones on page boundaries, and writes past
    /// the read snapshot, scanned under every pagination regime: the
    /// paginated scan must be indistinguishable from the one-shot scan it
    /// replaced.
    #[tokio::test]
    async fn paginated_scan_matches_reference() -> anyhow::Result<()> {
        let specs: Vec<KeySpec> = (0u8..25)
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
            .collect();
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
                    let expected = expected_scan(&fixture, &specs, read_ts, interval, order)?;
                    for size_hint in [0, 1, 2, 3, 10_000] {
                        let got = run_scan(&fixture, read_ts, interval, order, size_hint).await?;
                        assert_eq!(
                            got, expected,
                            "scan mismatch at read_ts={read_ts} order={order:?} \
                             size_hint={size_hint}"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Ten consecutive tombstoned keys form entire pages with no live rows.
    /// A scan that terminates on "page yielded fewer live rows than
    /// requested" would stop in the middle of the interval and silently drop
    /// every key after the tombstone run.
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
            let expected = expected_scan(&fixture, &specs, 25, &interval, order)?;
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
    ) -> anyhow::Result<(SqlitePersistence, tempfile::TempDir)> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test.sqlite3");
        let persistence = SqlitePersistence::new(path.to_str().unwrap())?;
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
        persistence: &SqlitePersistence,
        range: TimestampRange,
        order: Order,
        page_size: u32,
    ) -> anyhow::Result<Vec<DocumentLogEntry>> {
        persistence
            .load_documents(range, order, page_size, Arc::new(NoopRetentionValidator))
            .try_collect()
            .await
    }

    /// Two tablets writing interleaved updates and deletes, with several rows
    /// sharing the same timestamp: the compound cursor's `(table_id, id)`
    /// tiebreak is what keeps pagination correct when a page boundary lands
    /// in the middle of a same-`ts` run.
    #[tokio::test]
    async fn paginated_load_matches_reference() -> anyhow::Result<()> {
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

    /// Ten documents written at one single timestamp: every page boundary
    /// falls inside the same-`ts` run, so only the `(table_id, id)` tiebreak
    /// of the cursor separates the pages.
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
                            "table={tablet_n} range={range:?} order={order:?} \
                             page_size={page_size}"
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
        persistence.inner.lock().connection.execute(
            "UPDATE documents SET json_value = 'invalid json' WHERE table_id = ?",
            rusqlite::params![&other_tablet[..]],
        )?;

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
}
