//! A thin async driver over `turso_core`, used instead of the `turso` crate.
//!
//! The `turso` crate's `turso_sdk_kit` layer enables parking_lot's
//! `send_guard`, which cannot be compiled together with the
//! `deadlock_detection` the vector index enables. Driving `turso_core`
//! directly also lets rows be decoded by reference: the SDK copies every
//! value out of the VM register and then again on each `get_value`.

use std::{
    collections::HashMap,
    num::NonZero,
    pin::Pin,
    sync::Arc,
    task::{
        Context,
        Poll,
    },
};

use turso_core::{
    Connection,
    Database,
    MemoryIO,
    OpenOptions,
    PlatformIO,
    SqliteDialect,
    Statement,
    StepResult,
    IO,
};
pub(crate) use turso_core::{
    Numeric,
    Row,
    Value,
};

/// Open `path` with plain syscall I/O (`:memory:` in memory). io_uring is
/// not offered: under contention it measured slower, and turso_core's ring
/// leader can return while a follower's completion is still unreaped, which
/// would park that statement until some other I/O happens.
pub(crate) fn open(path: &str) -> anyhow::Result<Arc<Database>> {
    let io: Arc<dyn IO> = if path == ":memory:" {
        Arc::new(MemoryIO::new())
    } else {
        Arc::new(PlatformIO::new()?)
    };
    Ok(Database::open(
        io,
        path,
        OpenOptions::new(Arc::new(SqliteDialect)),
    )?)
}

/// One step of a statement: `true` when a row is available in
/// `stmt.row()`, `false` when the statement is done. I/O parks the future
/// on the waker Turso registers with its completions.
struct Step<'a> {
    stmt: &'a mut Statement,
}

impl Future for Step<'_> {
    type Output = anyhow::Result<bool>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let stmt = &mut *self.stmt;
        let result = match stmt.step_with_waker(cx.waker()) {
            Ok(StepResult::Row) => Ok(true),
            Ok(StepResult::Done) => Ok(false),
            Ok(StepResult::Busy) => Err(anyhow::anyhow!("database is locked")),
            Ok(StepResult::Interrupt) => Err(anyhow::anyhow!("statement interrupted")),
            Ok(StepResult::IO | StepResult::Yield | StepResult::Sleep { .. }) => {
                return match stmt._io().step() {
                    Ok(_) => Poll::Pending,
                    Err(e) => Poll::Ready(Err(e.into())),
                };
            },
            Err(e) => Err(e.into()),
        };
        Poll::Ready(result)
    }
}

/// A connection with its own prepared-statement cache: every query shape is
/// planned once per connection.
pub(crate) struct Conn {
    connection: Arc<Connection>,
    statements: HashMap<String, Statement>,
    /// A statement was started and not reset yet: its future was dropped
    /// mid-step. Until it is reset the connection holds that statement's read
    /// snapshot, and later statements on it would read from the past.
    in_flight: bool,
}

impl Conn {
    pub(crate) fn new(db: &Arc<Database>, cache_kib: i64) -> anyhow::Result<Self> {
        let connection = db.connect()?;
        // Negative cache_size is KiB, as in SQLite.
        connection.execute(format!("PRAGMA cache_size = -{cache_kib}"))?;
        Ok(Self {
            connection,
            statements: HashMap::new(),
            in_flight: false,
        })
    }

    pub(crate) fn is_autocommit(&self) -> bool {
        self.connection.get_auto_commit()
    }

    /// No statement left mid-step and no transaction open: safe to reuse.
    pub(crate) fn is_clean(&self) -> bool {
        !self.in_flight && self.is_autocommit()
    }

    /// Reset every cached statement, releasing whatever an interrupted one
    /// still holds; statements that refuse to reset are dropped. A
    /// transaction left open is the caller's to roll back.
    pub(crate) fn recover(&mut self) {
        let mut failed = false;
        for stmt in self.statements.values_mut() {
            failed |= stmt.reset().is_err();
        }
        if failed {
            self.statements.clear();
        }
        self.in_flight = false;
    }

    fn bind(&mut self, sql: &str, params: Vec<Value>) -> anyhow::Result<&mut Statement> {
        if !self.statements.contains_key(sql) {
            let stmt = self.connection.prepare(sql)?;
            self.statements.insert(sql.to_owned(), stmt);
        }
        let stmt = self
            .statements
            .get_mut(sql)
            .expect("statement inserted above");
        stmt.reset()?;
        for (i, value) in params.into_iter().enumerate() {
            stmt.bind_at(NonZero::new(i + 1).expect("1-based"), value)?;
        }
        Ok(stmt)
    }

    /// Start `sql` and leave it at its first row without resetting it: the
    /// state a future dropped mid-statement leaves behind.
    #[cfg(test)]
    pub(crate) async fn start_and_abandon(&mut self, sql: &str) -> anyhow::Result<bool> {
        self.in_flight = true;
        let stmt = self.bind(sql, vec![])?;
        Step { stmt }.await
    }

    /// Run `sql` to completion, returning the number of rows it changed.
    pub(crate) async fn execute(&mut self, sql: &str, params: Vec<Value>) -> anyhow::Result<u64> {
        self.in_flight = true;
        let stmt = self.bind(sql, params)?;
        let result = async {
            while (Step { stmt: &mut *stmt }).await? {}
            anyhow::Ok(u64::try_from(stmt.n_change())?)
        }
        .await;
        let reset = stmt.reset();
        self.finish(result, reset)
    }

    /// Settle a statement run: the connection is clean again only when its
    /// statement reset, and a reset failure never hides the run's own error.
    fn finish<T>(
        &mut self,
        result: anyhow::Result<T>,
        reset: turso_core::Result<()>,
    ) -> anyhow::Result<T> {
        match (result, reset) {
            (Ok(value), Ok(())) => {
                self.in_flight = false;
                Ok(value)
            },
            (Err(e), Ok(())) => {
                self.in_flight = false;
                Err(e)
            },
            (Ok(_), Err(reset)) => Err(reset.into()),
            (Err(e), Err(reset)) => Err(e.context(format!("statement reset also failed: {reset}"))),
        }
    }

    /// Run a statement once per parameter row, summing the rows changed.
    pub(crate) async fn execute_each(
        &mut self,
        sql: &str,
        rows: Vec<Vec<Value>>,
    ) -> anyhow::Result<usize> {
        let mut changed = 0;
        for params in rows {
            changed += self.execute(sql, params).await?;
        }
        Ok(usize::try_from(changed)?)
    }

    /// Decode up to `limit` rows of `sql` by reference with `decode`. The
    /// statement is reset afterwards, so no read transaction outlives the
    /// call.
    pub(crate) async fn query<T>(
        &mut self,
        sql: &str,
        params: Vec<Value>,
        limit: Option<usize>,
        mut decode: impl FnMut(&Row) -> anyhow::Result<T>,
    ) -> anyhow::Result<Vec<T>> {
        self.in_flight = true;
        let stmt = self.bind(sql, params)?;
        let result = async {
            let mut out = vec![];
            while limit.is_none_or(|limit| out.len() < limit) && (Step { stmt: &mut *stmt }).await?
            {
                let row = stmt.row().expect("step returned a row");
                out.push(decode(row)?);
            }
            anyhow::Ok(out)
        }
        .await;
        let reset = stmt.reset();
        self.finish(result, reset)
    }
}

pub(crate) fn integer(value: &Value) -> anyhow::Result<i64> {
    match value {
        Value::Numeric(Numeric::Integer(i)) => Ok(*i),
        other => anyhow::bail!("expected INTEGER, got {other:?}"),
    }
}

pub(crate) fn blob(value: &Value) -> anyhow::Result<&[u8]> {
    match value {
        Value::Blob(bytes) => Ok(bytes),
        other => anyhow::bail!("expected BLOB, got {other:?}"),
    }
}

pub(crate) fn text(value: &Value) -> anyhow::Result<&str> {
    match value {
        Value::Text(text) => Ok(text.as_str()),
        other => anyhow::bail!("expected TEXT, got {other:?}"),
    }
}

pub(crate) fn optional<'a, T>(
    value: &'a Value,
    decode: impl FnOnce(&'a Value) -> anyhow::Result<T>,
) -> anyhow::Result<Option<T>> {
    match value {
        Value::Null => Ok(None),
        value => decode(value).map(Some),
    }
}

pub(crate) fn int_param(value: i64) -> Value {
    Value::Numeric(Numeric::Integer(value))
}

pub(crate) fn blob_param(bytes: &[u8]) -> Value {
    Value::Blob(bytes.to_vec())
}

pub(crate) fn text_param(text: String) -> Value {
    Value::from_text(text)
}
