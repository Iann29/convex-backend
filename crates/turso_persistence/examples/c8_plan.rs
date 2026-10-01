//! Print Turso's plan for the index-scan page query and time one page at the
//! start and at the end of the largest index of a database file.
//! Usage: c8_plan /path/to/db.sqlite3
use std::{
    num::NonZero,
    sync::Arc,
    time::Instant,
};

use turso_core::{
    Database,
    OpenOptions,
    PlatformIO,
    SqliteDialect,
    StepResult,
    Value,
};

const PAGE: &str = r#"
SELECT B.key, B.ts, B.document_id, C.table_id, C.json_value, C.prev_ts, B.deleted
FROM indexes B
LEFT JOIN documents C
ON B.ts = C.ts AND B.table_id = C.table_id AND B.document_id = C.id
WHERE B.index_id = ?1
AND B.ts = (
    SELECT MAX(X.ts) FROM indexes X
    WHERE X.index_id = B.index_id AND X.key = B.key AND X.ts <= ?2
) AND B.key > ?3
ORDER BY B.key ASC
LIMIT ?4
"#;

fn rows(
    conn: &Arc<turso_core::Connection>,
    sql: &str,
    params: Vec<Value>,
) -> anyhow::Result<Vec<Vec<String>>> {
    let mut stmt = conn.prepare(sql)?;
    for (i, p) in params.into_iter().enumerate() {
        stmt.bind_at(NonZero::new(i + 1).unwrap(), p)?;
    }
    let mut out = vec![];
    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                out.push(row.get_values().map(|v| format!("{v:?}")).collect());
            },
            StepResult::Done => return Ok(out),
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                stmt._io().step()?;
            },
            other => anyhow::bail!("step: {other:?}"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let path = std::env::args().nth(1).expect("db path");
    let db = Database::open(
        Arc::new(PlatformIO::new()?),
        &path,
        OpenOptions::new(Arc::new(SqliteDialect)),
    )?;
    let conn = db.connect()?;
    let biggest = rows(
        &conn,
        "SELECT index_id, count(*) AS c FROM indexes GROUP BY index_id ORDER BY c DESC LIMIT 1",
        vec![],
    )?;
    println!("largest index: {biggest:?}");
    let mut stmt = conn
        .prepare("SELECT index_id FROM indexes GROUP BY index_id ORDER BY count(*) DESC LIMIT 1")?;
    let index_id = loop {
        match stmt.step()? {
            StepResult::Row => break stmt.row().unwrap().get_value(0).clone(),
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                stmt._io().step()?;
            },
            other => anyhow::bail!("step: {other:?}"),
        }
    };
    drop(stmt);
    for line in rows(&conn, &format!("EXPLAIN QUERY PLAN {PAGE}"), vec![])? {
        println!("plan: {}", line.join(" | "));
    }
    let keys = rows(
        &conn,
        "SELECT key FROM indexes WHERE index_id = ?1 ORDER BY key",
        vec![index_id.clone()],
    )?;
    println!("keys in index: {}", keys.len());
    let mut all = {
        let mut stmt = conn.prepare("SELECT key FROM indexes WHERE index_id = ?1 ORDER BY key")?;
        stmt.bind_at(NonZero::new(1).unwrap(), index_id.clone())?;
        let mut v = vec![];
        loop {
            match stmt.step()? {
                StepResult::Row => v.push(stmt.row().unwrap().get_value(0).clone()),
                StepResult::Done => break v,
                _ => {
                    stmt._io().step()?;
                },
            }
        }
    };
    all.dedup();
    for (label, cursor) in [
        ("start", Value::Blob(vec![])),
        ("middle", all[all.len() / 2].clone()),
        ("end", all[all.len() - 101].clone()),
    ] {
        let t = Instant::now();
        let got = rows(
            &conn,
            PAGE,
            vec![
                index_id.clone(),
                Value::Numeric(turso_core::Numeric::Integer(i64::MAX)),
                cursor,
                Value::Numeric(turso_core::Numeric::Integer(100)),
            ],
        )?;
        println!("page at {label}: {} rows in {:?}", got.len(), t.elapsed());
    }
    let desc = PAGE
        .replace("B.key > ?3", "B.key < ?3")
        .replace("ORDER BY B.key ASC", "ORDER BY B.key DESC");
    for line in rows(&conn, &format!("EXPLAIN QUERY PLAN {desc}"), vec![])? {
        println!("desc plan: {}", line.join(" | "));
    }
    for (label, cursor) in [
        ("end", Value::Blob(vec![0xff; 64])),
        ("middle", all[all.len() / 2].clone()),
        ("start", all[101].clone()),
    ] {
        let t = Instant::now();
        let got = rows(
            &conn,
            &desc,
            vec![
                index_id.clone(),
                Value::Numeric(turso_core::Numeric::Integer(i64::MAX)),
                cursor,
                Value::Numeric(turso_core::Numeric::Integer(100)),
            ],
        )?;
        println!(
            "desc page at {label}: {} rows in {:?}",
            got.len(),
            t.elapsed()
        );
    }
    let load = "SELECT id, ts, table_id, json_value, deleted, prev_ts FROM documents WHERE ts >= \
                ?4 AND ts < ?5 AND ts >= ?1 AND (ts > ?1 OR (ts = ?1 AND (table_id > ?2 OR \
                (table_id = ?2 AND id > ?3)))) ORDER BY ts ASC, table_id ASC, id ASC LIMIT ?6";
    for line in rows(&conn, &format!("EXPLAIN QUERY PLAN {load}"), vec![])? {
        println!("load plan: {}", line.join(" | "));
    }
    let lite = rusqlite::Connection::open(&path)?;
    let mut plan = lite.prepare(&format!("EXPLAIN QUERY PLAN {load}"))?;
    let lines: Vec<String> = plan
        .query_map(rusqlite::params![0, 0, 0, 0, 0, 0], |r| {
            r.get::<_, String>(3)
        })?
        .collect::<Result<_, _>>()?;
    println!("rusqlite load plan: {lines:?}");
    let all_ts: Vec<i64> = lite
        .prepare("SELECT ts FROM documents ORDER BY ts")?
        .query_map([], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    println!("documents: {}", all_ts.len());
    for (label, at) in [
        ("start", 0),
        ("middle", all_ts.len() / 2),
        ("end", all_ts.len() - 101),
    ] {
        let ts = all_ts[at];
        let params = vec![
            Value::Numeric(turso_core::Numeric::Integer(ts)),
            Value::Blob(vec![]),
            Value::Blob(vec![]),
            Value::Numeric(turso_core::Numeric::Integer(0)),
            Value::Numeric(turso_core::Numeric::Integer(i64::MAX)),
            Value::Numeric(turso_core::Numeric::Integer(100)),
        ];
        let t = Instant::now();
        let got = rows(&conn, load, params)?;
        let turso_t = t.elapsed();
        let t = Instant::now();
        let n = lite
            .prepare(load)?
            .query_map(
                rusqlite::params![
                    ts,
                    Vec::<u8>::new(),
                    Vec::<u8>::new(),
                    0i64,
                    i64::MAX,
                    100i64
                ],
                |_| Ok(()),
            )?
            .count();
        println!(
            "load page at {label}: turso {} rows in {:?} | rusqlite {} rows in {:?}",
            got.len(),
            turso_t,
            n,
            t.elapsed()
        );
    }
    for (label, sql) in [
        (
            "load asc",
            "SELECT id FROM documents WHERE ts >= ?4 AND ts < ?5 AND ts >= ?1 AND (ts > ?1 OR (ts \
             = ?1 AND (table_id > ?2 OR (table_id = ?2 AND id > ?3)))) ORDER BY ts ASC, table_id \
             ASC, id ASC LIMIT ?6",
        ),
        (
            "load desc",
            "SELECT id FROM documents WHERE ts <= ?1 AND (ts < ?1 OR (ts = ?1 AND (table_id < ?2 \
             OR (table_id = ?2 AND id < ?3)))) AND ts >= ?4 AND ts < ?5 ORDER BY ts DESC, \
             table_id DESC, id DESC LIMIT ?6",
        ),
        (
            "load asc table",
            "SELECT id FROM documents WHERE ts >= ?1 AND (ts > ?1 OR (ts = ?1 AND (table_id > ?2 \
             OR (table_id = ?2 AND id > ?3)))) AND table_id = ?4 AND ts >= ?5 AND ts < ?6 ORDER \
             BY ts ASC, table_id ASC, id ASC LIMIT ?7",
        ),
        (
            "load desc table",
            "SELECT id FROM documents WHERE ts <= ?1 AND (ts < ?1 OR (ts = ?1 AND (table_id < ?2 \
             OR (table_id = ?2 AND id < ?3)))) AND table_id = ?4 AND ts >= ?5 AND ts < ?6 ORDER \
             BY ts DESC, table_id DESC, id DESC LIMIT ?7",
        ),
        (
            "load asc table nocursor",
            "SELECT id FROM documents WHERE table_id = ?1 AND ts >= ?2 AND ts < ?3 ORDER BY ts \
             ASC, table_id ASC, id ASC LIMIT ?4",
        ),
        (
            "PLUS asc table",
            "SELECT id FROM documents WHERE ts >= ?1 AND (ts > ?1 OR (ts = ?1 AND (table_id > ?2 \
             OR (table_id = ?2 AND id > ?3)))) AND +table_id = ?4 AND ts >= ?5 AND ts < ?6 ORDER \
             BY ts ASC, table_id ASC, id ASC LIMIT ?7",
        ),
        (
            "PLUS desc table",
            "SELECT id FROM documents WHERE ts <= ?1 AND (ts < ?1 OR (ts = ?1 AND (table_id < ?2 \
             OR (table_id = ?2 AND id < ?3)))) AND +table_id = ?4 AND ts >= ?5 AND ts < ?6 ORDER \
             BY ts DESC, table_id DESC, id DESC LIMIT ?7",
        ),
        (
            "PLUS asc table nocursor",
            "SELECT id FROM documents WHERE +table_id = ?1 AND ts >= ?2 AND ts < ?3 ORDER BY ts \
             ASC, table_id ASC, id ASC LIMIT ?4",
        ),
    ] {
        for line in rows(&conn, &format!("EXPLAIN QUERY PLAN {sql}"), vec![])? {
            println!("{label}: {}", line.last().unwrap());
        }
        let sorts = rows(&conn, &format!("EXPLAIN {sql}"), vec![])?
            .iter()
            .filter(|l| l.iter().any(|c| c.contains("Sorter")))
            .count();
        println!("{label}: sorter opcodes {sorts}");
    }
    for plus in ["", "+"] {
        for (label, sql) in [
            (
                "exact nocursor asc",
                format!(
                    "SELECT id, ts, table_id, json_value, deleted, prev_ts FROM documents WHERE \
                     ts >= ?1 AND ts < ?2 AND {plus}table_id = ?3 ORDER BY ts ASC, table_id ASC, \
                     id ASC LIMIT ?4"
                ),
            ),
            (
                "exact cursor asc",
                format!(
                    "SELECT id, ts, table_id, json_value, deleted, prev_ts FROM documents WHERE \
                     ts >= ?1 AND ts < ?4 AND (ts > ?1 OR (ts = ?1 AND (table_id > ?2 OR \
                     (table_id = ?2 AND id > ?3)))) AND {plus}table_id = ?5 ORDER BY ts ASC, \
                     table_id ASC, id ASC LIMIT ?6"
                ),
            ),
            (
                "exact cursor desc",
                format!(
                    "SELECT id, ts, table_id, json_value, deleted, prev_ts FROM documents WHERE \
                     ts <= ?1 AND ts >= ?4 AND (ts < ?1 OR (ts = ?1 AND (table_id < ?2 OR \
                     (table_id = ?2 AND id < ?3)))) AND {plus}table_id = ?5 ORDER BY ts DESC, \
                     table_id DESC, id DESC LIMIT ?6"
                ),
            ),
        ] {
            let plan: Vec<String> = rows(&conn, &format!("EXPLAIN QUERY PLAN {sql}"), vec![])?
                .into_iter()
                .map(|l| l.last().unwrap().clone())
                .collect();
            let sorts = rows(&conn, &format!("EXPLAIN {sql}"), vec![])?
                .iter()
                .filter(|l| l.iter().any(|c| c.contains("Sorter")))
                .count();
            println!("plus={plus:1} {label}: sorters={sorts} {plan:?}");
        }
    }
    {
        // Time one table-filtered log page, as a table backfill reads it.
        let tables = rows(
            &conn,
            "SELECT table_id, count(*) c FROM documents GROUP BY table_id ORDER BY c DESC LIMIT 1",
            vec![],
        )?;
        println!("largest table: {:?}", tables.first().map(|r| r[1].clone()));
        let mut stmt = conn.prepare(
            "SELECT table_id FROM documents GROUP BY table_id ORDER BY count(*) DESC LIMIT 1",
        )?;
        let table = loop {
            match stmt.step()? {
                StepResult::Row => break stmt.row().unwrap().get_value(0).clone(),
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    stmt._io().step()?;
                },
                other => anyhow::bail!("step: {other:?}"),
            }
        };
        drop(stmt);
        for plus in ["", "+"] {
            let sql = format!(
                "SELECT id, ts, table_id, json_value, deleted, prev_ts FROM documents WHERE ts >= \
                 ?1 AND ts < ?2 AND {plus}table_id = ?3 ORDER BY ts ASC, table_id ASC, id ASC \
                 LIMIT ?4"
            );
            let t = Instant::now();
            let mut n = 0;
            for _ in 0..20 {
                n += rows(
                    &conn,
                    &sql,
                    vec![
                        Value::Numeric(turso_core::Numeric::Integer(0)),
                        Value::Numeric(turso_core::Numeric::Integer(i64::MAX)),
                        table.clone(),
                        Value::Numeric(turso_core::Numeric::Integer(100)),
                    ],
                )?
                .len();
            }
            println!(
                "table page plus={plus:1}: {:?}/page ({} rows/page)",
                t.elapsed() / 20,
                n / 20
            );
        }
    }
    for direction in ["ASC", "DESC"] {
        let sql = format!(
            "SELECT B.key, B.ts, B.document_id, C.table_id, C.json_value, C.prev_ts, B.deleted \
             FROM indexes B LEFT JOIN documents C ON B.ts = C.ts AND B.table_id = C.table_id AND \
             B.document_id = C.id WHERE B.index_id = ?1 AND B.ts <= ?2 AND B.key > ?3 AND B.key < \
             ?4 ORDER BY B.key {direction}, B.ts {direction} LIMIT ?5"
        );
        for line in rows(&conn, &format!("EXPLAIN QUERY PLAN {sql}"), vec![])? {
            println!("new {direction} plan: {}", line.last().unwrap());
        }
        let bytecode = rows(&conn, &format!("EXPLAIN {sql}"), vec![])?;
        let sorts = bytecode
            .iter()
            .filter(|l| l.iter().any(|c| c.contains("Sorter")))
            .count();
        println!("new {direction} sorter opcodes: {sorts}");
    }
    let variants = [
        ("full", PAGE.to_string()),
        (
            "no-subquery",
            PAGE.replace(
                "AND B.ts = (\n    SELECT MAX(X.ts) FROM indexes X\n    WHERE X.index_id = \
                 B.index_id AND X.key = B.key AND X.ts <= ?2\n)",
                "AND B.ts <= ?2",
            ),
        ),
        (
            "no-join",
            PAGE.replace(
                "LEFT JOIN documents C\nON B.ts = C.ts AND B.table_id = C.table_id AND \
                 B.document_id = C.id\n",
                "",
            )
            .replace(
                "C.table_id, C.json_value, C.prev_ts",
                "B.table_id, NULL, NULL",
            ),
        ),
    ];
    for (label, sql) in variants {
        assert!(
            label == "full" || sql != PAGE,
            "{label} rewrite did not apply"
        );
        let mut stmt = conn.prepare(&sql)?;
        let t = Instant::now();
        let mut n = 0;
        for i in 0..200 {
            stmt.reset()?;
            let cursor = all[(i * 37) % (all.len() - 101)].clone();
            for (j, p) in [
                index_id.clone(),
                Value::Numeric(turso_core::Numeric::Integer(i64::MAX)),
                cursor,
                Value::Numeric(turso_core::Numeric::Integer(100)),
            ]
            .into_iter()
            .enumerate()
            {
                stmt.bind_at(NonZero::new(j + 1).unwrap(), p)?;
            }
            loop {
                match stmt.step()? {
                    StepResult::Row => n += 1,
                    StepResult::Done => break,
                    StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                        stmt._io().step()?;
                    },
                    other => anyhow::bail!("{other:?}"),
                }
            }
        }
        let turso_t = t.elapsed() / 200;
        let mut lstmt = lite.prepare(&sql)?;
        let t = Instant::now();
        let mut m = 0;
        for i in 0..200 {
            let cursor = match &all[(i * 37) % (all.len() - 101)] {
                Value::Blob(b) => b.clone(),
                _ => unreachable!(),
            };
            let id = match &index_id {
                Value::Blob(b) => b.clone(),
                _ => unreachable!(),
            };
            m += lstmt
                .query_map(rusqlite::params![id, i64::MAX, cursor, 100i64], |r| {
                    r.get::<_, Vec<u8>>(0)
                })?
                .count();
        }
        println!(
            "scan {label}: turso {:?}/page ({n} rows) | rusqlite {:?}/page ({m} rows)",
            turso_t,
            t.elapsed() / 200
        );
    }
    Ok(())
}
