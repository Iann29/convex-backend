use std::{
    collections::HashMap,
    sync::{
        Arc,
        LazyLock,
    },
};

use anyhow::Context as _;
use clusters::{
    persistence_args_from_cluster_url,
    DbDriverTag,
    PersistenceArgs,
};
use cmd_util::env::env_config;
use common::{
    knobs::{
        APPLICATION_DEFAULT_CONCURRENT_QUERIES,
        DATABASE_USE_PREPARED_STATEMENTS,
        DEFAULT_APPLICATION_MAX_FUNCTION_CONCURRENCY,
    },
    persistence::{
        Persistence,
        PersistenceReader,
    },
    runtime::Runtime,
    shutdown::ShutdownSignal,
    types::DeploymentId,
};
use mysql::{
    ConvexMySqlPool,
    MySqlOptions,
    MySqlReaderOptions,
};
use postgres::{
    PostgresOptions,
    PostgresPersistence,
    PostgresReaderOptions,
};
use sqlite::SqlitePersistence;
use tokio_postgres::config::TargetSessionAttrs;
use turso_persistence::TursoPersistence;

/// Engine behind `--db sqlite`. Both read and write the same `db.sqlite3`,
/// so switching back and forth needs no migration; Turso leaves the file in
/// WAL mode, which the `sqlite` crate reads as well.
static SQLITE_ENGINE: LazyLock<String> =
    LazyLock::new(|| env_config("CONVEX_SQLITE_ENGINE", "rusqlite".to_owned()));

async fn connect_sqlite(db_spec: &str) -> anyhow::Result<Arc<dyn Persistence>> {
    match SQLITE_ENGINE.as_str() {
        "rusqlite" => {
            let persistence = Arc::new(SqlitePersistence::new(db_spec)?);
            tracing::info!("Connected to SQLite at {db_spec}");
            Ok(persistence)
        },
        "turso" => {
            let persistence = Arc::new(TursoPersistence::new(db_spec).await?);
            // Turso serves reads in parallel and a query spends its time on
            // the CPU, not waiting on I/O: beyond about two queries per CPU,
            // more in flight only contend. On 4 vCPUs, 8 against 16 gave as
            // many reads, 4–10% more writes and a lower write p50.
            let cpus = std::thread::available_parallelism().map_or(1, usize::from);
            let queries = (2 * cpus).clamp(4, DEFAULT_APPLICATION_MAX_FUNCTION_CONCURRENCY);
            if APPLICATION_DEFAULT_CONCURRENT_QUERIES.set(queries).is_ok() {
                tracing::info!(
                    "Connected to SQLite at {db_spec} with Turso; default query concurrency \
                     {queries} for {cpus} CPUs"
                );
            } else {
                tracing::info!("Connected to SQLite at {db_spec} with Turso");
            }
            Ok(persistence)
        },
        other => anyhow::bail!("CONVEX_SQLITE_ENGINE must be rusqlite or turso, got {other:?}"),
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ConnectPersistenceFlags {
    pub require_ssl: bool,
    pub allow_read_only: bool,
    pub skip_index_creation: bool,
}

pub enum PersistenceSeed<RT: Runtime> {
    Sqlite {
        db_spec: String,
    },
    Postgres {
        config: tokio_postgres::Config,
        options: PostgresOptions,
    },
    MySql {
        pool: Arc<ConvexMySqlPool<RT>>,
        db_name: String,
        options: MySqlOptions,
    },
}

pub fn persistence_seed<RT: Runtime>(
    db: DbDriverTag,
    db_spec: &str,
    flags: ConnectPersistenceFlags,
    deployment_name: &str,
    deployment_id: Option<DeploymentId>,
    runtime: RT,
) -> anyhow::Result<PersistenceSeed<RT>> {
    match db {
        DbDriverTag::Sqlite => Ok(PersistenceSeed::Sqlite {
            db_spec: db_spec.to_owned(),
        }),
        DbDriverTag::Postgres(version)
        | DbDriverTag::MySql(version)
        | DbDriverTag::MySqlMultitenant(version) => {
            let args = persistence_args_from_cluster_url(
                deployment_name,
                db_spec.parse()?,
                db,
                flags.require_ssl,
                true, /* require_leader */
            )?;
            match args {
                PersistenceArgs::Postgres {
                    mut url,
                    schema,
                    multitenant,
                } => {
                    let options = PostgresOptions {
                        allow_read_only: flags.allow_read_only,
                        version,
                        schema,
                        instance_name: deployment_name.into(),
                        multitenant,
                        skip_index_creation: flags.skip_index_creation,
                    };
                    // tokio-postgres forbids unknown query parameters, so we need to filter out
                    // `search_path` which is our "hack" for propagating the target schema name
                    // to the persistence layer
                    let query = url
                        .query_pairs()
                        .filter(|(k, _)| k != "search_path")
                        .map(|(k, v)| (k.into_owned(), v.into_owned()))
                        .collect::<HashMap<_, _>>();
                    let url = url.query_pairs_mut().clear().extend_pairs(query).finish();
                    Ok(PersistenceSeed::Postgres {
                        config: url
                            .as_str()
                            .parse()
                            .context("invalid postgres connection url")?,
                        options,
                    })
                },
                PersistenceArgs::MySql {
                    url,
                    db_name,
                    multitenant,
                    require_leader,
                } => {
                    let options = MySqlOptions {
                        allow_read_only: flags.allow_read_only,
                        version,
                        multitenant,
                        instance_name: deployment_name.into(),
                        deployment_id,
                    };
                    Ok(PersistenceSeed::MySql {
                        pool: Arc::new(ConvexMySqlPool::new(
                            &url,
                            *DATABASE_USE_PREPARED_STATEMENTS,
                            require_leader,
                            Some(runtime),
                        )?),
                        db_name,
                        options,
                    })
                },
            }
        },
        _ => unreachable!(),
    }
}

pub async fn connect_persistence<RT: Runtime>(
    db: DbDriverTag,
    db_spec: &str,
    flags: ConnectPersistenceFlags,
    deployment_name: &str,
    deployment_id: Option<DeploymentId>,
    runtime: RT,
    shutdown_signal: ShutdownSignal,
) -> anyhow::Result<Arc<dyn Persistence>> {
    match persistence_seed(db, db_spec, flags, deployment_name, deployment_id, runtime)? {
        PersistenceSeed::Sqlite { db_spec } => connect_sqlite(&db_spec).await,
        PersistenceSeed::Postgres {
            mut config,
            options,
        } => {
            config.target_session_attrs(TargetSessionAttrs::ReadWrite);
            let pool = PostgresPersistence::create_pool(config)?;
            let persistence =
                Arc::new(PostgresPersistence::with_pool(pool, options, shutdown_signal).await?);
            tracing::info!("Connected to Postgres database: {}", deployment_name);
            Ok(persistence)
        },
        PersistenceSeed::MySql {
            pool,
            db_name,
            options,
        } => {
            let persistence =
                mysql::connect_persistence(pool, db_name.clone(), options, shutdown_signal).await?;
            tracing::info!("Connected to MySQL database: {}", db_name);
            Ok(persistence)
        },
    }
}

pub async fn connect_persistence_reader<RT: Runtime>(
    db: DbDriverTag,
    db_spec: &str,
    require_ssl: bool,
    db_should_be_leader: bool,
    deployment_name: &str,
    deployment_id: Option<DeploymentId>,
    runtime: RT,
) -> anyhow::Result<Arc<dyn PersistenceReader>> {
    match persistence_seed(
        db,
        db_spec,
        ConnectPersistenceFlags {
            require_ssl,
            allow_read_only: true,
            skip_index_creation: false,
        },
        deployment_name,
        deployment_id,
        runtime,
    )? {
        PersistenceSeed::Sqlite { db_spec } => Ok(connect_sqlite(&db_spec).await?.reader()),
        PersistenceSeed::Postgres { config, options } => {
            let options = PostgresReaderOptions {
                version: options.version,
                schema: options.schema,
                instance_name: options.instance_name,
                multitenant: options.multitenant,
            };
            Ok(Arc::new(
                PostgresPersistence::new_reader(
                    PostgresPersistence::create_pool(config)
                        .context("failed to create postgres pool")?,
                    options,
                )
                .await?,
            ))
        },
        PersistenceSeed::MySql {
            pool,
            db_name,
            options,
        } => {
            let options = MySqlReaderOptions {
                db_should_be_leader,
                version: options.version,
                multitenant: options.multitenant,
                instance_name: options.instance_name,
                deployment_id: options.deployment_id,
            };
            mysql::connect_persistence_reader(pool, db_name, options)
        },
    }
}

pub async fn set_read_only<RT: Runtime>(
    db: DbDriverTag,
    db_spec: &str,
    flags: ConnectPersistenceFlags,
    instance_name: &str,
    deployment_id: Option<DeploymentId>,
    runtime: RT,
    read_only: bool,
) -> anyhow::Result<()> {
    match persistence_seed(db, db_spec, flags, instance_name, deployment_id, runtime)? {
        PersistenceSeed::Postgres { config, options } => {
            let pool = PostgresPersistence::create_pool(config)?;
            PostgresPersistence::set_read_only(pool, options, read_only).await?;
            Ok(())
        },
        PersistenceSeed::MySql {
            pool,
            db_name,
            options,
        } => {
            mysql::set_persistence_read_only(pool, db_name, options, read_only).await?;
            Ok(())
        },
        _ => anyhow::bail!("unsupported persistence type: {db:?}"),
    }
}
