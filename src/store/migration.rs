//! STO-12/13: forward-only offline migrations through the existing admitted
//! session factory and process-owned synchronous COMMIT lifecycle.
use super::{CommitOutcome, Store};
use anyhow::{Context, Result, bail, ensure};
use serde_json::json;

pub const SCHEMA_VERSION: i64 = 1;
pub const SEMANTIC_VERSION: i64 = 1;
pub(crate) const MIGRATION_LOCK: i64 = 0x5052_4f56_4d49_4752;
pub(crate) const STARTUP_LOCK: i64 = 0x5052_4f56_5354_4152;
const CORE: &str = include_str!("../../migrations/0001-core.sql");

/// OVR-19: migrator inputs, not settled production defaults.
pub struct MigrationOptions {
    pub lock_ms: u32,
    pub lock_retries: u32,
}
impl Store {
    /// Deployment must first stop old components and establish that their sessions
    /// cannot write. The bootstrap schema/observer must already exist (D-5).
    pub async fn migrate(&self, options: MigrationOptions) -> Result<()> {
        ensure!(
            options.lock_ms > 0 && options.lock_ms <= self.settings.statement_ms,
            "migration lock timeout must be positive and no greater than statement timeout"
        );
        for attempt in 0..=options.lock_retries {
            // Deliberately no pool, acquisition retry or reconnect on failure.
            let stage = if attempt == 0 { "initial" } else { "reconnect" };
            let session = self.dedicated().await.map_err(|error| {
                anyhow::anyhow!("migrate release {SCHEMA_VERSION}/{SEMANTIC_VERSION}: {stage} acquisition failed: {error:#}")
            })?;
            let mut tx = self.begin_dedicated(session).await?;
            let result=async {
                tx.batch(&format!("SET LOCAL lock_timeout={}",options.lock_ms)).await?;
                tx.rows("SELECT pg_advisory_xact_lock($1)",&[&MIGRATION_LOCK]).await?;
                tx.batch("CREATE TABLE IF NOT EXISTS provisiond.schema_migrations (version bigint PRIMARY KEY, semantics bigint NOT NULL, completed_at timestamptz NOT NULL DEFAULT transaction_timestamp())").await?;
                let history=tx.rows("SELECT version,semantics FROM provisiond.schema_migrations ORDER BY version",&[]).await?;
                if !history.is_empty() {
                    ensure!(history.len()==1 && history[0].get::<_,i64>(0)==SCHEMA_VERSION && history[0].get::<_,i64>(1)==SEMANTIC_VERSION,"unsupported migration history or shared-row semantics");
                    return Ok(false);
                }
                tx.batch(CORE).await?; // One SQL program; never split on semicolons.
                tx.rows("INSERT INTO provisiond.schema_migrations(version,semantics) VALUES($1,$2) RETURNING version",&[&SCHEMA_VERSION,&SEMANTIC_VERSION]).await?;
                anyhow::Ok(true)
            }.await;
            match result {
                Ok(false) => {
                    tx.rollback().await?;
                    return Ok(());
                }
                Ok(true) => {
                    let outcome = self.commit(tx)?.wait().await?;
                    ensure!(
                        outcome == CommitOutcome::Acknowledged,
                        "migrate release {SCHEMA_VERSION}: completion uncertain or rejected: {outcome:?}; inspect history on next invocation"
                    );
                    (self.trace)(
                        json!({"event":"migration_acknowledged","version":SCHEMA_VERSION,"semantics":SEMANTIC_VERSION}),
                    );
                    return Ok(());
                }
                Err(error) => {
                    let lock_timeout = error
                        .downcast_ref::<tokio_postgres::Error>()
                        .and_then(|e| e.code())
                        .is_some_and(|c| c.code() == "55P03");
                    drop(tx);
                    if !lock_timeout || attempt == options.lock_retries {
                        return Err(error).context("migrate SQL/DDL failure");
                    }
                    (self.trace)(
                        json!({"event":"migration_lock_retry","attempt":attempt+1,"limit":options.lock_retries}),
                    );
                }
            }
        }
        bail!("migration retry limit exhausted")
    }

    /// STO-13: called before an API listener or the engine's lifetime lock.
    pub async fn compatible_schema(&self) -> Result<()> {
        let session = self.read_session().await?;
        ensure!(
            session
                .scalar("SELECT to_regclass('provisiond.schema_migrations') IS NOT NULL")
                .await?
                == "t",
            "missing/uninitialized schema"
        );
        let version=session.scalar("SELECT COALESCE(json_agg(json_build_array(version,semantics))::text,'[]') FROM provisiond.schema_migrations").await?;
        let expected = json!([[SCHEMA_VERSION, SEMANTIC_VERSION]]);
        ensure!(
            serde_json::from_str::<serde_json::Value>(&version)? == expected,
            "unsupported schema/semantic version: {version}; supports {expected}"
        );
        (self.trace)(
            json!({"event":"schema_compatible","version":SCHEMA_VERSION,"semantics":SEMANTIC_VERSION}),
        );
        Ok(())
    }
}
