//! STO-12/13: forward-only offline migrations through the existing admitted
//! session factory and process-owned synchronous COMMIT lifecycle.
use super::{CommitOutcome, Store};
use anyhow::{Context, Result, ensure};
use serde_json::json;

pub const SCHEMA_VERSION: i64 = 3;
pub const SEMANTIC_VERSION: i64 = 3;
pub(crate) const MIGRATION_LOCK: i64 = 0x5052_4f56_4d49_4752;
pub(crate) const STARTUP_LOCK: i64 = 0x5052_4f56_5354_4152;
const CORE: &str = include_str!("../../migrations/0001-core.sql");
const SERVICE: &str = include_str!("../../migrations/0002-local-service.sql");
const RECONCILIATION: &str = include_str!("../../migrations/0003-reconciliation.sql");

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
        let mut connected = false;
        let mut lock_attempt = 0;
        loop {
            // Deliberately no pool, acquisition retry or reconnect on failure.
            let stage = if connected { "reconnect" } else { "initial" };
            let session = self.dedicated().await.map_err(|error| {
                anyhow::anyhow!("migrate release {SCHEMA_VERSION}/{SEMANTIC_VERSION}: {stage} acquisition failed: {error:#}")
            })?;
            connected = true;
            let mut tx = self.begin_dedicated(session).await?;
            let result=async {
                tx.batch(&format!("SET LOCAL lock_timeout={}",options.lock_ms)).await?;
                tx.rows("SELECT pg_advisory_xact_lock($1)",&[&MIGRATION_LOCK]).await?;
                tx.batch("CREATE TABLE IF NOT EXISTS provisiond.schema_migrations (version bigint PRIMARY KEY, semantics bigint NOT NULL, completed_at timestamptz NOT NULL DEFAULT transaction_timestamp())").await?;
                let history=tx.rows("SELECT version,semantics FROM provisiond.schema_migrations ORDER BY version",&[]).await?;
                ensure!(history.len()<=SCHEMA_VERSION as usize && history.iter().enumerate().all(|(i,r)|r.get::<_,i64>(0)==(i+1) as i64 && r.get::<_,i64>(1)==(i+1) as i64),"unsupported migration history or shared-row semantics");
                if history.len()==SCHEMA_VERSION as usize { return Ok(None); }
                // STO-12: one complete migration program and its history row per
                // acknowledged transaction, including on a fresh database.
                let version=history.len() as i64+1;
                tx.batch(match version {1=>CORE,2=>SERVICE,3=>RECONCILIATION,_=>unreachable!()}).await?;
                if super::test_defect("migration_failure") {tx.batch("SELECT 1/0 /* interrupted migration fixture */").await?;}
                tx.rows("INSERT INTO provisiond.schema_migrations(version,semantics) VALUES($1,$1) RETURNING version",&[&version]).await?;
                anyhow::Ok(Some(version))
            }.await;
            match result {
                Ok(None) => {
                    tx.rollback().await?;
                    return Ok(());
                }
                Ok(Some(version)) => {
                    let outcome = self.commit(tx)?.wait().await?;
                    ensure!(
                        outcome == CommitOutcome::Acknowledged,
                        "migrate release {SCHEMA_VERSION}: completion uncertain or rejected: {outcome:?}; inspect history on next invocation"
                    );
                    (self.trace)(
                        json!({"event":"migration_acknowledged","version":version,"semantics":version}),
                    );
                    if version == SCHEMA_VERSION {
                        return Ok(());
                    }
                    lock_attempt = 0;
                }
                Err(error) => {
                    let lock_timeout = error
                        .downcast_ref::<tokio_postgres::Error>()
                        .and_then(|e| e.code())
                        .is_some_and(|c| c.code() == "55P03");
                    drop(tx);
                    if !lock_timeout || lock_attempt == options.lock_retries {
                        return Err(error).context("migrate SQL/DDL failure");
                    }
                    lock_attempt += 1;
                    (self.trace)(
                        json!({"event":"migration_lock_retry","attempt":lock_attempt,"limit":options.lock_retries}),
                    );
                }
            }
        }
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
        let version=session.scalar("SELECT COALESCE(json_agg(json_build_array(version,semantics) ORDER BY version)::text,'[]') FROM provisiond.schema_migrations").await?;
        let expected = json!([[1, 1], [2, 2], [SCHEMA_VERSION, SEMANTIC_VERSION]]);
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
