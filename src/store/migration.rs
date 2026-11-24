//! STO-12/13: forward-only offline migrations through the existing admitted
//! session factory and process-owned synchronous COMMIT lifecycle.
use super::{CommitOutcome, Store};
use anyhow::{Context, Result, ensure};
use serde_json::json;

pub const SCHEMA_VERSION: i64 = 5;
pub const SEMANTIC_VERSION: i64 = 5;
pub(crate) const MIGRATION_LOCK: i64 = 0x5052_4f56_4d49_4752;
pub(crate) const STARTUP_LOCK: i64 = 0x5052_4f56_5354_4152;
const CORE: &str = include_str!("../../migrations/0001-core.sql");
const SERVICE: &str = include_str!("../../migrations/0002-local-service.sql");
const RECONCILIATION: &str = include_str!("../../migrations/0003-reconciliation.sql");
// STO-13: serving binaries accept only schema/semantics 5. The offline
// migrator additionally accepts the exact historical prefixes 0 through 4.
pub const MIN_SCHEMA_VERSION: i64 = 5;
pub const STOP_ORDER: &str = "stop all components; prevent restart; disable application login; drain all application sessions; migrate; enable login; start engine; start api";
const RELEASE: &str = include_str!("../../migrations/0005-offline-release.sql");
const RECOVERY: &str = include_str!("../../migrations/0004-recovery.sql");

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
        // STO-13: fail closed even on a no-op invocation. These are the complete
        // deployment login identities, not group roles or the migrator identity.
        let configured = std::env::var("PROVISIOND_APPLICATION_ROLES")
            .context("migrate requires PROVISIOND_APPLICATION_ROLES (comma-separated application login roles)")?;
        let roles: Vec<String> = configured.split(',').map(|r| r.trim().to_owned()).collect();
        ensure!(
            roles.iter().all(|r| !r.is_empty())
                && roles
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    == roles.len(),
            "migrate requires nonempty, distinct application login roles"
        );
        let mut connected = false;
        let mut lock_attempt = 0;
        loop {
            // Deliberately no pool, acquisition retry or reconnect on failure.
            let stage = if connected {
                "reconnect"
            } else {
                "initial acquisition"
            };
            let session = self.dedicated().await.map_err(|error| {
                anyhow::anyhow!("migrate release schema {SCHEMA_VERSION}/semantics {SEMANTIC_VERSION}: {stage} failed: {}", connection_reason(&error))
            })?;
            connected = true;
            let mut tx = self.begin_dedicated(session).await?;
            let result=async {
                tx.batch(&format!("SET LOCAL lock_timeout={}",options.lock_ms)).await?;
                tx.rows("SELECT pg_advisory_xact_lock($1)",&[&MIGRATION_LOCK]).await?;
                // pg_roles and pg_stat_activity session identities are public;
                // no query text, wait event or privileged statistics is needed.
                let stopped = tx.rows("SELECT r.name, p.rolcanlogin, (SELECT count(*) FROM pg_catalog.pg_stat_activity a WHERE a.usename=r.name) FROM unnest($1::text[]) AS r(name) LEFT JOIN pg_catalog.pg_roles p ON p.rolname=r.name", &[&roles]).await?;
                for row in stopped {
                    let name: String = row.get(0);
                    let login: Option<bool> = row.get(1);
                    let backends: i64 = row.get(2);
                    ensure!(login == Some(false) && backends == 0,
                        "migrate offline prerequisite refused: application role {name}: rolcanlogin={login:?}, remaining backends={backends}; require existing NOLOGIN role and zero backends before DDL");
                }
                tx.batch("CREATE TABLE IF NOT EXISTS provisiond.schema_migrations (version bigint PRIMARY KEY, semantics bigint NOT NULL, completed_at timestamptz NOT NULL DEFAULT transaction_timestamp())").await?;
                let history=tx.rows("SELECT version,semantics FROM provisiond.schema_migrations ORDER BY version",&[]).await?;
                let recorded = history.iter().map(|r| [r.get::<_,i64>(0), r.get::<_,i64>(1)]).collect::<Vec<_>>();
                ensure!(history.len()<=SCHEMA_VERSION as usize && history.iter().enumerate().all(|(i,r)|r.get::<_,i64>(0)==(i+1) as i64 && r.get::<_,i64>(1)==(i+1) as i64),"migrate recorded schema/semantics {recorded:?} unsupported; migration input range 0..=5, exact semantics 1..=5");
                if history.len()==SCHEMA_VERSION as usize {
                    let declarations = tx.rows("SELECT version,stop_order,contract_step,rollback_floor FROM provisiond.schema_migrations ORDER BY version", &[]).await?;
                    let recorded_declarations = declarations.iter().map(|r| json!([r.get::<_,i64>(0),r.get::<_,String>(1),r.get::<_,bool>(2),r.get::<_,i64>(3)])).collect::<Vec<_>>();
                    ensure!(declarations.iter().all(|r| r.get::<_,String>(1)==STOP_ORDER && r.get::<_,bool>(2) && r.get::<_,i64>(3)==r.get::<_,i64>(0)), "migrate recorded schema/semantics {recorded:?}, contract/rollback declarations {recorded_declarations:?} unsupported; schema range 0..=5, semantics 1..=5, contracts/floors 1..=5");
                    return Ok(None);
                }
                // STO-12: one complete migration program and its history row per
                // acknowledged transaction, including on a fresh database.
                let version=history.len() as i64+1;
                tx.batch(match version {1=>CORE,2=>SERVICE,3=>RECONCILIATION,4=>RECOVERY,5=>RELEASE,_=>unreachable!()}).await?;
                if super::test_defect("migration_failure") {tx.batch("SELECT 1/0 /* interrupted migration fixture */").await?;}
                if version == 5 {
                    tx.rows("INSERT INTO provisiond.schema_migrations(version,semantics,stop_order,contract_step,rollback_floor) VALUES($1,$1,$2,true,$1) RETURNING version", &[&version,&STOP_ORDER]).await?;
                } else {
                    tx.rows("INSERT INTO provisiond.schema_migrations(version,semantics) VALUES($1,$1) RETURNING version",&[&version]).await?;
                }
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
                        return Err(error).with_context(|| format!("migrate release {SCHEMA_VERSION}/{SEMANTIC_VERSION} SQL/DDL failure ({stage}): inspect history; no connection retry"));
                    }
                    lock_attempt += 1;
                    (self.trace)(
                        json!({"event":"migration_lock_retry","attempt":lock_attempt,"limit":options.lock_retries}),
                    );
                }
            }
        }
    }

    /// STO-13: called before an API listener, engine lifetime lock or recovery write.
    pub async fn compatible_schema(&self) -> Result<()> {
        self.compatible_schema_for("component").await
    }
    pub async fn compatible_schema_for(&self, binary: &str) -> Result<()> {
        let session = self.read_session().await?;
        let supported = format!(
            "{binary} supports schema range {MIN_SCHEMA_VERSION}..={SCHEMA_VERSION}, shared-row semantics {SEMANTIC_VERSION}, contract steps 1..=5 and rollback floor 5"
        );
        ensure!(
            session
                .scalar("SELECT to_regclass('provisiond.schema_migrations') IS NOT NULL")
                .await?
                == "t",
            "{binary}: recorded schema/semantics missing; {supported}"
        );
        let recorded = session.scalar("SELECT COALESCE(json_agg(json_build_array(version,semantics) ORDER BY version)::text,'[]') FROM provisiond.schema_migrations").await?;
        let expected = json!((1..=SCHEMA_VERSION).map(|v| [v, v]).collect::<Vec<_>>());
        ensure!(
            serde_json::from_str::<serde_json::Value>(&recorded)? == expected,
            "{binary}: recorded schema/semantics {recorded}; {supported}"
        );
        let declarations = session.scalar("SELECT json_agg(json_build_array(version,stop_order,contract_step,rollback_floor) ORDER BY version)::text FROM provisiond.schema_migrations").await
            .with_context(|| format!("{binary}: recorded schema/semantics {recorded}, unreadable contract/rollback declarations; {supported}"))?;
        let expected = json!(
            (1..=SCHEMA_VERSION)
                .map(|v| json!([v, STOP_ORDER, true, v]))
                .collect::<Vec<_>>()
        );
        ensure!(
            serde_json::from_str::<serde_json::Value>(&declarations)? == expected,
            "{binary}: recorded schema/semantics {recorded}, contract/rollback declarations {declarations}; {supported}"
        );
        (self.trace)(
            json!({"event":"schema_compatible","binary":binary,"version":SCHEMA_VERSION,"semantics":SEMANTIC_VERSION,"supported_min":MIN_SCHEMA_VERSION,"supported_max":SCHEMA_VERSION}),
        );
        Ok(())
    }
}

/// STO-13: keep the database's reason verbatim and include SQLSTATE, without
/// formatting a connection configuration (which can contain credentials).
fn connection_reason(error: &anyhow::Error) -> String {
    if let Some(db) = error
        .downcast_ref::<tokio_postgres::Error>()
        .and_then(|e| e.as_db_error())
    {
        format!("SQLSTATE {}: {}", db.code().code(), db.message())
    } else {
        format!("{error:#}")
    }
}
