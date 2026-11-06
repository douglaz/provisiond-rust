//! STO-13: explicit local-prototype entry configuration and compatibility checks.
use crate::store::{Process, Settings, Store, Trace, migration::MigrationOptions};
use anyhow::{Context, Result, ensure};
use std::{env, sync::Arc, time::Duration};

/// No deployment defaults are selected by the prototype. This named profile uses
/// D-6's provisional local fixture settings and cannot contact external providers.
pub fn local_store(process: Arc<Process>, trace: Trace) -> Result<Store> {
    ensure!(env::var("PROVISIOND_PROFILE").as_deref()==Ok("local-simulator"),
        "pre-conformance prototype requires PROVISIOND_PROFILE=local-simulator");
    let connection=env::var("PROVISIOND_DATABASE_URL").context("missing database connection")?
        .parse().map_err(|_|anyhow::anyhow!("invalid database connection configuration"))?;
    Store::new(Settings {
        connection,
        designated_name: env::var("PROVISIOND_STANDBY_NAME").context("missing designated standby name")?,
        designated_role: env::var("PROVISIOND_STANDBY_ROLE").context("missing designated standby role")?,
        designated_address: env::var("PROVISIOND_STANDBY_ADDRESS").context("missing designated standby address")?,
        statement_ms:1000, lock_ms:500, idle_transaction_ms:1000,
        call_bound:Duration::from_secs(2), check_bound:Duration::from_secs(3),
        checkout_bound:Duration::from_millis(250),
        keepalive_idle:Duration::from_secs(2),keepalive_interval:Duration::from_secs(1),keepalive_count:2,
    },process,1,1,trace)
}

pub async fn migrate(store: &Store) -> Result<()> {
    store.migrate(MigrationOptions { lock_ms:500, lock_retries:2 }).await
}
