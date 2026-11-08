//! PRV-13b: stop-cost adapter probe on an independently created test-only resource.
use anyhow::{Context, Result, ensure};
use provisiond::{
    engine, entry,
    store::{Process, Trace},
};
use std::{sync::Arc, time::Duration};
#[tokio::main]
async fn main() -> Result<()> {
    let trace: Trace = Arc::new(|e| println!("{e}"));
    let mut process = Process::new(trace.clone());
    let store = entry::local_store(process.clone(), trace)?;
    store.compatible_schema().await?;
    let args: Vec<_> = std::env::args().collect();
    ensure!(
        engine::test_simulator_stop_cost(
            store,
            args.get(1).context("account")?,
            args.get(2).context("test external identity")?
        )
        .await?,
        "stop-cost adapter did not confirm absence"
    );
    Arc::get_mut(&mut process)
        .context("store not released")?
        .shutdown(Duration::from_secs(1))
        .await?;
    Ok(())
}
