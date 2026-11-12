//! PRV-13b: stop-cost adapter probe on an independently created test-only resource.
use anyhow::{Context, Result, ensure};
use provisiond::{
    engine, entry,
    store::{Process, Trace},
    wire,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
#[tokio::main]
async fn main() -> Result<()> {
    let trace: Trace = Arc::new(|e| println!("{e}"));
    let mut process = Process::new(trace.clone());
    let store = entry::local_store(process.clone(), trace)?;
    store.compatible_schema().await?;
    let args: Vec<_> = std::env::args().collect();
    let result = engine::test_simulator_stop_cost(
        store,
        args.get(1).context("account")?,
        args.get(2).context("test external identity")?,
    )
    .await;
    if args.get(3).is_some_and(|arg| arg == "--report") {
        // PRV-5/13b: expose the existing adapter's actual safe failure, without
        // interpreting timeout as absence or introducing production deletion.
        let value = match result {
            Ok(confirmed) => json!({"confirmed":confirmed,"error":null}),
            Err(error) => {
                json!({"confirmed":false,"error":error.downcast_ref::<wire::Error>().context("typed provider failure")?.0})
            }
        };
        println!("{}", json!({"event":"stop_cost_result","value":value}));
    } else {
        ensure!(result?, "stop-cost adapter did not confirm absence");
    }
    Arc::get_mut(&mut process)
        .context("store not released")?
        .shutdown(Duration::from_secs(1))
        .await?;
    Ok(())
}
