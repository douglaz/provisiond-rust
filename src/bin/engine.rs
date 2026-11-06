//! OPS-15/47/50, STO-13: supported-schema startup and lifetime lock monitor.
use provisiond::{
    engine::Engine,
    entry,
    store::{Process, Trace, startup_milliseconds},
};
use std::{sync::Arc, time::Duration};
#[tokio::main]
async fn main() {
    let trace: Trace = Arc::new(|event| println!("{event}"));
    let mut process = Process::new(trace.clone());
    let result = async {
        let store = entry::local_engine_store(process.clone(), trace)?;
        if std::env::args().any(|a| a == "--check-schema") {
            return store.compatible_schema().await;
        }
        let wait = startup_milliseconds(&std::env::var("PROVISIOND_STARTUP_WAIT_MS")?)?;
        let engine = Engine::start(
            store,
            process.clone(),
            wait,
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .await?;
        let dispatcher = Arc::new(provisiond::engine::Dispatcher::load(&engine).await?);
        loop {
            engine.dispatch_next(&dispatcher).await?;
            engine.observe_lock().await?;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    .await;
    if let Err(error) = &result {
        eprintln!("provisiond engine: {error:#}");
    }
    // An uncertain sent transaction is never released merely to keep this process
    // alive. Whole-process exit is the bounded stop's final ownership boundary.
    if process_refcount(&process) > 1 {
        std::process::exit(1);
    }
    let shutdown = Arc::get_mut(&mut process)
        .expect("store released")
        .shutdown(Duration::from_secs(1))
        .await;
    if result.is_err() || shutdown.is_err() {
        std::process::exit(1);
    }
}
fn process_refcount(process: &Arc<Process>) -> usize {
    Arc::strong_count(process)
}
