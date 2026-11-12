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
        // OPS-47/STO-7: observe independently of a dispatch's possibly indefinite
        // claim COMMIT. Keep both futures alive until whole-process exit: neither
        // a pending claim receipt nor its sent transaction is a shutdown lever.
        let dispatch = dispatch(engine.clone(), dispatcher);
        let observation = observe(engine.clone());
        let controls = test_stops(engine);
        tokio::pin!(dispatch, observation, controls);
        let stopped = tokio::select! {
            result = &mut dispatch => result,
            result = &mut observation => result,
            result = &mut controls => result,
        };
        eprintln!("provisiond engine loop stopped: {stopped:?}");
        // Observe completion/failure of either required path. The lock observer
        // has already applied the original OPS-21 record bounds/exits. Exit
        // before either pinned future or any process-owned work can be dropped.
        std::process::exit(1)
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
// OPS-21: opt-in fixture requests use the executable's real record arbitration.
// Default builds cannot request stops through files or environment variables.
async fn test_stops(engine: Arc<Engine>) -> anyhow::Result<()> {
    #[cfg(feature = "store-test-hooks")]
    if let Ok(root) = std::env::var("PROVISIOND_TEST_CONTROL") {
        let root = std::path::Path::new(&root);
        let request = root.join("stop-operation");
        loop {
            if request.exists() {
                let id = std::fs::read_to_string(&request)?;
                std::fs::remove_file(&request)?;
                println!(
                    "{}",
                    serde_json::json!({"event":"test_stop_requested","operation":id})
                );
                engine.record_stop(&id)?;
                println!(
                    "{}",
                    serde_json::json!({"event":"test_record_snapshot","operation":id,"record":engine.test_record(&id)})
                );
                tokio::select! {
                    result = engine.wait_records() => {result?; anyhow::bail!("fixture stop recorded");},
                    _ = async {
                        loop {
                            let snapshot = root.join("snapshot-record");
                            if snapshot.exists() {
                                std::fs::remove_file(&snapshot).expect("fixture snapshot request");
                                println!("{}", serde_json::json!({"event":"test_record_snapshot","operation":id,"record":engine.test_record(&id)}));
                            }
                            if root.join("later-stop").exists() {break;}
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    } => {
                        let result = engine.record_stop(&id);
                        println!("{}", serde_json::json!({"event":"test_later_stop_arbitrated","operation":id,"record":engine.test_record(&id),"fatal":engine.is_fatal(),"error":result.as_ref().err().map(|e|format!("{e:#}"))}));
                        result?;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    let _ = engine;
    std::future::pending().await
}
async fn dispatch(
    engine: Arc<Engine>,
    dispatcher: Arc<provisiond::engine::Dispatcher>,
) -> anyhow::Result<()> {
    loop {
        #[cfg(feature = "store-test-hooks")]
        if let Ok(root) = std::env::var("PROVISIOND_TEST_CONTROL") {
            let hold = std::path::Path::new(&root).join("hold-dispatch");
            if hold.exists() {
                println!("{}", serde_json::json!({"event":"test_dispatch_paused"}));
                while hold.exists() {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
        }
        if let Err(error) = engine.dispatch_next(&dispatcher).await {
            // OPS-47: a pre-loss claim may finish during fan-out. Its next
            // new-work refusal must join the ongoing record drain, not win the
            // outer select and bypass it. Both paths remain polled while this
            // serialized observation waits; fatal engine errors still exit.
            engine.observe_lock().await?;
            return Err(error);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
async fn observe(engine: Arc<Engine>) -> anyhow::Result<()> {
    loop {
        engine.observe_lock().await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
fn process_refcount(process: &Arc<Process>) -> usize {
    Arc::strong_count(process)
}
