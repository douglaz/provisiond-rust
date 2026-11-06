//! STO-13: schema check precedes the deliberately non-serving prototype path.
use provisiond::{
    entry,
    store::{Process, Trace},
};
use std::{sync::Arc, time::Duration};
#[tokio::main]
async fn main() {
    let trace: Trace = Arc::new(|event| println!("{event}"));
    let mut process = Process::new(trace.clone());
    let result = async {
        let store = entry::local_store(process.clone(), trace)?;
        store.compatible_schema().await?;
        if std::env::args().any(|a| a == "--check-schema") {
            Ok(())
        } else {
            provisiond::api::serve(store, Arc::new(|event| println!("{event}"))).await
        }
    }
    .await;
    if let Err(error) = &result {
        eprintln!("provisiond api: {error:#}");
    }
    let shutdown = Arc::get_mut(&mut process)
        .expect("store released")
        .shutdown(Duration::from_secs(1))
        .await;
    if result.is_err() || shutdown.is_err() {
        std::process::exit(1);
    }
}
