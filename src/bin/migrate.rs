//! STO-12/13: forward-only offline migration entry point, supported schema 1/1.
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
        entry::migrate(&store).await
    }
    .await;
    if let Err(error) = &result {
        eprintln!("provisiond migrate: {error:#}");
    }
    let shutdown = Arc::get_mut(&mut process)
        .expect("store released")
        .shutdown(Duration::from_secs(1))
        .await;
    if result.is_err() || shutdown.is_err() {
        std::process::exit(1);
    }
}
