//! STO-13: schema compatibility precedes the explicit local HTTP service.
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
    // STO-7: if an owner remains, exit without unwinding its live work. Never
    // turn an ordinary service failure into an ownership-assertion panic.
    let Some(owner) = Arc::get_mut(&mut process) else {
        eprintln!("provisiond api: store owners remain at shutdown; process must exit");
        std::process::exit(1);
    };
    let shutdown = owner.shutdown(Duration::from_secs(1)).await;
    if let Err(error) = &shutdown {
        eprintln!("provisiond api: {error:#}");
    }
    if result.is_err() || shutdown.is_err() {
        std::process::exit(1);
    }
}
