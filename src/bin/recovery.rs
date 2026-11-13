//! STO-13/54/56: separate operator entry; deployment owns stop/fence prerequisites.
use anyhow::{Context, Result, ensure};
use provisiond::{
    entry, recovery,
    store::{Process, Trace},
};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() {
    let trace: Trace = Arc::new(|v| println!("{v}"));
    let mut process = Process::new(trace.clone());
    let result: Result<()> = async {
        let store = entry::local_store(process.clone(), trace)?;
        store.compatible_schema().await?;
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        match args.first().map(String::as_str) {
            Some("--check-schema") if args.len() == 1 => (),
            Some("report") if args.len() == 1 => println!("{}", recovery::current(&store).await?),
            Some("prepare") if args.len() == 1 => {
                ensure!(std::env::var("PROVISIOND_COMPONENTS_STOPPED").as_deref() == Ok("yes"), "pre-lock preparation requires stopped components");
                recovery::prepare(Arc::new(store)).await?;
            }
            Some("close") if args.len() == 2 => recovery::close(&store, &args[1]).await?,
            Some("establish") if args.len() == 3 => {
                ensure!(std::env::var("PROVISIOND_COMPONENTS_STOPPED").as_deref() == Ok("yes"), "deployment must stop components and prevent restart before establishment");
                let evidence = std::fs::read_to_string(std::env::var("PROVISIOND_RECOVERY_EVIDENCE").context("missing retained incident evidence file")?)?;
                let request = recovery::Establish {
                    id: args[1].clone(), predecessor: (args[2] != "none").then(|| args[2].clone()),
                    expected_instant: std::env::var("PROVISIOND_RECOVERY_EXPECTED_INSTANT").ok(),
                    evidence: serde_json::from_str(&evidence)?,
                    rederivation_seconds: std::env::var("PROVISIOND_RECOVERY_INTERVAL_SECONDS")?.parse()?,
                    derivation_gap: std::env::var("PROVISIOND_RECOVERY_DERIVATION_GAP")?.parse()?,
                };
                println!("{}", recovery::establish(&store, request).await?);
            }
            _ => anyhow::bail!("usage: recovery --check-schema | report | establish EVENT_ID PREDECESSOR_ID_OR_none"),
        }
        Ok(())
    }.await;
    if let Err(error) = &result {
        eprintln!("provisiond recovery: {error:#}");
    }
    let Some(owner) = Arc::get_mut(&mut process) else {
        std::process::exit(1);
    };
    let shutdown = owner.shutdown(Duration::from_secs(1)).await;
    if result.is_err() || shutdown.is_err() {
        std::process::exit(1);
    }
}
