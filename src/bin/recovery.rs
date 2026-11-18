//! STO-13/54/56: separate operator entry; deployment owns stop/fence prerequisites.
use anyhow::{Result, ensure};
use provisiond::{
    entry, recovery,
    store::{Process, Trace},
};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() {
    if std::env::var("PROVISIOND_RECOVERY_PARENT_PIPE").as_deref() == Ok("yes") {
        std::thread::spawn(|| {
            use std::io::Read;
            let mut input = std::io::stdin().lock();
            let mut byte = [0u8; 1];
            loop {
                match input.read(&mut byte) {
                    Ok(0) | Err(_) => std::process::exit(1),
                    Ok(_) => {}
                }
            }
        });
    }
    let trace: Trace = Arc::new(|v| eprintln!("{v}"));
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
                let store = Arc::new(store);
                recovery::prepare(store.clone()).await?;
                println!("{}", recovery::current(&store).await?);
            }
            Some("close") if args.len() == 2 => {
                recovery::close(&store, &args[1]).await?;
                println!("{}", recovery::incident(&store, &args[1]).await?);
            }
            Some("parent") if args.len() == 4 => {
                recovery::parent(&store, &args[1], &args[2], &args[3]).await?;
                println!("{}", recovery::incident(&store, &args[1]).await?);
            }
            Some("establish") if args.len() == 3 => {
                ensure!(std::env::var("PROVISIOND_COMPONENTS_STOPPED").as_deref() == Ok("yes"), "deployment must stop components and prevent restart before establishment");
                // STO-54: reporting input is optional, including on a retry after
                // the host's file was lost. Retained incident evidence wins on a
                // matching retry; initial establishment records unknown reasons.
                let input = std::env::var_os("PROVISIOND_RECOVERY_EVIDENCE")
                    .map(std::fs::read_to_string);
                let evidence = match input {
                    None => None,
                    Some(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Some(Err(_)) => {
                        eprintln!("STO-54: reporting input unreadable; loss endpoints remain unknown");
                        Some(serde_json::json!({"reporting_input":"malformed"}))
                    }
                    Some(Ok(body)) => Some(serde_json::from_str(&body).unwrap_or_else(|_| {
                        eprintln!("STO-54: reporting input malformed JSON; loss endpoints remain unknown");
                        serde_json::json!({"reporting_input":"malformed"})
                    })),
                }.unwrap_or_else(|| {
                    eprintln!("STO-54: no reporting input supplied; loss endpoints remain unknown");
                    serde_json::json!({"reporting_input":"absent"})
                });
                let request = recovery::Establish {
                    id: args[1].clone(), predecessor: (args[2] != "none").then(|| args[2].clone()),
                    expected_instant: std::env::var("PROVISIOND_RECOVERY_EXPECTED_INSTANT").ok(),
                    evidence,
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
