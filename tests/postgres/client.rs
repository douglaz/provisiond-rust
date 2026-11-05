//! Actual-client STO-7/CNF-56/CNF-365 fixture; no product entry point.
use provisiond::store::{Process, Settings, Store, Trace};
use serde_json::json;
use std::{
    io::{self, BufRead, Write},
    sync::Arc,
    time::Duration,
};
fn emit(value: serde_json::Value) {
    println!("{value}");
    io::stdout().flush().unwrap();
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let trace: Trace = Arc::new(emit);
    let mut process = Process::new(trace.clone());
    let mut config: tokio_postgres::Config = std::env::var("STORE_CONNECTION")?.parse()?;
    config.application_name("store_probe");
    let settings = Settings {
        connection: config,
        designated_name: "provisiond_sync".into(),
        designated_role: "replicator".into(),
        designated_address: "127.0.0.1".into(),
        statement_ms: 1000,
        lock_ms: 500,
        idle_transaction_ms: 1000,
        call_bound: Duration::from_secs(2),
        check_bound: Duration::from_secs(3),
        checkout_bound: Duration::from_millis(250),
        keepalive_idle: Duration::from_secs(2),
        keepalive_interval: Duration::from_secs(1),
        keepalive_count: 2,
    };
    let store = Arc::new(Store::new(settings, process.clone(), 1, 1, trace)?);
    let mut pending_precommit = None;
    let mut held = None;
    for line in io::stdin().lock().lines() {
        let line = line?;
        let mut words = line.split_whitespace();
        let command = words.next().unwrap_or("");
        if command == "exit" {
            break;
        }
        let result: anyhow::Result<_> = async {
            match command {
                "abandon" => {
                    let mut tx = store.begin().await?;
                    let pid = tx.session_id();
                    tx.execute("INSERT INTO fixture.destinations VALUES (700,'never-exposed')")
                        .await?;
                    drop(tx);
                    let next = store.write_session().await?;
                    anyhow::ensure!(next.pid != pid, "abandoned physical session reused");
                    Ok(json!({"old_session":pid,"new_session":next.pid}))
                }
                "failed" => {
                    let mut tx = store.begin().await?;
                    let pid = tx.session_id();
                    anyhow::ensure!(
                        tx.execute("SELECT 1/0").await.is_err(),
                        "expected failed transaction"
                    );
                    drop(tx);
                    let mut next = store.write_session().await?;
                    anyhow::ensure!(next.pid != pid, "aborted physical session reused");
                    next.ping().await?;
                    Ok(json!({"old_session":pid,"new_session":next.pid}))
                }
                "pre_start" => {
                    let worker_store = store.clone();
                    pending_precommit = Some(tokio::spawn(async move {
                        let mut tx = worker_store.begin().await?;
                        tx.execute("SELECT pg_sleep(30)").await?;
                        anyhow::Ok(())
                    }));
                    Ok(json!({}))
                }
                "pre_cancel" => {
                    let task = pending_precommit.take().unwrap();
                    task.abort();
                    anyhow::ensure!(
                        task.await.unwrap_err().is_cancelled(),
                        "precommit task cancellation"
                    );
                    let next = store.write_session().await?;
                    Ok(json!({"new_session":next.pid}))
                }
                "admit" => {
                    held = Some(store.read_session().await?);
                    Ok(json!({"session":held.as_ref().unwrap().pid}))
                }
                "release" => {
                    drop(held.take());
                    Ok(json!({}))
                }
                "ping" => {
                    held.as_mut().unwrap().ping().await?;
                    Ok(json!({}))
                }
                "settings" => Ok(json!({"settings":held.as_mut().unwrap().settings().await?})),
                "check" => {
                    held.as_mut().unwrap().test_check().await?;
                    Ok(json!({}))
                }
                "read" => {
                    drop(held.take());
                    let id = words.next().unwrap().parse()?;
                    Ok(json!({"value":store.destination(id).await?}))
                }
                _ => anyhow::bail!("unknown command"),
            }
        }
        .await;
        match result {
            Ok(value) => emit(json!({"event":"result","command":command,"ok":true,"value":value})),
            Err(e) => emit(
                json!({"event":"result","command":command,"ok":false,"error":format!("{e:#}")}),
            ),
        }
    }
    drop(held);
    drop(store);
    // Process registration is explicit; all Manager references have now gone.
    let p = Arc::get_mut(&mut process).expect("process owner references drained");
    p.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}
