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
fn request_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
fn main() -> anyhow::Result<()> {
    let mut runtime = Some(request_runtime());
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
    let mut pending_transaction = None;
    let mut receipt = None;
    let mut bounded_stop = false;
    let mut held: Option<provisiond::store::Checkout> = None;
    for line in io::stdin().lock().lines() {
        let line = line?;
        let mut words = line.split_whitespace();
        let command = words.next().unwrap_or("");
        if command == "exit" || command == "stop" {
            bounded_stop = command == "stop";
            if bounded_stop {
                emit(json!({"event":"stop_started"}));
            }
            break;
        }
        if command == "process_exit" {
            emit(json!({"event":"process_exit_requested","unresolved":true}));
            std::process::exit(74);
        }
        // STO-7: keep the process, Store and observer alive while destroying
        // every request-runtime resource, including its reactor.
        if command == "drop_runtime" {
            drop(runtime.take().unwrap());
            emit(json!({"event":"request_runtime_dropped"}));
            runtime = Some(request_runtime()); // independent control path only
            emit(json!({"event":"result","command":command,"ok":true,"value":{}}));
            continue;
        }
        let result: anyhow::Result<_> = runtime.as_ref().unwrap().block_on(async {
            match command {
                "prepare" => {
                    anyhow::ensure!(pending_transaction.is_none() && receipt.is_none(),"consumer already has a transaction/receipt");
                    let id:i32=words.next().unwrap().parse()?;
                    let mut tx=store.begin().await?;
                    let pid=tx.session_id();
                    tx.execute(&format!("INSERT INTO fixture.destinations VALUES ({id},'client-{id}')")).await?;
                    pending_transaction=Some(tx);
                    Ok(json!({"session":pid}))
                }
                "commit" => {
                    let tx=pending_transaction.take().unwrap();
                    receipt=Some(match std::env::var("STORE_TEST_DEFECT").as_deref() {
                        Ok("retention")=>store.test_commit(tx,provisiond::store::TestCommitDefect::ReleaseCheckout)?,
                        Ok("warning")=>store.test_commit(tx,provisiond::store::TestCommitDefect::IgnoreWarning)?,
                        _=>store.commit(tx)?,
                    });
                    Ok(json!({"registered":true}))
                }
                "await" | "assert_unacknowledged" => {
                    let outcome=receipt.take().unwrap().wait().await?;
                    if command=="assert_unacknowledged" && outcome==provisiond::store::CommitOutcome::Acknowledged {
                        emit(json!({"event":"assertion_failure","assertion":"replication warning must not acknowledge"}));
                        std::process::exit(43);
                    }
                    Ok(json!({"outcome":outcome.label(),"detail":format!("{outcome:?}")}))
                }
                "drop_request" => { drop(receipt.take()); Ok(json!({})) }
                "timeout_request" => {
                    anyhow::ensure!(tokio::time::timeout(Duration::from_millis(30),receipt.take().unwrap().wait()).await.is_err(),"expected middleware deadline during SyncRep");
                    Ok(json!({"middleware_timeout":true}))
                }
                "cancel_worker" => {
                    let waiter=receipt.take().unwrap();
                    let worker=tokio::spawn(async move {waiter.wait().await});
                    worker.abort();
                    anyhow::ensure!(worker.await.unwrap_err().is_cancelled(),"worker cancellation");
                    Ok(json!({}))
                }
                "assert_retained" => {
                    if store.write_checked_out()!=1 {
                        emit(json!({"event":"assertion_failure","assertion":"original write checkout retained","checked_out":store.write_checked_out()}));
                        std::process::exit(42);
                    }
                    let error=store.write_session().await.err().expect("write must be refused at checkout");
                    anyhow::ensure!(error.to_string().contains("waiting"),"refusal was not pool capacity: {error}");
                    Ok(json!({"checked_out":1,"checkout_refused":true}))
                }
                "past_deadline" => {
                    tokio::time::sleep(Duration::from_millis(2200)).await;
                    Ok(json!({"checked_out":store.write_checked_out()}))
                }
                "recycle" => { store.recycle_idle(); Ok(json!({"checked_out":store.write_checked_out()})) }
                "tx_begin" => {
                    let tx=store.begin().await?;
                    let pid=tx.session_id();
                    pending_transaction=Some(tx);
                    Ok(json!({"session":pid}))
                }
                "tx_execute" => {
                    pending_transaction.as_mut().unwrap().execute("INSERT INTO fixture.destinations VALUES (1800,'deadline-uncommitted')").await?;
                    Ok(json!({}))
                }
                "tx_cancel_execute" => {
                    let tx=pending_transaction.as_mut().unwrap();
                    anyhow::ensure!(tokio::time::timeout(Duration::from_millis(300),tx.execute("INSERT INTO fixture.destinations VALUES (1802,'cancel-retained')")).await.is_err(),"execute was not in flight");
                    Ok(json!({"retained":true}))
                }
                "setting_attack" => {
                    let mut tx=store.begin().await?;
                    let sql=line.split_once(' ').unwrap().1;
                    let refused=tx.test_caller_sql(sql).await.is_err();
                    let mode=tx.test_diagnostics();
                    drop(tx);
                    Ok(json!({"refused":refused,"diagnostics":mode}))
                }
                "corrupt_setting" => {
                    let mut tx=store.begin().await?;
                    tx.execute("SET LOCAL synchronous_commit=off").await?;
                    tx.execute("INSERT INTO fixture.destinations VALUES (1803,'must-not-commit')").await?;
                    let outcome=store.commit(tx)?.wait().await?;
                    Ok(json!({"outcome":outcome.label()}))
                }
                "notice_flood" | "notice_warning" => {
                    let mut tx=store.begin().await?;
                    let initial=tx.test_diagnostics().len();
                    tx.execute("DO $$ BEGIN FOR i IN 1..80 LOOP RAISE NOTICE 'fixture notice %',i; END LOOP; END $$").await?;
                    let retained=tx.test_diagnostics().len();
                    if command=="notice_warning" { tx.execute("INSERT INTO fixture.warn VALUES (1)").await?; }
                    let outcome=store.commit(tx)?.wait().await?;
                    Ok(json!({"initial":initial,"retained":retained,"outcome":outcome.label()}))
                }
                "tx_drop" => { drop(pending_transaction.take()); Ok(json!({})) }
                "write_admit" => {
                    let session=store.write_session().await?;
                    Ok(json!({"session":session.pid}))
                }
                "rollback" => { pending_transaction.take().unwrap().rollback().await?; Ok(json!({})) }
                "observe" => {
                    let pid:i32=words.next().unwrap().parse()?;
                    Ok(json!({"observed":held.as_mut().unwrap().test_query(&format!("SELECT pid::text || ':' || wait_event FROM pg_stat_activity WHERE pid={pid}")).await?}))
                }
                "reject_failed" => {
                    let mut tx=store.begin().await?;
                    anyhow::ensure!(tx.execute("SELECT 1/0").await.is_err(),"expected failure");
                    let outcome=store.commit(tx)?.wait().await?;
                    anyhow::ensure!(matches!(outcome,provisiond::store::CommitOutcome::Rejected{..}),"failed transaction dispatched");
                    Ok(json!({"outcome":outcome.label()}))
                }
                "deferred" => {
                    let mut tx=store.begin().await?;
                    tx.execute("INSERT INTO fixture.deferred VALUES (999),(999)").await?;
                    receipt=Some(store.commit(tx)?);
                    Ok(json!({}))
                }
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
                // STO-7: the harness withholds the reply; the caller then drops
                // the in-flight call and the next checkout must be a new session.
                "cancel_query" => {
                    drop(held.take());
                    let mut session = store.read_session().await?;
                    let pid = session.pid;
                    let call = session.test_query("SELECT 'cancelled query probe'::text");
                    anyhow::ensure!(
                        tokio::time::timeout(Duration::from_millis(300), call).await.is_err(),
                        "query finished before cancellation"
                    );
                    drop(session);
                    let next = store.read_session().await?;
                    anyhow::ensure!(next.pid != pid, "cancelled query session reused");
                    Ok(json!({"old_session":pid,"new_session":next.pid}))
                }
                "cancel_read" => {
                    drop(held.take());
                    let id = words.next().unwrap().parse()?;
                    // Pool size 1: the attempt checks out this same idle session.
                    let pid = store.read_session().await?.pid;
                    anyhow::ensure!(
                        tokio::time::timeout(Duration::from_millis(300), store.destination(id))
                            .await
                            .is_err(),
                        "destination finished before cancellation"
                    );
                    let next = store.read_session().await?;
                    anyhow::ensure!(next.pid != pid, "cancelled destination session reused");
                    Ok(json!({"old_session":pid,"new_session":next.pid}))
                }
                "read" => {
                    drop(held.take());
                    let id = words.next().unwrap().parse()?;
                    Ok(json!({"value":store.destination(id).await?}))
                }
                _ => anyhow::bail!("unknown command"),
            }
        });
        match result {
            Ok(value) => emit(json!({"event":"result","command":command,"ok":true,"value":value})),
            Err(e) => emit(
                json!({"event":"result","command":command,"ok":false,"error":format!("{e:#}")}),
            ),
        }
    }
    runtime.as_ref().unwrap().block_on(async {
        if let Some(task) = pending_precommit {
            task.abort();
            let _ = task.await;
        }
        drop(pending_transaction);
        drop(receipt);
        drop(held);
        drop(store);
        // Process registration is explicit; all Manager references have now gone.
        let p = Arc::get_mut(&mut process).expect("process owner references drained");
        let bound = if bounded_stop {
            Duration::from_secs(1)
        } else {
            Duration::from_secs(5)
        };
        if let Err(error) = p.shutdown(bound).await {
            emit(json!({"event":"stop_bound_expired","unresolved":true,"error":error.to_string()}));
            std::process::exit(75);
        }
        Ok(())
    })
}
