//! Real Rust business/startup paths. External entry observations are simulators.
use anyhow::{Context, Result};
use provisiond::{
    engine::{Engine, Outcome},
    entry,
    store::{Claim, CreateRequest, ExternalKind, Payment, Process, Trace},
};
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Write},
    sync::Arc,
    time::Duration,
};
fn emit(v: Value) {
    println!("{v}");
    io::stdout().flush().unwrap();
}
#[tokio::main]
async fn main() -> Result<()> {
    let trace: Trace = Arc::new(emit);
    let mut process = Process::new(trace.clone());
    let store = Arc::new(entry::local_store(process.clone(), trace.clone())?);
    let api = store.component();
    let mut engine: Option<Arc<Engine>> = None;
    let mut component = api.clone();
    let mut tx = None;
    let mut receipt = None;
    let mut pending_claim = None;
    let mut pending_defer: Option<tokio::task::JoinHandle<Result<bool>>> = None;
    let mut unpolled_defer: Option<futures_util::future::BoxFuture<'static, Result<bool>>> = None;
    let mut pending_execute = None;
    let mut pending_observe = None;
    let mut pending_wait = None;
    let mut pending_begin = None;
    let mut pool_holders = Vec::new();
    for line in io::stdin().lock().lines() {
        let line = line?;
        let (cmd, arg) = line.split_once(' ').unwrap_or((&line, ""));
        if cmd == "exit" {
            break;
        }
        let result:Result<Value>=async {
            match cmd {
                "begin_start"=> {let c=component.clone();pending_begin=Some(tokio::spawn(async move {c.begin().await}));Ok(json!({}))}
                "begin_join"=> {let t: provisiond::store::Transaction=pending_begin.take().context("no pending begin")?.await??;tx=Some(t);Ok(json!({}))}
                "claim_join"=> {let c: Option<Claim>=pending_claim.take().context("no claim")?.await??;Ok(c.map_or(Value::Null,|c|json!({"id":c.id,"number":c.number})))}
                "pool_fill"=> {let e=engine.as_ref().context("no engine")?;for _ in 0..3 {pool_holders.push(e.component().begin().await?);}Ok(json!({}))}
                "pool_release"=> {for t in pool_holders.drain(..) {t.rollback().await?;}Ok(json!({}))}
                "owners"=>Ok(engine.as_ref().context("no engine")?.test_owners()),
                "record_state"=>Ok(engine.as_ref().context("no engine")?.test_record(arg)),
                "new_work"=> {engine.as_ref().context("no engine")?.test_new_work()?;Ok(json!({}))}
                "claim_start"=> {let e=engine.clone().context("no engine")?;pending_claim=Some(tokio::spawn(async move {e.claim().await}));Ok(json!({}))}
                "claim_cancel"=> {let t=pending_claim.take().context("no claim")?;t.abort();let _=t.await;Ok(json!({}))}
                "claim_pending"=>Ok(json!({"pending":!pending_claim.as_ref().context("no claim")?.is_finished()})),
                "execute_start"=> {let e=engine.clone().context("no engine")?;let id=arg.to_owned();pending_execute=Some(tokio::spawn(async move {e.execute_simulator(&id,None).await}));Ok(json!({}))}
                "execute_join"=> {pending_execute.take().context("no execution")?.await??;Ok(json!({}))}
                "boundary_release"=> {engine.as_ref().context("no engine")?.test_release();Ok(json!({}))}
                "observe_pair"=> {let e=engine.clone().context("no engine")?;pending_observe=Some(tokio::spawn(async move {tokio::join!(e.observe_lock(),e.observe_lock())}));Ok(json!({}))}
                "observe_join"=> {let (a,b)=pending_observe.take().context("no observers")?.await?;a?;b?;Ok(json!({}))}
                "wait_start"=> {let e=engine.clone().context("no engine")?;pending_wait=Some(tokio::spawn(async move {emit(json!({"event":"waiter_polled"}));e.wait_records().await}));Ok(json!({}))}
                "wait_cancel"=> {let t=pending_wait.take().context("no waiter")?;t.abort();let _=t.await;Ok(json!({}))}
                "wait_pending"=>Ok(json!({"pending":!pending_wait.as_ref().context("no waiter")?.is_finished()})),
                "wait_join"=> {pending_wait.take().context("no waiter")?.await??;Ok(json!({}))}
                "unfenced"=> {
                    let c=Claim {id:"00000000-0000-0000-0000-000000000001".into(),tenant:"unfenced".into(),kind:"create".into(),number:1};
                    let mut t=api.begin().await?;
                    let r=match arg {"claim"=>t.claim().await.map(|_|()),"defer"=>t.defer(&c).await.map(|_|()),"license"=>t.license_create(&c).await.map(|_|()),"verified"=>t.settle_verified_create(&c,"unfenced",None).await.map(|_|()),"reacquire"=>t.reacquire(&c).await.map(|_|()),_=>t.settle(&c,"failed",&Value::Null,&Value::Null).await.map(|_|())};
                    let refused=r.as_ref().err().is_some_and(|e|e.to_string().contains("engine-only transaction requires generation fence"));
                    t.rollback().await?;Ok(json!({"refused":refused}))
                }
                "engine_start"=> {
                    let wait=if arg.is_empty(){500}else{provisiond::store::startup_milliseconds(arg)?};
                    let e=Engine::start(entry::local_engine_store(process.clone(),trace.clone())?,process.clone(),wait,Duration::from_secs(2),Duration::from_secs(1)).await?;
                    let result=json!({"generation":e.generation(),"lock_pid":e.lock_pid()});
                    component=e.component();engine=Some(e);Ok(result)
                }
                "join_cleanup"=> {
                    let e=engine.as_ref().context("no engine")?;
                    let (a,b)=tokio::join!(e.test_join_cleanup(arg,"late_attach_cleanup"),e.test_join_cleanup(arg,"tenant_suspended"));
                    Ok(json!({"episodes":[a?,b?]}))
                }
                "dispatch_provider"=> {let e=engine.as_ref().context("no engine")?;let dispatcher=std::sync::Arc::new(provisiond::engine::Dispatcher::load(e).await?);e.dispatch_next(&dispatcher).await?;Ok(json!({}))}
                "generation_holder"=> {let (t,c)=store.test_generation_holder().await?;tx=Some(t);Ok(match c {Some(c)=>json!({"id":c.id,"number":c.number}),None=>Value::Null})}
                "pair_hold"=> {let tenants:Vec<String>=serde_json::from_str(arg)?;let refs:Vec<_>=tenants.iter().map(String::as_str).collect();let mut t=component.begin().await?;t.serialize_tenants(&refs).await?;tx=Some(t);Ok(json!({}))}
                "begin"=> {tx=Some(component.begin().await?);Ok(json!({"session":tx.as_ref().unwrap().session_id()}))}
                "commit"=> {receipt=Some(component.commit(tx.take().context("no tx")?)?);Ok(json!({}))}
                "await"=> {let outcome=receipt.take().context("no receipt")?.wait().await?;Ok(json!({"outcome":outcome.label()}))}
                "drop_receipt"=> {drop(receipt.take());Ok(json!({}))}
                "rollback"=> {tx.take().context("no tx")?.rollback().await?;Ok(json!({}))}
                "external"=> {
                    let kind=match arg {"provider"=>ExternalKind::Provider,"rail"=>ExternalKind::Rail,"rescue"=>ExternalKind::Rescue,_=>anyhow::bail!("unknown external interface")};
                    let _call=component.external(kind)?;
                    emit(json!({"event":"simulator_effect","interface":arg,"effect":"one simulated call"}));Ok(json!({}))
                }
                "admit"=> {
                    let v:Value=serde_json::from_str(arg)?;
                    let r=CreateRequest {receipt_seconds:86400,tenant:v["tenant"].as_str().context("tenant")?.into(),scope_kind:v["scope_kind"].as_str().unwrap_or("tenant").into(),scope_id:v["scope_id"].as_str().or(v["tenant"].as_str()).context("scope")?.into(),key:v["key"].as_str().context("key")?.into(),fingerprint:v["key"].as_str().context("key")?.into(),provider_account:"simulator".into(),channel:"simulated-channel".into(),offer:v["offer"].as_str().unwrap_or("catalogue").into(),offer_is_resource:v["resource"].as_bool().unwrap_or(false),reserved_sats:v["reserve"].as_i64().context("reserve")?,setup:v["setup"].clone(),first_unit:v.get("first_unit").cloned(),offer_snapshot:v["snapshot"].clone()};
                    Ok(json!({"id":component.admit_create(&r).await?}))
                }
                "payment"=> {
                    let v:Value=serde_json::from_str(arg)?;
                    let p=Payment {deposit:v["deposit"].as_str().context("deposit")?.into(),rail:v["rail"].as_str().context("rail")?.into(),reference:v["reference"].as_str().context("reference")?.into(),amount_sats:v["amount"].as_i64().context("amount")?,observed_at:"2026-11-06T00:00:00Z".into(),settled_at:"2026-11-06T00:00:00Z".into()};
                    Ok(json!({"id":component.credit_payment(&p).await?}))
                }
                "claim_race"=> {
                    let e=engine.as_ref().context("no engine")?;
                    let mut warmed=Vec::new();
                    for _ in 0..3 {warmed.push(e.component().begin().await?);}
                    for t in warmed {t.rollback().await?;}
                    let (a,b)=tokio::join!(e.claim(),e.claim());
                    let values=[a?,b?].into_iter().map(|c|match c {Some(c)=>json!({"id":c.id,"tenant":c.tenant,"kind":c.kind,"number":c.number}),None=>Value::Null}).collect::<Vec<_>>();
                    Ok(json!({"claims":values}))
                }
                "claim_slots"=> {
                    let e=engine.as_ref().context("no engine")?;
                    let mut warmed=Vec::new();
                    for _ in 0..3 {warmed.push(e.component().begin().await?);}
                    for t in warmed {t.rollback().await?;}
                    let (a,b,c,d)=tokio::join!(e.claim(),e.claim(),e.claim(),e.claim());
                    let values=[a,b,c,d].into_iter().map(|r|match r {Ok(Some(c))=>json!({"id":c.id}),Ok(None)=>Value::Null,Err(e)=>json!({"error":e.to_string()})}).collect::<Vec<_>>();
                    Ok(json!({"claims":values}))
                }
                "claim"=> {let c=engine.as_ref().context("no engine")?.claim().await?;Ok(match c {Some(c)=>json!({"id":c.id,"tenant":c.tenant,"kind":c.kind,"number":c.number}),None=>Value::Null})}
                "defer_join"=> {let affected: bool=pending_defer.take().context("no defer")?.await??;Ok(json!({"affected":affected}))}
                "defer_cancel"=> {let t=pending_defer.take().context("no defer")?;t.abort();let cancelled=t.await.unwrap_err().is_cancelled();anyhow::ensure!(cancelled,"defer waiter completed before cancellation");Ok(json!({"cancelled":cancelled}))}
                "defer_unpolled"|"defer_start"|"defer"|"reacquire"|"settle"|"verified"|"license"|"outcome"=> {
                    let v:Value=serde_json::from_str(arg)?;
                    let c=Claim {id:v["id"].as_str().context("id")?.into(),tenant:v["tenant"].as_str().context("tenant")?.into(),kind:v["kind"].as_str().unwrap_or("create").into(),number:v["number"].as_i64().context("claim number")?};
                    let outcome=if v.get("external_id").is_some() {Outcome::VerifiedCreate {external_id:v["external_id"].as_str().unwrap().into(),earliest_date:None}}else {Outcome::Settled {status:v["status"].as_str().unwrap_or("failed").into(),result:Value::Null,error:json!({"kind":"conflict","retryable":true})}};
                    let e=engine.as_ref().context("no engine")?;
                    match cmd {
                        "reacquire"=> {let mut t=component.begin().await?;let affected=t.reacquire(&c).await?;let outcome=component.commit(t)?.wait().await?;Ok(json!({"affected":affected,"outcome":outcome.label()}))}
                        "defer"=>Ok(json!({"affected":e.defer(&c).await?})),
                        "defer_start"=> {let e=e.clone();pending_defer=Some(tokio::spawn(async move {e.defer(&c).await}));Ok(json!({}))}
                        "defer_unpolled"=> {let e=e.clone();let mut waiter: futures_util::future::BoxFuture<'static,Result<bool>>=Box::pin(async move {e.defer(&c).await});anyhow::ensure!(futures_util::poll!(&mut waiter).is_pending(),"defer unexpectedly completed on first poll");unpolled_defer=Some(waiter);Ok(json!({}))}
                        "settle"|"verified"=>Ok(json!({"affected":e.settle(&c,&outcome).await?})),
                        "outcome"=> {e.outcome(&c,outcome)?;Ok(json!({}))}
                        "license"=> {let mut t=component.begin().await?;let affected=t.license_create(&c).await?;let outcome=component.commit(t)?.wait().await?;Ok(json!({"affected":affected,"outcome":outcome.label()}))}
                        _=>unreachable!()
                    }
                }
                "execute_pending"=> {engine.as_ref().context("no engine")?.execute_simulator(arg,None).await?;Ok(json!({}))}
                "execute_return"=> {let (id,external)=arg.split_once(' ').map(|(id,external)|(id,external.to_owned())).unwrap_or_else(||(arg,format!("simulator-verified-{arg}")));engine.as_ref().context("no engine")?.execute_simulator(id,Some(Outcome::VerifiedCreate {external_id:external,earliest_date:None})).await?;Ok(json!({}))}
                "start_settlement"=> {engine.as_ref().context("no engine")?.start_settlement(arg)?;Ok(json!({}))}
                "record_stop"=> {engine.as_ref().context("no engine")?.record_stop(arg)?;Ok(json!({}))}
                "wait_records"=> {engine.as_ref().context("no engine")?.wait_records().await?;Ok(json!({}))}
                "poll_lock"=> {engine.as_ref().context("no engine")?.observe_lock().await?;Ok(json!({}))}
                "later_stop"=> {engine.as_ref().context("no engine")?.later_stop()?;Ok(json!({}))}
                "audit"=> {component.audit_ledger(arg).await?;Ok(json!({}))}
                _=>anyhow::bail!("unknown core command"),
            }
        }.await;
        let fatal = (cmd == "engine_start" && result.is_err())
            || result
                .as_ref()
                .err()
                .is_some_and(|e| e.downcast_ref::<provisiond::store::FatalEngine>().is_some())
            || engine.as_ref().is_some_and(|e| e.is_fatal());
        match result {
            Ok(value) => emit(json!({"event":"result","command":cmd,"ok":true,"value":value})),
            Err(e) => {
                emit(json!({"event":"result","command":cmd,"ok":false,"error":format!("{e:#}")}))
            }
        }
        if fatal {
            emit(json!({"event":"fatal_process_exit","new_transaction_permitted":false}));
            std::process::exit(71);
        }
    }
    drop(tx);
    drop(receipt);
    drop(unpolled_defer);
    drop(component);
    drop(engine);
    drop(api);
    drop(store);
    for _ in 0..100 {
        if Arc::strong_count(&process) == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let p = Arc::get_mut(&mut process).context("process references retained")?;
    if let Err(e) = p.shutdown(Duration::from_secs(2)).await {
        eprintln!("{e:#}");
        std::process::exit(75);
    }
    Ok(())
}
