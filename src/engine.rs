//! OPS-15/16/21/47/49/50: prototype startup, claims and bounded shielded records.
//! Provider execution/reconciliation and later operation kinds remain unimplemented.
use crate::store::{
    Claim, CommitOutcome, Component, EngineDatabase, FatalEngine, Store, Transaction,
};
use anyhow::{Context, Result, ensure};
use futures_util::future::{AbortHandle, Abortable, BoxFuture};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
pub enum Outcome {
    VerifiedCreate {
        external_id: String,
        earliest_date: Option<String>,
    },
    Settled {
        status: String,
        result: Value,
        error: Value,
    },
}
struct Record {
    deadline: Mutex<Option<Instant>>,
    result: Mutex<Option<std::result::Result<bool, String>>>,
}
struct Worker {
    claim: Claim,
    component: Component,
    outcome: Option<Outcome>,
    record: Option<Arc<Record>>,
    execution: Option<(AbortHandle, Arc<AtomicBool>)>,
}
pub struct Engine {
    database: EngineDatabase,
    process: Arc<crate::store::Process>,
    workers: Mutex<BTreeMap<String, Worker>>,
    retry_bound: Duration,
    stop_bound: Duration,
}
impl Engine {
    pub async fn start(
        store: Store,
        process: Arc<crate::store::Process>,
        wait_ms: u32,
        retry_bound: Duration,
        stop_bound: Duration,
    ) -> Result<Arc<Self>> {
        ensure!(
            !retry_bound.is_zero() && !stop_bound.is_zero(),
            "retry and stop bounds must be positive"
        );
        let database = EngineDatabase::start(store, wait_ms).await?;
        let engine = Arc::new(Self {
            database,
            process,
            workers: Mutex::new(BTreeMap::new()),
            retry_bound,
            stop_bound,
        });
        engine.restart_pass().await?;
        Ok(engine)
    }
    pub fn component(&self) -> Component {
        self.database.component()
    }
    pub fn generation(&self) -> &str {
        self.database.generation()
    }
    pub fn lock_pid(&self) -> i32 {
        self.database.lock_pid()
    }
    fn fatal<T>(&self, reason: &'static str) -> Result<T> {
        self.database.fatal(reason);
        Err(FatalEngine(reason).into())
    }
    pub fn is_fatal(&self) -> bool {
        self.database.is_fatal()
    }
    pub fn later_stop(&self) -> Result<()> {
        self.fatal("later stop: exit without repeating pending records")
    }
    /// OPS-49 excludes COMMIT's standby wait; elapsed precommit work is charged
    /// separately. Constraint errors return to the caller, serialization retries
    /// immediately, lock/connection/check errors consume one monotonic budget.
    async fn transaction<T: Send>(
        &self,
        component: &Component,
        mut operation: impl for<'a> FnMut(&'a mut Transaction) -> BoxFuture<'a, Result<T>>,
    ) -> Result<T> {
        let mut spent = Duration::ZERO;
        loop {
            let started = Instant::now();
            let attempt = async {
                let mut tx = component.begin().await?;
                let value = operation(&mut tx).await?;
                anyhow::Ok((value, tx))
            }
            .await;
            let mut serialization = false;
            match attempt {
                Ok((value, tx)) => {
                    let work_time = started.elapsed();
                    let (outcome, verification) = component.commit(tx)?.wait_accounted().await?;
                    if !matches!(&outcome,CommitOutcome::Rejected{reason} if reason.contains("40001"))
                    {
                        spent += work_time + verification;
                    }
                    match outcome {
                        CommitOutcome::Acknowledged => return Ok(value),
                        CommitOutcome::Rejected { reason } if reason.contains("40001") => {
                            serialization = true
                        }
                        CommitOutcome::Rejected { reason } if reason.contains("23") => {
                            anyhow::bail!("constraint rejected: {reason}")
                        }
                        _ => {}
                    }
                }
                Err(error) => {
                    if error.downcast_ref::<FatalEngine>().is_some() {
                        return Err(error);
                    }
                    let code = error
                        .downcast_ref::<tokio_postgres::Error>()
                        .and_then(|e| e.code())
                        .map(|c| c.code());
                    if code.is_some_and(|c| c.starts_with("23")) {
                        return Err(error);
                    }
                    serialization = code == Some("40001");
                    if !serialization {
                        spent += started.elapsed();
                    }
                }
            }
            if spent >= self.retry_bound {
                return self.fatal("store retry exhausted; outcome retained until process exit");
            }
            component.trace(json!({"event":"whole_transaction_retry","spent_ms":spent.as_millis(),"serialization":serialization,"external_call_repeated":false}));
            if !serialization {
                let backoff = Instant::now();
                tokio::time::sleep(Duration::from_millis(10)).await;
                spent += backoff.elapsed();
            }
        }
    }
    /// OPS-15/16: bounded discovery batches commit before per-row serialized writes.
    async fn restart_pass(&self) -> Result<()> {
        let component = self.component();
        let mut count = 0;
        loop {
            let mut tx = component.begin().await?;
            ensure!(
                tx.rows(
                    "SELECT id FROM provisiond.recovery_incidents WHERE closed_at IS NULL LIMIT 1",
                    &[]
                )
                .await?
                .is_empty(),
                "open recovery incident unsupported: ordinary restart forbidden"
            );
            let rows=tx.rows("SELECT id::text,tenant_id FROM provisiond.operations WHERE status='running' ORDER BY id LIMIT 32",&[]).await?;
            ensure!(
                component.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
                "startup discovery unacknowledged"
            );
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let id: String = row.get(0);
                let tenant: String = row.get(1);
                let mut tx = component.begin().await?;
                crate::ledger::serialize(&mut tx, &[&tenant]).await?;
                // The about-to-order predicate is inside the guarded write.
                let mut rows=tx.rows("UPDATE provisiond.operations SET status='failed',error=COALESCE(error,jsonb_build_object('kind','internal','message','interrupted by ordinary restart','retryable',true)),request=NULL,revision=revision+1,updated_at=clock_timestamp() WHERE id=$1::text::uuid AND status='running' AND kind='create' AND about_to_order_at IS NULL RETURNING kind,status,commitment_id::text",&[&id]).await?;
                if rows.is_empty() {
                    rows=tx.rows("UPDATE provisiond.operations SET status=CASE WHEN kind='refresh' THEN 'failed' ELSE 'needs_reconciliation' END,error=COALESCE(error,jsonb_build_object('kind','internal','message','interrupted by ordinary restart','retryable',true)),request=NULL,revision=revision+1,updated_at=clock_timestamp() WHERE id=$1::text::uuid AND status='running' RETURNING kind,status,commitment_id::text",&[&id]).await?;
                }
                if let Some(row) = rows.first() {
                    if row.get::<_, String>(0) == "create" && row.get::<_, String>(1) == "failed" {
                        tx.release_create(&id, row.get::<_, Option<String>>(2).as_deref())
                            .await?;
                    }
                    count += 1;
                }
                ensure!(
                    component.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
                    "startup row unacknowledged"
                );
            }
        }
        component.trace(json!({"event":"restart_pass_complete","running_found":count}));
        Ok(())
    }
    pub async fn claim(self: &Arc<Self>) -> Result<Option<Claim>> {
        self.observe_lock().await?;
        ensure!(
            self.workers.lock().expect("workers").len() < self.database.worker_limit(),
            "all configured workers occupied"
        );
        self.database.ensure_new_work()?;
        let component = self.component();
        // Ordinary pre-COMMIT failures consume OPS-49's whole-transaction budget.
        // Once a claim might have committed, a lost reply is fatal, never retried.
        let mut spent = Duration::ZERO;
        let claim = loop {
            let started = Instant::now();
            let attempt = async {
                let mut tx = component.begin().await?;
                let claim = tx.claim().await?;
                anyhow::Ok((tx, claim))
            }
            .await;
            match attempt {
                Ok((tx, claim)) => match component.commit(tx)?.wait().await? {
                    CommitOutcome::Acknowledged => break claim,
                    CommitOutcome::Rejected { reason } if reason.contains("40001") => continue,
                    _ => {
                        return self
                            .fatal("claim acknowledgment lost or rejected; no blind claim retry");
                    }
                },
                Err(error) => {
                    if error.downcast_ref::<FatalEngine>().is_some() {
                        return Err(error);
                    }
                    let code = error
                        .downcast_ref::<tokio_postgres::Error>()
                        .and_then(|e| e.code())
                        .map(|c| c.code());
                    if code == Some("40001") {
                        continue;
                    }
                    if code.is_some_and(|c| c.starts_with("23")) {
                        return Err(error);
                    }
                    spent += started.elapsed();
                    if spent >= self.retry_bound {
                        return self.fatal("claim store retry exhausted before dispatch");
                    }
                    component.trace(
                        json!({"event":"claim_precommit_retry","spent_ms":spent.as_millis()}),
                    );
                    let backoff = Instant::now();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    spent += backoff.elapsed();
                }
            }
        };
        if let Some(c) = &claim {
            self.workers.lock().expect("workers").insert(
                c.id.clone(),
                Worker {
                    claim: c.clone(),
                    component,
                    outcome: None,
                    record: None,
                    execution: None,
                },
            );
        }
        Ok(claim)
    }
    /// Cancellable simulator entry using the worker's own component ownership.
    pub async fn execute_simulator(
        self: &Arc<Self>,
        id: &str,
        outcome: Option<Outcome>,
    ) -> Result<()> {
        self.observe_lock().await?;
        self.database.ensure_new_work()?;
        let (claim, component) = {
            let workers = self.workers.lock().expect("workers");
            let w = workers.get(id).context("worker not found")?;
            ensure!(
                w.execution.is_none() && w.record.is_none(),
                "worker already executing or recording"
            );
            (w.claim.clone(), w.component.clone())
        };
        if claim.kind == "create" {
            let c = claim.clone();
            self.transaction(&component, move |tx| {
                let c = c.clone();
                Box::pin(async move {
                    ensure!(
                        tx.license_create(&c).await?,
                        "create dispatch guard refused"
                    );
                    Ok(())
                })
            })
            .await?;
        }
        let (abort, registration) = AbortHandle::new_pair();
        let done = Arc::new(AtomicBool::new(false));
        self.workers
            .lock()
            .expect("workers")
            .get_mut(id)
            .context("worker not found")?
            .execution = Some((abort, done.clone()));
        let engine = self.clone();
        let id = id.to_owned();
        self.process.submit(async move {
            let execution = async {
                let _call = component.external(crate::store::ExternalKind::Provider)?;
                component
                    .trace(json!({"event":"simulator_accepted","operation":id,"simulator":true}));
                if let Some(outcome) = outcome {
                    Ok(outcome)
                } else {
                    std::future::pending::<Result<Outcome>>().await
                }
            };
            if let Ok(Ok(outcome)) = Abortable::new(execution, registration).await {
                if let Some(w) = engine.workers.lock().expect("workers").get_mut(&id) {
                    w.outcome = Some(outcome);
                }
                component.trace(json!({"event":"execute_returned","operation":id}));
            }
            done.store(true, Ordering::Release);
        })?;
        Ok(())
    }
    pub fn outcome(&self, id: &str, outcome: Outcome) -> Result<()> {
        let mut workers = self.workers.lock().expect("workers");
        let worker = workers.get_mut(id).context("worker not found")?;
        ensure!(worker.record.is_none(), "execute already stopped");
        worker.outcome = Some(outcome);
        Ok(())
    }
    pub async fn settle(self: &Arc<Self>, c: &Claim, outcome: &Outcome) -> Result<bool> {
        if self
            .workers
            .lock()
            .expect("workers")
            .get(&c.id)
            .is_some_and(|w| w.claim.number == c.number)
        {
            self.outcome(&c.id, outcome.clone())?;
            let record = self.begin_settlement(&c.id)?;
            loop {
                let result = record.result.lock().expect("record").clone();
                if let Some(result) = result {
                    return result.map_err(anyhow::Error::msg);
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }

        let component = self
            .workers
            .lock()
            .expect("workers")
            .get(&c.id)
            .map(|w| w.component.clone())
            .unwrap_or_else(|| self.component());
        let operation_id = c.id.clone();
        let number = c.number;
        let c = c.clone();
        let outcome = outcome.clone();
        let affected = self
            .transaction(&component, move |tx| {
                let c = c.clone();
                let outcome = outcome.clone();
                Box::pin(async move { apply_outcome(tx, &c, &outcome).await })
            })
            .await?;
        if !affected {
            component.trace(json!({"event":"worker_zero_row","operation":operation_id,"observed":"guard did not match; worker ends"}));
        }
        self.retire(&operation_id, number);
        Ok(affected)
    }
    fn retire(&self, id: &str, number: i64) {
        let mut workers = self.workers.lock().expect("workers");
        if workers.get(id).is_some_and(|w| w.claim.number == number) {
            workers.remove(id);
        }
    }
    pub async fn defer(&self, c: &Claim) -> Result<bool> {
        let component = self
            .workers
            .lock()
            .expect("workers")
            .get(&c.id)
            .map(|w| w.component.clone())
            .unwrap_or_else(|| self.component());
        let id = c.id.clone();
        let number = c.number;
        let c = c.clone();
        let affected = self
            .transaction(&component, move |tx| {
                let c = c.clone();
                Box::pin(async move { tx.defer(&c).await })
            })
            .await?;
        self.retire(&id, number);
        Ok(affected)
    }
    /// Retain the after-return outcome and its entire settlement loop even if a
    /// stop drops the caller. A first stop adds its bound to this same task.
    fn begin_settlement(self: &Arc<Self>, id: &str) -> Result<Arc<Record>> {
        let (claim, component, outcome, record) = {
            let mut workers = self.workers.lock().expect("workers");
            let worker = workers.get_mut(id).context("worker not found")?;
            ensure!(worker.record.is_none(), "settlement already registered");
            let outcome = worker.outcome.clone().context("execute has not returned")?;
            let record = Arc::new(Record {
                deadline: Mutex::new(None),
                result: Mutex::new(None),
            });
            worker.record = Some(record.clone());
            (
                worker.claim.clone(),
                worker.component.clone(),
                outcome,
                record,
            )
        };
        let engine = self.clone();
        let pending = record.clone();
        self.process.submit(async move {
            let id = claim.id.clone();
            let number = claim.number;
            let result = engine
                .transaction(&component, move |tx| {
                    let claim = claim.clone();
                    let outcome = outcome.clone();
                    Box::pin(async move { apply_outcome(tx, &claim, &outcome).await })
                })
                .await
                .map_err(|e| format!("{e:#}"));
            component.trace(
                json!({"event":"settlement_finished","operation":id,"landed":result.is_ok()}),
            );
            if matches!(result,Ok(false)) {
                component.trace(json!({"event":"worker_zero_row","operation":id,"observed":"guard did not match; worker ends"}));
            }
            if let Err(error)=&result {engine.database.fatal(&format!("atomic settlement failed for {id}: {error}"));}
            let success = result.is_ok();
            *pending.result.lock().expect("record") = Some(result);
            if success {
                engine.retire(&id, number);
            }
        })?;
        Ok(record)
    }
    pub fn start_settlement(self: &Arc<Self>, id: &str) -> Result<()> {
        self.begin_settlement(id)?;
        Ok(())
    }
    /// OPS-21: registration is synchronous. Cancelling the waiter cannot cancel
    /// the record task; it is supervised by the surviving process, like COMMIT.
    pub fn record_stop(self: &Arc<Self>, id: &str) -> Result<()> {
        let (claim, component, outcome, record, execution) = {
            let mut workers = self.workers.lock().expect("workers");
            let worker = workers.get_mut(id).context("worker not found")?;
            if let Some(record) = &worker.record {
                let mut deadline = record.deadline.lock().expect("record deadline");
                if deadline.is_some() {
                    return self.fatal("later stop: exit without repeating pending record");
                }
                *deadline = Some(Instant::now() + self.stop_bound);
                worker.component.trace(json!({"event":"stop_record_started","operation":worker.claim.id,"bound_ms":self.stop_bound.as_millis(),"existing_settlement":true}));
                return Ok(());
            }
            let record = Arc::new(Record {
                deadline: Mutex::new(Some(Instant::now() + self.stop_bound)),
                result: Mutex::new(None),
            });
            worker.record = Some(record.clone());
            (
                worker.claim.clone(),
                worker.component.clone(),
                worker.outcome.clone(),
                record,
                worker.execution.clone(),
            )
        };
        if let Some((abort, _)) = &execution {
            abort.abort();
        }
        component.trace(json!({"event":"stop_record_started","operation":claim.id,"bound_ms":self.stop_bound.as_millis()}));
        let operation_id = claim.id.clone();
        let engine = self.clone();
        self.process.submit(async move {
            if let Some((_,done))=execution {
                while !done.load(Ordering::Acquire) {tokio::time::sleep(Duration::from_millis(1)).await;}
            }
            let result=engine.transaction(&component,move |tx| {
                let claim=claim.clone();let outcome=outcome.clone();
                Box::pin(async move {
                    if let Some(outcome)=outcome {return apply_outcome(tx,&claim,&outcome).await;}
                    // Read current durable position and markers in the record's
                    // transaction, never a stale in-memory marker snapshot.
                    crate::ledger::serialize(tx,&[&claim.tenant]).await?;
                    let rows=tx.rows("SELECT kind,about_to_order_at IS NOT NULL,write_started_at IS NOT NULL FROM provisiond.operations WHERE id=$1::text::uuid",&[&claim.id]).await?;
                    let row=rows.first().context("stopped operation missing")?;
                    let kind:String=row.get(0);
                    let ambiguous=match kind.as_str() {"create"=>row.get::<_,bool>(1),"refresh"=>false,"power"=>row.get::<_,bool>(2),_=>anyhow::bail!("unsupported stop kind")};
                    tx.settle(&claim,if ambiguous {"needs_reconciliation"}else{"failed"},&Value::Null,&json!({"kind":"conflict","details":{"reason":"cut_off"},"retryable":!ambiguous})).await
                })
            }).await.map_err(|e|format!("{e:#}"));
            engine.component().trace(json!({"event":"stop_record_finished","operation":operation_id,"landed":result.is_ok(),"detail":result.as_ref().err()}));
            *record.result.lock().expect("record")=Some(result);
        })?;
        Ok(())
    }
    pub async fn wait_records(&self) -> Result<()> {
        loop {
            let pending: Vec<_> = self
                .workers
                .lock()
                .expect("workers")
                .iter()
                .filter_map(|(id, w)| w.record.as_ref().map(|r| (id.clone(), r.clone())))
                .collect();
            let mut unfinished = Vec::new();
            let mut expired = false;
            for (id, record) in pending {
                let result = record.result.lock().expect("record");
                match &*result {
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        self.database
                            .fatal(&format!("stop record failed for {id}: {error}"));
                        return Err(
                            FatalEngine("stop record failed; exit without classification").into(),
                        );
                    }
                    None => {
                        expired |= record
                            .deadline
                            .lock()
                            .expect("record deadline")
                            .is_some_and(|deadline| Instant::now() >= deadline);
                        unfinished.push(id);
                    }
                }
            }
            if expired {
                self.database.fatal(&format!(
                    "stop bound expired; records not observed landed: {}",
                    unfinished.join(",")
                ));
                return Err(FatalEngine("stop record deadline expired; process must exit").into());
            }
            if unfinished.is_empty() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    /// Lock loss is the first stop only for workers without a pending record.
    pub async fn observe_lock(self: &Arc<Self>) -> Result<()> {
        if self.is_fatal() {
            return Err(FatalEngine("engine fatal; process must exit").into());
        }
        if !self.database.detect_lock_loss() {
            return Ok(());
        }
        let ids: Vec<_> = self
            .workers
            .lock()
            .expect("workers")
            .iter()
            .filter(|(_, w)| {
                w.record
                    .as_ref()
                    .is_none_or(|r| r.deadline.lock().expect("record deadline").is_none())
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            self.record_stop(&id)?;
        }
        self.wait_records().await?;
        self.fatal("startup lock lost; bounded records completed; exit nonzero")
    }
}
async fn apply_outcome(tx: &mut Transaction, c: &Claim, outcome: &Outcome) -> Result<bool> {
    match outcome {
        Outcome::VerifiedCreate {
            external_id,
            earliest_date,
        } => {
            tx.settle_verified_create(c, external_id, earliest_date.as_deref())
                .await
        }
        Outcome::Settled {
            status,
            result,
            error,
        } => tx.settle(c, status, result, error).await,
    }
}
