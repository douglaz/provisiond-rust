//! OPS-15/16/21/47/49/50: prototype startup, claims and bounded shielded records.
//! Provider execution/reconciliation and later operation kinds remain unimplemented.
use crate::store::{
    Claim, CommitOutcome, Component, EngineDatabase, FatalEngine, Store, Transaction,
};
use anyhow::{Context, Result, ensure};
use futures_util::future::BoxFuture;
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
struct RecordState {
    deadline: Option<Instant>,
    result: Option<std::result::Result<bool, String>>,
    slot: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
}
struct Record {
    state: Mutex<RecordState>,
}
#[derive(Default)]
struct Execution {
    stopped: AtomicBool,
    done: AtomicBool,
    dispatched: AtomicBool,
}
#[derive(Debug)]
struct ExecutionStopped;
impl std::fmt::Display for ExecutionStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("execution stopped before continuation")
    }
}
impl std::error::Error for ExecutionStopped {}
struct Worker {
    slot: Arc<tokio::sync::OwnedSemaphorePermit>,
    claim: Claim,
    component: Component,
    // OPS-6/21: execution and record retain this claim's result across replacement.
    outcome: Arc<Mutex<Option<Outcome>>>,
    record: Option<Arc<Record>>,
    execution: Option<Arc<Execution>>,
}
pub struct Engine {
    database: EngineDatabase,
    process: Arc<crate::store::Process>,
    workers: Mutex<BTreeMap<String, Worker>>,
    // OPS-21: active observation belongs to a claim, independently of the
    // current operation-id worker. Entries end with their terminal result.
    records: Mutex<BTreeMap<(String, i64), Arc<Record>>>,
    worker_slots: Arc<tokio::sync::Semaphore>,
    lock_observation: tokio::sync::Mutex<()>,
    #[cfg(feature = "store-test-hooks")]
    test_hold: AtomicBool,
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
        let worker_slots = Arc::new(tokio::sync::Semaphore::new(database.worker_limit()));
        let engine = Arc::new(Self {
            database,
            process,
            workers: Mutex::new(BTreeMap::new()),
            records: Mutex::new(BTreeMap::new()),
            worker_slots,
            lock_observation: tokio::sync::Mutex::new(()),
            #[cfg(feature = "store-test-hooks")]
            test_hold: AtomicBool::new(true),
            retry_bound,
            stop_bound,
        });
        engine.restart_pass().await?;
        Ok(engine)
    }
    // Concrete acceptance windows only; absent from default builds.
    async fn test_boundary(&self, name: &str) {
        #[cfg(feature = "store-test-hooks")]
        if std::env::var("STORE_TEST_BOUNDARY").as_deref() == Ok(name) {
            self.component()
                .trace(json!({"event":"execution_boundary_held","boundary":name}));
            while self.test_hold.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        let _ = name;
    }
    #[cfg(feature = "store-test-hooks")]
    pub fn test_owners(&self) -> Value {
        json!({"workers":self.workers.lock().expect("workers").len(),"available_slots":self.worker_slots.available_permits()})
    }
    #[cfg(feature = "store-test-hooks")]
    pub fn test_record(&self, id: &str) -> Value {
        let workers = self.workers.lock().expect("workers");
        let record = workers.get(id).and_then(|w| w.record.as_ref());
        record.map_or(Value::Null, |record| {
            let state = record.state.lock().expect("record");
            json!({"identity":format!("{:p}", Arc::as_ptr(record)),"deadline":state.deadline.map(|d|format!("{d:?}")),"result":state.result.as_ref().map(|r| match r { Ok(affected) => json!({"affected":affected}), Err(error) => json!({"error":error}) })})
        })
    }
    #[cfg(feature = "store-test-hooks")]
    pub fn test_new_work(&self) -> Result<()> {
        self.database.ensure_new_work()
    }
    #[cfg(feature = "store-test-hooks")]
    pub fn test_release(&self) {
        self.test_hold.store(false, Ordering::Release);
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
        mut operation: impl for<'a> FnMut(&'a mut Transaction) -> BoxFuture<'a, Result<T>> + Send,
    ) -> Result<T> {
        self.transaction_stoppable(component, &mut operation, None, None)
            .await
    }
    async fn transaction_record<T: Send>(
        &self,
        component: &Component,
        record: &Record,
        mut operation: impl for<'a> FnMut(&'a mut Transaction) -> BoxFuture<'a, Result<T>> + Send,
    ) -> Result<T> {
        self.transaction_stoppable(component, &mut operation, None, Some(record))
            .await
    }
    async fn transaction_stoppable<T: Send>(
        &self,
        component: &Component,
        operation: &mut (impl for<'a> FnMut(&'a mut Transaction) -> BoxFuture<'a, Result<T>> + Send),
        stop: Option<&AtomicBool>,
        record: Option<&Record>,
    ) -> Result<T> {
        let check_stop = || -> Result<()> {
            let expired = record.is_some_and(|r| {
                r.state
                    .lock()
                    .expect("record")
                    .deadline
                    .is_some_and(|d| Instant::now() >= d)
            });
            if expired && !crate::store::test_defect("deadline") {
                self.exit_unlanded();
            }
            if stop.is_some_and(|s| s.load(Ordering::Acquire))
                && !crate::store::test_defect("licensing_stop")
            {
                return Err(ExecutionStopped.into());
            }
            Ok(())
        };
        let mut spent = Duration::ZERO;
        loop {
            check_stop()?;
            let started = Instant::now();
            let attempt = async {
                let mut tx = component.begin().await?;
                check_stop()?;
                let value = operation(&mut tx).await?;
                anyhow::Ok((value, tx))
            }
            .await;
            let mut serialization = false;
            match attempt {
                Ok((value, tx)) => {
                    check_stop()?;
                    let work_time = started.elapsed();
                    let (outcome, verification) = component.commit(tx)?.wait_accounted().await?;
                    if !matches!(&outcome,CommitOutcome::Rejected{reason} if reason.contains("40001"))
                    {
                        spent += work_time + verification;
                    }
                    check_stop()?;
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
            check_stop()?;
            if spent >= self.retry_bound {
                return self.fatal("store retry exhausted; outcome retained until process exit");
            }
            component.trace(json!({"event":"whole_transaction_retry","spent_ms":spent.as_millis(),"serialization":serialization,"external_call_repeated":false}));
            if stop.is_some() {
                self.test_boundary("licensing_retry").await;
            }
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
        ensure!(
            self.database.lock_loss_state().is_none(),
            "engine stopping: no claims or periodic passes"
        );
        self.observe_lock().await?;
        // STO-55: claim callers reserve capacity before any await on a checkout.
        // Retain it through execution and recording, including sent COMMIT waits.
        let slot = self
            .worker_slots
            .clone()
            .try_acquire_owned()
            .context("all configured workers occupied")?;
        self.database.ensure_new_work()?;
        let component = self.component();
        if crate::store::test_defect("claim_cancel") {
            return self.claim_owned(component, slot).await;
        }
        let engine = self.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.process.submit(async move {
            let result = engine.claim_owned(component, slot).await;
            // A missing caller leaves the registered worker and its capacity owned.
            if sender.send(result).is_err() && engine.is_fatal() {
                std::process::exit(71);
            }
        })?;
        receiver.await.context("claim owner disappeared")?
    }
    async fn claim_owned(
        self: &Arc<Self>,
        component: Component,
        slot: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Option<Claim>> {
        // Ordinary pre-COMMIT failures consume OPS-49's whole-transaction budget.
        // Once a claim might have committed, a lost reply is fatal, never retried.
        let mut spent = Duration::ZERO;
        let claim = loop {
            #[cfg(feature = "store-test-hooks")]
            component.trace(json!({"event":"claim_attempt_started"}));
            self.database.ensure_new_work()?;
            let started = Instant::now();
            let attempt = async {
                let mut tx = component.begin_new_work().await?;
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
                    if self.database.lock_loss_state().is_some()
                        || error.downcast_ref::<FatalEngine>().is_some()
                    {
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
        self.test_boundary("claim_registration").await;
        if let Some(c) = &claim {
            self.workers.lock().expect("workers").insert(
                c.id.clone(),
                Worker {
                    slot: Arc::new(slot),
                    claim: c.clone(),
                    component: component.clone(),
                    outcome: Arc::new(Mutex::new(None)),
                    record: None,
                    execution: None,
                },
            );
        }
        if let Some(c) = &claim {
            component.trace(json!({"event":"claim_registered","operation":c.id,"number":c.number}));
            // OPS-47/21: a transaction dispatched before loss may finish after
            // fan-out took its snapshot. Give its worker the original loss bound.
            let loss = *self.database.lock_loss_state();
            if let Some(stopped_at) = loss {
                self.register_stop(&c.id, true, stopped_at)?;
            }
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
        let execution = Arc::new(Execution::default());
        let (claim, component, returned_outcome) = {
            let mut workers = self.workers.lock().expect("workers");
            let w = workers.get_mut(id).context("worker not found")?;
            ensure!(
                w.execution.is_none() && w.record.is_none(),
                "worker already executing or recording"
            );
            ensure!(
                w.claim.kind == "create",
                "provider execution for this operation kind is not implemented; only create simulator execution is supported"
            );
            w.execution = Some(execution.clone());
            (w.claim.clone(), w.component.clone(), w.outcome.clone())
        };
        let engine = self.clone();
        let id = id.to_owned();
        self.process.submit(async move {
            let result = async {
                let c = claim.clone();
                engine
                    .transaction_stoppable(
                        &component,
                        &mut move |tx| {
                            let c = c.clone();
                            Box::pin(async move {
                                ensure!(
                                    tx.license_create(&c).await?,
                                    "create dispatch guard refused"
                                );
                                Ok(())
                            })
                        },
                        Some(&execution.stopped),
                        None,
                    )
                    .await?;
                engine.test_boundary("licensed_entry").await;
                // OPS-21: stop registration and actual step-9 entry arbitrate on
                // the same lock. A timestamp is not a live worker's position.
                let call = {
                    let workers = engine.workers.lock().expect("workers");
                    let worker = workers.get(&id).context("worker not found")?;
                    if execution.stopped.load(Ordering::Acquire)
                        && !crate::store::test_defect("licensing_stop")
                    {
                        return Err(ExecutionStopped.into());
                    }
                    ensure!(
                        worker.claim.number == claim.number
                            && worker.execution.as_ref().is_some_and(|e| Arc::ptr_eq(e, &execution)),
                        "execution replaced before provider entry"
                    );
                    let call = component.external(crate::store::ExternalKind::Provider)?;
                    execution.dispatched.store(true, Ordering::Release);
                    component.trace(
                        json!({"event":"simulator_accepted","operation":id,"claim_number":claim.number,"simulator":true}),
                    );
                    call
                };
                let returned = if let Some(outcome) = outcome {
                    // The simulated verified result is already in hand. Stops
                    // after this boundary cannot replace it with a cut-off.
                    engine.test_boundary("outcome_publication").await;
                    Some(outcome)
                } else {
                    while !execution.stopped.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    None
                };
                drop(call);
                anyhow::Ok(returned)
            }
            .await;
            match result {
                Ok(Some(outcome)) => {
                    if crate::store::test_defect("replacement_outcome") {
                        // Restore the operation-only publication defect for the control.
                        if let Some(w) = engine.workers.lock().expect("workers").get(&id) {
                            *w.outcome.lock().expect("outcome") = Some(outcome);
                        }
                    } else {
                        *returned_outcome.lock().expect("outcome") = Some(outcome);
                    }
                    component.trace(json!({"event":"execute_returned","operation":id,"claim_number":claim.number}));
                }
                Err(error) if error.downcast_ref::<ExecutionStopped>().is_none() => {
                    engine.database.fatal(&format!(
                        "execution failed without an outcome for {id}: {error:#}"
                    ));
                }
                _ => {}
            }
            execution.done.store(true, Ordering::Release);
        })?;
        Ok(())
    }
    pub fn outcome(&self, claim: &Claim, outcome: Outcome) -> Result<()> {
        let workers = self.workers.lock().expect("workers");
        let worker = workers.get(&claim.id).context("worker not found")?;
        ensure!(worker.claim.number == claim.number, "worker claim replaced");
        ensure!(worker.record.is_none(), "execute already stopped");
        ensure!(worker.execution.is_none(), "worker already executing");
        *worker.outcome.lock().expect("outcome") = Some(outcome);
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
            self.outcome(c, outcome.clone())?;
            let record = self.begin_settlement(&c.id, Some(c.number))?;
            loop {
                let result = record.state.lock().expect("record").result.clone();
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
            .filter(|w| w.claim.number == c.number)
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
    /// OPS-8/49, STO-7/55: the process owns the transaction and retirement;
    /// cancelling its result waiter is not an OPS-21 user stop.
    pub async fn defer(self: &Arc<Self>, c: &Claim) -> Result<bool> {
        let (component, slot) = self
            .workers
            .lock()
            .expect("workers")
            .get(&c.id)
            .filter(|w| w.claim.number == c.number)
            .map(|w| (w.component.clone(), Some(w.slot.clone())))
            .unwrap_or_else(|| (self.component(), None));
        if crate::store::test_defect("defer_cancel") {
            return self.defer_owned(c.clone(), component, None).await;
        }
        let engine = self.clone();
        let c = c.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.process.submit(async move {
            let result = engine.defer_owned(c, component, slot).await;
            let _ = sender.send(result);
            // OPS-49/50: delivery is not observation; even a successful send
            // can race cancellation before the waiter polls its fatal result.
            if engine.is_fatal() {
                std::process::exit(71);
            }
        })?;
        receiver.await.context("defer owner disappeared")?
    }
    async fn defer_owned(
        &self,
        c: Claim,
        component: Component,
        slot: Option<Arc<tokio::sync::OwnedSemaphorePermit>>,
    ) -> Result<bool> {
        let id = c.id.clone();
        let number = c.number;
        let affected = self
            .transaction(&component, move |tx| {
                let c = c.clone();
                Box::pin(async move { tx.defer(&c).await })
            })
            .await?;
        self.retire(&id, number);
        // OPS-6/STO-55: replacement of the registry entry cannot release this
        // execution's permit before acknowledgment, or retire the newer claim.
        drop(slot);
        component.trace(json!({"event":"defer_finished","operation":id,"claim_number":number,"affected":affected}));
        Ok(affected)
    }
    /// Retain the after-return outcome and its entire settlement loop even if a
    /// stop drops the caller. A first stop adds its bound to this same task.
    fn begin_settlement(self: &Arc<Self>, id: &str, number: Option<i64>) -> Result<Arc<Record>> {
        let (claim, component, outcome, record) = {
            let mut workers = self.workers.lock().expect("workers");
            let worker = workers.get_mut(id).context("worker not found")?;
            ensure!(
                number.is_none_or(|n| worker.claim.number == n),
                "worker claim replaced"
            );
            ensure!(worker.record.is_none(), "settlement already registered");
            let outcome = worker
                .outcome
                .lock()
                .expect("outcome")
                .clone()
                .context("execute has not returned")?;
            let record = Arc::new(Record {
                state: Mutex::new(RecordState {
                    deadline: None,
                    result: None,
                    slot: Some(worker.slot.clone()),
                }),
            });
            worker.record = Some(record.clone());
            self.records.lock().expect("records").insert(
                (worker.claim.id.clone(), worker.claim.number),
                record.clone(),
            );
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
                .transaction_record(&component, &pending, move |tx| {
                    let claim = claim.clone();
                    let outcome = outcome.clone();
                    Box::pin(async move { apply_outcome(tx, &claim, &outcome).await })
                })
                .await
                .map_err(|e| format!("{e:#}"));
            engine.finish_record(&pending, result.clone(), &id, number);
            component.trace(
                json!({"event":"settlement_finished","operation":id,"landed":matches!(result,Ok(true)),"affected":result.as_ref().ok()}),
            );
            if let Err(error)=&result {engine.database.fatal(&format!("atomic settlement failed for {id}: {error}"));}
        })?;
        Ok(record)
    }
    pub fn start_settlement(self: &Arc<Self>, id: &str) -> Result<()> {
        self.begin_settlement(id, None)?;
        Ok(())
    }
    /// OPS-21: the absolute deadline is owned by the supervisor, independent of
    /// the presence or polling of a caller's wait_records future.
    fn watch_record(self: &Arc<Self>, record: Arc<Record>) -> Result<()> {
        if crate::store::test_defect("deadline") {
            return Ok(());
        }
        let engine = self.clone();
        self.process.submit(async move {
            loop {
                {
                    let state = record.state.lock().expect("record");
                    if matches!(state.result, Some(Ok(_))) {
                        return;
                    }
                    if state.deadline.is_some_and(|d| Instant::now() >= d) {
                        drop(state);
                        engine.exit_unlanded();
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    }
    fn pending_records(&self) -> Vec<((String, i64), Arc<Record>)> {
        if crate::store::test_defect("displaced_record") {
            return self
                .workers
                .lock()
                .expect("workers")
                .iter()
                .filter_map(|(id, w)| {
                    w.record
                        .as_ref()
                        .map(|r| ((id.clone(), w.claim.number), r.clone()))
                })
                .collect();
        }
        self.records
            .lock()
            .expect("records")
            .iter()
            .map(|(key, r)| (key.clone(), r.clone()))
            .collect()
    }
    fn exit_unlanded(&self) -> ! {
        let ids: Vec<_> = self
            .pending_records()
            .into_iter()
            .filter_map(|((id, number), r)| {
                r.state
                    .lock()
                    .expect("record")
                    .result
                    .as_ref()
                    .is_none_or(|result| result.is_err())
                    .then(|| format!("{id} (claim={number})"))
            })
            .collect();
        self.database.fatal(&format!(
            "stop bound expired; records not observed landed: {}",
            ids.join(",")
        ));
        // STO-7: process exit, never dropping a sent COMMIT in a surviving process.
        std::process::exit(71)
    }
    fn finish_record(
        &self,
        record: &Record,
        result: std::result::Result<bool, String>,
        id: &str,
        number: i64,
    ) {
        let mut workers = self.workers.lock().expect("workers");
        let mut records = self.records.lock().expect("records");
        let mut state = record.state.lock().expect("record");
        if state.deadline.is_some_and(|d| Instant::now() >= d)
            && !crate::store::test_defect("deadline")
        {
            drop(state);
            drop(records);
            drop(workers);
            self.exit_unlanded();
        }
        // OPS-22: a terminal guard refusal ends only this execution. Publish the
        // result for existing record owners, and retire under the same locks so
        // waiters cannot see completion while its capacity is still retained.
        if matches!(result, Ok(false)) && !crate::store::test_defect("terminal_zero") {
            self.component().trace(json!({"event":"worker_zero_row","operation":id,"claim_number":number,"observed":"guard did not match; worker ends"}));
        }
        if result.is_ok() && !crate::store::test_defect("terminal_capacity") {
            if workers.get(id).is_some_and(|w| w.claim.number == number) {
                workers.remove(id);
            }
            // Retain the original permit while pending even if a newer claim
            // replaces this worker in the registry. Never release its permit.
            state.slot.take();
        }
        if result.is_ok() {
            records.remove(&(id.to_owned(), number));
        }
        state.result = Some(result);
    }
    pub fn record_stop(self: &Arc<Self>, id: &str) -> Result<()> {
        self.register_stop(id, false, Instant::now())
    }
    fn register_stop(
        self: &Arc<Self>,
        id: &str,
        lock_loss: bool,
        stopped_at: Instant,
    ) -> Result<()> {
        let (claim, component, record, execution, returned_outcome) = {
            let mut workers = self.workers.lock().expect("workers");
            let loss = self.database.lock_loss_state();
            let Some(worker) = workers.get_mut(id) else {
                if lock_loss {
                    return Ok(());
                }
                anyhow::bail!("worker not found");
            };
            if let Some(record) = &worker.record {
                let mut state = record.state.lock().expect("record");
                if state.result.is_some() {
                    return Ok(());
                }
                if !lock_loss && loss.is_some() && !crate::store::test_defect("loss_stop") {
                    return self.fatal("later stop: exit without repeating pending record");
                }
                if state.deadline.is_some() {
                    if lock_loss {
                        return Ok(());
                    }
                    return self.fatal("later stop: exit without repeating pending record");
                }
                state.deadline = Some(stopped_at + self.stop_bound);
                worker.component.trace(json!({"event":"stop_record_started","operation":worker.claim.id,"bound_ms":self.stop_bound.as_millis(),"existing_settlement":true}));
                return self.watch_record(record.clone());
            }
            if !lock_loss && loss.is_some() && !crate::store::test_defect("loss_stop") {
                return self.fatal("later stop: exit without repeating pending record");
            }
            let record = Arc::new(Record {
                state: Mutex::new(RecordState {
                    deadline: Some(stopped_at + self.stop_bound),
                    result: None,
                    slot: Some(worker.slot.clone()),
                }),
            });
            worker.record = Some(record.clone());
            self.records.lock().expect("records").insert(
                (worker.claim.id.clone(), worker.claim.number),
                record.clone(),
            );
            if let Some(execution) = &worker.execution {
                execution.stopped.store(true, Ordering::Release);
            }
            (
                worker.claim.clone(),
                worker.component.clone(),
                record,
                worker.execution.clone(),
                worker.outcome.clone(),
            )
        };
        self.watch_record(record.clone())?;
        component.trace(json!({"event":"stop_record_started","operation":claim.id,"bound_ms":self.stop_bound.as_millis()}));
        let operation_id = claim.id.clone();
        let number = claim.number;
        let stale_outcome = if crate::store::test_defect("outcome_stop") {
            returned_outcome.lock().expect("outcome").clone()
        } else {
            None
        };
        let engine = self.clone();
        self.process.submit(async move {
            if let Some(execution) = &execution {
                while !execution.done.load(Ordering::Acquire) {tokio::time::sleep(Duration::from_millis(1)).await;}
            }
            // OPS-21: acknowledge execution termination before reading the final
            // outcome. An earlier None snapshot proves nothing about its return.
            let outcome = if crate::store::test_defect("outcome_stop") {stale_outcome} else {returned_outcome.lock().expect("outcome").clone()};
            #[cfg(feature = "store-test-hooks")]
            component.trace(json!({"event":"stop_record_outcome","operation":operation_id,"claim_number":number,"external_id":match &outcome {Some(Outcome::VerifiedCreate{external_id,..})=>Some(external_id),_=>None}}));
            let dispatched = execution.as_ref().is_some_and(|e| e.dispatched.load(Ordering::Acquire));
            let result=engine.transaction_record(&component,&record,move |tx| {
                let claim=claim.clone();let outcome=outcome.clone();
                Box::pin(async move {
                    if let Some(outcome)=outcome {return apply_outcome(tx,&claim,&outcome).await;}
                    crate::ledger::serialize(tx,&[&claim.tenant]).await?;
                    let rows=tx.rows("SELECT kind,write_started_at IS NOT NULL FROM provisiond.operations WHERE id=$1::text::uuid",&[&claim.id]).await?;
                    let row=rows.first().context("stopped operation missing")?;
                    let kind:String=row.get(0);
                    let ambiguous=match kind.as_str() {"create"=> {
                        if crate::store::test_defect("predispatch") {
                            tx.rows("SELECT about_to_order_at IS NOT NULL FROM provisiond.operations WHERE id=$1::text::uuid", &[&claim.id]).await?[0].get::<_,bool>(0)
                        } else {dispatched}
                    },"refresh"=>false,"power"=>row.get::<_,bool>(1),_=>anyhow::bail!("unsupported stop kind")};
                    tx.settle(&claim,if ambiguous {"needs_reconciliation"}else{"failed"},&Value::Null,&json!({"kind":"conflict","details":{"reason":"cut_off"},"retryable":!ambiguous})).await
                })
            }).await.map_err(|e|format!("{e:#}"));
            engine.finish_record(&record, result.clone(), &operation_id, number);
            component.trace(json!({"event":"stop_record_finished","operation":operation_id,"landed":matches!(result,Ok(true)) || (result.is_ok() && crate::store::test_defect("terminal_zero")),"affected":result.as_ref().ok(),"detail":result.as_ref().err()}));
        })?;
        Ok(())
    }
    pub async fn wait_records(&self) -> Result<()> {
        let mut pending: Vec<(String, Arc<Record>)> = Vec::new();
        loop {
            // An observer that captured A keeps its result even if worker B
            // later occupies the same operation ID in the registry.
            for ((id, number), record) in self.pending_records() {
                if !pending.iter().any(|(_, r)| Arc::ptr_eq(r, &record)) {
                    pending.push((format!("{id} (claim={number})"), record));
                }
            }
            let mut unfinished = false;
            for (id, record) in &pending {
                let state = record.state.lock().expect("record");
                match &state.result {
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        self.database
                            .fatal(&format!("stop record failed for {id}: {error}"));
                        return Err(
                            FatalEngine("stop record failed; exit without classification").into(),
                        );
                    }
                    None => {
                        unfinished = true;
                    }
                }
            }
            if !unfinished {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    /// Lock loss is the first stop only for workers without a pending record.
    pub async fn observe_lock(self: &Arc<Self>) -> Result<()> {
        let _observation = if crate::store::test_defect("lock_observers") {
            None
        } else {
            Some(self.lock_observation.lock().await)
        };
        if self.is_fatal() {
            return Err(FatalEngine("engine fatal; process must exit").into());
        }
        if !self.database.detect_lock_loss() {
            return Ok(());
        }
        let stopped_at = self.database.lock_loss_state().expect("detected lock loss");
        let ids: Vec<_> = self
            .workers
            .lock()
            .expect("workers")
            .iter()
            .filter(|(_, w)| {
                w.record
                    .as_ref()
                    .is_none_or(|r| r.state.lock().expect("record").deadline.is_none())
            })
            .map(|(id, _)| id.clone())
            .collect();
        self.test_boundary("lock_registration").await;
        for id in ids {
            self.register_stop(
                &id,
                !crate::store::test_defect("lock_observers"),
                stopped_at,
            )?;
        }
        #[cfg(feature = "store-test-hooks")]
        {
            if crate::store::test_defect("lock_record_reset") {
                for worker in self.workers.lock().expect("workers").values() {
                    if let Some(record) = &worker.record {
                        record.state.lock().expect("record").deadline =
                            Some(Instant::now() + self.stop_bound);
                    }
                }
            }
            self.component()
                .trace(json!({"event":"lock_registration_complete"}));
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
