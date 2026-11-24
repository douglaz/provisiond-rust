//! OPS-49/STO-7: conservative, completion-only attribution of actual SyncRep.
use super::{Manager, Session, Trace};
use anyhow::{Context, Result, ensure};
use deadpool::managed::Manager as _;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, oneshot};

// Local measurement precision, not a production retry or availability setting.
const PERIOD: Duration = Duration::from_millis(20);
const SAMPLE: &str = "SELECT pid, backend_start::text, query_start::text, state, wait_event_type, wait_event, query FROM pg_stat_activity WHERE pid = ANY($1) /* OPS-49 observer */";

#[derive(Default)]
struct Attempt {
    pid: i32,
    dispatched: Option<Instant>,
    backend_start: String,
    first_received: Option<Instant>,
    last_sent: Option<Instant>,
    exempt: Duration,
}
impl Attempt {
    fn end_run(&mut self) {
        if let (Some(first), Some(last)) = (self.first_received.take(), self.last_sent.take()) {
            self.exempt += last.saturating_duration_since(first);
        }
    }
}
#[derive(Default)]
struct State {
    attempts: BTreeMap<u64, Attempt>,
    exhausted: Option<String>,
}
pub(super) struct Accounting {
    epoch: Instant,
    state: Mutex<State>,
    fatal: Arc<AtomicBool>,
    drained: Notify,
    trace: Trace,
}
// The database owns the lifetime; the process reactor owns its I/O. Dropping the
// database closes this one observer, including any unfinished checked reconnect.
pub(super) struct Observer(Option<oneshot::Sender<()>>);
impl Drop for Observer {
    fn drop(&mut self) {
        if let Some(stop) = self.0.take() {
            let _ = stop.send(());
        }
    }
}
impl Accounting {
    pub(super) fn start(
        manager: Manager,
        session: Session,
        retry_bound: Duration,
        fatal: Arc<AtomicBool>,
    ) -> Result<(Arc<Self>, Observer)> {
        let accounting = Arc::new(Self {
            epoch: Instant::now(),
            state: Mutex::new(State::default()),
            fatal,
            drained: Notify::new(),
            trace: manager.trace.clone(),
        });
        let (stop, stopped) = oneshot::channel();
        let owner = accounting.clone();
        manager.process.clone().submit(async move {
            tokio::select! { _ = stopped => {}, _ = owner.observe(manager, session, retry_bound) => {} }
        })?;
        Ok((accounting, Observer(Some(stop))))
    }
    fn instant(&self, instant: Instant) -> u128 {
        instant.duration_since(self.epoch).as_micros()
    }
    pub(super) async fn register(&self, owner: u64, session: &Session) -> bool {
        let drained = self.drained.notified();
        {
            let mut state = self.state.lock().expect("accounting");
            if state.exhausted.is_none() {
                state.attempts.insert(
                    owner,
                    Attempt {
                        dispatched: Some(Instant::now()),
                        pid: session.pid,
                        backend_start: session.backend_start.clone(),
                        ..Attempt::default()
                    },
                );
                (self.trace)(
                    json!({"event":"commit_accounting_registered","owner":owner,"session":session.pid,"backend_start":session.backend_start,"dispatch_us":self.instant(Instant::now())}),
                );
                return true;
            }
            if state.attempts.is_empty() {
                return false;
            }
        }
        // OPS-49/STO-7, D-38: do not dispatch without an observer after
        // exhaustion, or return an error that can exit before sent receipts resolve.
        drained.await;
        false
    }
    pub(super) fn finish(&self, owner: u64, deliver: impl FnOnce(Duration)) {
        let mut state = self.state.lock().expect("accounting");
        let mut attempt = state.attempts.remove(&owner).expect("registered COMMIT");
        attempt.end_run();
        (self.trace)(
            json!({"event":"commit_accounting_completed","owner":owner,"session":attempt.pid,"backend_start":attempt.backend_start,"completion_us":self.instant(Instant::now()),"exempt_us":attempt.exempt.as_micros()}),
        );
        // Resolve the receipt before publishing fatal, under the same lock
        // that excludes every later registration from the final transition.
        deliver(attempt.exempt);
        self.exit_if_drained(&state);
    }
    fn exit_if_drained(&self, state: &State) {
        let Some(reason) = state
            .exhausted
            .as_ref()
            .filter(|_| state.attempts.is_empty())
        else {
            return;
        };
        self.fatal.store(true, Ordering::Release);
        (self.trace)(json!({"event":"engine_fatal","reason":reason}));
        self.drained.notify_waiters();
    }
    fn failed(&self, reason: &str) {
        #[allow(
            clippy::for_kv_map,
            reason = "owner key is used by optional accounting diagnostics"
        )]
        for (_owner, attempt) in self.state.lock().expect("accounting").attempts.iter_mut() {
            #[cfg(feature = "store-test-hooks")]
            let before = json!({"first_received_us":attempt.first_received.map(|t|self.instant(t)),"last_sent_us":attempt.last_sent.map(|t|self.instant(t)),"exempt_us":attempt.exempt.as_micros()});
            // OPS-49: the isolated sensitivity defect skips only this closure.
            if !super::test_defect("w5_observer_run") {
                attempt.end_run();
            }
            #[cfg(feature = "store-test-hooks")]
            (self.trace)(
                json!({"event":"test_observer_run_closed","owner":_owner,"session":attempt.pid,"backend_start":attempt.backend_start,"failed_us":self.instant(Instant::now()),"reason":reason,"before":before,"after":{"first_received_us":attempt.first_received.map(|t|self.instant(t)),"last_sent_us":attempt.last_sent.map(|t|self.instant(t)),"exempt_us":attempt.exempt.as_micros()}}),
            );
        }
        (self.trace)(
            json!({"event":"commit_observer_failed","level":"error","reason":reason,"unobserved_time_charged":true}),
        );
    }
    async fn sample(&self, session: &Session, pids: Vec<i32>) -> Result<()> {
        let sent = Instant::now();
        let mut in_flight = super::CloseUnlessFinished(Some(session));
        let rows = tokio::time::timeout(
            session.settings.call_bound,
            session.client.query(SAMPLE, &[&pids]),
        )
        .await
        .context("observer sample deadline")??;
        in_flight.0 = None;
        let received = Instant::now();
        let mut state = self.state.lock().expect("accounting");
        let mut missing = false;
        for (owner, attempt) in state.attempts.iter_mut() {
            // A registration made after this statement's submission has no sample.
            if !pids.contains(&attempt.pid) || attempt.dispatched.is_some_and(|d| d > sent) {
                continue;
            }
            let row = rows.iter().find(|r| r.get::<_, i32>(0) == attempt.pid);
            let identity = row.is_some_and(|r| {
                r.get::<_, Option<String>>(1).as_deref() == Some(&attempt.backend_start)
            });
            let visible = row.is_some_and(|r| {
                r.get::<_, Option<String>>(3).is_some() && r.get::<_, Option<String>>(6).is_some()
            });
            let matching = identity
                && row.is_some_and(|r| {
                    r.get::<_, Option<String>>(3).as_deref() == Some("active")
                        && r.get::<_, Option<String>>(4).as_deref() == Some("IPC")
                        && r.get::<_, Option<String>>(5).as_deref() == Some("SyncRep")
                        && r.get::<_, Option<String>>(6).as_deref()
                            == Some("COMMIT /* store COMMIT */")
                });
            if matching {
                attempt.first_received.get_or_insert(received);
                attempt.last_sent = Some(sent);
            } else {
                attempt.end_run();
            }
            missing |= !identity || !visible;
            (self.trace)(
                json!({"event":"commit_observer_sample","owner":owner,"session":attempt.pid,"backend_start":attempt.backend_start,"observer_session":session.pid,"sent_us":self.instant(sent),"received_us":self.instant(received),"identity_matches":identity,"matching":matching,"observed_backend_start":row.and_then(|r|r.get::<_,Option<String>>(1)),"query_start":row.and_then(|r|r.get::<_,Option<String>>(2)),"state":row.and_then(|r|r.get::<_,Option<String>>(3)),"wait_event_type":row.and_then(|r|r.get::<_,Option<String>>(4)),"wait_event":row.and_then(|r|r.get::<_,Option<String>>(5)),"store_commit":row.is_some_and(|r|r.get::<_,Option<String>>(6).as_deref()==Some("COMMIT /* store COMMIT */"))}),
            );
        }
        ensure!(
            !missing,
            "observer missing backend identity or activity visibility"
        );
        Ok(())
    }
    async fn observe(&self, manager: Manager, initial: Session, retry_bound: Duration) {
        let mut session = Some(initial);
        let mut ticks = tokio::time::interval(PERIOD);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticks.tick().await;
            if session
                .as_ref()
                .is_some_and(|s| s.client.is_closed() || s.protocol.snapshot().dead)
            {
                self.failed("observer connection closed");
                session.take();
            }
            if session.is_none() {
                // STO-55: reconnect attempts and backoff share the Store-retry bound.
                let started = Instant::now();
                let mut backoff = PERIOD;
                let mut alarmed = false;
                let mut last_error = None;
                loop {
                    let remaining = retry_bound.saturating_sub(started.elapsed());
                    let result = tokio::time::timeout(remaining, manager.create()).await;
                    match result {
                        Ok(Ok(connected)) => {
                            (self.trace)(
                                json!({"event":"commit_observer_reestablished","session":connected.pid,"backend_start":connected.backend_start,"checked":true}),
                            );
                            session = Some(connected);
                            break;
                        }
                        Ok(Err(error)) => last_error = Some(error),
                        Err(error) => {
                            if last_error.is_none() {
                                last_error = Some(error.into());
                            }
                        }
                    }
                    let reason = format!(
                        "Commit observer reconnect failed; database unavailability or connection exhaustion: {:#}",
                        last_error.as_ref().expect("failed reconnect")
                    );
                    self.failed(&reason);
                    tokio::time::sleep(backoff.min(retry_bound.saturating_sub(started.elapsed())))
                        .await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                    if !alarmed && started.elapsed() >= Duration::from_millis(2000) {
                        crate::entry::alarm("engine", &reason, 2000);
                        alarmed = true;
                    }
                    if started.elapsed() >= retry_bound {
                        crate::entry::alarm("engine", &reason, 0);
                        // STO-55 bounds reconnects; D-38 defers this accounting
                        // exit until every already registered sent receipt resolves.
                        let mut state = self.state.lock().expect("accounting");
                        state.exhausted = Some(reason);
                        self.exit_if_drained(&state);
                        return;
                    }
                }
            }
            let pids: Vec<_> = self
                .state
                .lock()
                .expect("accounting")
                .attempts
                .values()
                .map(|a| a.pid)
                .collect();
            if pids.is_empty() {
                continue;
            }
            if let Err(error) = self.sample(session.as_ref().expect("observer"), pids).await {
                self.failed(&format!("observer sample: {error:#}"));
                session.take();
            }
        }
    }
}
