//! OPS-49/STO-7: conservative, completion-only attribution of actual SyncRep.
use super::{Manager, Session, Trace};
use anyhow::{Context, Result, ensure};
use deadpool::managed::Manager as _;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

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
pub(super) struct Accounting {
    epoch: Instant,
    attempts: Mutex<BTreeMap<u64, Attempt>>,
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
    pub(super) fn start(manager: Manager, session: Session) -> Result<(Arc<Self>, Observer)> {
        let accounting = Arc::new(Self {
            epoch: Instant::now(),
            attempts: Mutex::new(BTreeMap::new()),
            trace: manager.trace.clone(),
        });
        let (stop, stopped) = oneshot::channel();
        let owner = accounting.clone();
        manager.process.clone().submit(async move {
            tokio::select! { _ = stopped => {}, _ = owner.observe(manager, session) => {} }
        })?;
        Ok((accounting, Observer(Some(stop))))
    }
    fn instant(&self, instant: Instant) -> u128 {
        instant.duration_since(self.epoch).as_micros()
    }
    pub(super) fn register(&self, owner: u64, session: &Session) {
        self.attempts.lock().expect("accounting").insert(
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
    }
    pub(super) fn finish(&self, owner: u64) -> Duration {
        let mut attempt = self
            .attempts
            .lock()
            .expect("accounting")
            .remove(&owner)
            .expect("registered COMMIT");
        attempt.end_run();
        (self.trace)(
            json!({"event":"commit_accounting_completed","owner":owner,"session":attempt.pid,"backend_start":attempt.backend_start,"completion_us":self.instant(Instant::now()),"exempt_us":attempt.exempt.as_micros()}),
        );
        attempt.exempt
    }
    fn failed(&self, reason: &str) {
        for (_owner, attempt) in self.attempts.lock().expect("accounting").iter_mut() {
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
        let mut attempts = self.attempts.lock().expect("accounting");
        let mut missing = false;
        for (owner, attempt) in attempts.iter_mut() {
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
    async fn observe(&self, manager: Manager, initial: Session) {
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
                match manager.create().await {
                    Ok(connected) => {
                        (self.trace)(
                            json!({"event":"commit_observer_reestablished","session":connected.pid,"backend_start":connected.backend_start,"checked":true}),
                        );
                        session = Some(connected);
                    }
                    Err(error) => {
                        self.failed(&format!("checked observer reconnect: {error:#}"));
                        continue;
                    }
                }
            }
            let pids: Vec<_> = self
                .attempts
                .lock()
                .expect("accounting")
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
