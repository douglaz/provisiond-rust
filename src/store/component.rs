//! OVR-9, STO-55, OPS-15/47/50: transaction entry and lifetime startup session.
use super::{CommitOutcome, Session, Store, Transaction, migration::STARTUP_LOCK};
use anyhow::{Result, ensure};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub struct FatalEngine(pub &'static str);
impl std::fmt::Display for FatalEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for FatalEngine {}

/// OVR-19: lexical whole milliseconds, exactly representable by both PG settings.
pub fn startup_milliseconds(value: &str) -> Result<u32> {
    ensure!(
        !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()),
        "startup bound must be positive whole milliseconds"
    );
    let ms: u32 = value.parse()?;
    ensure!(
        ms > 0 && ms <= i32::MAX as u32,
        "startup bound outside PostgreSQL milliseconds range"
    );
    Ok(ms)
}

#[derive(Clone)]
struct Fence {
    generation: String,
    fatal: Arc<AtomicBool>,
    entry: Arc<tokio::sync::Mutex<()>>,
}
/// Fixed component identity. Its permit travels with a sent COMMIT, including
/// after receipt cancellation, so lexical Drop cannot license an external call.
#[derive(Clone)]
pub struct Component {
    store: Arc<Store>,
    gate: Arc<Semaphore>,
    fence: Option<Fence>,
}
impl Store {
    pub fn component(self: &Arc<Self>) -> Component {
        Component {
            store: self.clone(),
            gate: Arc::new(Semaphore::new(1)),
            fence: None,
        }
    }
}
impl Component {
    pub async fn begin(&self) -> Result<Transaction> {
        let permit =
            self.gate.clone().try_acquire_owned().map_err(|_| {
                anyhow::anyhow!("component already owns transaction or external call")
            })?;
        let _entry = if let Some(fence) = &self.fence {
            Some(fence.entry.lock().await)
        } else {
            None
        };
        self.ensure_live()?;
        let mut tx = self.store.begin().await?;
        tx.permit = Some(permit);
        // Check after acquisition too: another component may detect supersession
        // while this one waits for a pool slot. No transaction business runs then.
        self.ensure_live()?;
        if let Some(fence) = &self.fence {
            let rows=tx.rows("SELECT generation::text FROM provisiond.startup_generation WHERE singleton FOR SHARE /* OPS-50 first statement */",&[]).await?;
            let matches = rows.len() == 1 && rows[0].get::<_, String>(0) == fence.generation;
            if !matches && !super::test_defect("generation") {
                fence.fatal.store(true, Ordering::Release);
                (self.store.trace)(
                    json!({"event":"engine_superseded","generation":fence.generation,"fatal":true}),
                );
                if let Err(error) = tx.rollback().await {
                    (self.store.trace)(
                        json!({"event":"superseded_rollback_failed","error":format!("{error:#}")}),
                    );
                }
                return Err(
                    FatalEngine("engine superseded; no further transaction permitted").into(),
                );
            }
            (self.store.trace)(
                json!({"event":"generation_checked","session":tx.session_id(),"generation":fence.generation,"equal":matches}),
            );
            tx.engine_fenced = true;
        }
        Ok(tx)
    }
    fn ensure_live(&self) -> Result<()> {
        if self
            .fence
            .as_ref()
            .is_some_and(|f| f.fatal.load(Ordering::Acquire))
        {
            return Err(FatalEngine("engine fatal; no further transaction permitted").into());
        }
        Ok(())
    }
    pub fn commit(&self, tx: Transaction) -> Result<super::CommitReceipt> {
        self.store.commit(tx)
    }
    pub(crate) fn trace(&self, value: serde_json::Value) {
        (self.store.trace)(value);
    }
    /// CNF-298, STO-55, LDG-69: applies to provider, rail and rescue adapters alike.
    pub fn external(&self, kind: ExternalKind) -> Result<ExternalCall> {
        self.ensure_live()?;
        let permit = self.gate.clone().try_acquire_owned().map_err(|_| {
            anyhow::anyhow!("external call forbidden while component owns transaction")
        })?;
        self.trace(json!({"event":"external_entry","kind":format!("{kind:?}"),"simulator":true}));
        Ok(ExternalCall { _permit: permit })
    }
}
#[derive(Debug, Clone, Copy)]
pub enum ExternalKind {
    Provider,
    Rail,
    Rescue,
}
pub struct ExternalCall {
    _permit: OwnedSemaphorePermit,
}

/// This type owns the only engine transaction constructors; it exposes no API
/// Store handle with which engine callers could bypass generation fencing.
pub struct EngineDatabase {
    store: Arc<Store>,
    lock: Session,
    fence: Fence,
    stopping: AtomicBool,
}
/// OVR-19's startup-lock row is an explicit assignment in the accepted local
/// profile. It shares the provisional pooled values, validated before transport use.
#[derive(Clone, Copy)]
struct StartupKeepalives {
    idle: Duration,
    interval: Duration,
    count: u32,
}
impl StartupKeepalives {
    fn local_profile(settings: &super::Settings) -> Result<Self> {
        let value = Self {
            idle: settings.keepalive_idle,
            interval: settings.keepalive_interval,
            count: settings.keepalive_count,
        };
        value.validate()
    }
    fn validate(self) -> Result<Self> {
        let value = self;
        for duration in [value.idle, value.interval] {
            ensure!(
                duration.as_secs() > 0
                    && duration.as_secs() <= i32::MAX as u64
                    && duration == Duration::from_secs(duration.as_secs()),
                "invalid startup-lock keepalive duration"
            );
        }
        ensure!(
            value.count > 0 && value.count <= i32::MAX as u32,
            "invalid startup-lock keepalive count"
        );
        Ok(value)
    }
    async fn connect(self, store: &Store) -> Result<Session> {
        use deadpool::managed::Manager as _;
        let mut manager = store.manager.clone();
        manager.settings.keepalive_idle = self.idle;
        manager.settings.keepalive_interval = self.interval;
        manager.settings.keepalive_count = self.count;
        manager.settings.validate()?;
        let session = manager.create().await?;
        let observed = session.scalar("SELECT current_setting('tcp_keepalives_idle') || ':' || current_setting('tcp_keepalives_interval') || ':' || current_setting('tcp_keepalives_count')").await?;
        ensure!(
            observed
                == format!(
                    "{}:{}:{}",
                    self.idle.as_secs(),
                    self.interval.as_secs(),
                    self.count
                ),
            "startup-lock keepalives did not apply"
        );
        (store.trace)(
            json!({"event":"startup_lock_keepalives_observed","session":session.pid,"idle_seconds":self.idle.as_secs(),"interval_seconds":self.interval.as_secs(),"count":self.count,"server":observed,"client_getters_verified":true,"profile_assignment":"shared with pooled local values"}),
        );
        Ok(session)
    }
}
impl EngineDatabase {
    pub async fn start(store: Store, wait_ms: u32) -> Result<Self> {
        startup_milliseconds(&wait_ms.to_string())?;
        if !super::test_defect("startup_order") {
            store.compatible_schema().await?;
        }
        let keepalives = StartupKeepalives::local_profile(&store.settings)?;
        let lock = keepalives.connect(&store).await?;
        let capacity=lock.scalar("SELECT (current_setting('max_connections')::int-current_setting('superuser_reserved_connections')::int-current_setting('reserved_connections')::int)::text").await?.parse::<usize>()?;
        ensure!(
            store.read.status().max_size + store.write.status().max_size < capacity,
            "engine pools plus lock exceed ordinary PostgreSQL capacity"
        );
        let started = Instant::now();
        loop {
            if lock
                .scalar(&format!(
                    "SELECT pg_try_advisory_lock({STARTUP_LOCK})::text"
                ))
                .await?
                == "true"
            {
                break;
            }
            ensure!(
                started.elapsed() < Duration::from_millis(wait_ms.into()),
                "startup lock refused within wait bound; no work performed"
            );
            tokio::time::sleep(
                Duration::from_millis(10)
                    .min(Duration::from_millis(wait_ms.into()).saturating_sub(started.elapsed())),
            )
            .await;
        }
        (store.trace)(json!({"event":"startup_lock_acquired","session":lock.pid}));
        if super::test_defect("startup_order") {
            store.compatible_schema().await?;
        }
        let mut advance = store.dedicated().await?;
        advance.settings.call_bound = advance
            .settings
            .call_bound
            .max(Duration::from_millis(wait_ms.into()));
        let mut tx = store.begin_dedicated(advance).await?;
        tx.batch(&format!(
            "SET LOCAL statement_timeout={wait_ms}; SET LOCAL lock_timeout={wait_ms}"
        ))
        .await?;
        (store.trace)(
            json!({"event":"advance_timeout_settings_applied","statement_ms":wait_ms,"lock_ms":wait_ms,"server_set_acknowledged":true}),
        );
        let rows=tx.rows_bound("UPDATE provisiond.startup_generation SET generation=gen_random_uuid() WHERE singleton RETURNING generation::text,current_setting('statement_timeout'),current_setting('lock_timeout') /* OPS-15 advance */",&[],Duration::from_millis(wait_ms.into()).max(store.settings.call_bound)).await?;
        ensure!(rows.len() == 1, "startup generation singleton missing");
        let generation = rows[0].get::<_, String>(0);
        (store.trace)(
            json!({"event":"advance_timeouts_observed","statement":rows[0].get::<_,String>(1),"lock":rows[0].get::<_,String>(2)}),
        );
        ensure!(
            store.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
            "fatal startup: generation advance unacknowledged; no claims"
        );
        (store.trace)(json!({"event":"generation_advanced","generation":generation}));
        Ok(Self {
            store: Arc::new(store),
            lock,
            fence: Fence {
                generation,
                fatal: Arc::new(AtomicBool::new(false)),
                entry: Arc::new(tokio::sync::Mutex::new(())),
            },
            stopping: AtomicBool::new(false),
        })
    }
    pub fn component(&self) -> Component {
        Component {
            store: self.store.clone(),
            gate: Arc::new(Semaphore::new(1)),
            fence: Some(self.fence.clone()),
        }
    }
    pub fn worker_limit(&self) -> usize {
        self.store.write.status().max_size
    }
    pub fn generation(&self) -> &str {
        &self.fence.generation
    }
    pub fn lock_pid(&self) -> i32 {
        self.lock.pid
    }
    pub fn detect_lock_loss(&self) -> bool {
        if self.lock.client.is_closed() || self.lock.protocol.snapshot().dead {
            if !self.stopping.swap(true, Ordering::AcqRel) {
                (self.store.trace)(
                    json!({"event":"startup_lock_lost","session":self.lock.pid,"stop_new_work":true}),
                );
            }
            true
        } else {
            false
        }
    }
    pub fn ensure_new_work(&self) -> Result<()> {
        ensure!(
            !self.detect_lock_loss() && !self.stopping.load(Ordering::Acquire),
            "engine stopping: no claims or periodic passes"
        );
        ensure!(!self.fence.fatal.load(Ordering::Acquire), "engine fatal");
        Ok(())
    }
    pub fn is_fatal(&self) -> bool {
        self.fence.fatal.load(Ordering::Acquire)
    }
    pub fn fatal(&self, reason: &str) {
        self.fence.fatal.store(true, Ordering::Release);
        (self.store.trace)(json!({"event":"engine_fatal","reason":reason}));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ovr19_startup_keepalives_reject_unsupported_durations() {
        let mut settings = StartupKeepalives {
            idle: Duration::from_secs(2),
            interval: Duration::from_secs(1),
            count: 2,
        };
        for value in [
            Duration::ZERO,
            Duration::from_millis(1500),
            Duration::from_secs(i32::MAX as u64 + 1),
        ] {
            let original = settings;
            settings.idle = value;
            assert!(settings.validate().is_err());
            settings = original;
            settings.interval = value;
            assert!(settings.validate().is_err());
            settings = original;
        }
        for count in [0, i32::MAX as u32 + 1] {
            settings.count = count;
            assert!(settings.validate().is_err());
        }
    }
    #[test]
    fn ops15_bound_is_exact_positive_pg_milliseconds() {
        for value in [
            "",
            "0",
            "-1",
            "NaN",
            "inf",
            "0.1",
            "1.0",
            "2147483648",
            "4294967296",
        ] {
            assert!(startup_milliseconds(value).is_err(), "{value}");
        }
        for value in ["1", "500", "2147483647"] {
            assert_eq!(startup_milliseconds(value).unwrap().to_string(), value);
        }
        assert_ne!(STARTUP_LOCK, super::super::migration::MIGRATION_LOCK);
    }
}
