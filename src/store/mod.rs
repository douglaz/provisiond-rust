//! OVR-9/STO-7/STO-55: sole transaction entry, admission, pools and COMMIT ownership.
mod business;
mod commit;
mod component;
mod customer;
pub(crate) use business::validate_charge;
pub use business::{Claim, CreateRequest, Payment};
pub use component::{
    Component, EngineDatabase, ExternalCall, ExternalKind, FatalEngine, startup_milliseconds,
};
pub mod migration;
mod process;
mod transport;
use anyhow::{Context, Result, bail, ensure};
#[cfg(feature = "store-test-hooks")]
pub use commit::TestCommitDefect;
pub use commit::{CommitOutcome, CommitReceipt};
use deadpool::managed::{self, Metrics, RecycleError, RecycleResult};
use futures_util::future::poll_fn;
pub use process::Process;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_postgres::{AsyncMessage, Client, Config, NoTls, SimpleQueryMessage};

/// Structured observations. Callbacks must not block or panic; never log credentials.
pub type Trace = Arc<dyn Fn(Value) + Send + Sync>;

/// Provisional OVR-19 values; deployment parameter selection remains D-6/W8.
#[derive(Clone)]
pub struct Settings {
    pub connection: Config,
    pub designated_name: String,
    pub designated_role: String,
    pub designated_address: String,
    pub statement_ms: u32,
    pub lock_ms: u32,
    pub idle_transaction_ms: u32,
    pub call_bound: Duration,
    pub check_bound: Duration,
    pub checkout_bound: Duration,
    pub keepalive_idle: Duration,
    pub keepalive_interval: Duration,
    pub keepalive_count: u32,
}
impl Settings {
    fn validate(&self) -> Result<()> {
        let max = self
            .statement_ms
            .max(self.lock_ms)
            .max(self.idle_transaction_ms);
        ensure!(
            self.statement_ms > 0
                && self.lock_ms > 0
                && self.idle_transaction_ms > 0
                && max <= i32::MAX as u32,
            "invalid server timeouts"
        );
        ensure!(
            self.call_bound >= Duration::from_millis(max.into())
                && self.check_bound >= self.call_bound,
            "client deadline shorter than server timeouts"
        );
        ensure!(
            !self.checkout_bound.is_zero()
                && self.keepalive_idle.as_secs() > 0
                && self.keepalive_interval.as_secs() > 0
                && self.keepalive_idle == Duration::from_secs(self.keepalive_idle.as_secs())
                && self.keepalive_interval
                    == Duration::from_secs(self.keepalive_interval.as_secs())
                && self.keepalive_count > 0
                && self.keepalive_count <= i32::MAX as u32,
            "invalid checkout/keepalive bounds"
        );
        ensure!(
            !self.designated_name.is_empty()
                && !self.designated_role.is_empty()
                && !self.designated_address.is_empty(),
            "missing designated identity"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct Manager {
    settings: Settings,
    process: Arc<Process>,
    trace: Trace,
}
pub type Checkout = managed::Object<Manager>;
type Pool = managed::Pool<Manager>;

/// Separate ordinary read/write pools. Checkout failure starts no transaction (STO-55).
pub struct Store {
    read: Pool,
    write: Pool,
    settings: Settings,
    trace: Trace,
    process: Arc<Process>,
    manager: Manager,
}
impl Store {
    pub fn new(
        settings: Settings,
        process: Arc<Process>,
        read_limit: usize,
        write_limit: usize,
        trace: Trace,
    ) -> Result<Self> {
        settings.validate()?;
        ensure!(read_limit > 0 && write_limit > 0, "empty pool budget");
        let manager = Manager {
            settings: settings.clone(),
            process: process.clone(),
            trace: trace.clone(),
        };
        let read = Pool::builder(manager.clone())
            .max_size(read_limit)
            .wait_timeout(Some(settings.checkout_bound))
            .runtime(deadpool::Runtime::Tokio1)
            .build()?;
        let write = Pool::builder(manager.clone())
            .max_size(write_limit)
            .wait_timeout(Some(settings.checkout_bound))
            .runtime(deadpool::Runtime::Tokio1)
            .build()?;
        Ok(Self {
            read,
            write,
            manager,
            settings,
            trace,
            process,
        })
    }
    async fn checkout(&self, pool: &Pool) -> Result<Checkout> {
        #[cfg(feature = "store-test-hooks")]
        if pool.status().available == 0 && pool.status().size == pool.status().max_size {
            (self.trace)(json!({"event":"pool_checkout_waiting"}));
        }
        let acquisition = async {
            pool.get()
                .await
                .map_err(|e| anyhow::anyhow!("pool checkout/create/check: {e}"))
        };
        if test_defect("checkout") {
            return acquisition.await;
        }
        // STO-55: one bound includes waiting, recycling, construction and admission.
        tokio::time::timeout(self.settings.checkout_bound, acquisition)
            .await
            .context("whole pool checkout deadline (waiting/create/admission)")?
    }
    pub async fn read_session(&self) -> Result<Checkout> {
        self.checkout(&self.read).await
    }
    pub async fn write_session(&self) -> Result<Checkout> {
        self.checkout(&self.write).await
    }

    /// STO-7: repeat SELECT and post-read check on a new session after any failure.
    /// The old value never survives the failed attempt. Two whole-read attempts;
    /// this is not OPS-49's later engine retry policy.
    pub async fn destination(&self, id: i32) -> Result<String> {
        let mut last = None;
        for attempt in 1..=2 {
            // A cancelled attempt drops this guard and discards the physical session.
            let mut attempt_session = DiscardUnlessFinished(Some(self.read_session().await?));
            let session = attempt_session.0.as_ref().expect("attempt session");
            let result = session.destination(id, attempt).await;
            let session = attempt_session.0.take().expect("attempt session");
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    (self.trace)(
                        json!({"event":"destination_discarded","session":session.pid,"attempt":attempt}),
                    );
                    drop(managed::Object::take(session));
                    last = Some(error);
                }
            }
        }
        Err(last.expect("two attempts"))
    }

    /// Discard idle capacity; outstanding objects remain owned until completion.
    pub fn recycle_idle(&self) {
        drop(self.write.retain(|_, _| false));
        drop(self.read.retain(|_, _| false));
    }
    pub fn write_checked_out(&self) -> usize {
        let status = self.write.status();
        status.size - status.available
    }
    pub fn write_available(&self) -> usize {
        self.write.status().available
    }
    pub fn checkout_bound(&self) -> Duration {
        self.settings.checkout_bound
    }
}

pub struct Session {
    client: Arc<Client>,
    protocol: Arc<transport::Protocol>,
    pub pid: i32,
    settings: Settings,
    trace: Trace,
    notices: Arc<Mutex<Diagnostics>>,
    close_driver: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    tainted: AtomicBool,
}
impl managed::Manager for Manager {
    type Type = Session;
    type Error = anyhow::Error;
    async fn create(&self) -> Result<Session> {
        ensure!(
            self.process.accepting.load(Ordering::Acquire),
            "store is stopping"
        );
        let mut config = self.settings.connection.clone();
        config
            .keepalives(true)
            .keepalives_idle(self.settings.keepalive_idle)
            .keepalives_interval(self.settings.keepalive_interval)
            .keepalives_retries(self.settings.keepalive_count);
        // A thin TCP observer retains command tags without changing the driver.
        // This prototype supports one explicit TCP endpoint with TLS disabled.
        ensure!(
            config.get_hosts().len() == 1
                && config.get_ssl_mode() == tokio_postgres::config::SslMode::Disable,
            "store spike requires one direct TCP endpoint with sslmode=disable"
        );
        let tokio_postgres::config::Host::Tcp(host) = config.get_hosts()[0].clone() else {
            bail!("TCP endpoint required");
        };
        let port = config.get_ports().first().copied().unwrap_or(5432);
        let protocol = Arc::new(transport::Protocol::default());
        let settings = self.settings.clone();
        let trace = self.trace.clone();
        let connect_protocol = protocol.clone();
        // STO-7: socket registration and authentication use the process reactor,
        // not the runtime that happens to poll pool.get(). Moving a registered
        // socket's driver later would leave its I/O tied to that caller.
        let (client, mut connection) = self
            .process
            .run(async move {
                tokio::time::timeout(settings.call_bound, async {
                    let socket = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
                    socket.set_nodelay(true)?;
                    let reference = socket2::SockRef::from(&socket);
                    reference.set_keepalive(true)?;
                    reference.set_tcp_keepalive(
                        &socket2::TcpKeepalive::new()
                            .with_time(settings.keepalive_idle)
                            .with_interval(settings.keepalive_interval)
                            .with_retries(settings.keepalive_count),
                    )?;
                    ensure!(
                        reference.keepalive()?
                            && reference.tcp_keepalive_time()? == settings.keepalive_idle
                            && reference.tcp_keepalive_interval()? == settings.keepalive_interval
                            && reference.tcp_keepalive_retries()? == settings.keepalive_count,
                        "client keepalive settings did not apply"
                    );
                    trace(json!({"event":"client_keepalives_observed","idle_seconds":reference.tcp_keepalive_time()?.as_secs(),"interval_seconds":reference.tcp_keepalive_interval()?.as_secs(),"count":reference.tcp_keepalive_retries()?}));
                    config
                        .connect_raw(transport::Transport::new(socket, connect_protocol), NoTls)
                        .await
                        .context("connect authentication")
                })
                .await
                .context("connect deadline")?
            })
            .await??;
        let notices = Arc::new(Mutex::new(Diagnostics::default()));
        let diagnostics = notices.clone();
        let trace = self.trace.clone();
        let driver_protocol = protocol.clone();
        let (close_driver, mut close) = tokio::sync::oneshot::channel();
        self.process.submit(async move {
            loop {
                let message = tokio::select! {
                    message = poll_fn(|cx| connection.poll_message(cx)) => message,
                    _ = &mut close => { trace(json!({"event":"driver_closed_unused"})); break; }
                };
                match message {
                    Some(Ok(AsyncMessage::Notice(n))) => {
                        let diagnostic=json!({"event":"driver_notice","severity":n.severity(),"sqlstate":n.code().code(),"message":n.message(),"detail":n.detail()});
                        let mut retained = diagnostics.lock().expect("notice lock");
                        if retained.committing && n.severity() == "WARNING" {
                            retained.commit_warning = true;
                        }
                        if retained.values.len() < NOTICE_CAP || test_defect("notices") {
                            retained.values.push(diagnostic.clone());
                        }
                        drop(retained);
                        trace(diagnostic);
                    }
                    Some(Ok(_)) => {},
                    Some(Err(e)) => { trace(json!({"event":"driver_dead","error":e.to_string()})); break; }
                    None => { trace(json!({"event":"driver_closed"})); break; }
                }
            }
            driver_protocol.dead();
        })?;
        let mut session = Session {
            client: Arc::new(client),
            protocol,
            pid: 0,
            settings: self.settings.clone(),
            trace: self.trace.clone(),
            notices,
            close_driver: Mutex::new(Some(close_driver)),
            tainted: AtomicBool::new(false),
        };
        // STO-7: these apply on every new physical session, before admission.
        let s = &session.settings;
        session.query(&format!("SET statement_timeout={}; SET lock_timeout={}; SET idle_in_transaction_session_timeout={}; SET default_transaction_isolation='read committed'; SET search_path=pg_catalog,fixture; SET synchronous_commit=on; SET default_transaction_read_only=on; SET tcp_keepalives_idle={}; SET tcp_keepalives_interval={}; SET tcp_keepalives_count={}",s.statement_ms,s.lock_ms,s.idle_transaction_ms,s.keepalive_idle.as_secs(),s.keepalive_interval.as_secs(),s.keepalive_count)).await?;
        session.pid = session
            .scalar("SELECT pg_backend_pid()::text")
            .await?
            .parse()?;
        session.check("admission").await?;
        (self.trace)(json!({"event":"session_admitted","session":session.pid}));
        Ok(session)
    }
    async fn recycle(&self, obj: &mut Session, _: &Metrics) -> RecycleResult<anyhow::Error> {
        if !self.process.accepting.load(Ordering::Acquire)
            || obj.client.is_closed()
            || obj.tainted.load(Ordering::Acquire)
        {
            return Err(RecycleError::Message("closed/stopping".into()));
        }
        // No transaction object can return dirty: pre-COMMIT abandonment removes it.
        Ok(())
    }
}
impl Session {
    /// STO-7: deadline-driven closure does not wait for checkout/transaction Drop
    /// or recycling. This is never used by the sent-COMMIT owner.
    fn close(&self) {
        self.tainted.store(true, Ordering::Release);
        if let Some(close) = self.close_driver.lock().expect("driver close lock").take() {
            let _ = close.send(());
        }
    }
    async fn query(&self, sql: &str) -> Result<Vec<SimpleQueryMessage>> {
        ensure!(!self.tainted.load(Ordering::Acquire), "unusable session");
        // A caller that drops this future mid-call leaves a reply outstanding; the
        // guard closes the session so recycling rejects it.
        let mut in_flight = CloseUnlessFinished(Some(self));
        let outcome =
            tokio::time::timeout(self.settings.call_bound, self.client.simple_query(sql)).await;
        in_flight.0 = None;
        match outcome {
            Ok(result) => result.context("store query"),
            Err(error) => {
                self.close();
                Err(error).context("pre-COMMIT call deadline")
            }
        }
    }
    async fn scalar(&self, sql: &str) -> Result<String> {
        let messages = self.query(sql).await?;
        for message in messages {
            if let SimpleQueryMessage::Row(row) = message {
                return row
                    .get(0)
                    .map(str::to_owned)
                    .context("unexpected NULL scalar");
            }
        }
        bail!("missing scalar")
    }
    pub async fn ping(&mut self) -> Result<()> {
        ensure!(self.scalar("SELECT 1::text").await? == "1", "ping");
        Ok(())
    }
    pub fn diagnostics(&self) -> Vec<Value> {
        self.notices.lock().expect("notice lock").values.clone()
    }
    pub async fn settings(&mut self) -> Result<Vec<String>> {
        let mut result = Vec::new();
        for name in [
            "statement_timeout",
            "lock_timeout",
            "idle_in_transaction_session_timeout",
            "default_transaction_isolation",
            "search_path",
            "synchronous_commit",
            "tcp_keepalives_idle",
            "tcp_keepalives_interval",
            "tcp_keepalives_count",
            "synchronous_standby_names",
        ] {
            result.push(format!(
                "{name}={}",
                self.scalar(&format!("SHOW {name}")).await?
            ));
        }
        Ok(result)
    }
    /// STO-7: one fixed primary FLUSH target, refreshed implicit statement snapshots.
    /// Never executes state-changing SQL or uses insertion position.
    async fn check(&self, kind: &str) -> Result<()> {
        let result = tokio::time::timeout(self.settings.check_bound, async {
            let target=self.scalar(&format!("SELECT pg_current_wal_flush_lsn()::text /* store {kind} target */")).await?;
            (self.trace)(json!({"event":"check_target","kind":kind,"session":self.pid,"target":target}));
            loop {
                let messages=self.query("SELECT pid::text, usename::text, application_name, host(client_addr), state, sync_state, flush_lsn::text FROM store_guard.sample() /* store poll */").await?;
                let rows:Vec<_>=messages.into_iter().filter_map(|m|if let SimpleQueryMessage::Row(r)=m {Some(r)} else {None}).collect();
                ensure!(rows.len()==1,"expected exactly one designated walsender, got {}",rows.len());
                let values:Vec<_>= (0..7).map(|i|rows[0].get(i).map(str::to_owned).context("unknown replication observation (NULL)")).collect::<Result<_>>()?;
                (self.trace)(json!({"event":"check_observation","kind":kind,"session":self.pid,"target":target,"observed":values}));
                ensure!(values[1]==self.settings.designated_role && values[2]==self.settings.designated_name && values[3]==self.settings.designated_address,"wrong designated identity");
                ensure!(values[4]=="streaming" && values[5]=="sync","not designated synchronous streaming peer");
                if lsn(&values[6])? >= lsn(&target)? { return Ok(()); }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await;
        if result.is_err() {
            self.close();
        }
        result.context("read-only check deadline")?
    }
    async fn destination(&self, id: i32, attempt: u32) -> Result<String> {
        let value=self.scalar(&format!("SELECT destination FROM fixture.destinations WHERE id={id} /* destination SELECT */")).await?;
        (self.trace)(json!({"event":"destination_read","session":self.pid,"attempt":attempt}));
        self.check("post_read").await?;
        (self.trace)(json!({"event":"destination_exposed","session":self.pid,"attempt":attempt}));
        Ok(value)
    }
    #[cfg(feature = "store-test-hooks")]
    pub async fn test_query(&mut self, sql: &str) -> Result<String> {
        self.scalar(sql).await
    }
    #[cfg(feature = "store-test-hooks")]
    pub async fn test_check(&mut self) -> Result<()> {
        self.check("test").await
    }
}
fn lsn(value: &str) -> Result<u64> {
    let (hi, lo) = value.split_once('/').context("invalid pg_lsn")?;
    Ok((u64::from(u32::from_str_radix(hi, 16)?) << 32) | u64::from(u32::from_str_radix(lo, 16)?))
}

impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
}

/// STO-7: closes a session whose pre-COMMIT call was cancelled before it finished.
struct CloseUnlessFinished<'a>(Option<&'a Session>);
impl Drop for CloseUnlessFinished<'_> {
    fn drop(&mut self) {
        if let Some(session) = self.0.take() {
            (session.trace)(json!({"event":"call_cancelled_session_closed","session":session.pid}));
            session.close();
        }
    }
}

/// STO-7: discards a read attempt's checkout unless the attempt finished.
struct DiscardUnlessFinished(Option<Checkout>);
impl Drop for DiscardUnlessFinished {
    fn drop(&mut self) {
        if let Some(checkout) = self.0.take() {
            (checkout.trace)(
                json!({"event":"destination_cancelled_session_discarded","session":checkout.pid}),
            );
            checkout.close();
            drop(managed::Object::take(checkout));
        }
    }
}

/// STO-7: pre-COMMIT cancellation or failure closes the physical session before
/// another checkout can use it. This guard never sends a PostgreSQL cancel packet.
pub struct Transaction {
    checkout: Option<Connection>,
    failed: bool,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    pub(crate) tenants: Vec<String>,
    engine_fenced: bool,
}
impl Store {
    pub async fn begin(&self) -> Result<Transaction> {
        self.begin_checked(|| Ok(())).await
    }
    /// OPS-47/50: arbitrate dispatch after every acquisition wait. The returned
    /// short guard covers only the first poll, which submits tokio-postgres's
    /// simple query before waiting for its reply; never hold it across I/O.
    async fn begin_checked<G>(&self, admission: impl FnOnce() -> Result<G>) -> Result<Transaction> {
        let checkout = self.write_session().await?;
        let transaction = Transaction {
            checkout: Some(Connection::Pooled(checkout)),
            failed: false,
            permit: None,
            tenants: Vec::new(),
            engine_fenced: false,
        };
        transaction
            .checkout
            .as_ref()
            .expect("checkout")
            .notices
            .lock()
            .expect("notice lock")
            .reset();
        {
            let query = transaction
                .checkout
                .as_ref()
                .expect("checkout")
                .query("BEGIN READ WRITE");
            tokio::pin!(query);
            let mut admission = Some(admission);
            std::future::poll_fn(|cx| {
                if let Some(check) = admission.take() {
                    let _guard = match check() {
                        Ok(guard) => guard,
                        Err(error) => return std::task::Poll::Ready(Err(error)),
                    };
                    query.as_mut().poll(cx)
                } else {
                    query.as_mut().poll(cx)
                }
            })
            .await?;
        }
        Ok(transaction)
    }
}
impl Transaction {
    pub fn session_id(&self) -> i32 {
        self.checkout.as_ref().expect("live transaction").pid
    }
    /// Trusted fixture SQL is absent from production builds. Business statements
    /// are private constants with parameters; callers cannot supply SQL (STO-7).
    #[cfg(feature = "store-test-hooks")]
    pub async fn execute(&mut self, statement: &str) -> Result<u64> {
        self.execute_internal(statement).await
    }
    #[cfg(feature = "store-test-hooks")]
    async fn execute_internal(&mut self, statement: &str) -> Result<u64> {
        ensure!(!self.failed, "transaction has a failed/cancelled statement");
        self.failed = true;
        let session = self.checkout.as_ref().context("transaction consumed")?;
        let mut in_flight = CloseUnlessFinished(if test_defect("execute_cancel") {
            None
        } else {
            Some(session)
        });
        let result = tokio::time::timeout(
            session.settings.call_bound,
            session.client.execute(statement, &[]),
        )
        .await;
        in_flight.0 = None;
        match result {
            Ok(Ok(rows)) => {
                self.failed = false;
                Ok(rows)
            }
            Ok(Err(error)) => {
                self.failed = true;
                Err(error).context("transaction statement")
            }
            Err(error) => {
                self.failed = true;
                session.close();
                Err(error).context("pre-COMMIT transaction deadline")
            }
        }
    }
    /// No production caller-SQL surface exists. This hook demonstrates refusal
    /// without parsing or executing even indirect settings functions (STO-7).
    #[cfg(feature = "store-test-hooks")]
    pub async fn test_caller_sql(&mut self, sql: &str) -> Result<()> {
        if test_defect("caller_sql") {
            self.execute(sql).await?;
            return Ok(());
        }
        bail!("caller SQL is not an admitted business statement")
    }
    #[cfg(feature = "store-test-hooks")]
    pub async fn test_scalar(&self, sql: &str) -> Result<String> {
        self.checkout
            .as_ref()
            .context("transaction consumed")?
            .scalar(sql)
            .await
    }
    #[cfg(feature = "store-test-hooks")]
    pub fn test_diagnostics(&self) -> Vec<Value> {
        self.checkout.as_ref().expect("checkout").diagnostics()
    }
    pub async fn rollback(mut self) -> Result<()> {
        self.checkout
            .as_ref()
            .context("transaction consumed")?
            .query("ROLLBACK")
            .await?;
        drop(self.checkout.take());
        Ok(())
    }
}
impl Drop for Transaction {
    fn drop(&mut self) {
        if let Some(checkout) = self.checkout.take() {
            (checkout.trace)(json!({"event":"precommit_session_discarded","session":checkout.pid}));
            checkout.discard();
        }
    }
}

// STO-7: diagnostic payload retention is bounded independently of warning safety.
const NOTICE_CAP: usize = 16;
#[derive(Default)]
struct Diagnostics {
    values: Vec<Value>,
    committing: bool,
    commit_warning: bool,
}
impl Diagnostics {
    fn reset(&mut self) {
        self.values.clear();
        self.committing = false;
        self.commit_warning = false;
    }
}
pub(crate) fn test_defect(name: &str) -> bool {
    #[cfg(feature = "store-test-hooks")]
    {
        std::env::var("STORE_TEST_DEFECT").as_deref() == Ok(name)
    }
    #[cfg(not(feature = "store-test-hooks"))]
    {
        let _ = name;
        false
    }
}

/// Dedicated adapters share the same transaction and COMMIT owner (STO-7/12/51).
enum Connection {
    Pooled(Checkout),
    Dedicated(Session),
}
impl std::ops::Deref for Connection {
    type Target = Session;
    fn deref(&self) -> &Session {
        match self {
            Self::Pooled(c) => c,
            Self::Dedicated(s) => s,
        }
    }
}
impl Connection {
    fn unpool(self) -> Session {
        match self {
            Self::Pooled(c) => managed::Object::take(c),
            Self::Dedicated(s) => s,
        }
    }
    fn discard(self) {
        drop(self.unpool());
    }
}
impl Store {
    /// CNF-368/369 isolated fault holder, outside ordinary pools. This deliberately
    /// disables server timeouts only for the test's independently owned session.
    #[cfg(feature = "store-test-hooks")]
    pub async fn test_generation_holder(&self) -> Result<(Transaction, Option<Claim>)> {
        let session = self.dedicated().await?;
        session
            .query("SET statement_timeout=0; SET idle_in_transaction_session_timeout=0")
            .await?;
        let mut tx = self.begin_dedicated(session).await?;
        tx.rows(
            "SELECT generation FROM provisiond.startup_generation WHERE singleton FOR SHARE",
            &[],
        )
        .await?;
        let claim = tx.claim_inner().await?;
        Ok((tx, claim))
    }
    async fn dedicated(&self) -> Result<Session> {
        use deadpool::managed::Manager as _;
        self.manager.create().await
    }
    async fn begin_dedicated(&self, session: Session) -> Result<Transaction> {
        let tx = Transaction {
            checkout: Some(Connection::Dedicated(session)),
            failed: false,
            permit: None,
            tenants: Vec::new(),
            engine_fenced: false,
        };
        let session = tx.checkout.as_ref().expect("checkout");
        session.notices.lock().expect("notice lock").reset();
        session.query("BEGIN READ WRITE").await?;
        Ok(tx)
    }
}
impl Transaction {
    /// Internal parameterized statements only; never a production caller-SQL API.
    pub(crate) async fn rows(
        &mut self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>> {
        let bound = self
            .checkout
            .as_ref()
            .context("transaction consumed")?
            .settings
            .call_bound;
        self.rows_bound(sql, params, bound).await
    }
    async fn rows_bound(
        &mut self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        bound: Duration,
    ) -> Result<Vec<tokio_postgres::Row>> {
        ensure!(!self.failed, "transaction has a failed/cancelled statement");
        self.failed = true;
        let session = self.checkout.as_ref().context("transaction consumed")?;
        let mut in_flight = CloseUnlessFinished(Some(session));
        let outcome = tokio::time::timeout(bound, session.client.query(sql, params)).await;
        in_flight.0 = None;
        match outcome {
            Ok(Ok(rows)) => {
                self.failed = false;
                Ok(rows)
            }
            Ok(Err(e)) => Err(e).context("transaction statement"),
            Err(e) => {
                session.close();
                Err(e).context("pre-COMMIT transaction deadline")
            }
        }
    }
    async fn batch(&mut self, sql: &str) -> Result<()> {
        ensure!(!self.failed, "transaction has a failed/cancelled statement");
        self.failed = true;
        self.checkout
            .as_ref()
            .context("transaction consumed")?
            .query(sql)
            .await?;
        self.failed = false;
        Ok(())
    }
}
