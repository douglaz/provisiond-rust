//! API-7/33/43/57, WIR-4/4a: local customer service; no provider dependency.
mod funding;
use crate::{
    auth::{self, Admission},
    engine::{Enqueue, Queue},
    http::{self, Request},
    local::Parameters,
    pricing,
    store::{CommitOutcome, CreateRequest, Store, Trace, Transaction},
    wire::{self, Error},
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
};
struct State {
    store: Arc<Store>,
    p: Parameters,
    admission: Admission,
    catalog: BTreeMap<String, Value>,
    queue: Box<dyn Enqueue>,
    sources: Mutex<BTreeMap<IpAddr, Arc<Semaphore>>>,
    rates: Mutex<BTreeMap<String, (Instant, i64)>>,
    trace: Trace,
}
fn unsupported() -> Error {
    Error::new(
        "unsupported",
        "operation outside local prototype",
        false,
        json!({"provider_account":null,"capability":"local_subset"}),
    )
}
fn gate(name: &str) -> Error {
    Error::new(
        "halted",
        "purchase gate closed",
        true,
        json!({"gate":name,"retry_after_ms":1000}),
    )
}
fn overload() -> Error {
    Error::new(
        "overloaded",
        "store checkout unavailable",
        true,
        json!({"retry_after_ms":250}),
    )
}
fn as_error(e: anyhow::Error) -> Error {
    if let Some(e) = e.downcast_ref::<Error>() {
        return e.clone();
    }
    if format!("{e:#}").contains("pool checkout") {
        overload()
    } else {
        Error::new(
            "internal",
            "local request could not complete",
            true,
            json!({}),
        )
    }
}
// WIR-34's closed operator list is method-scoped. WIR-29/30 share customer
// paths and select their customer projection through ordinary authentication.
fn operator_route(method: &str, path: &str) -> bool {
    let parts: Vec<_> = path.split('/').collect();
    matches!(
        (method, parts.as_slice()),
        ("GET", ["", "v1", "episodes" | "address-resolution"])
            | ("GET", ["", "v1", "episodes", _])
            | ("GET", ["", "v1", "machines", _, "attachments"])
            | ("GET", ["", "v1", "provider-accounts", _, "billing"])
            | ("POST", ["", "v1", "abuse-cases"])
            | ("POST", ["", "v1", "operations", _, "actions", "resolve"])
            | ("POST", ["", "v1", "deposits", _, "actions", "attribute"])
            | (
                "POST",
                [
                    "",
                    "v1",
                    "tenants",
                    _,
                    "actions",
                    "suspend" | "resume" | "assign-provider-account",
                ],
            )
            | (
                "POST",
                ["", "v1", "episodes", _, "actions", "retry" | "keep"]
            )
            | (
                "POST",
                [
                    "",
                    "v1",
                    "abuse-cases",
                    _,
                    "actions",
                    "close" | "record-transmission" | "revise-deadline",
                ],
            )
            | (
                "POST",
                [
                    "",
                    "v1",
                    "machines",
                    _,
                    "actions",
                    "record-network-restriction",
                ],
            )
            | (
                "POST",
                [
                    "",
                    "v1",
                    "machines",
                    _,
                    "attachments",
                    _,
                    "actions",
                    "release",
                ],
            )
            | (
                "POST",
                [
                    "",
                    "v1",
                    "provider-accounts",
                    _,
                    "actions",
                    "record-status" | "record-billing",
                ],
            )
    )
}
impl State {
    fn rate(&self, principal: &str, write: bool) -> Result<()> {
        self.rate_allowance(principal, write, true)
    }
    fn rate_allowance(&self, principal: &str, write: bool, consume: bool) -> Result<()> {
        let mut rates = self.rates.lock().expect("rates");
        let interval = Duration::from_millis(self.p.n("rate_interval_ms") as u64);
        rates.retain(|_, (start, _)| start.elapsed() < interval);
        let key = format!("{principal}:{write}");
        if !rates.contains_key(&key) && rates.len() >= 4096 {
            return Err(Error::new(
                "rate_limited",
                "traffic capacity",
                true,
                json!({"retry_after_ms":self.p.n("rate_interval_ms")}),
            )
            .into());
        }
        let entry = rates.entry(key).or_insert((Instant::now(), 0));
        let limit = self.p.n(if write { "write_limit" } else { "read_limit" });
        if entry.1 >= limit {
            return Err(Error::new("rate_limited","request pacing",true,json!({"retry_after_ms":(interval-entry.0.elapsed().min(interval)).as_millis().max(1) as u64})).into());
        }
        if consume {
            entry.1 += 1;
        }
        Ok(())
    }
    async fn authenticate(&self, r: &Request) -> Result<Value> {
        let token = r
            .headers
            .get("authorization")
            .and_then(|s| s.strip_prefix("Bearer "))
            .unwrap_or("");
        let digest = wire::digest(token.as_bytes());
        let tenant=self.store.value("SELECT COALESCE((SELECT jsonb_build_object('id',id,'status',status,'digest',credential_digest) FROM provisiond.tenants WHERE credential_digest=$1),'null'::jsonb)::text",&[&digest]).await?;
        let stored = tenant["digest"]
            .as_str()
            .unwrap_or("0000000000000000000000000000000000000000000000000000000000000000");
        if token.is_empty()
            || !bool::from(digest.as_bytes().ct_eq(stored.as_bytes()))
            || tenant.is_null()
        {
            return Err(Error::new(
                "authentication",
                "authentication required",
                false,
                json!({"reason":"token"}),
            )
            .into());
        }
        Ok(tenant)
    }
    async fn receipt(
        &self,
        tenant: &str,
        key: &str,
        fingerprint: &str,
    ) -> Result<Option<(u16, Value)>> {
        // Includes the same-session post-read check, because a receipt may carry destinations.
        let v=self.store.destination_value("SELECT COALESCE((SELECT jsonb_build_object('fingerprint',fingerprint,'resource_id',resource_id,'resource_kind',resource_kind,'status',status,'body',response_body,'expires',provisiond.wire_time(expires_at),'expired',expires_at<=clock_timestamp()) FROM provisiond.idempotency_records WHERE scope_kind='tenant' AND scope_id=$1 AND key=$2),'null'::jsonb)::text",&[&tenant,&key]).await?;
        if v.is_null() {
            return Ok(None);
        }
        if v["fingerprint"] != fingerprint {
            return Err(Error::conflict("idempotency_mismatch").into());
        }
        if v["expired"] == true {
            return Err(Error::new(
                "gone",
                "receipt retention ended; read machines and balance before deciding",
                false,
                json!({"retained_until":v["expires"]}),
            )
            .into());
        }
        if v["resource_kind"] == "operation" {
            return Ok(Some((
                202,
                self.operation(
                    tenant,
                    v["resource_id"].as_str().context("receipt resource")?,
                )
                .await?,
            )));
        }
        Ok(Some((
            v["status"].as_u64().context("status")? as u16,
            v["body"].clone(),
        )))
    }
    async fn operation(&self, tenant: &str, id: &str) -> Result<Value> {
        let v=self.store.value("SELECT COALESCE((SELECT body FROM provisiond.operation_views WHERE tenant_id=$1 AND id::text=$2),'null'::jsonb)::text",&[&tenant,&id]).await?;
        if !v.is_null() {
            return Ok(v);
        }
        let gone=self.store.value("SELECT COALESCE((SELECT jsonb_build_object('retained_until',provisiond.wire_time(expires_at)) FROM provisiond.idempotency_records WHERE scope_kind='tenant' AND scope_id=$1 AND resource_id::text=$2 AND resource_kind='operation' LIMIT 1),'null'::jsonb)::text",&[&tenant,&id]).await?;
        Err(if gone.is_null() {
            Error::missing()
        } else {
            Error::new(
                "gone",
                "operation removed by retention; read machines and balance",
                false,
                gone,
            )
        }
        .into())
    }
    async fn handle(&self, r: &Request, source: IpAddr, correlation: &str) -> Result<(u16, Value)> {
        let path = r.target.split('?').next().unwrap_or(&r.target);
        let write = r.method != "GET";
        if r.method == "POST" && path == "/v1/enrol/token" {
            // WIR-49 uses per-source concurrency only. Disconnect is selected by connection().
            let slots = {
                let mut m = self.sources.lock().expect("sources");
                m.retain(|_, v| {
                    Arc::strong_count(v) > 1
                        || v.available_permits() < self.p.n("source_concurrency") as usize
                });
                if m.len() >= 4096 && !m.contains_key(&source) {
                    return Err(overload().into());
                }
                m.entry(source)
                    .or_insert_with(|| {
                        Arc::new(Semaphore::new(self.p.n("source_concurrency") as usize))
                    })
                    .clone()
            };
            let _permit = slots.try_acquire_owned().map_err(|_| {
                Error::new(
                    "rate_limited",
                    "too many held admission requests",
                    true,
                    // API-49/WIR-49: constant unrelated to admission/remaining delay.
                    json!({"retry_after_ms":1000}),
                )
            })?;
            let v = wire::parse(&r.body)?;
            wire::least(wire::unknown(&v, &[]))?;
            (self.trace)(json!({"event":"token_request_held","correlation_id":correlation}));
            tokio::time::sleep(Duration::from_millis(self.admission.delay_ms)).await;
            let (token, expiry) = self.admission.issue()?;
            (self.trace)(json!({"event":"token_issued","correlation_id":correlation}));
            return Ok((
                200,
                json!({"admission_token":token,"expires_at":auth::time(expiry)}),
            ));
        }
        if path == "/v1/enrol" && r.method == "POST" {
            self.rate(&source.to_string(), true)?;
            let v = wire::parse(&r.body)?;
            self.admission
                .consume(v["admission_token"].as_str().unwrap_or(""))?;
            wire::least(wire::unknown(&v, &["admission_token"]))?;
            let spending = auth::secret("pvd_s_")?;
            let recovery = auth::secret("pvd_r_")?;
            let handle = uuid::Uuid::new_v4().to_string();
            let id = uuid::Uuid::new_v4().to_string();
            let expiry = auth::now() + self.p.n("tenant_seconds");
            let component = self.store.component();
            let mut tx = component.begin().await?;
            tx.rows("SELECT pg_advisory_xact_lock(1347572816,0)", &[])
                .await?;
            let count: i64 = tx
                .rows(
                    "SELECT count(*) FROM provisiond.tenants WHERE status='pending'",
                    &[],
                )
                .await?[0]
                .get(0);
            if count >= self.p.n("pending_limit") {
                return Err(Error::new(
                    "rate_limited",
                    "pending enrolment capacity",
                    true,
                    json!({"retry_after_ms":1000}),
                )
                .into());
            }
            tx.rows(
                "INSERT INTO provisiond.tenant_identities(id) VALUES($1) RETURNING id",
                &[&id],
            )
            .await?;
            tx.rows("INSERT INTO provisiond.tenants(id,credential_digest,recovery_digest,status) VALUES($1,$2,$3,'pending') RETURNING id",&[&id,&wire::digest(spending.as_bytes()),&wire::digest(recovery.as_bytes())]).await?;
            tx.rows("INSERT INTO provisiond.enrolments(handle,tenant_id,expires_at) VALUES($1::text::uuid,$2,$3::text::timestamptz) RETURNING handle",&[&handle,&id,&auth::time(expiry)]).await?;
            ensure!(
                component.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
                "enrolment unacknowledged"
            );
            return Ok((
                200,
                json!({"handle":handle,"tenant_id":id,"status":"pending","expires_at":auth::time(expiry),"spending_token":spending,"recovery_credential":recovery,"disclosures":[{"code":"store_both_secrets","text":"Shown once. Store both secrets; keep recovery separate from your everyday agent. Revocation is outside this prototype."},{"code":"no_identity_recovery","text":"Losing both means losing the balance. No identity recovery or refund."},{"code":"signup_expiry","text":"An unfunded signup expires at expires_at. A below-minimum credit does not extend it and remains recoverable only by operator re-attribution, outside this prototype."}]}),
            ));
        }
        if let Some(handle) = path
            .strip_prefix("/v1/enrol/")
            .filter(|_| r.method == "GET")
        {
            self.rate(&source.to_string(), false)?;
            let v=self.store.value("SELECT COALESCE((SELECT jsonb_build_object('status',t.status) FROM provisiond.enrolments e JOIN provisiond.tenants t ON t.id=e.tenant_id WHERE e.handle::text=$1),'null'::jsonb)::text",&[&handle]).await?;
            if v.is_null() {
                return Err(Error::missing().into());
            }
            return Ok((200, v));
        }
        // WIR-34: operator methods have no customer authentication oracle.
        if operator_route(&r.method, path) {
            return Err(Error::missing().into());
        }
        // API-29: bound unauthenticated work by source, before its database
        // lookup. Bearer spelling cannot create fresh buckets; no source persists.
        // Valid principals retain their independent read/write budgets below.
        let source_key = format!("source:{source}");
        self.rate_allowance(&source_key, write, false)?;
        let tenant = match self.authenticate(r).await {
            Ok(tenant) => tenant,
            Err(error) => {
                // Only a credential refusal spends the abuse allowance. Successful
                // callers sharing an address retain their own API-50 budgets.
                if error
                    .downcast_ref::<Error>()
                    .is_some_and(|e| e.0["kind"] == "authentication")
                {
                    self.rate(&source_key, write)?;
                }
                return Err(error);
            }
        };
        let id = tenant["id"].as_str().context("tenant")?;
        if tenant["status"] == "pending"
            && !(path == "/v1/deposits" && r.method == "POST"
                || path.starts_with("/v1/deposits/") && r.method == "GET")
        {
            return Err(Error::new(
                "not_activated",
                "funding required",
                false,
                json!({"activation_minimum_sats":self.p.n("activation_sats")}),
            )
            .into());
        }
        self.rate(id, write)?;
        if r.method == "GET" {
            return Ok((200, self.get(r, id, path).await?));
        }
        let key = r
            .headers
            .get("idempotency-key")
            .map(String::as_str)
            .unwrap_or("");
        if !(8..=200).contains(&key.len())
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
        {
            return Err(Error::new(
                "invalid_request",
                "invalid idempotency key",
                false,
                json!({}),
            )
            .into());
        }
        let v = wire::parse(&r.body)?;
        if path == "/v1/machines" {
            if let Some(account) = v["provider_account"].as_str() {
                self.authorize_account(id, account, false).await?;
            }
            let min = self
                .catalog
                .get(v["provider_account"].as_str().unwrap_or(""))
                .map(pricing::wind_seconds)
                .transpose()?
                .unwrap_or(1);
            wire::validate_create(&v, min)?;
        } else if path == "/v1/deposits" {
            let mut bad = wire::unknown(&v, &["amount_sats"]);
            if !v["amount_sats"].as_i64().is_some_and(|v| v > 0) {
                bad.push("/amount_sats".into());
            }
            wire::least(bad)?;
        } else {
            return Err(unsupported().into());
        }
        let fingerprint = wire::fingerprint(&r.method, &r.target, &v);
        if let Some(response) = self.receipt(id, key, &fingerprint).await? {
            return Ok(response);
        }
        if tenant["status"] == "suspended" {
            return Err(Error::new("suspended", "tenant suspended", false, json!({})).into());
        }
        // API-7 step 5a repeats under tenant serialization to close concurrent replay.
        let component = self.store.component();
        let mut tx = match component.begin().await {
            Ok(tx) => tx,
            Err(error) => {
                #[cfg(feature = "store-test-hooks")]
                if crate::store::test_defect("w4_overload") {
                    self.store
                        .test_overload_entry(correlation, &wire::digest(key.as_bytes()))
                        .await?;
                }
                return Err(error);
            }
        };
        (self.trace)(
            json!({"event":"http_business_begin","correlation_id":correlation,"tenant":id,"key_digest":wire::digest(key.as_bytes())}),
        );
        tx.serialize_tenants(&[id]).await?;
        let prior=tx.rows("SELECT fingerprint FROM provisiond.idempotency_records WHERE scope_kind='tenant' AND scope_id=$1 AND key=$2",&[&id,&key]).await?;
        if !prior.is_empty() {
            tx.rollback().await?;
            return self
                .receipt(id, key, &fingerprint)
                .await?
                .context("replay disappeared");
        }
        let status: String = tx
            .rows("SELECT status FROM provisiond.tenants WHERE id=$1", &[&id])
            .await?[0]
            .get(0);
        if status == "suspended" {
            return Err(Error::new("suspended", "tenant suspended", false, json!({})).into());
        }
        if r.method != "POST" {
            return Err(unsupported().into());
        }
        let response = if path == "/v1/deposits" {
            self.deposit(&mut tx, id, key, &fingerprint, &v, &status)
                .await?
        } else {
            self.create(&mut tx, id, key, &fingerprint, &v, correlation)
                .await?
        };
        ensure!(
            component.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
            "HTTP write unacknowledged; resend same key"
        );
        if path == "/v1/deposits" {
            return self
                .receipt(id, key, &fingerprint)
                .await?
                .context("missing deposit receipt");
        }
        Ok(response)
    }
    async fn authorize_account(&self, tenant: &str, account: &str, healthy: bool) -> Result<()> {
        let v=self.store.value("SELECT to_json(EXISTS(SELECT 1 FROM provisiond.tenant_provider_accounts a JOIN provisiond.provider_account_status s USING(provider_account) WHERE tenant_id=$1 AND provider_account=$2 AND (NOT $3 OR s.status='healthy')))::text",&[&tenant,&account,&healthy]).await?;
        if v != true {
            return Err(Error::missing().into());
        }
        Ok(())
    }
    async fn deposit(
        &self,
        tx: &mut Transaction,
        tenant: &str,
        key: &str,
        fingerprint: &str,
        v: &Value,
        status: &str,
    ) -> Result<(u16, Value)> {
        let halted: bool = tx
            .rows(
                "SELECT solvency_halt FROM provisiond.funding_health WHERE singleton",
                &[],
            )
            .await?[0]
            .get(0);
        if halted {
            return Err(gate("solvency").into());
        }
        let mut expiry = auth::now() + self.p.n("deposit_seconds");
        if status == "pending" {
            let remaining:i64=tx.rows("SELECT floor(extract(epoch FROM expires_at))::bigint FROM provisiond.enrolments WHERE tenant_id=$1",&[&tenant]).await?[0].get(0);
            expiry = expiry.min(remaining - self.p.n("finality_seconds"));
        }
        if expiry <= auth::now() {
            return Err(Error::conflict("signup_window_closed").into());
        }
        let id = uuid::Uuid::new_v4().to_string();
        let body = funding::deposit_body(
            &id,
            v["amount_sats"].as_i64().context("amount")?,
            expiry,
            &self.p,
        );
        let invoice = body["lightning"]["invoice"].as_str().context("invoice")?;
        let address = body["onchain"]["address"].as_str().context("address")?;
        tx.rows("INSERT INTO provisiond.deposits(id,tenant_id,requested_sats,payment_hash,address,derivation_index,idempotency_key,expires_at,invoice) VALUES($1::text::uuid,$2,$3,$4,$5,nextval('provisiond.deposit_derivation_index'),$6,$7::text::timestamptz,$8) RETURNING id",&[&id,&tenant,&v["amount_sats"].as_i64(),&wire::digest(id.as_bytes()),&address,&key,&auth::time(expiry),&invoice]).await?;
        tx.rows("INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at) VALUES('tenant',$1,$2,$3,'deposit',$4::text::uuid,200,$5::text::jsonb,transaction_timestamp()+$6::bigint*interval '1 second') RETURNING resource_id",&[&tenant,&key,&fingerprint,&id,&body.to_string(),&self.p.n("receipt_seconds")]).await?;
        Ok((200, body))
    }
}
impl State {
    async fn ceiling(
        &self,
        tx: &mut Transaction,
        tenant: &str,
        name: &str,
        amount: i64,
        limit: i64,
        write: bool,
    ) -> Result<()> {
        let interval = self.p.n("ceiling_seconds");
        let start = auth::now() / interval * interval;
        let used:i64=tx.rows("SELECT COALESCE((SELECT used FROM provisiond.ceiling_counters WHERE principal=$1 AND ceiling=$2 AND interval_start=$3),0)",&[&tenant,&name,&start]).await?[0].get(0);
        // API-7 step 5c: the zero-amount availability probe must refuse an
        // exhausted allowance before capability/tail reads price the new spend.
        let refused =
            (amount == 0 && used >= limit) || used.checked_add(amount).is_none_or(|n| n > limit);
        let error = || {
            Error::new(
                "ceiling_exceeded",
                "principal allowance exhausted",
                true,
                json!({"ceiling":name,"limit":limit,"interval_seconds":interval,"retry_after_ms":(start+interval-auth::now()).max(1)*1000}),
            )
        };
        if refused {
            return Err(error().into());
        }
        if write {
            let rows=tx.rows("INSERT INTO provisiond.ceiling_counters(principal,ceiling,interval_start,used) VALUES($1,$2,$3,$4) ON CONFLICT(principal,ceiling,interval_start) DO UPDATE SET used=provisiond.ceiling_counters.used+EXCLUDED.used WHERE provisiond.ceiling_counters.used+EXCLUDED.used<=$5 RETURNING used",&[&tenant,&name,&start,&amount,&limit]).await?;
            if rows.is_empty() {
                return Err(error().into());
            }
        }
        Ok(())
    }
    async fn create(
        &self,
        tx: &mut Transaction,
        tenant: &str,
        key: &str,
        fingerprint: &str,
        v: &Value,
        correlation: &str,
    ) -> Result<(u16, Value)> {
        self.ceiling(
            tx,
            tenant,
            "machines_created",
            1,
            self.p.n("created_limit"),
            false,
        )
        .await?;
        self.ceiling(tx, tenant, "spend", 0, self.p.n("spend_limit"), false)
            .await?;
        let account = v["provider_account"].as_str().context("account")?;
        let offer_id = v["offer"].as_str().context("offer")?;
        let descriptor = self
            .catalog
            .get(account)
            .context("missing configured descriptor")?;
        let snapshot:Value=serde_json::from_str(&tx.rows("SELECT jsonb_build_object('status',s.status,'offers',COALESCE(l.offers,'[]'::jsonb),'stale',l.read_at IS NULL OR l.read_at<transaction_timestamp()-$2::bigint*interval '1 second','solvency',(SELECT solvency_halt FROM provisiond.funding_health WHERE singleton),'rate',r.value,'rate_available',COALESCE(r.available,false),'closed_ms',COALESCE((SELECT greatest(0,ceil(extract(epoch FROM closed_until-transaction_timestamp())*1000))::bigint FROM provisiond.channel_closures WHERE provider_account=$1 AND channel=$3),0),'ledger_valid',(SELECT ledger_valid FROM provisiond.spending_health WHERE singleton))::text FROM provisiond.provider_account_status s LEFT JOIN provisiond.offer_listings l USING(provider_account) LEFT JOIN provisiond.rates r ON r.currency=$4 WHERE s.provider_account=$1",&[&account,&self.p.n("listing_age_seconds"),&descriptor["channel"].as_str(),&descriptor["currency"].as_str()]).await?[0].get::<_,String>(0))?;
        let offer = snapshot["offers"]
            .as_array()
            .and_then(|a| a.iter().find(|o| o["id"] == offer_id));
        if let Some(o) = offer {
            let cap = pricing::capability(o["kind"].as_str().unwrap_or(""));
            if !pricing::has(descriptor, cap) {
                return Err(Error::new(
                    "unsupported",
                    "ordering capability unavailable",
                    false,
                    json!({"provider_account":account,"capability":cap}),
                )
                .into());
            }
        }
        let mut refusals = Vec::new();
        let healthy = snapshot["status"] == "healthy";
        let sellable = offer.is_some_and(|o| pricing::sellable(o, descriptor));
        let rate = snapshot["rate_available"] == true;
        if !healthy {
            refusals.push(Error::new(
                "conflict",
                "provider account unavailable",
                snapshot["status"] != "terminated",
                json!({"reason":"state"}),
            ));
        }
        if !sellable {
            refusals.push(Error::new(
                "invalid_request",
                "offer not sellable",
                false,
                json!({"reason":"offer_not_sellable"}),
            ));
        }
        if snapshot["closed_ms"].as_i64().unwrap_or(0) > 0 {
            refusals.push(Error::new(
                "rate_limited",
                "ordering channel closed",
                true,
                json!({"retry_after_ms":snapshot["closed_ms"]}),
            ));
        }
        if snapshot["solvency"] == true || snapshot["ledger_valid"] != true {
            refusals.push(gate("solvency"));
        }
        if !rate {
            refusals.push(gate("rate_unavailable"));
        }
        let cap = v["max_commitment_sats"].as_i64();
        let mut unchecked = Vec::new();
        for check in ["commitment_cap", "balance"] {
            if check == "commitment_cap" && cap.is_none() {
                continue;
            }
            for (applies, because) in [
                (!healthy, "provider_account_not_healthy"),
                (!sellable, "offer_not_sellable"),
                (!rate, "rate_unavailable"),
            ] {
                if applies {
                    unchecked.push(json!({"check":check,"because":because}));
                }
            }
        }
        let priced = if healthy && sellable && rate {
            let o = offer.context("offer")?;
            let price = pricing::calculate(
                o,
                descriptor,
                &snapshot["rate"],
                v["runway_seconds"].as_i64().context("runway")?,
            )?;
            if let Some(cap) = cap {
                if cap < price.reserve {
                    refusals.push(Error::new("invalid_request","commitment exceeds supplied cap",false,json!({"field":"/max_commitment_sats","max_commitment_sats":cap,"required_sats":price.reserve})));
                }
            }
            let (_, available) = crate::ledger::balance(tx, tenant).await?;
            if available < price.reserve {
                refusals.push(Error::new(
                    "insufficient_balance",
                    "available balance cannot fund commitment",
                    false,
                    json!({"available_sats":available,"required_sats":price.reserve}),
                ));
            }
            Some(price)
        } else {
            None
        };
        if !refusals.is_empty() {
            return Err(tail_error(refusals, unchecked).into());
        }
        if snapshot["stale"] == true {
            return Err(
                Error::new("provider", "stored offer listing too old", true, json!({})).into(),
            );
        }
        let price = priced.context("price")?;
        let o = offer.context("offer")?;
        self.ceiling(
            tx,
            tenant,
            "spend",
            price.reserve,
            self.p.n("spend_limit"),
            false,
        )
        .await?;
        // API-63: lock, retain refusing status, then healthy conditional write before opening money.
        let locked:String=tx.rows("SELECT status FROM provisiond.provider_account_status WHERE provider_account=$1 FOR UPDATE",&[&account]).await?[0].get(0);
        let guarded=tx.rows("UPDATE provisiond.provider_account_status SET status=status WHERE provider_account=$1 AND status='healthy' RETURNING status",&[&account]).await?;
        if guarded.is_empty() {
            return Err(tail_error(
                vec![Error::new(
                    "conflict",
                    "provider account unavailable",
                    locked != "terminated",
                    json!({"reason":"state"}),
                )],
                vec![],
            )
            .into());
        }
        let declared = o["cancellation_bound"]
            .as_str()
            .or_else(|| descriptor["cancellation_bound"].as_str())
            .map(|s| format!("{s}T00:00:00Z"));
        let request = CreateRequest {
            tenant: tenant.into(),
            scope_kind: "tenant".into(),
            scope_id: tenant.into(),
            key: key.into(),
            fingerprint: fingerprint.into(),
            provider_account: account.into(),
            channel: descriptor["channel"].as_str().context("channel")?.into(),
            offer: offer_id.into(),
            offer_is_resource: descriptor["offer_is_resource"] == true,
            reserved_sats: price.reserve,
            setup: price.setup.clone(),
            first_unit: price.unit.clone(),
            offer_snapshot: json!({"offer_id":offer_id,"machine_kind":o["kind"],"install_strategies":o["install_strategies"],"recurring_price":o["recurring_price"],"currency":o["currency"],"recurring_period":"hour","declared_cancellation_bound":declared,"billing_unit_seconds":o["billing_unit"]["seconds"],"accepted_terms":{"offer":o,"rate":snapshot["rate"],"pricing_descriptor":descriptor,"wind_sats":price.wind_sats,"customer_num":price.per_second_num,"customer_den":price.per_second_den,"channel":descriptor["channel"]}}),
        };
        let id = self.queue.enqueue(tx, &request).await.map_err(|e| {
            if e.to_string().contains("resource offer already") {
                anyhow::Error::from(Error::conflict("state"))
            } else {
                e
            }
        })?;
        tx.rows("UPDATE provisiond.operations SET request=$2::text::jsonb,correlation_id=$3,admitted_sats=$4 WHERE id=$1::text::uuid RETURNING id",&[&id,&v.to_string(),&correlation,&price.reserve]).await?;
        self.ceiling(
            tx,
            tenant,
            "machines_created",
            1,
            self.p.n("created_limit"),
            true,
        )
        .await?;
        self.ceiling(
            tx,
            tenant,
            "spend",
            price.reserve,
            self.p.n("spend_limit"),
            true,
        )
        .await?;
        let body: Value = serde_json::from_str(
            &tx.rows(
                "SELECT body::text FROM provisiond.operation_views WHERE id=$1::text::uuid",
                &[&id],
            )
            .await?[0]
                .get::<_, String>(0),
        )?;
        tx.rows("UPDATE provisiond.idempotency_records SET response_body=$3::text::jsonb,expires_at=transaction_timestamp()+$4::bigint*interval '1 second' WHERE scope_kind='tenant' AND scope_id=$1 AND key=$2 RETURNING resource_id",&[&tenant,&key,&body.to_string(),&self.p.n("receipt_seconds")]).await?;
        Ok((202, body))
    }
}
fn tail_error(refusals: Vec<Error>, unchecked: Vec<Value>) -> Error {
    let mut e = refusals[0].clone();
    e.0["retryable"] = json!(refusals.iter().all(|e| e.0["retryable"] == true));
    e.0["details"]["refusals"] = json!(refusals.into_iter().map(|e| e.0).collect::<Vec<_>>());
    e.0["details"]["not_checked"] = json!(unchecked);
    e
}
impl State {
    async fn get(&self, r: &Request, tenant: &str, path: &str) -> Result<Value> {
        if let Some(id) = path.strip_prefix("/v1/deposits/") {
            let v=self.store.destination_value("SELECT COALESCE((SELECT r.response_body || jsonb_build_object('credits',COALESCE((SELECT jsonb_agg(jsonb_build_object('rail',p.rail,'amount_sats',p.amount_sats,'credited_at',provisiond.wire_time(p.created_at)) ORDER BY p.created_at,p.id) FROM provisiond.payments p WHERE p.deposit_id=d.id),'[]'::jsonb),'credited_sats',COALESCE((SELECT sum(amount_sats) FROM provisiond.payments WHERE deposit_id=d.id),0),'expired',d.expires_at<=clock_timestamp(),'gate',CASE WHEN f.solvency_halt THEN 'solvency' ELSE NULL END,'lightning',(r.response_body->'lightning')||jsonb_build_object('cancelled',d.lightning_cancelled)) FROM provisiond.deposits d JOIN provisiond.idempotency_records r ON r.resource_id=d.id AND r.resource_kind='deposit' CROSS JOIN provisiond.funding_health f WHERE d.id::text=$2 AND d.tenant_id=$1),'null'::jsonb)::text",&[&tenant,&id]).await?;
            if v.is_null() {
                return Err(Error::missing().into());
            }
            return Ok(v);
        }
        if let Some(id) = path.strip_prefix("/v1/operations/") {
            return self.operation(tenant, id).await;
        }
        if let Some(id) = path.strip_prefix("/v1/machines/") {
            let v=self.store.value("SELECT COALESCE((SELECT body FROM provisiond.machine_views WHERE tenant_id=$1 AND id::text=$2),'null'::jsonb)::text",&[&tenant,&id]).await?;
            if v.is_null() {
                return Err(Error::missing().into());
            }
            return Ok(v);
        }
        if path == "/v1/providers" {
            return self.store.value("SELECT jsonb_build_object('providers',COALESCE(jsonb_agg(jsonb_build_object('account',a.provider_account,'kind',p.descriptor->'kind','capabilities',p.descriptor->'capabilities') ORDER BY a.provider_account),'[]'::jsonb))::text FROM provisiond.tenant_provider_accounts a JOIN provisiond.provider_account_status s USING(provider_account) JOIN provisiond.provider_accounts p ON p.account=a.provider_account WHERE a.tenant_id=$1 AND s.status='healthy'",&[&tenant]).await;
        }
        if let Some(account) = path
            .strip_prefix("/v1/providers/")
            .and_then(|s| s.strip_suffix("/offers"))
        {
            self.authorize_account(tenant, account, true).await?;
            let d = self.catalog.get(account).context("descriptor")?;
            if !pricing::has(d, "list_offers") {
                return Err(Error::new(
                    "unsupported",
                    "listing unavailable",
                    false,
                    json!({"provider_account":account,"capability":"list_offers"}),
                )
                .into());
            }
            let v=self.store.value("SELECT jsonb_build_object('offers',COALESCE(l.offers,'[]'::jsonb),'read_at',provisiond.wire_time(l.read_at),'rate',r.value,'rate_available',r.available)::text FROM provisiond.provider_accounts a LEFT JOIN provisiond.offer_listings l ON l.provider_account=a.account LEFT JOIN provisiond.rates r ON r.currency=$2 WHERE a.account=$1",&[&account,&d["currency"].as_str()]).await?;
            if v["rate_available"] != true {
                return Err(gate("rate_unavailable").into());
            }
            let mut out = Vec::new();
            for o in v["offers"].as_array().context("listing")? {
                if !pricing::sellable(o, d) {
                    continue;
                }
                let min = pricing::wind_seconds(d)?;
                let price = pricing::calculate(o, d, &v["rate"], min)?;
                out.push(json!({"id":o["id"],"name":o["name"],"kind":o["kind"],"regions":o["regions"],"recurring_price":{"amount_sats":price.recurring_sats,"period":"hour"},"setup_fee_sats":price.setup["sats"],"first_unit_sats":price.unit.as_ref().map_or(json!(0),|u|u["sats"].clone()),"billing_unit_seconds":o["billing_unit"]["seconds"],"min_runway_seconds":min,"quoted_at":v["read_at"],"binding":false,"max_rate_outage_seconds":d["max_rate_outage_seconds"],"traffic_bound":"unlimited","install_strategies":o["install_strategies"],"max_image_bytes":null,"guest_requirements":null}));
            }
            return Ok(json!({"offers":out}));
        }
        if path == "/v1/balance" {
            let mut v=self.store.value("SELECT jsonb_build_object('balance_sats',COALESCE((SELECT balance_after FROM provisiond.ledger_entries WHERE tenant_id=$1 ORDER BY seq DESC LIMIT 1),0),'committed_sats',COALESCE((SELECT sum(reserved_sats) FROM provisiond.commitments WHERE tenant_id=$1 AND state='open'),0),'earliest_runway_until',(SELECT provisiond.wire_time(min(runway_until)) FROM provisiond.machines WHERE tenant_id=$1 AND state IN ('running','stopped','cancellation_scheduled')))::text",&[&tenant]).await?;
            v["available_sats"] = json!(
                v["balance_sats"].as_i64().context("balance")?
                    - v["committed_sats"].as_i64().context("commitment")?
            );
            if query(&r.target, "commitments").as_deref() == Some("true") {
                let page = self.page(r, tenant, "commitments").await?;
                v["commitments"] = page["items"].clone();
                v["next_cursor"] = page["next_cursor"].clone();
            }
            return Ok(v);
        }
        if path == "/v1/machines" || path == "/v1/operations" {
            let collection = if path.ends_with("machines") {
                "machines"
            } else {
                "operations"
            };
            let p = self.page(r, tenant, collection).await?;
            let mut v = json!({collection:p["items"],"next_cursor":p["next_cursor"]});
            if collection == "operations" {
                v["poll_after_ms"] = json!(1000);
            }
            return Ok(v);
        }
        Err(unsupported().into())
    }
    async fn page(&self, r: &Request, tenant: &str, collection: &str) -> Result<Value> {
        let limit = query(&r.target, "limit")
            .map(|s| s.parse::<i64>().map_err(|_| Error::field("/limit")))
            .transpose()?
            .unwrap_or(50)
            .clamp(1, 200);
        let filter = query(
            &r.target,
            if collection == "machines" {
                "state"
            } else {
                "status"
            },
        )
        .unwrap_or_default();
        let filter = if collection == "commitments" {
            String::new()
        } else {
            filter
        };
        let operation_query = |key| {
            if collection == "operations" {
                query(&r.target, key).unwrap_or_default()
            } else {
                String::new()
            }
        };
        let requested = operation_query("requested_by");
        let reason = operation_query("system_reason");
        let terminal = operation_query("terminal");
        for (value, allowed, field) in [
            (
                &filter,
                if collection == "machines" {
                    vec![
                        "requested",
                        "provisioning",
                        "running",
                        "stopped",
                        "rescue",
                        "rebuilding",
                        "deleting",
                        "cancellation_scheduled",
                        "deleted",
                        "failed",
                        "unknown",
                    ]
                } else {
                    vec![
                        "queued",
                        "running",
                        "succeeded",
                        "failed",
                        "needs_reconciliation",
                    ]
                },
                "status",
            ),
            (
                &requested,
                vec!["caller", "system", "operator"],
                "requested_by",
            ),
            (
                &reason,
                vec![
                    "exhausted",
                    "late_attach_cleanup",
                    "tenant_suspended",
                    "rate_outage_bound",
                ],
                "system_reason",
            ),
        ] {
            if !value.is_empty() && !value.split(',').all(|s| allowed.contains(&s)) {
                return Err(Error::field(&wire::pointer(field)).into());
            }
        }
        if !terminal.is_empty() && !matches!(terminal.as_str(), "true" | "false") {
            return Err(Error::field("/terminal").into());
        }
        let mut cursor_time = String::new();
        let mut cursor_id = String::new();
        if let Some(cursor) = query(&r.target, "cursor") {
            let data = self
                .admission
                .open(&cursor)
                .ok_or_else(|| Error::field("/cursor"))?;
            let value: Value = serde_json::from_str(&data).map_err(|_| Error::field("/cursor"))?;
            if value["tenant"] != tenant
                || value["collection"] != collection
                || value["filter"] != filter
                || value["requested"] != requested
                || value["reason"] != reason
                || value["terminal"] != terminal
            {
                return Err(Error::field("/cursor").into());
            }
            cursor_time = value["time"]
                .as_str()
                .ok_or_else(|| Error::field("/cursor"))?
                .into();
            cursor_id = value["id"]
                .as_str()
                .ok_or_else(|| Error::field("/cursor"))?
                .into();
        }
        let table = match collection {
            "machines" => "provisiond.machine_views",
            "operations" => "provisiond.operation_views",
            _ => {
                "(SELECT id,tenant_id,opened_at AS created_at,jsonb_build_object('machine_id',machine_id,'operation_id',operation_id,'reserved_sats',reserved_sats,'runway_until',(SELECT provisiond.wire_time(runway_until) FROM provisiond.machines WHERE id=c.machine_id)) AS body FROM provisiond.commitments c WHERE state='open')"
            }
        };
        let status_field = if collection == "machines" {
            "state"
        } else {
            "status"
        };
        let sql = format!(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object('body',body,'time',created_at::text,'id',id) ORDER BY created_at DESC,id),'[]'::jsonb)::text FROM (SELECT * FROM {table} v WHERE tenant_id=$1 AND ($2='' OR body->>'{status_field}'=ANY(string_to_array($2,','))) AND ($3='' OR body->>'requested_by'=ANY(string_to_array($3,','))) AND ($4='' OR body->>'system_reason'=ANY(string_to_array($4,','))) AND ($5='' OR (body->>'terminal')::boolean=$5::boolean) AND ($6='' OR created_at<NULLIF($6,'')::timestamptz OR (created_at=NULLIF($6,'')::timestamptz AND id::text>$7)) ORDER BY created_at DESC,id LIMIT $8) page"
        );
        let rows = self
            .store
            .value(
                &sql,
                &[
                    &tenant,
                    &filter,
                    &requested,
                    &reason,
                    &terminal,
                    &cursor_time,
                    &cursor_id,
                    &(limit + 1),
                ],
            )
            .await?;
        let rows = rows.as_array().context("page")?;
        let selected: Vec<_> = rows.iter().take(limit as usize).collect();
        let next = if rows.len() > limit as usize {
            let last = selected.last().context("last row")?;
            Some(self.admission.seal(&json!({"tenant":tenant,"collection":collection,"filter":filter,"requested":requested,"reason":reason,"terminal":terminal,"time":last["time"],"id":last["id"]}).to_string()))
        } else {
            None
        };
        Ok(
            json!({"items":selected.iter().map(|v|v["body"].clone()).collect::<Vec<_>>(),"next_cursor":next}),
        )
    }
}
fn query(target: &str, key: &str) -> Option<String> {
    target.split_once('?')?.1.split('&').find_map(|p| {
        let (k, v) = p.split_once('=').unwrap_or((p, ""));
        (decode_query(k).as_deref() == Some(key))
            .then(|| decode_query(v))
            .flatten()
    })
}
// WIR-2/3/5a: decode query semantics without changing the fingerprint's raw target.
fn decode_query(s: &str) -> Option<String> {
    let mut value = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        value.push(match b {
            b'%' => {
                let a = char::from(bytes.next()?).to_digit(16)?;
                let b = char::from(bytes.next()?).to_digit(16)?;
                (a * 16 + b) as u8
            }
            b'+' => b' ',
            other => other,
        });
    }
    String::from_utf8(value).ok()
}
pub async fn serve(store: Store, trace: Trace) -> Result<()> {
    store.compatible_schema().await?;
    let store = Arc::new(store);
    let p = Parameters::load()?;
    // OVR-10b, STO-13: validate the whole local service configuration before
    // binding or reporting readiness. --check-schema never enters this path.
    let address = http::loopback(
        &std::env::var("PROVISIOND_API_LISTEN").context("missing PROVISIOND_API_LISTEN")?,
    )
    .context("PROVISIOND_API_LISTEN must be a loopback socket address with a positive port")?;
    let rail_address = http::loopback(
        &std::env::var("PROVISIOND_RAIL_SIMULATOR").context("missing PROVISIOND_RAIL_SIMULATOR")?,
    )
    .context("PROVISIOND_RAIL_SIMULATOR must be a loopback socket address with a positive port")?;
    let descriptors=store.value("SELECT COALESCE(jsonb_object_agg(account,descriptor),'{}'::jsonb)::text FROM provisiond.provider_accounts",&[]).await?;
    for descriptor in descriptors
        .as_object()
        .context("configured descriptors")?
        .values()
    {
        pricing::validate_descriptor(descriptor)?;
    }
    let catalog = descriptors
        .as_object()
        .context("catalog")?
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let admission = Admission::new(
        p.n("admission_delay_ms") as u64,
        p.n("admission_validity_seconds"),
        p.n("admission_capacity") as usize,
    )?;
    let state = Arc::new(State {
        store: store.clone(),
        p: p.clone(),
        admission,
        catalog,
        queue: Box::new(Queue),
        sources: Mutex::new(BTreeMap::new()),
        rates: Mutex::new(BTreeMap::new()),
        trace: trace.clone(),
    });
    let listener = TcpListener::bind(address).await?;
    let maintenance_store = store.clone();
    let maintenance_parameters = p.clone();
    let mut maintenance =
        tokio::spawn(async move { housekeeping(maintenance_store, maintenance_parameters).await });
    let mut funding = tokio::spawn(funding::watch(store, p, rail_address));
    let mut funding_observed = false;
    let mut maintenance_observed = false;
    let mut requests = JoinSet::new();
    trace(
        json!({"event":"api_listening","address":address.to_string(),"parameters":state.p.value}),
    );
    let result: Result<()> = async {
        loop {
            tokio::select! {
                result = &mut funding => {
                    funding_observed = true;
                    return task_stopped("funding watcher", result);
                }
                result = &mut maintenance => {
                    maintenance_observed = true;
                    return task_stopped("housekeeping", result);
                }
                Some(result) = requests.join_next(), if !requests.is_empty() => {
                    ensure!(result.is_ok(), "API request task panicked");
                }
                accepted = listener.accept() => {
                    let (socket, peer) = accepted?;
                    let state = state.clone();
                    requests.spawn(async move {
                        // Caller disconnects carry no secret diagnostic.
                        let _ = connection(socket, peer, state).await;
                    });
                }
            }
        }
    }
    .await;
    drop(listener);
    trace(json!({"event":"api_stopping","pending_requests":requests.len()}));
    // STO-7/55, OPS-49: cancellation drops only caller work. The existing
    // Process still owns every sent COMMIT, including one whose waiter ends here.
    // Reap both periodic owners and every handler before releasing State/Store.
    for (name, task, observed) in [
        ("funding watcher", &mut funding, funding_observed),
        ("housekeeping", &mut maintenance, maintenance_observed),
    ] {
        if !observed {
            task.abort();
            let result = task.await;
            if !matches!(&result, Err(error) if error.is_cancelled()) {
                if let Err(error) = task_stopped(name, result) {
                    trace(
                        json!({"event":"api_background_task_failed","component":name,"error":format!("{error:#}")}),
                    );
                }
            }
        }
    }
    let drained = tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(result) = requests.join_next().await {
            if result.is_err() {
                trace(json!({"event":"api_request_task_failed"}));
            }
        }
    })
    .await
    .is_ok();
    requests.abort_all();
    while requests.join_next().await.is_some() {}
    trace(json!({"event":"api_tasks_finished","requests_drained":drained}));
    result
}
fn task_stopped(
    name: &str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(Err(error)) => Err(error).with_context(|| format!("API {name} failed")),
        Ok(Ok(())) => anyhow::bail!("API {name} stopped unexpectedly"),
        Err(_) => anyhow::bail!("API {name} panicked or was cancelled"),
    }
}
async fn connection(mut socket: TcpStream, peer: SocketAddr, state: Arc<State>) -> Result<()> {
    let r = tokio::time::timeout(Duration::from_secs(5), http::read(&mut socket)).await??;
    let correlation = r
        .headers
        .get("x-correlation-id")
        .filter(|v| v.len() <= 128 && v.bytes().all(|b| b.is_ascii_graphic()))
        .cloned()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let mut headers = vec![
        ("X-Correlation-Id".into(), correlation.clone()),
        (
            "Access-Control-Expose-Headers".into(),
            "Retry-After, X-Correlation-Id".into(),
        ),
        ("Access-Control-Allow-Origin".into(), "*".into()),
        ("Vary".into(), "Origin".into()),
    ];
    let (status, body) = if r.method == "OPTIONS" && r.target.starts_with("/v1") {
        headers.extend([
            (
                "Access-Control-Allow-Methods".into(),
                "GET, POST, OPTIONS".into(),
            ),
            (
                "Access-Control-Allow-Headers".into(),
                "Authorization, Content-Type, Idempotency-Key, X-Correlation-Id".into(),
            ),
            ("Access-Control-Max-Age".into(), "3600".into()),
        ]);
        (200, json!({}))
    } else {
        // WIR-49: dropping the held caller cancels issuance and frees its source slot.
        let result = if r.target.split('?').next() == Some("/v1/enrol/token") && r.method == "POST"
        {
            let mut peek = [0];
            tokio::select! {result=state.handle(&r,peer.ip(),&correlation)=>result,_=socket.peek(&mut peek)=>{(state.trace)(json!({"event":"token_request_abandoned","correlation_id":correlation}));return Ok(());}}
        } else {
            state.handle(&r, peer.ip(), &correlation).await
        };
        match result {
            Ok(v) => v,
            Err(e) => {
                let e = as_error(e);
                (
                    e.status(),
                    json!({"error":e.0,"correlation_id":correlation}),
                )
            }
        }
    };
    let pacing = body["error"]["details"]["retry_after_ms"]
        .as_u64()
        .or_else(|| body["poll_after_ms"].as_u64());
    if let Some(ms) = pacing {
        headers.push(("Retry-After".into(), ms.div_ceil(1000).to_string()));
    }
    (state.trace)(
        json!({"event":"http_response","method":r.method,"path":r.target.split('?').next(),"status":status,"correlation_id":correlation}),
    );
    http::respond(&mut socket, status, &body, &headers).await
}

// API-34/42, STO-14/25: only expired pending credentials and settled request records.
async fn housekeeping(store: Arc<Store>, p: Parameters) -> Result<()> {
    let component = store.component();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let work=store.value("SELECT to_json(EXISTS(SELECT 1 FROM provisiond.enrolments e JOIN provisiond.tenants t ON t.id=e.tenant_id WHERE t.status='pending' AND e.expires_at<clock_timestamp()) OR EXISTS(SELECT 1 FROM provisiond.operations WHERE status IN ('succeeded','failed') AND updated_at<clock_timestamp()-$1::bigint*interval '1 second'))::text",&[&p.n("retention_seconds")]).await?;
        if work != true {
            continue;
        }
        let mut tx = component.begin().await?;
        tx.rows("SELECT pg_advisory_xact_lock(1347572816,0)", &[])
            .await?;
        let expired=tx.rows("SELECT e.tenant_id FROM provisiond.enrolments e JOIN provisiond.tenants t ON t.id=e.tenant_id WHERE t.status='pending' AND e.expires_at<transaction_timestamp() AND NOT EXISTS(SELECT 1 FROM provisiond.deposits d WHERE d.tenant_id=t.id AND d.expires_at+$1::bigint*interval '1 second'>transaction_timestamp()) ORDER BY e.tenant_id",&[&p.n("finality_seconds")]).await?;
        for row in expired {
            let tenant: String = row.get(0);
            tx.serialize_tenants(&[&tenant]).await?;
            tx.rows(
                "DELETE FROM provisiond.tenants WHERE id=$1 AND status='pending' RETURNING id",
                &[&tenant],
            )
            .await?;
            tx.rows("DELETE FROM provisiond.enrolments e WHERE tenant_id=$1 AND NOT EXISTS(SELECT 1 FROM provisiond.tenants t WHERE t.id=e.tenant_id) RETURNING handle",&[&tenant]).await?;
        }
        tx.rows("DELETE FROM provisiond.operations WHERE status IN ('succeeded','failed') AND updated_at<transaction_timestamp()-$1::bigint*interval '1 second' RETURNING id",&[&p.n("retention_seconds")]).await?;
        ensure!(
            component.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
            "housekeeping unacknowledged"
        );
    }
}
