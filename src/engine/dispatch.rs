//! OPS-43 ordered first-refusal dispatch and OPS-27 independent verification.
use super::*;
use crate::{
    pricing,
    provider::Client,
    wire::{self, Error},
};
pub struct Dispatcher {
    pub(super) client: Client,
    pub(super) catalog: BTreeMap<String, Value>,
}
impl Dispatcher {
    pub async fn load(engine: &Arc<Engine>) -> Result<Self> {
        let v=engine.component().value("SELECT COALESCE(jsonb_object_agg(account,descriptor),'{}'::jsonb)::text FROM provisiond.provider_accounts",&[]).await?;
        for descriptor in v.as_object().context("configured descriptors")?.values() {
            pricing::validate_descriptor(descriptor)?;
        }
        let catalog = v
            .as_object()
            .context("configured descriptors")?
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(Self {
            client: Client::configured()?,
            catalog,
        })
    }
}
impl Engine {
    /// OPS-49: each preflight SELECT is a complete autocommit store unit.
    /// Release its session before backoff; never repeat any provider I/O here.
    /// OPS-21: cancelling a read is safe (unlike a sent COMMIT) and stops dispatch.
    pub(super) async fn preflight_value(
        &self,
        c: &Claim,
        component: &Component,
        e: &Execution,
        site: &str,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Value> {
        let mut spent = Duration::ZERO;
        loop {
            if e.stopped.load(Ordering::Acquire) {
                return Err(ExecutionStopped.into());
            }
            let started = Instant::now();
            let result = tokio::select! {
                biased;
                _ = async {while !e.stopped.load(Ordering::Acquire) {tokio::time::sleep(Duration::from_millis(1)).await;}} => return Err(ExecutionStopped.into()),
                result = component.value(sql, params) => result,
            };
            if e.stopped.load(Ordering::Acquire) {
                return Err(ExecutionStopped.into());
            }
            let error = match result {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            let code = error
                .downcast_ref::<tokio_postgres::Error>()
                .and_then(|error| error.code())
                .map(|code| code.code());
            if error.downcast_ref::<FatalEngine>().is_some()
                || code.is_some_and(|code| code.starts_with("23"))
            {
                return Err(error);
            }
            let serialization = code == Some("40001");
            if !serialization {
                spent += started.elapsed();
            }
            component.trace(json!({"event":"preflight_read_retry","operation":c.id,"claim_number":c.number,"site":site,"spent_ms":spent.as_millis(),"serialization":serialization,"external_call_repeated":false}));
            if spent < self.retry_bound && !serialization {
                let backoff = Instant::now();
                tokio::time::sleep(Duration::from_millis(10)).await;
                spent += backoff.elapsed();
            }
            if e.stopped.load(Ordering::Acquire) {
                return Err(ExecutionStopped.into());
            }
            if spent >= self.retry_bound {
                component.trace(json!({"event":"preflight_read_exhausted","operation":c.id,"claim_number":c.number,"site":site,"spent_ms":spent.as_millis(),"bound_ms":self.retry_bound.as_millis()}));
                return self.fatal("preflight store retry exhausted; no worker classification");
            }
        }
    }
    pub async fn dispatch_next(self: &Arc<Self>, dispatcher: &Arc<Dispatcher>) -> Result<()> {
        if self.worker_slots.available_permits() == 0 {
            return Ok(());
        }
        if let Some(c) = self.claim().await? {
            if !matches!(c.kind.as_str(), "create" | "delete") {
                self.settle(
                    &c,
                    &Outcome::Settled {
                        status: "failed".into(),
                        result: Value::Null,
                        error: Error::new(
                            "unsupported",
                            "operation outside local dispatcher",
                            false,
                            json!({"provider_account":null,"capability":c.kind}),
                        )
                        .0,
                    },
                )
                .await?;
                return Ok(());
            }
            self.execute_order(&c.id, dispatcher.clone()).await?;
        }
        Ok(())
    }
    async fn execute_order(self: &Arc<Self>, id: &str, dispatcher: Arc<Dispatcher>) -> Result<()> {
        self.observe_lock().await?;
        self.database.ensure_new_work()?;
        let execution = Arc::new(Execution::default());
        let (claim, component, outcome, slot) = {
            let mut workers = self.workers.lock().expect("workers");
            let w = workers.get_mut(id).context("worker")?;
            ensure!(
                w.execution.is_none() && w.record.is_none(),
                "worker already executing"
            );
            w.execution = Some(execution.clone());
            (
                w.claim.clone(),
                w.component.clone(),
                w.outcome.clone(),
                w.slot.clone(),
            )
        };
        let engine = self.clone();
        self.process.submit(async move{
   let _execution_slot=slot;
   let returned=if claim.kind=="delete" {engine.run_delete(&claim,&component,&execution,&dispatcher).await} else {engine.run_order(&claim,&component,&execution,&dispatcher).await};
   match returned{
    Ok(Some(value))=>{*outcome.lock().expect("outcome")=Some(value);component.trace(json!({"event":"execute_returned","operation":claim.id,"claim_number":claim.number}));}
    Ok(None)=>{execution.done.store(true,Ordering::Release);engine.retire(&claim.id,claim.number);return;}
    Err(error) if error.downcast_ref::<ExecutionStopped>().is_some()=>{},
    Err(error) if error.downcast_ref::<FatalEngine>().is_some()=>{engine.database.fatal("dispatch store supervision failed");},
    Err(error)=>{
     let mut error=error.downcast_ref::<Error>().cloned().unwrap_or_else(||Error::new("internal","worker could not complete",true,json!({"accepted":null})));
     let dispatched=execution.dispatched.load(Ordering::Acquire);
     // PRV-5/OPS-11: the driver retains upstream HTTP status in details.status.
     let uncertain_kind=matches!(error.0["kind"].as_str(),Some("network"|"timeout"|"internal")) || error.0["kind"]=="provider" && error.0["details"]["status"].as_u64().is_none_or(|code|code>=500) || error.0["kind"]=="conflict" && error.0["details"]["reason"]=="cut_off";
     let ambiguous=dispatched&&(uncertain_kind||error.0["details"]["accepted"]==true);
     if !dispatched && uncertain_kind {error.0["retryable"]=json!(true);}
     if ambiguous{error.0["retryable"]=json!(false);error.0["details"]["attempted"]=json!(if claim.kind=="delete" {"delete_machine"}else{"create_machine"});}
     *outcome.lock().expect("outcome")=Some(if claim.kind=="delete" {Outcome::DeleteFailure{error:error.0}} else {Outcome::Settled{status:if ambiguous{"needs_reconciliation"}else{"failed"}.into(),result:Value::Null,error:error.0}});
    }
   }
   execution.done.store(true,Ordering::Release);
   let should_settle={let workers=engine.workers.lock().expect("workers");workers.get(&claim.id).is_some_and(|w|w.claim.number==claim.number&&w.record.is_none()&&w.outcome.lock().expect("outcome").is_some())};
   if should_settle{let _=engine.start_settlement(&claim.id);}
  })?;
        Ok(())
    }
    pub(super) async fn order_step(
        &self,
        c: &Claim,
        component: &Component,
        e: &Execution,
        step: u8,
    ) -> Result<()> {
        if e.stopped.load(Ordering::Acquire) {
            return Err(ExecutionStopped.into());
        }
        component.trace(
            json!({"event":"dispatch_step","operation":c.id,"claim_number":c.number,"step":step}),
        );
        #[cfg(feature = "store-test-hooks")]
        if let Ok(root) = std::env::var("PROVISIOND_TEST_CONTROL") {
            let path = std::path::Path::new(&root).join(format!("hold-step-{step}"));
            if path.exists() {
                component.trace(json!({"event":"dispatch_step_held","operation":c.id,"step":step}));
                while path.exists() {
                    if e.stopped.load(Ordering::Acquire) {
                        return Err(ExecutionStopped.into());
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
            if std::path::Path::new(&root)
                .join(format!("fail-step-{step}"))
                .exists()
            {
                return Err(Error::new(
                    "network",
                    "controlled worker failure",
                    true,
                    json!({"accepted":null}),
                )
                .into());
            }
        }
        Ok(())
    }
    async fn run_order(
        &self,
        c: &Claim,
        component: &Component,
        e: &Execution,
        d: &Dispatcher,
    ) -> Result<Option<Outcome>> {
        self.order_step(c, component, e, 1).await?;
        let data=self.preflight_value(c,component,e,"account","SELECT jsonb_build_object('request',o.request,'summary',o.request_summary,'account',o.provider_account,'status',s.status,'admitted_sats',o.admitted_sats)::text FROM provisiond.operations o LEFT JOIN provisiond.provider_account_status s ON s.provider_account=o.provider_account WHERE o.id::text=$1",&[&c.id]).await?;
        if data["status"] != "healthy" {
            return Err(Error::new("conflict","provider account unavailable",data["status"]!="terminated",json!({"reason":if data["status"]=="terminated"{"account_terminated"}else{"state"}})).into());
        }
        let tenant_status=self.preflight_value(c,component,e,"tenant","SELECT COALESCE((SELECT to_jsonb(status) FROM provisiond.tenants WHERE id=$1),'null'::jsonb)::text",&[&c.tenant]).await?;
        if tenant_status == "suspended" {
            return Err(Error::conflict("tenant_suspended").into());
        }
        let account = data["account"].as_str().context("account")?;
        let descriptor = d.catalog.get(account).context("configured descriptor")?;
        self.order_step(c, component, e, 2).await?;
        let closure=self.preflight_value(c,component,e,"channel","SELECT COALESCE((SELECT greatest(0,ceil(extract(epoch FROM closed_until-clock_timestamp())*1000))::bigint FROM provisiond.channel_closures WHERE provider_account=$1 AND channel=$2),0)::text",&[&account,&descriptor["channel"].as_str()]).await?;
        if closure.as_i64().unwrap_or(0) > 0 {
            return Err(Error::new(
                "rate_limited",
                "ordering channel closed",
                true,
                json!({"retry_after_ms":closure}),
            )
            .into());
        }
        self.order_step(c, component, e, 3).await?;
        let r = &data["request"];
        let summary = &data["summary"];
        wire::validate_create(r, pricing::wind_seconds(descriptor)?)?;
        validate_admission_fields(summary, r, account, &data["admitted_sats"]).map_err(|_| {
            Error::new(
                "conflict",
                "incomplete admission record",
                true,
                json!({"reason":"incomplete_record"}),
            )
        })?;
        let accepted = &summary["offer"]["accepted_terms"]["offer"];
        let kind = summary["offer"]["machine_kind"]
            .as_str()
            .filter(|s| matches!(*s, "virtual" | "bare_metal"));
        if kind.is_none()
            || !accepted.is_object()
            || !summary["setup_fee"]["native_minor"].is_i64()
            || !summary["offer"]["billing_unit_seconds"].is_i64()
            || !accepted["recurring_price"].is_string()
            || !accepted["setup_fee"].is_string()
            || !accepted["billing_unit"].is_object()
        {
            return Err(Error::new(
                "conflict",
                "incomplete admission record",
                true,
                json!({"reason":"incomplete_record"}),
            )
            .into());
        }
        let cap = pricing::capability(kind.expect("validated kind"));
        if !pricing::has(descriptor, cap) {
            return Err(Error::new(
                "unsupported",
                "ordering capability unavailable",
                false,
                json!({"provider_account":account,"capability":cap}),
            )
            .into());
        }
        self.order_step(c, component, e, 4).await?;
        if !pricing::has(descriptor, "list_offers") {
            return Err(Error::new(
                "unsupported",
                "listing unavailable",
                false,
                json!({"provider_account":account,"capability":"list_offers"}),
            )
            .into());
        }
        let offers = provider_io(
            e,
            d.client.call(
                component,
                descriptor,
                "/offers",
                &json!({"account":account,"operation":c.id}),
            ),
        )
        .await?;
        self.order_step(c, component, e, 5).await?;
        let current = offers
            .as_array()
            .and_then(|a| a.iter().find(|o| o["id"] == r["offer"]));
        if !current.is_some_and(|o| {
            Some(o["kind"].as_str().unwrap_or("")) == kind && pricing::sellable(o, descriptor)
        }) {
            return Err(Error::new(
                "invalid_request",
                "offer not sellable",
                false,
                json!({"reason":"offer_not_sellable"}),
            )
            .into());
        }
        let current = current.context("current offer")?;
        self.order_step(c, component, e, 6).await?;
        let terms = &summary["offer"]["accepted_terms"];
        if let Some(error) = Client::price_moved(
            current,
            accepted,
            &terms["pricing_descriptor"],
            &terms["rate"],
            r["runway_seconds"].as_i64().context("accepted runway")?,
            data["admitted_sats"].as_i64().context("admitted reserve")?,
        )? {
            return Err(error.into());
        }
        self.order_step(c, component, e, 7).await?;
        d.client.validate(r, descriptor, current, &c.id)?;
        self.order_step(c, component, e, 8).await?;
        if crate::store::test_defect("w4_ack") {
            d.client.call(component,descriptor,"/order",&json!({"account":account,"operation":c.id,"correlator":c.id,"channel":descriptor["channel"],"request":r})).await?;
            component.trace(json!({"event":"defect_ack_boundary_bypassed","operation":c.id}));
        }
        let claim = c.clone();
        let licensed = self
            .transaction_stoppable(
                component,
                &mut move |tx| {
                    let claim = claim.clone();
                    Box::pin(async move { tx.license_order(&claim).await })
                },
                Some(&e.stopped),
                None,
            )
            .await?;
        if !licensed {
            component.trace(json!({"event":"dispatch_license_refused","operation":c.id,"claim_number":c.number}));
            return Ok(None);
        }
        component.trace(json!({"event":"dispatch_license_acknowledged","operation":c.id,"claim_number":c.number}));
        self.order_step(c, component, e, 9).await?;
        // OPS-21: same arbitration as the W3 executor; position changes at invocation.
        let permit = {
            let workers = self.workers.lock().expect("workers");
            let w = workers.get(&c.id).context("worker")?;
            if e.stopped.load(Ordering::Acquire) {
                return Err(ExecutionStopped.into());
            }
            ensure!(
                w.claim.number == c.number
                    && w.execution
                        .as_ref()
                        .is_some_and(|x| std::ptr::eq(x.as_ref(), e)),
                "execution replaced"
            );
            let permit = component.external(crate::store::ExternalKind::Provider)?;
            e.dispatched.store(true, Ordering::Release);
            permit
        };
        let forwarded: serde_json::Map<String, Value> = [
            "offer",
            "hostname",
            "region",
            "image",
            "ssh_public_keys",
            "user_data",
            "labels",
            "provider_options",
        ]
        .into_iter()
        .filter_map(|key| r.get(key).map(|value| (key.to_owned(), value.clone())))
        .collect();
        let command = json!({"account":account,"operation":c.id,"correlator":c.id,"channel":descriptor["channel"],"request":forwarded});
        // PRV-9 temporary-key mode is a real branch; it is separately exercised.
        let owned_key = if descriptor["temporary_keys"] == true {
            provider_io(e, d.client.call_entered(descriptor, "/keys", &command)).await?["owned"]
                == true
        } else {
            false
        };
        let reply =
            match provider_io(e, d.client.call_entered(descriptor, "/order", &command)).await {
                Ok(reply) => reply,
                Err(error) => {
                    // OPS-12/49 sensitivity control: deliberately repeat an
                    // uncertain mutation only in an isolated fixture build.
                    #[cfg(feature = "store-test-hooks")]
                    if crate::store::test_defect("w5_blind_retry")
                        || crate::store::test_defect("w6_blind_repeat")
                    {
                        component.trace(json!({"event":"defect_provider_retry","operation":c.id}));
                        let _ = d.client.call_entered(descriptor, "/order", &command).await;
                    }
                    if owned_key
                        && error.downcast_ref::<Error>().is_some_and(|error| {
                            error.0["details"]["accepted"] == false
                                && !matches!(
                                    error.0["kind"].as_str(),
                                    Some("network" | "timeout" | "internal")
                                )
                        })
                    {
                        let _ = provider_io(
                            e,
                            d.client.call_entered(descriptor, "/keys/delete", &command),
                        )
                        .await;
                    }
                    return Err(error);
                }
            };
        drop(permit);
        ensure!(reply["accepted"] == true, "order acceptance absent");
        // OPS-13/PRV-5: accepted reply and later reads are surviving evidence,
        // including when malformed data would otherwise become an untyped error
        // at execute_order's handoff. No identifier here authorizes settlement.
        let mut identifiers = ProviderIdentifiers::default();
        identifiers.remember("order_reply", &reply);
        let verification = async {
        let mut resources = BTreeMap::new();
        for channel in descriptor["channels"]
            .as_array()
            .context("reachable ordering channels")?
        {
            let found=provider_io(e,async {d.client.call(component,descriptor,"/search",&json!({"account":account,"operation":c.id,"correlator":c.id,"channel":channel})).await.map_err(|mut error| {error.0["details"]["accepted"]=json!(true);error})}).await?;
            if let Some(resources) = found["resources"].as_array() {
                for resource in resources {
                    identifiers.remember("search", resource);
                }
            }
            if found["authoritative"] != true {
                return Err(Error::new(
                    "provider",
                    "non-authoritative verification",
                    false,
                    json!({"accepted":true}),
                )
                .into());
            }
            for resource in found["resources"].as_array().context("search resources")? {
                if resource["correlator"]!=c.id || resource["account"]!=account || resource["channel"]!=*channel {
                    return Err(Error::new("provider","non-exact correlator evidence",false,json!({"accepted":true})).into());
                }
                resources.insert(
                    resource["external_id"]
                        .as_str()
                        .context("resource id")?
                        .to_owned(),
                    resource.clone(),
                );
            }
        }
        if resources.len() != 1 {
            return Err(Error::new(
                "provider",
                "verification did not find exactly one resource",
                false,
                json!({"accepted":true,"matches":resources.len()}),
            )
            .into());
        }
        let external = resources.keys().next().context("resource")?;
        let machine = provider_io(e, async {
            d.client
                .call(
                    component,
                    descriptor,
                    "/get",
                    &json!({"account":account,"operation":c.id,"external_id":external}),
                )
                .await
                .map_err(|mut error| {
                    error.0["details"]["accepted"] = json!(true);
                    error
                })
        })
        .await?;
        identifiers.remember("get", &machine);
        // WIR-11/STO-48: this read establishes state. OPS-21/49 retain its
        // instant with the verified outcome across constraint I/O and settlement.
        let observed_at = chrono::Utc::now();
        if machine["present"] != true || machine["account"]!=account || machine["external_id"]!=*external || machine["correlator"]!=c.id {
            return Err(Error::new(
                "provider",
                "resource presence unconfirmed",
                false,
                json!({"accepted":true}),
            )
            .into());
        }
        // PRV-13c: one post-confirmation read. Failure leaves null, never reclassifies success.
        let date = provider_io(
            e,
            d.client.call(
                component,
                descriptor,
                "/constraints",
                &json!({"account":account,"operation":c.id,"external_id":external}),
            ),
        )
        .await
        .ok()
        .and_then(|v| v["earliest_cancellation_date"].as_str().map(str::to_owned));
        component.trace(json!({"event":"provider_verified","operation":c.id,"matches":1,"direct_read":true,"post_order_read":date.is_some(),"state_observed_at":observed_at.to_rfc3339()}));
        Ok(Some(Outcome::VerifiedCreateDetails {
            external_id: external.clone(),
            earliest_date: date,
            machine,
            observed_at,
        }))
        }.await;
        verification.map_err(|error| identifiers.failure(error))
    }
}

/// OPS-13, DOM-18, WIR-10b: only safe opaque identifiers, labeled by observation
/// source. Keep distinct observations rather than replacing the accepted reply.
#[derive(Default)]
struct ProviderIdentifiers(BTreeMap<&'static str, std::collections::BTreeSet<String>>);
impl ProviderIdentifiers {
    fn remember(&mut self, source: &'static str, value: &Value) {
        if let Some(id) = value["external_id"].as_str().filter(|id| {
            !id.is_empty()
                && id.len() <= 128
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-:".contains(&b))
        }) {
            self.0.entry(source).or_default().insert(id.to_owned());
        }
    }
    fn failure(self, error: anyhow::Error) -> anyhow::Error {
        if error.downcast_ref::<FatalEngine>().is_some() {
            return error;
        }
        let mut error = error.downcast_ref::<Error>().cloned().unwrap_or_else(|| {
            if error.downcast_ref::<ExecutionStopped>().is_some() {
                Error::new(
                    "conflict",
                    "operation execution was cut off by a stop",
                    false,
                    json!({"reason":"cut_off"}),
                )
            } else {
                Error::new(
                    "internal",
                    "provider verification could not complete",
                    false,
                    json!({}),
                )
            }
        });
        error.0["details"]["accepted"] = json!(true);
        if !self.0.is_empty() {
            error.0["details"]["provider_identifiers"] = json!(self.0);
        }
        error.into()
    }
}

// OPS-23: validate every persisted admission field before the first driver read.
fn validate_admission_fields(
    s: &Value,
    r: &Value,
    account: &str,
    admitted_sats: &Value,
) -> Result<()> {
    let snapshot = &s["offer"];
    let accepted = &snapshot["accepted_terms"]["offer"];
    let rate = &snapshot["accepted_terms"]["rate"];
    let admitted = admitted_sats
        .as_i64()
        .filter(|n| *n > 0)
        .context("admitted reserve")?;
    ensure!(
        rate["source"]
            .as_str()
            .is_some_and(|source| !source.is_empty()),
        "accepted rate source"
    );
    chrono::DateTime::parse_from_rfc3339(
        rate["observed_at"]
            .as_str()
            .context("accepted rate observation")?,
    )?;
    pricing::validate_descriptor(&snapshot["accepted_terms"]["pricing_descriptor"])?;
    ensure!(
        s["kind"] == "create"
            && s["provider_account"] == account
            && r["provider_account"] == account
            && snapshot["offer_id"] == r["offer"]
            && accepted["id"] == r["offer"],
        "admission identity"
    );
    ensure!(
        snapshot["machine_kind"] == accepted["kind"]
            && matches!(
                snapshot["machine_kind"].as_str(),
                Some("virtual" | "bare_metal")
            )
            && snapshot["install_strategies"].is_array()
            && snapshot["install_strategies"] == accepted["install_strategies"],
        "admitted product kind/strategies"
    );
    ensure!(
        snapshot["currency"].is_string()
            && snapshot["currency"] == accepted["currency"]
            && accepted["currency"] == rate["currency"]
            && snapshot["recurring_price"] == accepted["recurring_price"]
            && snapshot["recurring_period"] == "hour"
            && accepted["period_seconds"] == 3600,
        "accepted native currency/price/period"
    );
    let scale = u32::try_from(rate["minor_scale"].as_u64().context("native precision")?)?;
    let recurring = pricing::minor(
        accepted["recurring_price"]
            .as_str()
            .context("recurring price")?,
        scale,
    )?;
    let setup = pricing::minor(
        accepted["setup_fee"].as_str().context("setup price")?,
        scale,
    )?;
    let period = accepted["period_seconds"]
        .as_i64()
        .filter(|n| *n > 0)
        .context("native period")?;
    let seconds = accepted["billing_unit"]["seconds"]
        .as_i64()
        .filter(|n| *n >= 0)
        .context("billing duration")?;
    ensure!(
        snapshot["billing_unit_seconds"].as_i64() == Some(seconds),
        "copied duration"
    );
    ensure!(
        accepted["billing_unit"]["native_floor"].is_null()
            || accepted["billing_unit"]["native_floor"].is_string(),
        "accepted native floor"
    );
    let floor = accepted["billing_unit"]["native_floor"]
        .as_str()
        .map(|v| pricing::minor(v, scale))
        .transpose()?
        .unwrap_or(0);
    crate::store::validate_charge(&s["setup_fee"])?;
    ensure!(
        s["setup_fee"]["currency"] == accepted["currency"]
            && s["setup_fee"]["native_minor"].as_i64() == Some(setup),
        "accepted setup evidence"
    );
    let unit_native =
        (i128::from(recurring) * i128::from(seconds) + i128::from(period) - 1) / i128::from(period);
    let unit_native = i64::try_from(unit_native.max(floor.into()))?;
    ensure!(
        s.get("first_unit").is_some() == (unit_native > 0),
        "first-unit presence"
    );
    if unit_native > 0 {
        crate::store::validate_charge(&s["first_unit"])?;
        ensure!(
            s["first_unit"]["sats"].as_i64().is_some_and(|n| n > 0)
                && s["first_unit"]["native_minor"].as_i64() == Some(unit_native)
                && s["first_unit"]["seconds"].as_i64() == Some(seconds)
                && s["first_unit"]["currency"] == accepted["currency"],
            "first-unit evidence"
        );
    }
    for charge in [Some(&s["setup_fee"]), s.get("first_unit")]
        .into_iter()
        .flatten()
    {
        chrono::DateTime::parse_from_rfc3339(
            charge["rate_observed_at"]
                .as_str()
                .context("conversion observation")?,
        )?;
    }
    // OPS-23/43: accepted conversion and admitted authority are API-written
    // fields. Validate their coherent accepted basis before the first driver read;
    // step 6 alone compares the current offer with that already validated basis.
    let expected = pricing::calculate(
        accepted,
        &snapshot["accepted_terms"]["pricing_descriptor"],
        rate,
        r["runway_seconds"].as_i64().context("accepted runway")?,
    )?;
    ensure!(admitted == expected.reserve, "accepted reserve mismatch");
    ensure!(
        s["setup_fee"] == expected.setup,
        "accepted setup conversion mismatch"
    );
    ensure!(
        s.get("first_unit") == expected.unit.as_ref(),
        "accepted unit conversion mismatch"
    );
    let declared = accepted["cancellation_bound"]
        .as_str()
        .or_else(|| snapshot["accepted_terms"]["pricing_descriptor"]["cancellation_bound"].as_str())
        .map(|date| {
            chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")?;
            anyhow::Ok(format!("{date}T00:00:00Z"))
        })
        .transpose()?;
    ensure!(
        snapshot.get("declared_cancellation_bound") == Some(&json!(declared)),
        "accepted cancellation-bound copy"
    );
    Ok(())
}

// OPS-21: cancel external I/O only. Licensing/sent-COMMIT ownership stays in its
// existing transaction future. A ready provider return wins over a concurrent stop.
pub(super) async fn provider_io<T>(
    execution: &Execution,
    call: impl std::future::Future<Output = std::result::Result<T, Error>>,
) -> Result<T> {
    tokio::select! {
        biased;
        result=call=>result.map_err(Into::into),
        _=async {while !execution.stopped.load(Ordering::Acquire) {tokio::time::sleep(Duration::from_millis(1)).await;}}=>Err(ExecutionStopped.into()),
    }
}
