//! OPS-43 ordered first-refusal dispatch and OPS-27 independent verification.
use super::*;
use crate::{
    pricing,
    provider::Client,
    wire::{self, Error},
};
pub struct Dispatcher {
    client: Client,
    catalog: BTreeMap<String, Value>,
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
    pub async fn dispatch_next(self: &Arc<Self>, dispatcher: &Arc<Dispatcher>) -> Result<()> {
        if self.worker_slots.available_permits() == 0 {
            return Ok(());
        }
        if let Some(c) = self.claim().await? {
            if c.kind != "create" {
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
        let (claim, component, outcome) = {
            let mut workers = self.workers.lock().expect("workers");
            let w = workers.get_mut(id).context("worker")?;
            ensure!(
                w.execution.is_none() && w.record.is_none(),
                "worker already executing"
            );
            w.execution = Some(execution.clone());
            (w.claim.clone(), w.component.clone(), w.outcome.clone())
        };
        let engine = self.clone();
        self.process.submit(async move{
   let returned=engine.run_order(&claim,&component,&execution,&dispatcher).await;
   match returned{
    Ok(Some(value))=>{*outcome.lock().expect("outcome")=Some(value);component.trace(json!({"event":"execute_returned","operation":claim.id,"claim_number":claim.number}));}
    Ok(None)=>{execution.done.store(true,Ordering::Release);engine.retire(&claim.id,claim.number);return;}
    Err(error) if error.downcast_ref::<ExecutionStopped>().is_some()=>{},
    Err(error) if error.downcast_ref::<FatalEngine>().is_some()=>{engine.database.fatal("dispatch store supervision failed");},
    Err(error)=>{
     let mut error=error.downcast_ref::<Error>().cloned().unwrap_or_else(||Error::new("internal","worker could not complete",true,json!({"accepted":null})));
     let dispatched=execution.dispatched.load(Ordering::Acquire);
     let uncertain_kind=matches!(error.0["kind"].as_str(),Some("network"|"timeout"|"internal")) || error.0["kind"]=="provider" && error.0["details"]["upstream_status"].as_u64().is_none_or(|code|code>=500) || error.0["kind"]=="conflict" && error.0["details"]["reason"]=="cut_off";
     let ambiguous=dispatched&&(uncertain_kind||error.0["details"]["accepted"]==true);
     if !dispatched && uncertain_kind {error.0["retryable"]=json!(true);}
     if ambiguous{error.0["retryable"]=json!(false);error.0["details"]["attempted"]=json!("create_machine");}
     *outcome.lock().expect("outcome")=Some(Outcome::Settled{status:if ambiguous{"needs_reconciliation"}else{"failed"}.into(),result:Value::Null,error:error.0});
    }
   }
   execution.done.store(true,Ordering::Release);
   let should_settle={let workers=engine.workers.lock().expect("workers");workers.get(&claim.id).is_some_and(|w|w.claim.number==claim.number&&w.record.is_none()&&w.outcome.lock().expect("outcome").is_some())};
   if should_settle{let _=engine.start_settlement(&claim.id);}
  })?;
        Ok(())
    }
    async fn order_step(
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
        let data=component.value("SELECT jsonb_build_object('request',o.request,'summary',o.request_summary,'account',o.provider_account,'status',s.status)::text FROM provisiond.operations o LEFT JOIN provisiond.provider_account_status s ON s.provider_account=o.provider_account WHERE o.id::text=$1",&[&c.id]).await?;
        if data["status"] != "healthy" {
            return Err(Error::new("conflict","provider account unavailable",data["status"]!="terminated",json!({"reason":if data["status"]=="terminated"{"account_terminated"}else{"state"}})).into());
        }
        let tenant_status=component.value("SELECT COALESCE((SELECT to_jsonb(status) FROM provisiond.tenants WHERE id=$1),'null'::jsonb)::text",&[&c.tenant]).await?;
        if tenant_status == "suspended" {
            return Err(Error::conflict("tenant_suspended").into());
        }
        let account = data["account"].as_str().context("account")?;
        let descriptor = d.catalog.get(account).context("configured descriptor")?;
        self.order_step(c, component, e, 2).await?;
        let closure=component.value("SELECT COALESCE((SELECT greatest(0,ceil(extract(epoch FROM closed_until-clock_timestamp())*1000))::bigint FROM provisiond.channel_closures WHERE provider_account=$1 AND channel=$2),0)::text",&[&account,&descriptor["channel"].as_str()]).await?;
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
        for cap in [pricing::capability(kind.expect("validated kind"))] {
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
        let offers = d
            .client
            .call(
                component,
                descriptor,
                "/offers",
                &json!({"account":account,"operation":c.id}),
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
        let scale = data["summary"]["offer"]["accepted_terms"]["rate"]["minor_scale"]
            .as_u64()
            .context("accepted currency scale")? as u32;
        if let Some(error) = Client::price_moved(current, accepted, scale)? {
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
        let command = json!({"account":account,"operation":c.id,"correlator":c.id,"channel":descriptor["channel"],"request":r});
        // PRV-9 temporary-key mode is a real branch; it is separately exercised.
        let owned_key = if descriptor["temporary_keys"] == true {
            d.client.call_entered(descriptor, "/keys", &command).await?["owned"] == true
        } else {
            false
        };
        let reply = match d.client.call_entered(descriptor, "/order", &command).await {
            Ok(reply) => reply,
            Err(error) => {
                if owned_key
                    && error.0["details"]["accepted"] == false
                    && !matches!(
                        error.0["kind"].as_str(),
                        Some("network" | "timeout" | "internal")
                    )
                {
                    let _ = d
                        .client
                        .call_entered(descriptor, "/keys/delete", &command)
                        .await;
                }
                return Err(error.into());
            }
        };
        drop(permit);
        ensure!(reply["accepted"] == true, "order acceptance absent");
        let mut resources = BTreeMap::new();
        for channel in descriptor["channels"]
            .as_array()
            .context("reachable ordering channels")?
        {
            let found=d.client.call(component,descriptor,"/search",&json!({"account":account,"operation":c.id,"correlator":c.id,"channel":channel})).await.map_err(|mut error| {error.0["details"]["accepted"]=json!(true);error})?;
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
        let machine = d
            .client
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
            })?;
        if machine["present"] != true {
            return Err(Error::new(
                "provider",
                "resource presence unconfirmed",
                false,
                json!({"accepted":true}),
            )
            .into());
        }
        // PRV-13c: one post-confirmation read. Failure leaves null, never reclassifies success.
        let date = d
            .client
            .call(
                component,
                descriptor,
                "/constraints",
                &json!({"account":account,"operation":c.id,"external_id":external}),
            )
            .await
            .ok()
            .and_then(|v| v["earliest_cancellation_date"].as_str().map(str::to_owned));
        component.trace(json!({"event":"provider_verified","operation":c.id,"matches":1,"direct_read":true,"post_order_read":date.is_some()}));
        Ok(Some(Outcome::VerifiedCreateDetails {
            external_id: external.clone(),
            earliest_date: date,
            machine,
        }))
    }
}
