//! OPS-41/42/45, PRV-29/36: one immediate-destroy attempt, never a retry loop.
use super::*;
use crate::wire::Error;
impl Engine {
    pub(super) async fn run_delete(
        &self,
        c: &Claim,
        component: &Component,
        e: &Execution,
        d: &Dispatcher,
    ) -> Result<Option<Outcome>> {
        self.order_step(c, component, e, 101).await?;
        // OPS-2/23, WIR-22: repeat caller validation before decision/fence,
        // capability, marker or dispatch. System cleanup and operator retries
        // never had a customer acknowledgment to repeat.
        let data=self.preflight_value(c,component,e,"delete_target","SELECT jsonb_build_object('account',o.provider_account,'external_id',m.external_id,'machine_id',m.id,'machine_account',m.provider_account,'request',o.request,'requested_by',o.requested_by,'summary',o.request_summary)::text FROM provisiond.operations o LEFT JOIN provisiond.machines m ON m.id=o.machine_id AND m.tenant_id=o.tenant_id WHERE o.id::text=$1",&[&c.id]).await?;
        if data["requested_by"] == "caller" {
            let mut bad = crate::wire::unknown(&data["request"], &["acknowledge_destruction"]);
            if data["request"]["acknowledge_destruction"] != true {
                bad.push("/acknowledge_destruction".into());
            }
            crate::wire::least(bad)?;
        }
        let summary = &data["summary"];
        if data["machine_id"].is_null()
            || data["account"] != data["machine_account"]
            || summary["kind"] != "delete"
            || summary["machine_id"] != data["machine_id"]
            || summary["provider_account"] != data["account"]
            || summary["external_id"] != data["external_id"]
            || !data["external_id"].as_str().is_some_and(|id| {
                !id.is_empty()
                    && id.len() <= 128
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._-:".contains(&b))
            })
        {
            return Err(Error::new(
                "conflict",
                "incomplete delete record",
                true,
                json!({"reason":"incomplete_record"}),
            )
            .into());
        }
        let claim = c.clone();
        let decision = self
            .transaction_stoppable(
                component,
                &mut move |tx| {
                    let c = claim.clone();
                    Box::pin(async move { tx.delete_decision(&c).await })
                },
                Some(&e.stopped),
                None,
            )
            .await?;
        component.trace(json!({"event":"delete_decision","operation":c.id,"decision":decision}));
        if decision == "finished" {
            return Ok(None);
        }
        if decision == "no_rate" {
            component.trace(json!({"event":"delete_no_rate_stopped","operation":c.id,"fence_written":false,"provider_called":false,"steps_4_5":"not run"}));
            return self.fatal(
                "no-rate cancellation branch outside local profile; no fence or provider call",
            );
        }
        let account = data["account"].as_str().context("delete account")?;
        let descriptor = d.catalog.get(account).context("delete descriptor")?;
        if !crate::pricing::has(descriptor, "delete_machine") {
            return Err(Error::new(
                "unsupported",
                "deletion unavailable",
                false,
                json!({"provider_account":account,"capability":"delete_machine"}),
            )
            .into());
        }
        self.order_step(c, component, e, 102).await?;
        let claim = c.clone();
        let licensed = self
            .transaction_stoppable(
                component,
                &mut move |tx| {
                    let c = claim.clone();
                    Box::pin(async move { tx.license_delete(&c).await })
                },
                Some(&e.stopped),
                None,
            )
            .await?;
        if !licensed {
            return Ok(None);
        }
        component.trace(
            json!({"event":"delete_marker_acknowledged","operation":c.id,"claim_number":c.number}),
        );
        self.order_step(c, component, e, 103).await?;
        let permit = {
            let workers = self.workers.lock().expect("workers");
            let w = workers.get(&c.id).context("delete worker")?;
            if e.stopped.load(Ordering::Acquire) {
                return Err(ExecutionStopped.into());
            }
            ensure!(
                w.claim.number == c.number
                    && w.execution
                        .as_ref()
                        .is_some_and(|x| std::ptr::eq(x.as_ref(), e)),
                "delete execution replaced"
            );
            let permit = component.external(crate::store::ExternalKind::Provider)?;
            e.dispatched.store(true, Ordering::Release);
            permit
        };
        let result = super::dispatch::provider_io(
            e,
            d.client.call_entered(
                descriptor,
                "/delete",
                &json!({"account":account,"operation":c.id,"external_id":data["external_id"]}),
            ),
        )
        .await?;
        let observed_at = chrono::Utc::now();
        drop(permit);
        if result["gone"] == true {
            return Ok(Some(Outcome::DeleteGone { observed_at }));
        }
        Err(Error::new("provider","delete outcome requires non-mutating evidence",false,json!({"accepted":if result["accepted"]==true{json!(true)}else{Value::Null},"attempted":"delete_machine"})).into())
    }
}
