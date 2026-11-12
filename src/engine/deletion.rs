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
        let data=self.preflight_value(c,component,e,"delete_target","SELECT jsonb_build_object('account',o.provider_account,'external_id',m.external_id)::text FROM provisiond.operations o JOIN provisiond.machines m ON m.id=o.machine_id WHERE o.id::text=$1",&[&c.id]).await?;
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
