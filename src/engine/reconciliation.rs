//! OPS-27/28/29/30/32/33: read-only provider evidence, retained across store retries.
use super::*;
use crate::store::Resolution;

impl Engine {
    // OPS-49 fixture: retain completed read-only evidence without a store owner.
    // Ordinary binaries never read this file or pause their reconciliation.
    async fn reconciliation_read_boundary(
        &self,
        component: &Component,
        subject: &str,
    ) -> Result<()> {
        #[cfg(feature = "store-test-hooks")]
        if let Ok(root) = std::env::var("PROVISIOND_TEST_CONTROL") {
            let hold = std::path::Path::new(&root).join("hold-reconciliation-record");
            if hold.exists() {
                component.trace(json!({"event":"reconciliation_evidence_held","subject":subject,"checkout_held":false}));
                while hold.exists() {
                    self.database.ensure_new_work()?;
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
        }
        let _ = (component, subject);
        Ok(())
    }
    async fn reconcile_delete(
        &self,
        component: &Component,
        d: &Dispatcher,
        descriptor: &Value,
        row: &Value,
    ) -> Result<()> {
        let id = row["id"].as_str().context("delete operation")?;
        let tenant = row["tenant_id"].as_str().context("delete tenant")?;
        let account = row["provider_account"].as_str().context("delete account")?;
        let wait = chrono::DateTime::parse_from_rfc3339(
            row["wait_ended"].as_str().context("delete wait end")?,
        )?;
        if ((chrono::Utc::now() - wait.with_timezone(&chrono::Utc)).num_milliseconds() as f64)
            < row["effective_visibility"]
                .as_f64()
                .context("effective window")?
                * 1000.0
        {
            return Ok(());
        }
        let external = row["request_summary"]["external_id"]
            .as_str()
            .context("retained delete identity")?;
        let result = d
            .client
            .call(
                component,
                descriptor,
                "/get",
                &json!({"account":account,"operation":id,"external_id":external,"resolution":true}),
            )
            .await;
        let observed_at = chrono::Utc::now();
        let verb = match result {
            Ok(v)
                if v.is_null()
                    || v["present"] == false
                        && v["external_id"] == external
                        && v["account"] == account =>
            {
                Some("applied")
            }
            Ok(v)
                if v["present"] == true
                    && v["external_id"] == external
                    && v["account"] == account =>
            {
                Some("not_applied")
            }
            _ => None,
        };
        self.reconciliation_read_boundary(component, id).await?;
        component.trace(json!({"event":"delete_resolution_read","operation":id,"resolution":verb}));
        if let Some(verb) = verb {
            let r = Resolution {
                id: id.into(),
                tenant: tenant.into(),
                verb: verb.into(),
                resolver: "engine".into(),
                automatic: true,
                evidence: json!({"source":"direct_read","external_id":external,"past_effective_window":true}),
                machine: Value::Null,
                earliest_date: None,
                observed_at,
            };
            self.transaction(component, move |tx| {
                let r = r.clone();
                Box::pin(async move { tx.resolve(&r).await })
            })
            .await?;
        } else {
            let id = id.to_owned();
            self.transaction(component,move|tx|{let id=id.clone();Box::pin(async move{tx.rows("UPDATE provisiond.operations SET resolution_attempted_at=clock_timestamp() WHERE id=$1::text::uuid AND status='needs_reconciliation' AND resolution IS NULL RETURNING id",&[&id]).await?;Ok(())})}).await?;
        }
        Ok(())
    }
    /// OPS-47/50/49: supervised periodic work uses generation-first transactions
    /// and refuses a new pass after detected lifetime-lock loss.
    pub async fn reconcile(&self, d: &Dispatcher) -> Result<()> {
        self.database.ensure_new_work()?;
        // OPS-32/LDG-69: no worker permit or machine hold is acquired, and no
        // pool checkout is retained across the provider reads below.
        let component = self.component();
        let pending=self.transaction(&component,|tx|Box::pin(async move {
            // OPS-33: inability to search an account must not freeze money.
            // This is local bookkeeping, independent of provider read eligibility.
            // A retained positive in-progress match remains exempt from this timer.
            let expired=tx.rows("SELECT o.id::text,o.tenant_id FROM provisiond.operations o JOIN provisiond.provider_accounts a ON a.account=o.provider_account JOIN provisiond.provider_account_status s ON s.provider_account=o.provider_account WHERE o.kind='create' AND o.status='needs_reconciliation' AND o.resolution IS NULL AND s.status<>'healthy' AND COALESCE(o.error->'details'->'reconciliation'->>'in_progress','false')<>'true' AND clock_timestamp()>=COALESCE(o.mutation_wait_ended_at,o.updated_at)+(a.descriptor->>'negative_seconds')::bigint*interval '1 second' AND EXISTS(SELECT 1 FROM provisiond.commitments c WHERE c.id=o.commitment_id AND c.state='open') ORDER BY o.tenant_id,o.id",&[]).await?;
            for row in expired {
                let id:String=row.get(0);let tenant:String=row.get(1);
                tx.release_negative(&id,&tenant).await?;
            }
            let rows=tx.rows("SELECT COALESCE(jsonb_agg(to_jsonb(o)||jsonb_build_object('wait_ended',COALESCE(o.mutation_wait_ended_at,o.updated_at),'effective_visibility',greatest(COALESCE((a.descriptor->>'visibility_seconds')::numeric,1),COALESCE((SELECT max(extract(epoch FROM p.observed_at-p.dispatched_at)) FROM provisiond.provider_observations p WHERE p.provider_account=o.provider_account),0)))),'[]')::text FROM provisiond.operations o JOIN provisiond.provider_accounts a ON a.account=o.provider_account WHERE o.status='needs_reconciliation' AND o.resolution IS NULL AND EXISTS(SELECT 1 FROM provisiond.provider_account_status s WHERE s.provider_account=o.provider_account AND s.status='healthy')",&[]).await?;
            Ok(serde_json::from_str::<Value>(&rows[0].get::<_,String>(0))?)
        })).await?;
        for row in pending.as_array().context("pending operations")? {
            self.database.ensure_new_work()?;
            let Some(account) = row["provider_account"].as_str() else {
                continue;
            };
            let Some(descriptor) = d.catalog.get(account) else {
                continue;
            };
            if row["kind"] == "create" {
                self.reconcile_create(&component, d, descriptor, row)
                    .await?;
            } else if row["kind"] == "delete" {
                self.reconcile_delete(&component, d, descriptor, row)
                    .await?;
            }
        }
        for (account, descriptor) in &d.catalog {
            self.database.ensure_new_work()?;
            let account_owned = account.clone();
            let healthy=self.transaction(&component,move|tx|{let account=account_owned.clone();Box::pin(async move{Ok(tx.rows("SELECT status='healthy' FROM provisiond.provider_account_status WHERE provider_account=$1",&[&account]).await?.first().is_some_and(|r|r.get::<_,bool>(0)))})}).await?;
            if healthy {
                self.account_pass(&component, d, account, descriptor)
                    .await?;
            }
        }
        Ok(())
    }
    async fn account_pass(
        &self,
        component: &Component,
        d: &Dispatcher,
        account: &str,
        descriptor: &Value,
    ) -> Result<()> {
        let mut resources = BTreeMap::new();
        let mut complete = true;
        for channel in descriptor["channels"]
            .as_array()
            .context("account channels")?
        {
            let mut cursor = Value::Null;
            let mut visited = std::collections::BTreeSet::new();
            loop {
                self.database.ensure_new_work()?;
                let page=d.client.call(component,descriptor,"/enumerate",&json!({"account":account,"channel":channel,"cursor":cursor,"resolution":true})).await;
                let Ok(page) = page else {
                    complete = false;
                    break;
                };
                let Some(list) = page["resources"].as_array() else {
                    complete = false;
                    break;
                };
                for resource in list {
                    if resource["account"] != account
                        || !resource["external_id"].as_str().is_some_and(safe_identity)
                    {
                        complete = false;
                        continue;
                    }
                    resources.insert(
                        resource["external_id"]
                            .as_str()
                            .expect("validated identity")
                            .to_owned(),
                        resource.clone(),
                    );
                }
                if page["next"].is_null() {
                    complete &= page["complete"] == true;
                    break;
                }
                cursor = page["next"].clone();
                if !visited.insert(cursor.to_string()) {
                    complete = false;
                    break;
                }
            }
        }
        let observed_at = chrono::Utc::now();
        self.reconciliation_read_boundary(component, account)
            .await?;
        let owned = account.to_owned();
        let managed=self.transaction(component,move|tx|{let account=owned.clone();Box::pin(async move{
            let rows=tx.rows("SELECT COALESCE(jsonb_agg(to_jsonb(m)||jsonb_build_object('wait_end',(SELECT COALESCE(o.mutation_wait_ended_at,o.updated_at) FROM provisiond.operations o WHERE o.machine_id=m.id AND o.kind='create' ORDER BY o.created_at LIMIT 1),'visibility',greatest(COALESCE((SELECT (descriptor->>'visibility_seconds')::numeric FROM provisiond.provider_accounts WHERE account=$1),1),COALESCE((SELECT max(extract(epoch FROM observed_at-dispatched_at)) FROM provisiond.provider_observations WHERE provider_account=$1),0)))),'[]')::text FROM provisiond.machines m WHERE provider_account=$1",&[&account]).await?;
            Ok(serde_json::from_str::<Value>(&rows[0].get::<_,String>(0))?)
        })}).await?;
        let mut known = std::collections::BTreeSet::new();
        for machine in managed.as_array().context("managed account machines")? {
            let external = machine["external_id"]
                .as_str()
                .context("managed identity")?;
            known.insert(external.to_owned());
            if !machine["meter_stopped_at"].is_null() {
                continue;
            }
            let observation = if let Some(resource) = resources.get(external) {
                Some((false, resource.clone(), observed_at))
            } else if complete {
                let eligible = machine["wait_end"]
                    .as_str()
                    .map(|s| {
                        chrono::DateTime::parse_from_rfc3339(s).map(|t| {
                            (observed_at - t.with_timezone(&chrono::Utc)).num_milliseconds() as f64
                                >= machine["visibility"].as_f64().unwrap_or(f64::INFINITY) * 1000.0
                        })
                    })
                    .transpose()?
                    .unwrap_or(true);
                if !eligible {
                    continue;
                }
                match d
                    .client
                    .call(
                        component,
                        descriptor,
                        "/get",
                        &json!({"account":account,"external_id":external,"resolution":true}),
                    )
                    .await
                {
                    Ok(v) if v.is_null() => Some((true, v, chrono::Utc::now())),
                    Ok(v)
                        if v["present"] == false
                            && v["external_id"] == external
                            && v["account"] == account =>
                    {
                        Some((true, v, chrono::Utc::now()))
                    }
                    Ok(v)
                        if v["present"] == true
                            && v["external_id"] == external
                            && v["account"] == account =>
                    {
                        Some((false, v, chrono::Utc::now()))
                    }
                    _ => None,
                }
            } else {
                None
            };
            if let Some((gone, resource, at)) = observation {
                let id = machine["id"].as_str().context("machine id")?.to_owned();
                let tenant = machine["tenant_id"]
                    .as_str()
                    .context("machine tenant")?
                    .to_owned();
                self.transaction(component,move|tx|{let id=id.clone();let tenant=tenant.clone();let resource=resource.clone();Box::pin(async move{
                    crate::ledger::serialize(tx,&[&tenant]).await?;
                    if gone {tx.record_gone(&id,&at).await?;} else if resource["present"]==true {
                        tx.rows("UPDATE provisiond.machines SET state=$2,state_observed_at=$3::text::timestamptz WHERE id=$1::text::uuid AND meter_stopped_at IS NULL AND (state_observed_at IS NULL OR state_observed_at<=$3::text::timestamptz) RETURNING id",&[&id,&resource["state"].as_str().unwrap_or("unknown"),&at.to_rfc3339()]).await?;
                    }
                    Ok(())
                })}).await?;
            }
        }
        for external in resources.keys().filter(|id| !known.contains(*id)) {
            component.trace(json!({"event":"unrecorded_provider_resource","provider_account":account,"external_id":external,"auto_attached":false}));
        }
        component.trace(json!({"event":"account_pass_complete","provider_account":account,"complete":complete,"resources":resources.len()}));
        // OPS-32/PRV-9: key cleanup is independent of create resolution. A pending
        // operation or provisioning machine never licenses deletion of its key.
        if descriptor["temporary_keys"] == true {
            if let Ok(keys) = d
                .client
                .call(
                    component,
                    descriptor,
                    "/keys/list",
                    &json!({"account":account,"resolution":true}),
                )
                .await
            {
                for key in keys.as_array().context("temporary keys")? {
                    let Some(operation) = key["operation"]
                        .as_str()
                        .filter(|s| uuid::Uuid::parse_str(s).is_ok())
                    else {
                        continue;
                    };
                    let id = operation.to_owned();
                    let eligible=self.transaction(component,move|tx|{let id=id.clone();Box::pin(async move{Ok(tx.rows("SELECT NOT EXISTS(SELECT 1 FROM provisiond.operations WHERE id::text=$1 AND status NOT IN ('succeeded','failed')) AND NOT EXISTS(SELECT 1 FROM provisiond.machines WHERE correlator_kind='operation_uuid' AND correlator_value=$1 AND state='provisioning')",&[&id]).await?[0].get::<_,bool>(0))})}).await?;
                    if eligible {
                        let _ = d
                            .client
                            .call(
                                component,
                                descriptor,
                                "/keys/delete",
                                &json!({"account":account,"operation":operation}),
                            )
                            .await;
                    }
                }
            }
        }
        Ok(())
    }
    async fn reconcile_create(
        &self,
        component: &Component,
        d: &Dispatcher,
        descriptor: &Value,
        row: &Value,
    ) -> Result<()> {
        let id = row["id"].as_str().context("operation")?;
        let tenant = row["tenant_id"].as_str().context("tenant")?;
        let account = row["provider_account"].as_str().context("account")?;
        let wait =
            chrono::DateTime::parse_from_rfc3339(row["wait_ended"].as_str().context("wait end")?)?;
        let age = (chrono::Utc::now() - wait.with_timezone(&chrono::Utc)).num_milliseconds();
        let negative = descriptor["negative_seconds"].as_i64().unwrap_or(60) * 1000;
        let visibility = row["effective_visibility"]
            .as_f64()
            .context("visibility window")?
            * 1000.0;
        // PRV-36: no resolution read before the effective window from wait end.
        if (age as f64) < visibility {
            return Ok(());
        }
        let mut resources = BTreeMap::new();
        let mut authoritative = row["correlator_kind"] == "operation_uuid"
            && row["correlator_value"].is_string()
            && descriptor["correlator"] == "operation_id";
        let mut in_progress = false;
        let mut identifiers = std::collections::BTreeSet::new();
        if authoritative {
            for channel in descriptor["channels"].as_array().context("channels")? {
                let found=d.client.call(component,descriptor,"/search",&json!({"account":account,"operation":id,"correlator":row["correlator_value"],"channel":channel,"resolution":true})).await;
                match found {
                    Ok(found) => {
                        authoritative &=
                            found["authoritative"] == true && found["resources"].is_array();
                        in_progress |= found["in_progress"] == true;
                        if let Some(list) = found["resources"].as_array() {
                            for resource in list {
                                if let Some(external) = resource["external_id"].as_str() {
                                    if safe_identity(external) {
                                        identifiers.insert(external.to_owned());
                                    }
                                    // OPS-29: local equality is required even when a driver
                                    // mistakenly returns a heuristic candidate.
                                    if (resource["correlator"] == row["correlator_value"]
                                        || crate::store::test_defect("w6_non_exact"))
                                        && resource["account"] == account
                                        && resource["channel"] == *channel
                                        && safe_identity(external)
                                    {
                                        resources.insert(external.to_owned(), resource.clone());
                                    } else {
                                        authoritative = false;
                                    }
                                } else {
                                    authoritative = false;
                                }
                            }
                        }
                    }
                    Err(error) => {
                        authoritative = false;
                        component.trace(json!({"event":"resolution_read_refused","operation":id,"channel":channel,"kind":error.0["kind"]}));
                        if error.0["kind"] == "authentication" {
                            let account = account.to_owned();
                            self.transaction(component,move |tx|{let account=account.clone();Box::pin(async move{tx.rows("UPDATE provisiond.provider_account_status SET status='credentials_rejected',source='provider',observed_at=clock_timestamp() WHERE provider_account=$1 AND status<>'terminated' RETURNING provider_account",&[&account]).await?;Ok(())})}).await?;
                        }
                    }
                }
            }
        }
        // OPS-27/33: a failed/partial reread does not retract the last positive
        // in-progress match. Only complete evidence of delivered resources clears it.
        in_progress |= row["error"]["details"]["reconciliation"]["in_progress"] == true
            && !(authoritative && !resources.is_empty());
        self.reconciliation_read_boundary(component, id).await?;
        let evidence = json!({"source":"all_channel_search","authoritative":authoritative,"matches":resources.len(),"provider_identifiers":identifiers,"in_progress":in_progress});
        component.trace(json!({"event":"resolution_search","operation":id,"evidence":evidence}));
        let mut resolution = None;
        if authoritative && resources.len() == 1 && !in_progress {
            let external = resources.keys().next().context("matched resource")?;
            if let Ok(machine)=d.client.call(component,descriptor,"/get",&json!({"account":account,"operation":id,"external_id":external,"resolution":true})).await {
                let observed_at=chrono::Utc::now();
                if machine["present"]==true && machine["external_id"]==*external && machine["account"]==account && (machine["correlator"]==row["correlator_value"] || crate::store::test_defect("w6_non_exact")) {
                    let date=d.client.call(component,descriptor,"/constraints",&json!({"account":account,"operation":id,"external_id":external,"resolution":true})).await.ok().and_then(|v|v["earliest_cancellation_date"].as_str().map(str::to_owned));
                    resolution=Some(Resolution{id:id.into(),tenant:tenant.into(),verb:"observed".into(),resolver:"engine".into(),automatic:true,evidence:evidence.clone(),machine,earliest_date:date,observed_at});
                } else if machine.is_null() || machine["present"]==false && machine["external_id"]==*external && machine["account"]==account {
                    resolution=Some(Resolution{id:id.into(),tenant:tenant.into(),verb:"absent".into(),resolver:"engine".into(),automatic:true,evidence:json!({"search":evidence,"direct_absence":true,"setup_fee_charged":resources[external]["setup_fee_charged"],"first_unit_charged":resources[external]["first_unit_charged"]}),machine:Value::Null,earliest_date:None,observed_at});
                }
            }
        } else if authoritative && resources.is_empty() && !in_progress && age >= negative {
            resolution = Some(Resolution {
                id: id.into(),
                tenant: tenant.into(),
                verb: "absent".into(),
                resolver: "engine".into(),
                automatic: true,
                evidence: evidence.clone(),
                machine: Value::Null,
                earliest_date: None,
                observed_at: chrono::Utc::now(),
            });
        }
        if let Some(resolution) = resolution {
            let operation = resolution.id.clone();
            let verb = resolution.verb.clone();
            let affected = self
                .transaction(component, move |tx| {
                    let r = resolution.clone();
                    Box::pin(async move { tx.resolve(&r).await })
                })
                .await?;
            component.trace(json!({"event":"resolution_recorded","operation":operation,"resolution":verb,"affected":affected}));
        } else {
            // Positive, in-progress evidence is never a negative timeout.
            let release =
                age >= negative && !in_progress && (!authoritative || resources.len() != 1);
            let id = id.to_owned();
            let tenant = tenant.to_owned();
            self.transaction(component,move |tx|{let id=id.clone();let tenant=tenant.clone();let evidence=evidence.clone();Box::pin(async move{
                crate::ledger::serialize(tx,&[&tenant]).await?;
                let ids=evidence["provider_identifiers"].to_string();
                tx.rows("UPDATE provisiond.operations SET resolution_attempted_at=clock_timestamp(),error=jsonb_set(error,'{details}',COALESCE(error->'details','{}')||jsonb_build_object('reconciliation', $2::text::jsonb,'provider_identifiers',COALESCE(error->'details'->'provider_identifiers','{}')||jsonb_build_object('reconciliation',$3::text::jsonb))),updated_at=CASE WHEN error->'details'->'reconciliation' IS DISTINCT FROM $2::text::jsonb THEN clock_timestamp() ELSE updated_at END,revision=revision+CASE WHEN error->'details'->'reconciliation' IS DISTINCT FROM $2::text::jsonb THEN 1 ELSE 0 END WHERE id=$1::text::uuid AND status='needs_reconciliation' AND resolution IS NULL RETURNING id",&[&id,&evidence.to_string(),&ids]).await?;
                if release {tx.release_negative(&id,&tenant).await?;}
                Ok(())
            })}).await?;
        }
        Ok(())
    }
}
fn safe_identity(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-:".contains(&b))
}
