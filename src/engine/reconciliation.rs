//! OPS-27/28/29/30/32/33: read-only provider evidence, retained across store retries.
use super::*;
use crate::store::Resolution;

impl Engine {
    /// OPS-47/50/49: supervised periodic work uses generation-first transactions
    /// and refuses a new pass after detected lifetime-lock loss.
    pub async fn reconcile(&self, d: &Dispatcher) -> Result<()> {
        self.database.ensure_new_work()?;
        let component = self.component();
        let pending=self.transaction(&component,|tx|Box::pin(async move {
            let rows=tx.rows("SELECT COALESCE(jsonb_agg(to_jsonb(o)||jsonb_build_object('wait_ended',COALESCE(o.mutation_wait_ended_at,o.updated_at),'effective_visibility',greatest(COALESCE((a.descriptor->>'visibility_seconds')::numeric,1),COALESCE((SELECT max(extract(epoch FROM p.observed_at-p.dispatched_at)) FROM provisiond.provider_observations p WHERE p.provider_account=o.provider_account),0)))),'[]')::text FROM provisiond.operations o JOIN provisiond.provider_accounts a ON a.account=o.provider_account WHERE o.status='needs_reconciliation' AND o.resolution IS NULL",&[]).await?;
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
                                    if resource["correlator"] == row["correlator_value"]
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
        let evidence = json!({"source":"all_channel_search","authoritative":authoritative,"matches":resources.len(),"provider_identifiers":identifiers,"in_progress":in_progress});
        component.trace(json!({"event":"resolution_search","operation":id,"evidence":evidence}));
        let mut resolution = None;
        if authoritative && resources.len() == 1 && !in_progress {
            let external = resources.keys().next().context("matched resource")?;
            if let Ok(machine)=d.client.call(component,descriptor,"/get",&json!({"account":account,"operation":id,"external_id":external,"resolution":true})).await {
                let observed_at=chrono::Utc::now();
                if machine["present"]==true && machine["external_id"]==*external && machine["account"]==account && machine["correlator"]==row["correlator_value"] {
                    let date=d.client.call(component,descriptor,"/constraints",&json!({"account":account,"operation":id,"external_id":external,"resolution":true})).await.ok().and_then(|v|v["earliest_cancellation_date"].as_str().map(str::to_owned));
                    resolution=Some(Resolution{id:id.into(),tenant:tenant.into(),verb:"observed".into(),resolver:"engine".into(),evidence:evidence.clone(),machine,earliest_date:date,observed_at});
                } else if machine.is_null() || machine["present"]==false && machine["external_id"]==*external && machine["account"]==account {
                    resolution=Some(Resolution{id:id.into(),tenant:tenant.into(),verb:"absent".into(),resolver:"engine".into(),evidence:json!({"search":evidence,"direct_absence":true,"setup_fee_charged":resources[external]["setup_fee_charged"],"first_unit_charged":resources[external]["first_unit_charged"]}),machine:Value::Null,earliest_date:None,observed_at});
                }
            }
        } else if authoritative && resources.is_empty() && !in_progress && age >= negative {
            resolution = Some(Resolution {
                id: id.into(),
                tenant: tenant.into(),
                verb: "absent".into(),
                resolver: "engine".into(),
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
            let release = age >= negative && !in_progress;
            let id = id.to_owned();
            let tenant = tenant.to_owned();
            self.transaction(component,move |tx|{let id=id.clone();let tenant=tenant.clone();Box::pin(async move{
                if release {tx.release_negative(&id,&tenant).await?;} else {
                    tx.rows("UPDATE provisiond.operations SET resolution_attempted_at=clock_timestamp() WHERE id=$1::text::uuid AND status='needs_reconciliation' AND resolution IS NULL RETURNING id",&[&id]).await?;
                }
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
