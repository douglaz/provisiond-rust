//! OPS-27/29/30/33/36, STO-3/19: evidence transitions, never worker claims.
use super::{Claim, Transaction};
use crate::{ledger, pricing};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

#[derive(Clone)]
pub(crate) struct Resolution {
    pub id: String,
    pub tenant: String,
    pub verb: String,
    pub resolver: String,
    pub evidence: Value,
    pub machine: Value,
    pub earliest_date: Option<String>,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}
impl Transaction {
    /// STO-52/OPS-39: insert/index arbitration is the dedup; joining adds a set member.
    pub(crate) async fn enqueue_cleanup(&mut self, machine: &str, reason: &str) -> Result<String> {
        let rows=self.rows("INSERT INTO provisiond.episodes(machine_id,key,reasons,state) VALUES($1::text::uuid,'delete',jsonb_build_array($2::text),'attempting') ON CONFLICT(machine_id,key) WHERE state<>'closed' DO NOTHING RETURNING id::text",&[&machine,&reason]).await?;
        let Some(row) = rows.first() else {
            return Ok(self.rows("UPDATE provisiond.episodes SET reasons=CASE WHEN reasons ? $2 THEN reasons ELSE reasons||jsonb_build_array($2::text) END WHERE machine_id=$1::text::uuid AND key='delete' AND state<>'closed' RETURNING id::text",&[&machine,&reason]).await?.first().context("open cleanup episode")?.get(0));
        };
        let episode: String = row.get(0);
        let id: String = self.rows("INSERT INTO provisiond.operations(tenant_id,idempotency_key,kind,status,machine_id,provider_account,request,request_summary,correlation_id,requested_by,system_reason,episode_id) SELECT tenant_id,$2,'delete','queued',id,provider_account,'{}',jsonb_build_object('kind','delete','machine_id',id,'provider_account',provider_account,'external_id',external_id),gen_random_uuid()::text,'system',$3,$4::text::uuid FROM provisiond.machines WHERE id=$1::text::uuid RETURNING id::text",&[&machine,&format!("cleanup:{episode}"),&reason,&episode]).await?[0].get(0);
        self.rows("UPDATE provisiond.episodes SET current_operation_id=$2::text::uuid WHERE id=$1::text::uuid RETURNING id",&[&episode,&id]).await?;
        Ok(episode)
    }
    /// OPS-33: release money only. STO-58's exclusion survives an unresolved outcome.
    pub(crate) async fn release_negative(&mut self, id: &str, tenant: &str) -> Result<bool> {
        ensure!(
            self.engine_fenced,
            "engine resolution requires generation fence"
        );
        ledger::serialize(self, &[tenant]).await?;
        let rows=self.rows("UPDATE provisiond.operations SET resolution_attempted_at=clock_timestamp() WHERE id=$1::text::uuid AND tenant_id=$2 AND status='needs_reconciliation' AND kind='create' AND resolution IS NULL RETURNING commitment_id::text",&[&id,&tenant]).await?;
        if let Some(row) = rows.first() {
            self.rows("UPDATE provisiond.commitments SET state='closed',reserved_sats=0,closed_at=clock_timestamp(),version=version+1 WHERE id=$1::text::uuid AND state='open' RETURNING id",&[&row.get::<_,Option<String>>(0)]).await?;
            return Ok(true);
        }
        Ok(false)
    }
    /// OPS-27/STO-19: caller already owns an engine fence or API receipt transaction.
    /// A lost-reply repeat short-circuits before money; no claim-number predicate.
    pub(crate) async fn resolve(&mut self, r: &Resolution) -> Result<bool> {
        let result = self.resolve_inner(r).await;
        self.failed |= result.is_err();
        result
    }
    async fn resolve_inner(&mut self, r: &Resolution) -> Result<bool> {
        ledger::serialize(self, &[&r.tenant]).await?;
        let status = match r.verb.as_str() {
            "observed" | "applied" => "succeeded",
            "absent" | "not_applied" | "abandoned" => "failed",
            _ => anyhow::bail!("unsupported resolution"),
        };
        let result = if r.verb == "observed" {
            json!({"external_id":r.machine["external_id"]})
        } else {
            Value::Null
        };
        let rows=self.rows("UPDATE provisiond.operations SET status=$3,resolution=$4,resolved_by=$5,resolution_evidence=$6::text::jsonb,resolved_at=transaction_timestamp(),result=$7::text::jsonb,revision=revision+1,updated_at=clock_timestamp() WHERE id=$1::text::uuid AND tenant_id=$2 AND status='needs_reconciliation' AND resolution IS NULL AND ((kind='create' AND $4 IN ('observed','absent','abandoned')) OR (kind='delete' AND $4 IN ('applied','not_applied','abandoned'))) RETURNING kind,provider_account,commitment_id::text,request_summary::text",&[&r.id,&r.tenant,&status,&r.verb,&r.resolver,&r.evidence.to_string(),&result.to_string()]).await?;
        let Some(row) = rows.first() else {
            return Ok(false);
        };
        let kind: String = row.get(0);
        ensure!(kind == "create", "non-create resolution not yet supported");
        let commitment: Option<String> = row.get(2);
        let summary: Value = serde_json::from_str(&row.get::<_, String>(3))?;
        if r.verb == "observed" {
            let external = r.machine["external_id"]
                .as_str()
                .context("observed identity")?;
            let c = Claim {
                id: r.id.clone(),
                tenant: r.tenant.clone(),
                kind,
                number: 0,
            };
            self.land_create(&c, external, r.earliest_date.as_deref())
                .await?;
            self.attach_details(&c, &r.machine, &r.observed_at).await?;
            let released = self
                .rows(
                    "SELECT state FROM provisiond.commitments WHERE id=$1::text::uuid",
                    &[&commitment],
                )
                .await?[0]
                .get::<_, String>(0)
                != "open";
            if released {
                self.late_attach(&r.id, &r.tenant, &summary).await?;
            }
        } else {
            self.release_create(&r.id, commitment.as_deref()).await?;
            // LDG-39/67: abandonment absorbs both obligations. Absence requires
            // independent charged-fee/unit evidence, each considered separately.
            for (field, cause) in [
                ("setup_fee", "unrecoverable_setup_fee"),
                ("first_unit", "unrecoverable_first_unit"),
            ] {
                if r.verb == "abandoned" || r.evidence[format!("{field}_charged")] == true {
                    let charge = &summary[field];
                    if charge["native_minor"].as_i64().unwrap_or(0) > 0 {
                        self.order_deficiency(r, row.get::<_, String>(1).as_str(), cause, charge)
                            .await?;
                    }
                }
            }
        }
        if r.verb == "observed" {
            self.record_visibility(&r.id, &r.observed_at).await?;
        }
        Ok(true)
    }
    pub(crate) async fn record_visibility(
        &mut self,
        id: &str,
        observed: &chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        self.rows("INSERT INTO provisiond.provider_observations(provider_account,operation_id,dispatched_at,observed_at) SELECT provider_account,id,COALESCE(about_to_order_at,write_started_at),$2::text::timestamptz FROM provisiond.operations WHERE id=$1::text::uuid AND COALESCE(about_to_order_at,write_started_at) IS NOT NULL AND COALESCE(about_to_order_at,write_started_at)<=$2::text::timestamptz ON CONFLICT DO NOTHING RETURNING operation_id",&[&id,&observed.to_rfc3339()]).await?;
        Ok(())
    }
    async fn order_deficiency(
        &mut self,
        r: &Resolution,
        account: &str,
        cause: &str,
        charge: &Value,
    ) -> Result<()> {
        // An absent order has no machine. Keep its operation as the subject.
        self.rows("INSERT INTO provisiond.operator_deficiencies(provider_account,machine_id,subject_kind,subject_id,absorbed_seconds,operation_id,cause,native_minor,currency,clamped_sats,rate_num,rate_den,idempotency_key) SELECT $1,NULL,'operation',$2::text::uuid,0,$2::text::uuid,$3,$4,$5,$6,(value->>'rate_num')::bigint,(value->>'rate_den')::bigint,$7 FROM (VALUES(1)) one(n) LEFT JOIN provisiond.rates ON currency=$5 AND available ON CONFLICT(idempotency_key) DO NOTHING RETURNING id",&[&account,&r.id,&cause,&charge["native_minor"].as_i64(),&charge["currency"].as_str(),&charge["sats"].as_i64(),&format!("{cause}:{}",r.id)]).await?;
        Ok(())
    }
    async fn late_attach(&mut self, id: &str, tenant: &str, summary: &Value) -> Result<()> {
        let machine: String = self
            .rows(
                "SELECT machine_id::text FROM provisiond.operations WHERE id=$1::text::uuid",
                &[&id],
            )
            .await?[0]
            .get(0);
        let account = summary["provider_account"]
            .as_str()
            .context("provider account")?;
        let offer = &summary["offer"]["accepted_terms"]["offer"];
        let descriptor: Value = serde_json::from_str(
            &self
                .rows(
                    "SELECT descriptor::text FROM provisiond.provider_accounts WHERE account=$1",
                    &[&account],
                )
                .await?[0]
                .get::<_, String>(0),
        )?;
        let rate = self
            .rows(
                "SELECT value::text FROM provisiond.rates WHERE currency=$1 AND available",
                &[&offer["currency"].as_str()],
            )
            .await?;
        let (_, available) = ledger::balance(self, tenant).await?;
        let floor = rate
            .first()
            .map(|row| -> Result<_> {
                let rate = serde_json::from_str(&row.get::<_, String>(0))?;
                pricing::calculate(
                    offer,
                    &descriptor,
                    &rate,
                    pricing::wind_seconds(&descriptor)?,
                )
            })
            .transpose()?;
        if let Some(price) = floor.filter(|p| p.wind_sats <= available) {
            self.rows("INSERT INTO provisiond.commitments(tenant_id,machine_id,operation_id,reserved_sats,state) VALUES($1,$2::text::uuid,$3::text::uuid,$4,'open') RETURNING id",&[&tenant,&machine,&id,&price.wind_sats]).await?;
        } else {
            let basis = &summary["offer"]["accepted_terms"]["rate"];
            let native = pricing::minor(
                offer["recurring_price"].as_str().context("price")?,
                basis["minor_scale"].as_u64().context("precision")? as u32,
            )?;
            let wind = pricing::wind_seconds(&descriptor)?;
            let period = offer["period_seconds"].as_i64().context("period")?;
            let cost = i64::try_from(
                (i128::from(native) * i128::from(wind) + i128::from(period) - 1)
                    / i128::from(period),
            )?;
            self.rows("INSERT INTO provisiond.operator_deficiencies(provider_account,machine_id,subject_kind,subject_id,absorbed_seconds,operation_id,cause,native_minor,currency,clamped_sats,rate_num,rate_den,idempotency_key) SELECT $1,$2::text::uuid,'machine',$2::text::uuid,0,$3::text::uuid,'late_attach_cleanup',$4,$5,0,(value->>'rate_num')::bigint,(value->>'rate_den')::bigint,$6 FROM (VALUES(1)) one(n) LEFT JOIN provisiond.rates ON currency=$5 AND available ON CONFLICT(idempotency_key) DO NOTHING RETURNING id",&[&account,&machine,&id,&cost,&offer["currency"].as_str(),&format!("late_attach_cleanup:{id}")]).await?;
        }
        self.enqueue_cleanup(&machine, "late_attach_cleanup")
            .await?;
        ledger::balance(self, tenant).await?;
        Ok(())
    }
}
