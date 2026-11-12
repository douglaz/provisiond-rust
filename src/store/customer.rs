//! WIR-11/15/16/25/26/27, API-54: read-only customer projections.
use super::{CloseUnlessFinished, DiscardUnlessFinished, Store};
use anyhow::{Context, Result};
use serde_json::Value;
use tokio_postgres::types::ToSql;
impl super::Session {
    pub(crate) async fn value(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Value> {
        let mut guard = CloseUnlessFinished(Some(self));
        let result =
            tokio::time::timeout(self.settings.call_bound, self.client.query(sql, params)).await;
        guard.0 = None;
        match result {
            Ok(rows) => {
                let rows = rows?;
                let text: String = rows.first().context("missing read result")?.get(0);
                Ok(serde_json::from_str(&text)?)
            }
            Err(e) => {
                self.close();
                Err(e).context("read call deadline")
            }
        }
    }
}
impl Store {
    pub(crate) async fn value(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Value> {
        self.read_session().await?.value(sql, params).await
    }
    /// STO-7: even a replay body must pass a newly sampled same-session check.
    pub(crate) async fn destination_value(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Value> {
        let mut last = None;
        for attempt in 1..=2 {
            let mut held = DiscardUnlessFinished(Some(self.read_session().await?));
            let s = held.0.as_ref().expect("session");
            let result=async{
                let v=s.value(sql,params).await?;
                if !v.is_null() && v.get("resource_kind").is_none_or(|kind|kind=="deposit") {
                    (self.trace)(serde_json::json!({"event":"destination_read","session":s.pid,"attempt":attempt}));
                    s.check("post_read").await?;
                }
                anyhow::Ok(v)
            }.await;
            let s = held.0.take().expect("session");
            match result {
                Ok(v) => {
                    if !v.is_null() && v.get("resource_kind").is_none_or(|kind| kind == "deposit") {
                        (self.trace)(
                            serde_json::json!({"event":"destination_exposed","session":s.pid,"attempt":attempt}),
                        );
                    }
                    return Ok(v);
                }
                Err(e) => {
                    drop(deadpool::managed::Object::take(s));
                    last = Some(e);
                }
            }
        }
        Err(last.expect("two attempts"))
    }
}
impl super::Transaction {
    /// OPS-43 step 8, API-63: both contending rows precede the operation claim guard.
    pub(crate) async fn license_order(&mut self, c: &super::Claim) -> Result<bool> {
        anyhow::ensure!(
            self.engine_fenced,
            "engine-only transaction requires generation fence"
        );
        let account: String = self
            .rows(
                "SELECT provider_account FROM provisiond.operations WHERE id=$1::text::uuid",
                &[&c.id],
            )
            .await?
            .first()
            .context("operation missing")?
            .get(0);
        let rows=self.rows("SELECT status FROM provisiond.provider_account_status WHERE provider_account=$1 FOR UPDATE",&[&account]).await?;
        let status = rows
            .first()
            .map(|r| r.get::<_, String>(0))
            .unwrap_or_else(|| "account_unreachable".into());
        if self.rows("UPDATE provisiond.provider_account_status SET status=status WHERE provider_account=$1 AND status='healthy' RETURNING provider_account",&[&account]).await?.is_empty(){return Err(crate::wire::Error::new("conflict","provider account unavailable",status!="terminated",serde_json::json!({"reason":if status=="terminated"{"account_terminated"}else{"state"}})).into());}
        if self.rows("UPDATE provisiond.tenants SET status=status WHERE id=$1 AND status<>'suspended' RETURNING id",&[&c.tenant]).await?.is_empty(){return Err(crate::wire::Error::conflict("tenant_suspended").into());}
        let rows=self.rows("UPDATE provisiond.operations SET correlator_kind=COALESCE(correlator_kind,'operation_uuid'),correlator_value=COALESCE(correlator_value,id::text),request_summary=jsonb_set(request_summary,'{correlator}',jsonb_build_object('kind',COALESCE(correlator_kind,'operation_uuid'),'value',COALESCE(correlator_value,id::text))),about_to_order_at=COALESCE(about_to_order_at,transaction_timestamp()),revision=revision+CASE WHEN about_to_order_at IS NULL THEN 1 ELSE 0 END,updated_at=CASE WHEN about_to_order_at IS NULL THEN clock_timestamp() ELSE updated_at END WHERE id=$1::text::uuid AND status='running' AND kind='create' AND claim_number=$2 RETURNING id",&[&c.id,&c.number]).await?;
        Ok(!rows.is_empty())
    }
}
impl super::Transaction {
    /// OPS-27/WIR-11/STO-48: persist the read's instant with the verified state.
    /// LDG-38/39/72: the first seed separately uses this billable creation's instant.
    pub(crate) async fn attach_details(
        &mut self,
        c: &super::Claim,
        machine: &Value,
        observed_at: &chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        let rows=self.rows("SELECT o.machine_id::text,o.request_summary::text,m.first_unit_debit_id IS NOT NULL,m.state_observed_at IS NOT NULL,COALESCE(t.status,'active') FROM provisiond.operations o JOIN provisiond.machines m ON m.id=o.machine_id LEFT JOIN provisiond.tenants t ON t.id=o.tenant_id WHERE o.id=$1::text::uuid AND o.status='succeeded'",&[&c.id]).await?;
        let Some(row) = rows.first() else {
            return Ok(());
        };
        if row.get::<_, bool>(3) {
            return Ok(());
        }
        let id: String = row.get(0);
        let summary: Value = serde_json::from_str(&row.get::<_, String>(1))?;
        let terms = &summary["offer"]["accepted_terms"];
        let offer = &terms["offer"];
        let rate: Value = serde_json::from_str(
            &self
                .rows(
                    "SELECT COALESCE((SELECT jsonb_build_object('value',value,'available',available) FROM provisiond.rates WHERE currency=$1),'null'::jsonb)::text",
                    &[&offer["currency"].as_str()],
                )
                .await?[0]
                .get::<_, String>(0),
        )?;
        let descriptor: Value = serde_json::from_str(
            &self
                .rows(
                    "SELECT descriptor::text FROM provisiond.provider_accounts WHERE account=$1",
                    &[&summary["provider_account"].as_str()],
                )
                .await?[0]
                .get::<_, String>(0),
        )?;
        // LDG-33/40: settle the accepted effect even without a current rate,
        // but halt re-derivation rather than publishing a stale-rate runway.
        let price = (rate["available"] == true)
            .then(|| {
                crate::pricing::calculate(
                    offer,
                    &descriptor,
                    &rate["value"],
                    crate::pricing::wind_seconds(&descriptor)?,
                )
            })
            .transpose()?;
        let customer_num = price.as_ref().map(|p| p.per_second_num);
        let customer_den = price.as_ref().map(|p| p.per_second_den);
        let protected = price.as_ref().map(|p| p.wind_sats);
        let seconds = summary["offer"]["billing_unit_seconds"]
            .as_i64()
            .context("unit seconds")?;
        let seed_seconds = if row.get::<_, bool>(2) { seconds } else { 0 };
        let observed_at = observed_at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        self.rows("UPDATE provisiond.machines SET name=$2,region=$3,public_ips=$4::text::jsonb,state=$5,state_observed_at=$11::text::timestamptz,correlator_kind='operation_uuid',correlator_value=$6,customer_rate_num=$7,customer_rate_den=$8,protected_sats=$9,meter_seed_at=transaction_timestamp()+$10::bigint*interval '1 second' WHERE id=$1::text::uuid AND state_observed_at IS NULL RETURNING id",&[&id,&machine["name"].as_str(),&machine["region"].as_str(),&machine["public_ips"].to_string(),&machine["state"].as_str().unwrap_or("unknown"),&c.id,&customer_num,&customer_den,&protected,&seed_seconds,&observed_at]).await?;
        self.rows("INSERT INTO provisiond.meter_totals(subject_kind,subject_id,billing_period,high_water_mark) SELECT 'machine',id,to_char(transaction_timestamp() AT TIME ZONE 'UTC','YYYY-MM'),meter_seed_at FROM provisiond.machines WHERE id=$1::text::uuid ON CONFLICT DO NOTHING RETURNING subject_id",&[&id]).await?;
        if price.is_some() {
            self.rows("UPDATE provisiond.machines m SET runway_until=greatest(transaction_timestamp(),meter_seed_at)+CASE WHEN customer_rate_num>0 THEN floor(greatest(0,c.reserved_sats-protected_sats)::numeric*customer_rate_den/customer_rate_num)::bigint ELSE 0 END*interval '1 second' FROM provisiond.commitments c WHERE m.id=$1::text::uuid AND c.machine_id=m.id AND c.state='open' RETURNING m.id",&[&id]).await?;
        }
        if row.get::<_, String>(4) == "suspended" {
            self.enqueue_cleanup(&id, "tenant_suspended").await?;
        }
        Ok(())
    }
}

#[cfg(feature = "store-test-hooks")]
impl Store {
    pub(crate) async fn test_overload_entry(
        &self,
        correlation: &str,
        key_digest: &str,
    ) -> Result<()> {
        let session = self.read_session().await?;
        session
            .query("BEGIN READ WRITE /* isolated overload sensitivity control */")
            .await?;
        (self.trace)(
            serde_json::json!({"event":"http_business_begin","correlation_id":correlation,"key_digest":key_digest,"session":session.pid,"defect":true}),
        );
        session.query("ROLLBACK").await?;
        Ok(())
    }
}
