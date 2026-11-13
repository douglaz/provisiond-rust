//! OPS-11/41/42/45/48, LDG-35/38/65/74: cancellation and gone-write transactions.
use super::{Claim, Transaction};
use crate::{
    ledger::{self, Entry},
    pricing,
    wire::Error,
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Datelike, Utc};
use serde_json::{Value, json};

impl Transaction {
    /// OPS-48: a closed episode is permanent; only the current attempt drives it.
    pub(crate) async fn delete_episode_state(
        &mut self,
        id: &str,
        status: &str,
        close: Option<&str>,
    ) -> Result<()> {
        if let Some(reason) = close {
            self.rows("WITH closed AS (UPDATE provisiond.episodes e SET state='closed',closed_at=transaction_timestamp(),close_reason=$2 FROM provisiond.operations o WHERE o.id=$1::text::uuid AND e.id=o.episode_id AND e.state<>'closed' RETURNING e.id,e.machine_id) UPDATE provisiond.machines m SET destroy_committed=NULL FROM closed c WHERE m.id=c.machine_id AND m.destroy_committed=c.id RETURNING m.id",&[&id,&reason]).await?;
        } else {
            self.rows("UPDATE provisiond.episodes e SET state=CASE WHEN $2='needs_reconciliation' THEN 'uncertain' ELSE 'stalled' END FROM provisiond.operations o WHERE o.id=$1::text::uuid AND e.id=o.episode_id AND e.current_operation_id=o.id AND e.state<>'closed' AND $2 IN ('failed','needs_reconciliation') RETURNING e.id",&[&id,&status]).await?;
        }
        Ok(())
    }
    /// OPS-41's order is decided on this transaction's reads before capability.
    pub(crate) async fn delete_decision(&mut self, c: &Claim) -> Result<String> {
        ensure!(
            self.engine_fenced,
            "delete worker requires generation fence"
        );
        ensure!(c.kind == "delete", "delete outcome requires delete claim");
        ledger::serialize(self, &[&c.tenant]).await?;
        let rows=self.rows("SELECT o.machine_id::text,o.episode_id::text,o.requested_by,m.state,m.destroy_committed::text,e.state,COALESCE(t.status,'active') FROM provisiond.operations o JOIN provisiond.machines m ON m.id=o.machine_id LEFT JOIN provisiond.episodes e ON e.id=o.episode_id LEFT JOIN provisiond.tenants t ON t.id=o.tenant_id WHERE o.id=$1::text::uuid AND o.status='running' AND o.claim_number=$2 FOR UPDATE OF o,m",&[&c.id,&c.number]).await?;
        let Some(row) = rows.first() else {
            return Ok("finished".into());
        };
        let machine: String = row.get(0);
        let episode: Option<String> = row.get(1);
        let gone = row.get::<_, String>(3) == "deleted";
        if gone || row.get::<_, Option<String>>(5).as_deref() == Some("closed") {
            self.settle_inner(
                c,
                "succeeded",
                &json!({"no_mutation_required":true}),
                &Value::Null,
            )
            .await?;
            return Ok("finished".into());
        }
        if let Some(episode) = episode {
            let retry = row.get::<_, String>(2) == "operator"
                && row.get::<_, Option<String>>(4).as_deref() == Some(&episode);
            let suspended = row.get::<_, String>(6) == "suspended";
            if !retry && !suspended {
                let Some(funded) = self.derive_runway(&machine).await? else {
                    return Ok("no_rate".into());
                };
                if funded {
                    self.settle_inner(
                        c,
                        "succeeded",
                        &json!({"no_mutation_required":true}),
                        &Value::Null,
                    )
                    .await?;
                    self.delete_episode_state(&c.id, "succeeded", Some("funded"))
                        .await?;
                    return Ok("finished".into());
                }
            }
            let fence=self.rows("UPDATE provisiond.machines m SET destroy_committed=$2::text::uuid WHERE id=$1::text::uuid AND (destroy_committed IS NULL OR destroy_committed=$2::text::uuid) AND EXISTS(SELECT 1 FROM provisiond.episodes WHERE id=$2::text::uuid AND state<>'closed') RETURNING id",&[&machine,&episode]).await?;
            if fence.is_empty() {
                self.settle_inner(
                    c,
                    "succeeded",
                    &json!({"no_mutation_required":true}),
                    &Value::Null,
                )
                .await?;
                self.delete_episode_state(&c.id, "succeeded", Some("funded"))
                    .await?;
                return Ok("finished".into());
            }
        }
        Ok("dispatch".into())
    }
    /// LDG-33/65: no rate leaves the stored date unchanged, never a stale reprice.
    pub(crate) async fn derive_runway(&mut self, machine: &str) -> Result<Option<bool>> {
        let rows=self.rows("SELECT m.native_terms::text,p.descriptor::text,r.value::text FROM provisiond.machines m JOIN provisiond.provider_accounts p ON p.account=m.provider_account JOIN provisiond.rates r ON r.currency=m.native_terms->'offer'->>'currency' AND r.available WHERE m.id=$1::text::uuid",&[&machine]).await?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let terms: Value = serde_json::from_str(&row.get::<_, String>(0))?;
        let account: String = self
            .rows(
                "SELECT provider_account FROM provisiond.machines WHERE id=$1::text::uuid",
                &[&machine],
            )
            .await?[0]
            .get(0);
        let descriptor = self.effective_descriptor(&account).await?;
        let rate = serde_json::from_str(&row.get::<_, String>(2))?;
        let price = pricing::calculate(
            &terms["offer"],
            &descriptor,
            &rate,
            pricing::wind_seconds(&descriptor)?,
        )?;
        let funded=self.rows("UPDATE provisiond.machines m SET customer_rate_num=$2::bigint,customer_rate_den=$3::bigint,protected_sats=$4::bigint,runway_until=greatest(transaction_timestamp(),COALESCE((SELECT max(high_water_mark) FROM provisiond.meter_totals WHERE subject_kind='machine' AND subject_id=m.id),transaction_timestamp()))+CASE WHEN $2::bigint>0 THEN floor(greatest(0,COALESCE((SELECT reserved_sats FROM provisiond.commitments WHERE machine_id=m.id AND state='open'),0)-$4::bigint)::numeric*$3::bigint/$2::bigint)::bigint ELSE 0 END*interval '1 second' WHERE m.id=$1::text::uuid RETURNING runway_until>transaction_timestamp()",&[&machine,&price.per_second_num,&price.per_second_den,&price.wind_sats]).await?[0].get(0);
        Ok(Some(funded))
    }
    /// OPS-45: acknowledge this write-once marker before dispatch; no null guard.
    pub(crate) async fn license_delete(&mut self, c: &Claim) -> Result<bool> {
        ensure!(
            self.engine_fenced,
            "delete worker requires generation fence"
        );
        ensure!(c.kind == "delete", "delete outcome requires delete claim");
        let account=self.rows("SELECT s.status FROM provisiond.operations o JOIN provisiond.provider_account_status s ON s.provider_account=o.provider_account WHERE o.id=$1::text::uuid",&[&c.id]).await?;
        if account
            .first()
            .is_none_or(|r| r.get::<_, String>(0) != "healthy")
        {
            return Err(Error::new(
                "conflict",
                "provider account unavailable",
                false,
                json!({"reason":"state"}),
            )
            .into());
        }
        Ok(!self.rows("UPDATE provisiond.operations SET write_started_at=COALESCE(write_started_at,transaction_timestamp()),request_summary=request_summary||jsonb_build_object('write_started_at',COALESCE(write_started_at,transaction_timestamp()),'rescue_exited_cleanly',rescue_exited_cleanly),revision=revision+CASE WHEN write_started_at IS NULL THEN 1 ELSE 0 END WHERE id=$1::text::uuid AND status='running' AND claim_number=$2 AND kind='delete' RETURNING id",&[&c.id,&c.number]).await?.is_empty())
    }
    pub(crate) async fn delete_failure(&mut self, c: &Claim, error: &Value) -> Result<bool> {
        ensure!(
            self.engine_fenced,
            "delete worker requires generation fence"
        );
        ensure!(c.kind == "delete", "delete outcome requires delete claim");
        ledger::serialize(self, &[&c.tenant]).await?;
        let row=self.rows("SELECT write_started_at IS NOT NULL,rescue_exited_cleanly IS NOT DISTINCT FROM false FROM provisiond.operations WHERE id=$1::text::uuid",&[&c.id]).await?;
        let row = row.first().context("delete attempt")?;
        let ambiguous = error["details"]["accepted"] == true
            || (row.get::<_, bool>(0) || row.get::<_, bool>(1))
                && (matches!(
                    error["kind"].as_str(),
                    Some("network" | "timeout" | "internal")
                ) || error["kind"] == "provider"
                    && error["details"]["status"].as_u64().is_none_or(|n| n >= 500)
                    || error["kind"] == "conflict" && error["details"]["reason"] == "cut_off");
        let mut error = error.clone();
        if ambiguous {
            error["retryable"] = json!(false);
        }
        self.settle_inner(
            c,
            if ambiguous {
                "needs_reconciliation"
            } else {
                "failed"
            },
            &Value::Null,
            &error,
        )
        .await
    }
    pub(crate) async fn settle_delete_gone(
        &mut self,
        c: &Claim,
        observed: &DateTime<Utc>,
    ) -> Result<bool> {
        ensure!(
            self.engine_fenced,
            "delete worker requires generation fence"
        );
        ensure!(c.kind == "delete", "delete outcome requires delete claim");
        if !self
            .settle_inner(c, "succeeded", &json!({"gone":true}), &Value::Null)
            .await?
        {
            return Ok(false);
        }
        let machine: String = self
            .rows(
                "SELECT machine_id::text FROM provisiond.operations WHERE id=$1::text::uuid",
                &[&c.id],
            )
            .await?[0]
            .get(0);
        self.record_gone(&machine, observed).await?;
        self.record_visibility(&c.id, observed).await?;
        Ok(true)
    }
    /// API-64/68: all effects and the caller's receipt share one transaction.
    pub(crate) async fn episode_action(
        &mut self,
        id: &str,
        tenant: &str,
        verb: &str,
        principal: &str,
        body: &Value,
    ) -> Result<()> {
        ledger::serialize(self, &[tenant]).await?;
        let rows=self.rows("UPDATE provisiond.episodes SET state=CASE WHEN $2='retry' THEN 'attempting' ELSE 'closed' END,closed_at=CASE WHEN $2='keep' THEN transaction_timestamp() ELSE NULL END,close_reason=CASE WHEN $2='keep' THEN 'kept' ELSE NULL END,kept_by=CASE WHEN $2='keep' THEN $3 ELSE NULL END,keep_evidence=CASE WHEN $2='keep' THEN $4::text::jsonb ELSE NULL END WHERE id=$1::text::uuid AND state='stalled' RETURNING machine_id::text",&[&id,&verb,&principal,&body.to_string()]).await?;
        let row = rows.first().ok_or_else(|| Error::conflict("state"))?;
        let machine: String = row.get(0);
        if verb == "keep" {
            self.rows("UPDATE provisiond.machines SET destroy_committed=NULL WHERE id=$1::text::uuid AND destroy_committed=$2::text::uuid RETURNING id",&[&machine,&id]).await?;
            self.derive_runway(&machine).await?;
        } else {
            let op:String=self.rows("INSERT INTO provisiond.operations(tenant_id,idempotency_key,kind,status,machine_id,provider_account,request,request_summary,correlation_id,requested_by,episode_id) SELECT tenant_id,gen_random_uuid()::text,'delete','queued',id,provider_account,'{}',jsonb_build_object('kind','delete','machine_id',id,'provider_account',provider_account,'external_id',external_id),gen_random_uuid()::text,'operator',$2::text::uuid FROM provisiond.machines WHERE id=$1::text::uuid RETURNING id::text",&[&machine,&id]).await?[0].get(0);
            self.rows("UPDATE provisiond.episodes SET current_operation_id=$2::text::uuid WHERE id=$1::text::uuid RETURNING id",&[&id,&op]).await?;
        }
        Ok(())
    }
    /// LDG-38/74, OPS-48: every gone writer closes its elapsed tail BEFORE release.
    pub(crate) async fn record_gone(
        &mut self,
        machine: &str,
        observed: &DateTime<Utc>,
    ) -> Result<()> {
        let rows=self.rows("SELECT tenant_id,state,meter_stopped_at IS NOT NULL FROM provisiond.machines WHERE id=$1::text::uuid",&[&machine]).await?;
        let row = rows.first().context("gone machine")?;
        let tenant: String = row.get(0);
        ledger::serialize(self, &[&tenant]).await?;
        if self.rows("SELECT meter_stopped_at IS NOT NULL FROM provisiond.machines WHERE id=$1::text::uuid",&[&machine]).await?[0].get::<_,bool>(0) {return Ok(())}
        self.exit_increment(machine, &tenant, observed).await?;
        self.rows("UPDATE provisiond.machines SET state='deleted',state_observed_at=$2::text::timestamptz,meter_stopped_at=$2::text::timestamptz,destroy_committed=NULL,updated_at=clock_timestamp() WHERE id=$1::text::uuid AND meter_stopped_at IS NULL RETURNING id",&[&machine,&observed.to_rfc3339()]).await?;
        self.rows("UPDATE provisiond.episodes SET state='closed',close_reason='resource_gone',closed_at=transaction_timestamp() WHERE machine_id=$1::text::uuid AND state<>'closed' RETURNING id",&[&machine]).await?;
        self.rows("UPDATE provisiond.commitments SET state='closed',reserved_sats=0,version=version+1,closed_at=clock_timestamp() WHERE machine_id=$1::text::uuid AND state='open' RETURNING id",&[&machine]).await?;
        Ok(())
    }
    async fn exit_increment(
        &mut self,
        machine: &str,
        tenant: &str,
        stop: &DateTime<Utc>,
    ) -> Result<()> {
        let row=self.rows("SELECT native_terms::text,meter_quarantined FROM provisiond.machines WHERE id=$1::text::uuid",&[&machine]).await?.remove(0);
        let terms: Value = row
            .get::<_, Option<String>>(0)
            .map(|v| serde_json::from_str(&v))
            .transpose()?
            .unwrap_or(Value::Null);
        if row.get::<_, bool>(1) {
            // LDG-72: preserve meter evidence; only an already-open outage may close.
            let outages=self.rows("SELECT id::text,absorbed_from::text FROM provisiond.operator_deficiencies WHERE subject_kind='machine' AND subject_id=$1::text::uuid AND cause='rate_outage' AND absorbed_until IS NULL",&[&machine]).await?;
            for outage in outages {
                let from = parse_pg_time(&outage.get::<_, String>(1))?;
                let currency = terms["offer"]["currency"]
                    .as_str()
                    .context("quarantined outage currency")?;
                let returned=self.rows("SELECT min(accepted_at)::text FROM provisiond.fixture_rate_history WHERE currency=$1 AND available AND accepted_at>$2::text::timestamptz",&[&currency,&from.to_rfc3339()]).await?[0].get::<_,Option<String>>(0).map(|v|parse_pg_time(&v)).transpose()?;
                let end = (*stop).min(returned.unwrap_or(*stop));
                let native = native_elapsed(&terms, from, end)?;
                self.rows("UPDATE provisiond.operator_deficiencies SET absorbed_until=$2::text::timestamptz,absorbed_seconds=ceil(extract(epoch FROM $2::text::timestamptz-absorbed_from))::bigint,native_minor=$3 WHERE id=$1::text::uuid AND absorbed_until IS NULL RETURNING id",&[&outage.get::<_,String>(0),&end.to_rfc3339(),&native]).await?;
            }
            return Ok(());
        }
        let offer = &terms["offer"];
        let currency = offer["currency"].as_str().context("currency")?;
        let mark=self.rows("SELECT max(high_water_mark)::text FROM provisiond.meter_totals WHERE subject_kind='machine' AND subject_id=$1::text::uuid",&[&machine]).await?[0].get::<_,Option<String>>(0).context("billable machine must have a seed")?;
        let mut start = parse_pg_time(&mark)?;
        if *stop <= start {
            return Ok(());
        }
        let descriptor:Value=serde_json::from_str(&self.rows("SELECT p.descriptor::text FROM provisiond.provider_accounts p JOIN provisiond.machines m ON m.provider_account=p.account WHERE m.id=$1::text::uuid",&[&machine]).await?[0].get::<_,String>(0))?;
        let mut outage_window = None;
        while start < *stop {
            let (year, month) = if start.month() == 12 {
                (start.year() + 1, 1)
            } else {
                (start.year(), start.month() + 1)
            };
            let month_end = chrono::NaiveDate::from_ymd_opt(year, month, 1)
                .context("month boundary")?
                .and_hms_opt(0, 0, 0)
                .context("month start")?
                .and_utc();
            let history=self.rows("SELECT value::text,available FROM provisiond.fixture_rate_history WHERE currency=$1 AND accepted_at<=$2::text::timestamptz ORDER BY accepted_at DESC,sequence DESC LIMIT 1",&[&currency,&start.to_rfc3339()]).await?;
            let next=self.rows("SELECT min(accepted_at)::text FROM provisiond.fixture_rate_history WHERE currency=$1 AND accepted_at>$2::text::timestamptz",&[&currency,&start.to_rfc3339()]).await?[0].get::<_,Option<String>>(0).map(|s|parse_pg_time(&s)).transpose()?;
            let available = history.first().is_some_and(|r| r.get::<_, bool>(1));
            if !available && outage_window.is_none() {
                let returned=self.rows("SELECT min(accepted_at)::text FROM provisiond.fixture_rate_history WHERE currency=$1 AND available AND accepted_at>$2::text::timestamptz",&[&currency,&start.to_rfc3339()]).await?[0].get::<_,Option<String>>(0).map(|v|parse_pg_time(&v)).transpose()?;
                outage_window = Some((start, (*stop).min(returned.unwrap_or(*stop))));
            } else if available {
                outage_window = None;
            }
            let rate_end = outage_window.map(|v| v.1).or(next).unwrap_or(*stop);
            let end = (*stop).min(month_end).min(rate_end);
            let period = start.format("%Y-%m").to_string();
            let micros = (end - start).num_microseconds().context("elapsed range")?;
            let native = pricing::minor(
                offer["recurring_price"].as_str().context("native price")?,
                terms["rate"]["minor_scale"]
                    .as_u64()
                    .context("native precision")? as u32,
            )?;
            let native_cost = i64::try_from(
                (i128::from(native) * i128::from(micros)
                    + i128::from(offer["period_seconds"].as_i64().context("period")?) * 1_000_000
                    - 1)
                    / (i128::from(offer["period_seconds"].as_i64().context("period")?) * 1_000_000),
            )?;
            if let Some(rate) = history.first().filter(|r| r.get::<_, bool>(1)) {
                let rate: Value = serde_json::from_str(&rate.get::<_, String>(0))?;
                let price = pricing::calculate(
                    offer,
                    &descriptor,
                    &rate,
                    pricing::wind_seconds(&descriptor)?,
                )?;
                let exact_num = (i128::from(micros) * i128::from(price.per_second_num)).to_string();
                let exact_den = (i128::from(price.per_second_den) * 1_000_000).to_string();
                // Integer division/modulo avoids finite decimal division changing ceil.
                let arithmetic=self.rows("WITH credit AS (SELECT COALESCE((SELECT credit_num FROM provisiond.meter_totals WHERE subject_kind='machine' AND subject_id=$1::text::uuid AND billing_period=$2),0) n,COALESCE((SELECT credit_den FROM provisiond.meter_totals WHERE subject_kind='machine' AND subject_id=$1::text::uuid AND billing_period=$2),1) d), fraction AS (SELECT n,d,$3::text::numeric*d-n*$4::text::numeric AS n1,$4::text::numeric*d AS d1 FROM credit), debit AS (SELECT *,div(n1,d1)+CASE WHEN mod(n1,d1)>0 THEN 1 ELSE 0 END AS posted FROM fraction) SELECT posted::bigint,(provisiond.reduced_credit(n*$4::text::numeric+posted*d1-$3::text::numeric*d,d1))[1],(provisiond.reduced_credit(n*$4::text::numeric+posted*d1-$3::text::numeric*d,d1))[2],(n>=0 AND n<d) FROM debit",&[&machine,&period,&exact_num,&exact_den]).await?.remove(0);
                ensure!(
                    arithmetic.get::<_, bool>(3),
                    "meter rounding credit out of range"
                );
                let computed: i64 = arithmetic.get(0);
                ensure!(computed >= 0, "negative usage magnitude");
                let commitment=self.rows("SELECT id::text,reserved_sats,version FROM provisiond.commitments WHERE machine_id=$1::text::uuid AND state='open'",&[&machine]).await?;
                let reserved = commitment.first().map(|r| r.get::<_, i64>(1)).unwrap_or(0);
                let debit = computed.min(reserved);
                let key = format!(
                    "usage:machine:{machine}:{period}:{}",
                    end.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
                );
                let customer_native = if computed > 0 {
                    (i128::from(native_cost) * i128::from(debit) / i128::from(computed)) as i64
                } else {
                    0
                };
                if debit > 0 {
                    let commitment = commitment.first().context("open commitment")?;
                    let id: String = commitment.get(0);
                    let version: i64 = commitment.get(2);
                    let conversion = json!({"native_minor":customer_native,"currency":currency,"rate_num":rate["rate_num"],"rate_den":rate["rate_den"],"rate_source":rate["source"],"rate_observed_at":rate["observed_at"],"haircut_bps":rate["haircut_bps"],"rounding_rule_version":"ceil-credit-v1","billing_period":period,"increment_end":end.to_rfc3339()});
                    ledger::append(
                        self,
                        Entry {
                            tenant,
                            key: &key,
                            kind: "usage_debit",
                            amount: -debit,
                            deposit: None,
                            operation: None,
                            machine: Some(machine),
                            commitment: Some(&id),
                            conversion: Some(&conversion),
                        },
                    )
                    .await?;
                    ensure!(self.rows("UPDATE provisiond.commitments SET reserved_sats=reserved_sats-$2,version=version+1 WHERE id=$1::text::uuid AND version=$3 RETURNING id",&[&id,&debit,&version]).await?.len()==1,"commitment version changed");
                }
                if debit < computed {
                    self.exit_deficiency(
                        machine,
                        "clamp_overflow",
                        (native_cost - customer_native, computed - debit, Some(&rate)),
                        &key,
                        None,
                    )
                    .await?;
                }
                self.rows("INSERT INTO provisiond.meter_totals(subject_kind,subject_id,billing_period,high_water_mark,credit_num,credit_den) VALUES('machine',$1::text::uuid,$2,$3::text::timestamptz,$4::text::numeric,$5::text::numeric) ON CONFLICT(subject_kind,subject_id,billing_period) DO UPDATE SET high_water_mark=EXCLUDED.high_water_mark,credit_num=EXCLUDED.credit_num,credit_den=EXCLUDED.credit_den RETURNING subject_id",&[&machine,&period,&end.to_rfc3339(),&arithmetic.get::<_,String>(1),&arithmetic.get::<_,String>(2)]).await?;
            } else {
                let window = outage_window.context("retained maximal outage")?;
                let key = format!("rate_outage:{machine}:{}", window.0.to_rfc3339());
                self.exit_deficiency(
                    machine,
                    "rate_outage",
                    (native_elapsed(&terms, window.0, window.1)?, 0, None),
                    &key,
                    Some(window),
                )
                .await?;
                self.rows("INSERT INTO provisiond.meter_totals(subject_kind,subject_id,billing_period,high_water_mark) VALUES('machine',$1::text::uuid,$2,$3::text::timestamptz) ON CONFLICT(subject_kind,subject_id,billing_period) DO UPDATE SET high_water_mark=EXCLUDED.high_water_mark RETURNING subject_id",&[&machine,&period,&end.to_rfc3339()]).await?;
            }
            start = end;
        }
        Ok(())
    }
    async fn exit_deficiency(
        &mut self,
        machine: &str,
        cause: &str,
        valuation: (i64, i64, Option<&Value>),
        key: &str,
        window: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> Result<()> {
        let (native, clamped, rate) = valuation;
        // STO-37: a no-rate native cost has no satoshi-origin clamp.
        let clamped = (cause == "clamp_overflow").then_some(clamped);
        let rate_num = rate.and_then(|v| v["rate_num"].as_i64());
        let rate_den = rate.and_then(|v| v["rate_den"].as_i64());
        let from = window.map(|v| v.0);
        let until = window.map(|v| v.1);
        self.rows("INSERT INTO provisiond.operator_deficiencies(provider_account,machine_id,subject_kind,subject_id,operation_id,cause,native_minor,currency,clamped_sats,rate_num,rate_den,idempotency_key,absorbed_seconds,absorbed_from,absorbed_until) SELECT m.provider_account,m.id,'machine',m.id,COALESCE((SELECT id FROM provisiond.operations WHERE machine_id=m.id ORDER BY created_at DESC LIMIT 1),m.id),$2,$3,m.native_terms->'offer'->>'currency',$4,$8::bigint,$9::bigint,$5,CASE WHEN $6::text IS NULL THEN 0 ELSE ceil(extract(epoch FROM $7::text::timestamptz-$6::text::timestamptz))::bigint END,$6::text::timestamptz,$7::text::timestamptz FROM provisiond.machines m WHERE m.id=$1::text::uuid ON CONFLICT(idempotency_key) DO NOTHING RETURNING id",&[&machine,&cause,&native,&clamped,&key,&from.map(|v|v.to_rfc3339()),&until.map(|v|v.to_rfc3339()),&rate_num,&rate_den]).await?;
        Ok(())
    }
}
fn parse_pg_time(s: &str) -> Result<DateTime<Utc>> {
    let mut s = s.replace(' ', "T");
    if s.ends_with("+00") {
        s.push_str(":00");
    }
    Ok(DateTime::parse_from_rfc3339(&s)?.with_timezone(&Utc))
}

fn native_elapsed(terms: &Value, from: DateTime<Utc>, until: DateTime<Utc>) -> Result<i64> {
    let native = pricing::minor(
        terms["offer"]["recurring_price"]
            .as_str()
            .context("native price")?,
        terms["rate"]["minor_scale"]
            .as_u64()
            .context("native precision")? as u32,
    )?;
    let micros = (until - from)
        .num_microseconds()
        .context("native elapsed range")?
        .max(0);
    let denominator = i128::from(
        terms["offer"]["period_seconds"]
            .as_i64()
            .context("native period")?,
    ) * 1_000_000;
    Ok(i64::try_from(
        (i128::from(native) * i128::from(micros) + denominator - 1) / denominator,
    )?)
}
