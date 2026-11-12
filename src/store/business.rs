//! STO-1/3/23/27/30/31, OPS-6/8/22, LDG-11/31/39: narrow atomic writes.
use super::{CommitOutcome, Component, Transaction};
use crate::ledger::{self, Entry};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

#[derive(Clone, Debug)]
pub struct Claim {
    pub id: String,
    pub tenant: String,
    pub kind: String,
    pub number: i64,
}
#[derive(Clone)]
pub struct CreateRequest {
    pub tenant: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub key: String,
    pub fingerprint: String,
    /// STO-33/35: admission-origin replay/readability duration, never renewed.
    pub receipt_seconds: i64,
    pub provider_account: String,
    pub channel: String,
    pub offer: String,
    pub offer_is_resource: bool,
    pub reserved_sats: i64,
    pub setup: Value,
    pub first_unit: Option<Value>,
    pub offer_snapshot: Value,
}
#[derive(Clone)]
pub struct Payment {
    pub deposit: String,
    pub rail: String,
    pub reference: String,
    pub amount_sats: i64,
    pub observed_at: String,
    pub settled_at: String,
}
impl Component {
    /// LDG-70: a measured mismatch latches spending closed in the acknowledged
    /// audit transaction. Repair/re-enable is an offline operator responsibility.
    pub async fn audit_ledger(&self, tenant: &str) -> Result<()> {
        let mut tx = self.begin().await?;
        let valid = ledger::audit(&mut tx, tenant).await?;
        if !valid {
            tx.rows("UPDATE provisiond.spending_health SET ledger_valid=false WHERE singleton RETURNING singleton",&[]).await?;
        }
        ensure!(
            self.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
            "audit result unacknowledged"
        );
        ensure!(valid, "ledger audit mismatch; spending stopped");
        Ok(())
    }
    pub async fn admit_create(&self, request: &CreateRequest) -> Result<String> {
        let mut tx = self.begin().await?;
        let id = tx.admit_create(request).await?;
        ensure!(
            self.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
            "admission not acknowledged"
        );
        Ok(id)
    }
    pub async fn credit_payment(&self, payment: &Payment) -> Result<String> {
        let mut tx = self.begin().await?;
        let id = tx.credit_payment(payment).await?;
        if super::test_defect("payment_atomic") {
            // Isolated negative: omit payment persistence, durably commit the
            // credit alone, then simulate failure before payment persistence.
            ensure!(
                self.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
                "negative intermediate commit"
            );
            self.trace(
                json!({"event":"defect_payment_split_durable","reference":payment.reference}),
            );
            anyhow::bail!("simulated failure between durable credit and payment");
        }
        ensure!(
            self.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
            "payment not acknowledged; rail must replay"
        );
        Ok(id)
    }
}
impl Transaction {
    pub async fn serialize_tenants(&mut self, tenants: &[&str]) -> Result<()> {
        let result = ledger::serialize(self, tenants).await;
        self.failed |= result.is_err();
        result
    }
    /// STO-51/OPS-8: a refused re-acquire cannot steal another running hold.
    pub async fn reacquire(&mut self, c: &Claim) -> Result<bool> {
        self.engine_only()?;
        let result = self.reacquire_inner(c).await;
        self.failed |= result.is_err();
        result
    }
    async fn reacquire_inner(&mut self, c: &Claim) -> Result<bool> {
        self.batch("SAVEPOINT machine_reacquire").await?;
        let result=self.rows("UPDATE provisiond.operations SET yielded_at=NULL,revision=revision+1,updated_at=clock_timestamp() WHERE id=$1::text::uuid AND status='running' AND claim_number=$2 AND yielded_at IS NOT NULL RETURNING id",&[&c.id,&c.number]).await;
        match result {
            Ok(rows) => {
                self.batch("RELEASE SAVEPOINT machine_reacquire").await?;
                Ok(!rows.is_empty())
            }
            Err(error)
                if error
                    .downcast_ref::<tokio_postgres::Error>()
                    .is_some_and(|e| e.code().is_some_and(|c| c.code() == "23505")) =>
            {
                self.failed = false;
                self.batch(
                    "ROLLBACK TO SAVEPOINT machine_reacquire; RELEASE SAVEPOINT machine_reacquire",
                )
                .await?;
                self.defer_inner(c).await?;
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
    pub async fn admit_create(&mut self, r: &CreateRequest) -> Result<String> {
        let result = self.admit_create_inner(r).await;
        self.failed |= result.is_err();
        result
    }
    pub async fn credit_payment(&mut self, p: &Payment) -> Result<String> {
        let result = self.credit_payment_inner(p).await;
        self.failed |= result.is_err();
        result
    }
    // OPS-50: the runtime proof is installed only by Component::begin after its
    // first-statement generation check. API admission/payment remain unfenced.
    fn engine_only(&mut self) -> Result<()> {
        if !self.engine_fenced && !super::test_defect("surface") {
            self.failed = true;
            anyhow::bail!("engine-only transaction requires generation fence");
        }
        Ok(())
    }
    pub async fn claim(&mut self) -> Result<Option<Claim>> {
        self.engine_only()?;
        let result = self.claim_inner().await;
        self.failed |= result.is_err();
        result
    }
    pub async fn defer(&mut self, c: &Claim) -> Result<bool> {
        self.engine_only()?;
        let result = self.defer_inner(c).await;
        self.failed |= result.is_err();
        result
    }
    pub async fn settle(
        &mut self,
        c: &Claim,
        target: &str,
        result: &Value,
        error: &Value,
    ) -> Result<bool> {
        self.engine_only()?;
        let result = async {
            ensure!(
                c.kind != "create" || target != "succeeded",
                "create success requires verified atomic settlement"
            );
            self.settle_inner(c, target, result, error).await
        }
        .await;
        self.failed |= result.is_err();
        result
    }
    pub async fn license_create(&mut self, c: &Claim) -> Result<bool> {
        self.engine_only()?;
        let result = self.license_create_inner(c).await;
        self.failed |= result.is_err();
        result
    }
    pub async fn settle_verified_create(
        &mut self,
        c: &Claim,
        external_id: &str,
        earliest_date: Option<&str>,
    ) -> Result<bool> {
        self.engine_only()?;
        let result = self
            .settle_verified_create_inner(c, external_id, earliest_date)
            .await;
        self.failed |= result.is_err();
        result
    }
    async fn admit_create_inner(&mut self, r: &CreateRequest) -> Result<String> {
        ensure!(r.receipt_seconds > 0, "invalid receipt horizon");
        ensure!(
            r.reserved_sats > 0 && matches!(r.scope_kind.as_str(), "tenant" | "operator"),
            "invalid admitting request"
        );
        ensure!(
            r.scope_kind != "tenant" || r.scope_id == r.tenant,
            "tenant principal mismatch"
        );
        let offer = r
            .offer_snapshot
            .as_object()
            .context("accepted offer must be an object")?;
        ensure!(
            offer.keys().all(|k| matches!(
                k.as_str(),
                "offer_id"
                    | "billing_unit_seconds"
                    | "accepted_terms"
                    | "machine_kind"
                    | "install_strategies"
                    | "recurring_price"
                    | "currency"
                    | "recurring_period"
                    | "declared_cancellation_bound"
            )),
            "unrecognized retained offer field"
        );
        ensure!(
            r.offer_snapshot["offer_id"].as_str() == Some(&r.offer)
                && matches!(
                    r.offer_snapshot["machine_kind"].as_str(),
                    Some("virtual" | "bare_metal")
                )
                && r.offer_snapshot["install_strategies"]
                    .as_array()
                    .is_some_and(|s| s.is_empty())
                && r.offer_snapshot["recurring_price"].is_string()
                && r.offer_snapshot["currency"].is_string()
                && matches!(
                    r.offer_snapshot["recurring_period"].as_str(),
                    Some("hour" | "month")
                ),
            "unsupported or incomplete accepted simulator offer"
        );
        validate_charge(&r.setup)?;
        if let Some(unit) = &r.first_unit {
            validate_charge(unit)?;
            ensure!(
                unit["sats"].as_i64().unwrap_or(0) > 0,
                "zero first unit has no obligation"
            );
        }
        ledger::serialize(self, &[&r.tenant]).await?;
        let prior=self.rows("SELECT resource_id::text,fingerprint FROM provisiond.idempotency_records WHERE scope_kind=$1 AND scope_id=$2 AND key=$3",&[&r.scope_kind,&r.scope_id,&r.key]).await?;
        if let Some(row) = prior.first() {
            ensure!(
                row.get::<_, String>(1) == r.fingerprint,
                "idempotency fingerprint conflict"
            );
            return Ok(row.get(0));
        }
        ensure!(
            self.rows(
                "SELECT ledger_valid FROM provisiond.spending_health WHERE singleton",
                &[]
            )
            .await?
            .first()
            .is_some_and(|r| r.get::<_, bool>(0)),
            "ledger audit halted spending"
        );
        let (_, available) = ledger::balance(self, &r.tenant).await?;
        ensure!(
            available >= r.reserved_sats,
            "insufficient available balance"
        );
        let id: String = self.rows("SELECT gen_random_uuid()::text", &[]).await?[0].get(0);
        if r.offer_is_resource {
            let rows=self.rows("INSERT INTO provisiond.resource_exclusions(provider_account,ordering_channel,offer_id,operation_id) VALUES($1,$2,$3,$4::text::uuid) ON CONFLICT DO NOTHING RETURNING operation_id",&[&r.provider_account,&r.channel,&r.offer,&id]).await?;
            ensure!(
                rows.len() == 1,
                "resource offer already has unresolved create; conflict/state"
            );
        }
        let commitment:String=self.rows("INSERT INTO provisiond.commitments(tenant_id,operation_id,reserved_sats,state) VALUES($1,$2::text::uuid,$3,'open') RETURNING id::text",&[&r.tenant,&id,&r.reserved_sats]).await?[0].get(0);
        let mut summary = json!({"kind":"create","provider_account":r.provider_account,"offer":r.offer_snapshot,"setup_fee":r.setup});
        if let Some(unit) = &r.first_unit {
            summary["first_unit"] = unit.clone();
        }
        let summary = summary.to_string();
        self.rows("INSERT INTO provisiond.operations(id,tenant_id,idempotency_key,kind,status,provider_account,request,request_summary,correlation_id,commitment_id) VALUES($1::text::uuid,$2,$3,'create','queued',$4,'{}',$5::text::jsonb,$1,$6::text::uuid) RETURNING id",&[&id,&r.tenant,&r.key,&r.provider_account,&summary,&commitment]).await?;
        let receipt = json!({"operation_id":id}).to_string();
        self.rows("INSERT INTO provisiond.idempotency_records(scope_kind,scope_id,key,fingerprint,resource_kind,resource_id,status,response_body,expires_at) VALUES($1,$2,$3,$4,'operation',$5::text::uuid,202,$6::text::jsonb,transaction_timestamp()+$7::bigint*interval '1 second') RETURNING resource_id",&[&r.scope_kind,&r.scope_id,&r.key,&r.fingerprint,&id,&receipt,&r.receipt_seconds]).await?;
        Ok(id)
    }
    async fn credit_payment_inner(&mut self, p: &Payment) -> Result<String> {
        ensure!(
            p.amount_sats > 0 && matches!(p.rail.as_str(), "lightning" | "onchain"),
            "invalid settled payment"
        );
        let rows=self.rows("SELECT COALESCE(attributed_tenant_id,tenant_id) FROM provisiond.deposits WHERE id=$1::text::uuid",&[&p.deposit]).await?;
        let tenant: String = rows.first().context("unknown deposit")?.get(0);
        ledger::serialize(self, &[&tenant]).await?;
        let prior=self.rows("SELECT id::text,deposit_id::text,rail,amount_sats FROM provisiond.payments WHERE payment_ref=$1",&[&p.reference]).await?;
        if let Some(row) = prior.first() {
            ensure!(
                row.get::<_, String>(1) == p.deposit
                    && row.get::<_, String>(2) == p.rail
                    && row.get::<_, i64>(3) == p.amount_sats,
                "payment identity conflict"
            );
            return Ok(row.get(0));
        }
        let key = format!("payment:{}", p.reference);
        let (entry, _) = ledger::append(
            self,
            Entry {
                tenant: &tenant,
                key: &key,
                kind: "topup",
                amount: p.amount_sats,
                deposit: Some(&p.deposit),
                operation: None,
                machine: None,
                commitment: None,
                conversion: None,
            },
        )
        .await?;
        if super::test_defect("payment_atomic") {
            return Ok(entry);
        }
        let rows=self.rows("INSERT INTO provisiond.payments(deposit_id,rail,payment_ref,amount_sats,ledger_entry_id,credited_tenant_id,observed_at,settled_at) VALUES($1::text::uuid,$2,$3,$4,$5::text::uuid,$6,$7::text::timestamptz,$8::text::timestamptz) RETURNING id::text",&[&p.deposit,&p.rail,&p.reference,&p.amount_sats,&entry,&tenant,&p.observed_at,&p.settled_at]).await?;
        Ok(rows[0].get(0))
    }
    /// STO-1/OPS-6: savepoint confines a per-machine unique refusal; the number
    /// increment is rolled back before the short queued defer, in this transaction.
    pub(super) async fn claim_inner(&mut self) -> Result<Option<Claim>> {
        let rows=self.rows("SELECT id::text FROM provisiond.operations WHERE status='queued' AND available_at<=clock_timestamp() ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1",&[]).await?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let id: String = row.get(0);
        self.batch("SAVEPOINT machine_claim").await?;
        let result=self.rows("UPDATE provisiond.operations SET status='running',claim_number=claim_number+1,revision=revision+1,updated_at=clock_timestamp() WHERE id=$1::text::uuid AND status='queued' RETURNING tenant_id,kind,claim_number",&[&id]).await;
        match result {
            Ok(rows) => {
                self.batch("RELEASE SAVEPOINT machine_claim").await?;
                let row = &rows[0];
                Ok(Some(Claim {
                    id,
                    tenant: row.get(0),
                    kind: row.get(1),
                    number: row.get(2),
                }))
            }
            Err(error)
                if error
                    .downcast_ref::<tokio_postgres::Error>()
                    .is_some_and(|e| e.code().is_some_and(|c| c.code() == "23505")) =>
            {
                self.failed = false;
                self.batch("ROLLBACK TO SAVEPOINT machine_claim; RELEASE SAVEPOINT machine_claim")
                    .await?;
                self.rows("UPDATE provisiond.operations SET available_at=clock_timestamp()+interval '50 milliseconds',revision=revision+1,updated_at=clock_timestamp() WHERE id=$1::text::uuid AND status='queued' RETURNING id",&[&id]).await?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
    async fn defer_inner(&mut self, c: &Claim) -> Result<bool> {
        let guard = c.number;
        ensure!(guard > 0, "invalid claim number");
        let bypass = super::test_defect("claim_guard");
        let rows=self.rows("UPDATE provisiond.operations SET status='queued',available_at=clock_timestamp()+interval '50 milliseconds',yielded_at=NULL,revision=revision+1,updated_at=clock_timestamp() WHERE id=$1::text::uuid AND status='running' AND (claim_number=$2 OR $3) AND tenant_id=$4 AND kind=$5 RETURNING id",&[&c.id,&guard,&bypass,&c.tenant,&c.kind]).await?;
        Ok(!rows.is_empty())
    }
    /// STO-3: exact repeats preserve revision and timestamp. Status guard always
    /// precedes money. A zero row prevents every related effect.
    async fn settle_inner(
        &mut self,
        c: &Claim,
        target: &str,
        result: &Value,
        error: &Value,
    ) -> Result<bool> {
        ensure!(
            matches!(target, "succeeded" | "failed" | "needs_reconciliation"),
            "unknown operation status"
        );
        ledger::serialize(self, &[&c.tenant]).await?;
        let guard = c.number;
        ensure!(guard > 0, "invalid claim number");
        let bypass = super::test_defect("claim_guard");
        let result = if result.is_null() {
            None
        } else {
            Some(crate::wire::redact(result.clone()).to_string())
        };
        let error = if error.is_null() {
            None
        } else {
            Some(crate::wire::redact(error.clone()).to_string())
        };
        let rows=self.rows("UPDATE provisiond.operations SET status=$3,result=$4::text::jsonb,error=$5::text::jsonb,request=NULL,mutation_wait_ended_at=CASE WHEN $3='needs_reconciliation' THEN COALESCE(mutation_wait_ended_at,clock_timestamp()) ELSE mutation_wait_ended_at END,revision=CASE WHEN status='running' THEN revision+1 ELSE revision END,updated_at=CASE WHEN status='running' THEN clock_timestamp() ELSE updated_at END WHERE id=$1::text::uuid AND tenant_id=$7 AND kind=$8 AND (claim_number=$2 OR $6) AND (status='running' OR (status=$3 AND result IS NOT DISTINCT FROM $4::text::jsonb AND error IS NOT DISTINCT FROM $5::text::jsonb AND request IS NULL)) RETURNING kind,commitment_id::text",&[&c.id,&guard,&target,&result,&error,&bypass,&c.tenant,&c.kind]).await?;
        let Some(row) = rows.first() else {
            return Ok(false);
        };
        if row.get::<_, String>(0) == "create" && target == "failed" {
            self.release_create(&c.id, row.get::<_, Option<String>>(1).as_deref())
                .await?;
        }
        Ok(true)
    }
    pub(crate) async fn release_create(
        &mut self,
        id: &str,
        commitment: Option<&str>,
    ) -> Result<()> {
        self.rows("UPDATE provisiond.commitments SET state='closed',reserved_sats=0,closed_at=clock_timestamp(),version=version+1 WHERE id=$1::text::uuid AND state='open' RETURNING id",&[&commitment]).await?;
        self.rows("DELETE FROM provisiond.resource_exclusions WHERE operation_id=$1::text::uuid RETURNING operation_id",&[&id]).await?;
        Ok(())
    }
    /// OPS-43 step 8 repeat: preserve timestamps and revision after a lost reply.
    async fn license_create_inner(&mut self, c: &Claim) -> Result<bool> {
        let rows=self.rows("UPDATE provisiond.operations SET about_to_order_at=COALESCE(about_to_order_at,transaction_timestamp()),revision=revision+CASE WHEN about_to_order_at IS NULL THEN 1 ELSE 0 END,updated_at=CASE WHEN about_to_order_at IS NULL THEN clock_timestamp() ELSE updated_at END WHERE id=$1::text::uuid AND status='running' AND kind='create' AND claim_number=$2 RETURNING id",&[&c.id,&c.number]).await?;
        Ok(!rows.is_empty())
    }
    /// OPS-27/LDG-31/39: independently verified simulator result, still-open
    /// original commitment only. No late attach, resolution or metering API.
    async fn settle_verified_create_inner(
        &mut self,
        c: &Claim,
        external_id: &str,
        earliest_date: Option<&str>,
    ) -> Result<bool> {
        ensure!(
            c.kind == "create",
            "verified create outcome on another kind"
        );
        let result = json!({"external_id":external_id});
        if !self
            .settle_inner(c, "succeeded", &result, &Value::Null)
            .await?
        {
            return Ok(false);
        }
        self.land_create(c, external_id, earliest_date).await?;
        Ok(true)
    }
    /// OPS-27: shared effects after the caller has won its own status guard.
    pub(crate) async fn land_create(
        &mut self,
        c: &Claim,
        external_id: &str,
        earliest_date: Option<&str>,
    ) -> Result<()> {
        let row=self.rows("SELECT provider_account,commitment_id::text,request_summary::text FROM provisiond.operations WHERE id=$1::text::uuid",&[&c.id]).await?.remove(0);
        let account: String = row.get(0);
        let commitment: String = row.get(1);
        let summary: Value = serde_json::from_str(&row.get::<_, String>(2))?;
        let offer = &summary["offer"];
        let machine_kind = offer["machine_kind"]
            .as_str()
            .context("missing machine kind")?;
        let offer_id = offer["offer_id"]
            .as_str()
            .context("missing accepted offer")?;
        let seconds = offer["billing_unit_seconds"]
            .as_i64()
            .or_else(|| summary["first_unit"]["seconds"].as_i64())
            .unwrap_or(0);
        let declared = offer["declared_cancellation_bound"].as_str();
        let strategies = offer["install_strategies"].to_string();
        let rows=self.rows("INSERT INTO provisiond.machines(tenant_id,provider_account,external_id,offer_id,kind,state,install_strategies,billing_unit_seconds,declared_cancellation_bound,earliest_cancellation_date) VALUES($1,$2,$3,$4,$5,'running',$6::text::jsonb,$7,$8::text::timestamptz,$9::text::timestamptz) ON CONFLICT(provider_account,external_id) DO UPDATE SET external_id=EXCLUDED.external_id WHERE provisiond.machines.tenant_id=EXCLUDED.tenant_id RETURNING id::text",&[&c.tenant,&account,&external_id,&offer_id,&machine_kind,&strategies,&seconds,&declared,&earliest_date]).await?;
        let machine: String = rows
            .first()
            .context("machine belongs to another tenant")?
            .get(0);
        self.rows("UPDATE provisiond.operations SET machine_id=$2::text::uuid WHERE id=$1::text::uuid AND machine_id IS DISTINCT FROM $2::text::uuid RETURNING id",&[&c.id,&machine]).await?;
        self.rows("UPDATE provisiond.commitments SET machine_id=$2::text::uuid WHERE id=$1::text::uuid AND machine_id IS NULL RETURNING id",&[&commitment,&machine]).await?;
        for (field, kind) in [
            ("setup_fee", "setup_fee_debit"),
            ("first_unit", "first_unit_debit"),
        ] {
            let charge = &summary[field];
            let requested = charge["sats"].as_i64().unwrap_or(0);
            if requested == 0 {
                continue;
            }
            validate_charge(charge)?;
            let key = format!("{kind}:{}", c.id);
            if !self.rows("SELECT id FROM provisiond.ledger_entries WHERE tenant_id=$1 AND idempotency_key=$2",&[&c.tenant,&key]).await?.is_empty() {continue;}
            let row=self.rows("SELECT reserved_sats,version,state FROM provisiond.commitments WHERE id=$1::text::uuid",&[&commitment]).await?.remove(0);
            let released = row.get::<_, String>(2) != "open";
            let remaining: i64 = if released { 0 } else { row.get(0) };
            let version: i64 = row.get(1);
            let debit = requested.min(remaining);
            let native = charge["native_minor"]
                .as_i64()
                .context("missing native amount")?;
            let customer_native =
                (i128::from(native) * i128::from(debit) / i128::from(requested)) as i64;
            let mut conversion = charge.clone();
            conversion["native_minor"] = json!(customer_native);
            if debit > 0 {
                let (entry, _) = ledger::append(
                    self,
                    Entry {
                        tenant: &c.tenant,
                        key: &key,
                        kind,
                        amount: -debit,
                        deposit: None,
                        operation: Some(&c.id),
                        machine: Some(&machine),
                        commitment: Some(&commitment),
                        conversion: Some(&conversion),
                    },
                )
                .await?;
                let rows=self.rows("UPDATE provisiond.commitments SET reserved_sats=reserved_sats-$2,version=version+1 WHERE id=$1::text::uuid AND version=$3 RETURNING id",&[&commitment,&debit,&version]).await?;
                ensure!(rows.len() == 1, "commitment version changed");
                if kind == "first_unit_debit" {
                    self.rows("UPDATE provisiond.machines SET first_unit_debit_id=$2::text::uuid WHERE id=$1::text::uuid AND first_unit_debit_id IS NULL RETURNING id",&[&machine,&entry]).await?;
                }
            }
            if debit < requested {
                // LDG-66: opening evidence is independent of the debit's accepted
                // conversion. An unavailable opening rate remains null on repeats.
                let cause = if released {
                    if field == "setup_fee" {
                        "unrecoverable_setup_fee"
                    } else {
                        "unrecoverable_first_unit"
                    }
                } else {
                    "clamp_overflow"
                };
                self.rows("INSERT INTO provisiond.operator_deficiencies(provider_account,machine_id,subject_kind,subject_id,absorbed_seconds,operation_id,cause,native_minor,currency,clamped_sats,rate_num,rate_den,idempotency_key) SELECT $1,$2::text::uuid,'machine',$2::text::uuid,0,$3::text::uuid,$8,$4,$5,$6,(r.value->>'rate_num')::bigint,(r.value->>'rate_den')::bigint,$7 FROM (VALUES(1)) AS one(n) LEFT JOIN provisiond.rates r ON r.currency=$5 AND r.available ON CONFLICT(idempotency_key) DO NOTHING RETURNING id",&[&account,&machine,&c.id,&(native-customer_native),&charge["currency"].as_str(),&(requested-debit),&key,&cause]).await?;
            }
        }
        self.rows("DELETE FROM provisiond.resource_exclusions WHERE operation_id=$1::text::uuid RETURNING operation_id",&[&c.id]).await?;
        ledger::balance(self, &c.tenant).await?;
        Ok(())
    }
}
pub(crate) fn validate_charge(c: &Value) -> Result<()> {
    ensure!(
        c.as_object().is_some_and(|o| o.keys().all(|k| matches!(
            k.as_str(),
            "sats"
                | "native_minor"
                | "currency"
                | "rate_num"
                | "rate_den"
                | "rate_source"
                | "rate_observed_at"
                | "haircut_bps"
                | "rounding_rule_version"
                | "seconds"
        ))),
        "unrecognized retained charge field"
    );

    ensure!(
        c["sats"].as_i64().is_some_and(|v| v >= 0)
            && c["native_minor"].as_i64().is_some_and(|v| v >= 0)
            && c["currency"].as_str().is_some(),
        "invalid exact charge"
    );
    ensure!(
        c["rate_num"].as_i64().is_some_and(|v| v > 0)
            && c["rate_den"].as_i64().is_some_and(|v| v > 0)
            && c["rate_source"].is_string()
            && c["rate_observed_at"].is_string()
            && c["haircut_bps"].as_i64().is_some()
            && c["rounding_rule_version"].is_string(),
        "missing conversion evidence"
    );
    Ok(())
}
