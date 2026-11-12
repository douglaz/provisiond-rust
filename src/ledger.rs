//! LDG-35/69/70: serialization and append-only money writes on store transactions.
use crate::store::Transaction;
use anyhow::{Result, ensure};

/// Tenant IDs, rather than live tenant rows, define the primitive (LDG-35).
pub(crate) async fn serialize(tx: &mut Transaction, tenants: &[&str]) -> Result<()> {
    let mut tenants = tenants.to_vec();
    tenants.sort_unstable();
    tenants.dedup();
    for tenant in tenants {
        if tx.tenants.iter().any(|t| t == tenant) {
            continue;
        }
        ensure!(
            tx.tenants.last().is_none_or(|last| last.as_str() < tenant),
            "tenant serialization must ascend"
        );
        tx.rows(
            "SELECT pg_advisory_xact_lock(1347572814,hashtext($1)) /* LDG-35 */",
            &[&tenant],
        )
        .await?;
        tx.tenants.push(tenant.to_owned());
    }
    Ok(())
}
pub(crate) async fn balance(tx: &mut Transaction, tenant: &str) -> Result<(i64, i64)> {
    let row=tx.rows("SELECT COALESCE((SELECT balance_after FROM provisiond.ledger_entries WHERE tenant_id=$1 ORDER BY seq DESC LIMIT 1),0)::bigint, COALESCE((SELECT sum(reserved_sats) FROM provisiond.commitments WHERE tenant_id=$1 AND state='open'),0)::bigint",&[&tenant]).await?.remove(0);
    let balance: i64 = row.get(0);
    let reserved: i64 = row.get(1);
    ensure!(
        balance >= reserved,
        "negative available balance; fail closed"
    );
    Ok((balance, balance - reserved))
}

pub(crate) struct Entry<'a> {
    pub tenant: &'a str,
    pub key: &'a str,
    pub kind: &'a str,
    pub amount: i64,
    pub deposit: Option<&'a str>,
    pub operation: Option<&'a str>,
    pub machine: Option<&'a str>,
    pub commitment: Option<&'a str>,
    pub conversion: Option<&'a serde_json::Value>,
}
/// Caller holds serialization; returns (stable id, newly inserted).
pub(crate) async fn append(tx: &mut Transaction, entry: Entry<'_>) -> Result<(String, bool)> {
    ensure!(
        tx.tenants.iter().any(|t| t == entry.tenant),
        "append requires tenant serialization"
    );
    let existing=tx.rows("SELECT id::text,amount_sats,kind FROM provisiond.ledger_entries WHERE tenant_id=$1 AND idempotency_key=$2",&[&entry.tenant,&entry.key]).await?;
    if let Some(row) = existing.first() {
        ensure!(
            row.get::<_, i64>(1) == entry.amount && row.get::<_, String>(2) == entry.kind,
            "ledger idempotency mismatch"
        );
        return Ok((row.get(0), false));
    }
    ensure!(entry.amount != 0, "zero ledger entry forbidden");
    let snapshot = entry
        .conversion
        .cloned()
        .unwrap_or(serde_json::json!({}))
        .to_string();
    let rows=tx.rows("INSERT INTO provisiond.ledger_entries(tenant_id,seq,kind,amount_sats,balance_after,idempotency_key,deposit_id,operation_id,machine_id,commitment_id,native_minor,native_currency,rate_num,rate_den,rate_source,rate_observed_at,haircut_bps,rounding_rule_version,billing_period,increment_end) SELECT $1,COALESCE((SELECT seq FROM provisiond.ledger_entries WHERE tenant_id=$1 ORDER BY seq DESC LIMIT 1),0)+1,$2,$3,COALESCE((SELECT balance_after FROM provisiond.ledger_entries WHERE tenant_id=$1 ORDER BY seq DESC LIMIT 1),0)+$3,$4,$5::text::uuid,$6::text::uuid,$7::text::uuid,$8::text::uuid,($9::text::jsonb->>'native_minor')::bigint,$9::text::jsonb->>'currency',($9::text::jsonb->>'rate_num')::bigint,($9::text::jsonb->>'rate_den')::bigint,$9::text::jsonb->>'rate_source',($9::text::jsonb->>'rate_observed_at')::timestamptz,($9::text::jsonb->>'haircut_bps')::bigint,$9::text::jsonb->>'rounding_rule_version',$9::text::jsonb->>'billing_period',($9::text::jsonb->>'increment_end')::timestamptz RETURNING id::text",&[&entry.tenant,&entry.kind,&entry.amount,&entry.key,&entry.deposit,&entry.operation,&entry.machine,&entry.commitment,&snapshot]).await?;
    Ok((rows[0].get(0), true))
}
/// LDG-70: audit all recorded prefixes as well as latest balance, and fail closed.
pub(crate) async fn audit(tx: &mut Transaction, tenant: &str) -> Result<bool> {
    serialize(tx, &[tenant]).await?;
    let valid:bool=tx.rows("SELECT NOT EXISTS(SELECT 1 FROM (SELECT balance_after,sum(amount_sats) OVER(ORDER BY seq) AS recomputed FROM provisiond.ledger_entries WHERE tenant_id=$1) e WHERE balance_after<>recomputed)",&[&tenant]).await?[0].get(0);
    let available_valid = balance(tx, tenant).await.is_ok();
    Ok(valid && available_valid)
}
