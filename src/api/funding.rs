//! OVR-10b/17, STO-30/31/46: funding-side simulated final settlement adapter.
use crate::{
    auth, http,
    local::Parameters,
    store::{CommitOutcome, Component, Payment, Store},
    wire::Error,
};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

pub(super) fn deposit_body(id: &str, amount: i64, expiry: i64, p: &Parameters) -> Value {
    let expires = auth::time(expiry);
    json!({"id":id,"requested_sats":amount,"activation_minimum_sats":p.n("activation_sats"),"expires_at":expires,
 "lightning":{"invoice":format!("TEST/SIMULATOR/NONPAYABLE/LIGHTNING/{id}/EXPIRES/{expiry}"),"floor_sats":p.n("lightning_floor_sats"),"enforced":true},
 "onchain":{"address":format!("TEST/SIMULATOR/NONPAYABLE/ONCHAIN/{id}"),"floor_sats":p.n("onchain_floor_sats"),"enforced":false},
 "disclosures":[
 {"code":"test_only","text":"NONPAYABLE TEST/SIMULATOR destinations. Never send real funds."},
 {"code":"onchain_expiry_unwatched","text":"After expires_at the on-chain address stays payable but is no longer watched; a late payment may be lost. During a solvency halt it stays payable and observed payments are credited as unsecured claims."},
 {"code":"double_pay_no_refund","text":"Paying both destinations credits both. Nothing is refundable."},
 {"code":"below_activation_minimum","text":"Credits below the cumulative activation minimum leave the tenant pending and do not extend signup expiry."},
 {"code":"orphan_recovery_is_operator_only","text":"Expired signup credits are retained. Recovery requires contacting the operator with this deposit id; there is no self-serve recovery."}]})
}
/// Trusted adapter observations only. No customer route calls this boundary.
pub(super) async fn credit(
    component: &Component,
    store: &Arc<Store>,
    p: &Parameters,
    v: &Value,
) -> Result<bool> {
    let rail = v["rail"].as_str().context("rail")?;
    let final_settlement = match rail {
        "lightning" => v["settled"] == true && v["preimage_known"] == true,
        "onchain" => v["confirmations"].as_i64().unwrap_or(0) >= p.n("confirmation_depth"),
        _ => false,
    };
    if !final_settlement {
        return Ok(false);
    }
    let destination = v["destination"].as_str().context("destination")?;
    let binding=store.value("SELECT COALESCE((SELECT jsonb_build_object('id',id,'payment_hash',payment_hash,'requested_sats',requested_sats) FROM provisiond.deposits WHERE ($1='lightning' AND invoice=$2) OR ($1='onchain' AND address=$2)),'null'::jsonb)::text",&[&rail,&destination]).await?;
    ensure!(!binding.is_null(), "unbound rail destination");
    let reference = if rail == "lightning" {
        let hash = v["payment_hash"].as_str().context("payment hash")?;
        ensure!(
            hash == binding["payment_hash"],
            "wrong invoice settlement identity"
        );
        hash.to_owned()
    } else {
        let hash = v["txid"].as_str().context("txid")?;
        ensure!(
            hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid transaction identity"
        );
        format!("{}:{}", hash, v["vout"].as_u64().context("outpoint index")?)
    };
    let amount = v["amount_sats"]
        .as_i64()
        .filter(|n| *n > 0 && *n <= crate::wire::SAFE_INTEGER)
        .context("settled amount")?;
    // A simulated Lightning settlement has the invoice's exact amount; on-chain is actual output value.
    if rail == "lightning" {
        ensure!(
            amount
                == binding["requested_sats"]
                    .as_i64()
                    .context("invoice amount")?,
            "invoice value mismatch"
        );
    }
    let payment = Payment {
        deposit: binding["id"].as_str().context("deposit")?.to_owned(),
        rail: rail.into(),
        reference,
        amount_sats: amount,
        observed_at: auth::time(auth::now()),
        settled_at: v["settled_at"].as_str().context("settled instant")?.into(),
    };
    let mut tx = component.begin().await?;
    tx.credit_payment(&payment).await?;
    let tenant:String=tx.rows("SELECT COALESCE(attributed_tenant_id,tenant_id) FROM provisiond.deposits WHERE id=$1::text::uuid",&[&payment.deposit]).await?[0].get(0);
    // API-35/57: serialize the same tenant, credit, activate and distribute atomically.
    let activated=tx.rows("UPDATE provisiond.tenants SET status='active',activated_at=transaction_timestamp() WHERE id=$1 AND status='pending' AND (SELECT COALESCE(sum(amount_sats),0) FROM provisiond.ledger_entries WHERE tenant_id=$1 AND kind IN ('topup','correction')) >= $2::bigint RETURNING id",&[&tenant,&p.n("activation_sats")]).await?;
    if !activated.is_empty() {
        tx.rows("SELECT pg_advisory_xact_lock(1347572815,0)", &[])
            .await?;
        let assigned=tx.rows("INSERT INTO provisiond.tenant_provider_accounts(tenant_id,provider_account,policy_version) SELECT $1,a.account,'least-assigned-v1' FROM provisiond.provider_accounts a JOIN provisiond.provider_account_status s ON s.provider_account=a.account WHERE a.assignable AND s.status='healthy' ORDER BY (SELECT count(*) FROM provisiond.tenant_provider_accounts t WHERE t.provider_account=a.account),a.account LIMIT 1 RETURNING provider_account",&[&tenant]).await?;
        if assigned.is_empty() {
            component
                .trace(json!({"event":"activation_without_healthy_assignment","tenant":tenant}));
        }
    }
    ensure!(
        component.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
        "credit unacknowledged; replay observation"
    );
    component.trace(json!({"event":"rail_credit_acknowledged","rail":rail,"deposit":payment.deposit,"activated":!activated.is_empty()}));
    Ok(true)
}
pub(super) async fn watch(store: Arc<Store>, p: Parameters) -> Result<()> {
    let address = http::loopback(&std::env::var("PROVISIOND_RAIL_SIMULATOR")?)?;
    let component = store.component();
    loop {
        let observations = {
            let _call = component.external(crate::store::ExternalKind::Rail)?;
            http::call(
                address,
                "GET",
                "/settlements",
                &json!({}),
                Duration::from_secs(2),
            )
            .await
        };
        match observations {
            Ok(v) => {
                for v in v.as_array().context("rail observation array")? {
                    if let Err(e) = credit(&component, &store, &p, v).await {
                        component.trace(json!({"event":"rail_credit_retry_pending","store_refusal":e.downcast_ref::<Error>().is_some(),"error_contexts":e.chain().take(2).map(ToString::to_string).collect::<Vec<_>>()}));
                    }
                }
            }
            Err(_) => component.trace(json!({"event":"rail_observation_unavailable"})),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
