//! OVR-19: explicit, validated parameters of the local simulator profile.
use anyhow::{Context, Result, ensure};
use serde_json::Value;
#[derive(Clone)]
pub struct Parameters {
    pub value: Value,
}
impl Parameters {
    pub fn load() -> Result<Self> {
        let path = std::env::var("PROVISIOND_LOCAL_PARAMETERS")
            .context("missing local parameters file")?;
        Self::new(serde_json::from_slice(&std::fs::read(path)?)?)
    }
    pub fn new(value: Value) -> Result<Self> {
        let p = Self { value };
        for k in [
            "activation_sats",
            "deposit_seconds",
            "finality_seconds",
            "tenant_seconds",
            "pending_limit",
            "admission_delay_ms",
            "admission_validity_seconds",
            "admission_capacity",
            "source_concurrency",
            "rate_interval_ms",
            "read_limit",
            "write_limit",
            "created_limit",
            "spend_limit",
            "ceiling_seconds",
            "retention_seconds",
            "receipt_seconds",
            "listing_age_seconds",
        ] {
            ensure!(
                p.n(k) > 0 && p.n(k) <= crate::wire::SAFE_INTEGER,
                "missing or invalid parameter {k}"
            );
        }
        for key in [
            "tenant_seconds",
            "deposit_seconds",
            "finality_seconds",
            "admission_validity_seconds",
            "retention_seconds",
            "receipt_seconds",
        ] {
            ensure!(
                chrono::Utc::now()
                    .timestamp()
                    .checked_add(p.n(key))
                    .and_then(|t| chrono::DateTime::from_timestamp(t, 0))
                    .is_some(),
                "timestamp parameter out of range: {key}"
            );
        }
        ensure!(
            p.n("ceiling_seconds") <= crate::wire::SAFE_INTEGER / 1000,
            "ceiling pacing exceeds wire range"
        );
        ensure!(
            p.n("tenant_seconds") > p.n("deposit_seconds") + p.n("finality_seconds"),
            "pending TTL must exceed expiry plus finality"
        );
        ensure!(
            p.n("receipt_seconds") == p.n("retention_seconds"),
            "receipt_seconds and retention_seconds must equal one operation replay/readability horizon"
        );
        // API-50: three fleet reads per advertised 1000 ms cadence, including
        // a partial interval. Windows start on the caller's first request.
        ensure!(
            p.n("read_limit")
                >= ((p.n("rate_interval_ms") + 999) / 1000)
                    .checked_mul(3)
                    .context("read budget overflow")?,
            "read budget must admit list, balance and machine polls at advertised cadence"
        );
        for k in [
            "lightning_floor_sats",
            "onchain_floor_sats",
            "confirmation_depth",
        ] {
            ensure!(p.n(k) > 0, "invalid rail parameter {k}");
        }
        Ok(p)
    }
    pub fn n(&self, k: &str) -> i64 {
        self.value[k].as_i64().unwrap_or(0)
    }
}
