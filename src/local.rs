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
        ensure!(
            p.n("tenant_seconds") > p.n("deposit_seconds") + p.n("finality_seconds"),
            "pending TTL must exceed expiry plus finality"
        );
        ensure!(
            p.n("receipt_seconds") > p.n("retention_seconds"),
            "receipt horizon must exceed operation retention"
        );
        ensure!(
            p.n("read_limit") >= 3,
            "read budget must admit list, balance and machine polls"
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
