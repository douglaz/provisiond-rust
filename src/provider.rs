//! PRV-44/47/50: independent local simulator driver, private to engine.
use crate::{
    http, pricing,
    store::{Component, ExternalKind},
    wire::{self, Error},
};
use serde_json::{Value, json};
use std::{net::SocketAddr, time::Duration};
#[derive(Clone)]
pub(crate) struct Client {
    address: SocketAddr,
}
impl Client {
    pub(crate) fn configured() -> anyhow::Result<Self> {
        Ok(Self {
            address: http::loopback(&std::env::var("PROVISIOND_PROVIDER_SIMULATOR")?)?,
        })
    }
    pub(crate) async fn call(
        &self,
        component: &Component,
        d: &Value,
        path: &str,
        v: &Value,
    ) -> Result<Value, Error> {
        let _permit = component.external(ExternalKind::Provider).map_err(|_| {
            Error::new(
                "internal",
                "external ownership refused",
                true,
                json!({"accepted":null}),
            )
        })?;
        self.call_entered(d, path, v).await
    }
    pub(crate) async fn call_entered(
        &self,
        d: &Value,
        path: &str,
        v: &Value,
    ) -> Result<Value, Error> {
        let bound = Duration::from_secs(d["call_seconds"].as_u64().unwrap_or(1));
        let v = http::call(
            self.address,
            if ["/offers", "/search", "/get", "/constraints"].contains(&path) {
                "GET"
            } else {
                "POST"
            },
            path,
            v,
            bound,
        )
        .await
        .map_err(|e| {
            Error::new(
                if format!("{e:#}").contains("deadline") {
                    "timeout"
                } else {
                    "network"
                },
                "simulator response unavailable",
                true,
                json!({"accepted":null}),
            )
        })?;
        if v["error"].is_object() {
            return Err(Error(v["error"].clone()));
        }
        Ok(v)
    }
    /// PRV-47 category selection precedes least caller pointer selection.
    pub(crate) fn validate(
        &self,
        r: &Value,
        d: &Value,
        offer: &Value,
        correlator: &str,
    ) -> Result<(), Error> {
        let mut bad = Vec::new();
        if !r["hostname"].as_str().is_some_and(|s| {
            s.bytes()
                .all(|c| c.is_ascii_alphanumeric() || b".-".contains(&c))
        }) {
            bad.push("/hostname".into());
        }
        if let Some(region) = r["region"].as_str() {
            if !offer["regions"]
                .as_array()
                .is_some_and(|a| a.iter().any(|v| v == region))
            {
                bad.push("/region".into());
            }
        }
        if !r["image"].is_null() && r["image"] != "test-image" {
            bad.push("/image".into());
        }
        if let Some(options) = r["provider_options"].as_object() {
            for (k, v) in options {
                if k != "placement" || v != "test" {
                    bad.push(format!("/provider_options{}", wire::pointer(k)));
                }
            }
        }
        // The local driver forwards no generated key bytes; only this opaque name/tag.
        let system_invalid =
            correlator.len() > d["max_correlator_bytes"].as_u64().unwrap_or(128) as usize;
        if !bad.is_empty() {
            return wire::least(bad).map_err(|mut e| {
                e.0["details"]["accepted"] = Value::Null;
                e
            });
        }
        if system_invalid {
            return Err(Error::new(
                "internal",
                "generated provider value rejected",
                true,
                json!({"accepted":null}),
            ));
        }
        Ok(())
    }
    pub(crate) fn price_moved(
        current: &Value,
        accepted: &Value,
        scale: u32,
    ) -> anyhow::Result<Option<Error>> {
        let mut shortfall = 0_i64;
        for k in ["recurring_price", "setup_fee"] {
            let a = pricing::minor(
                accepted[k]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("accepted price"))?,
                scale,
            )?;
            let c = pricing::minor(
                current[k]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("current price"))?,
                scale,
            )?;
            shortfall = shortfall
                .checked_add((c - a).max(0))
                .ok_or_else(|| anyhow::anyhow!("price overflow"))?;
        }
        let unit = |v: &Value| -> anyhow::Result<i64> {
            let seconds = v["billing_unit"]["seconds"]
                .as_i64()
                .ok_or_else(|| anyhow::anyhow!("unit"))?;
            let recurring = pricing::minor(
                v["recurring_price"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("recurring"))?,
                scale,
            )?;
            let period = v["period_seconds"]
                .as_i64()
                .filter(|n| *n > 0)
                .ok_or_else(|| anyhow::anyhow!("period"))?;
            let floor = v["billing_unit"]["native_floor"]
                .as_str()
                .map(|s| pricing::minor(s, scale))
                .transpose()?
                .unwrap_or(0);
            let value = i128::from(recurring) * i128::from(seconds);
            Ok(i64::try_from(
                (value / i128::from(period) + i128::from(value % i128::from(period) != 0))
                    .max(floor.into()),
            )?)
        };
        shortfall = shortfall
            .checked_add((unit(current)? - unit(accepted)?).max(0))
            .ok_or_else(|| anyhow::anyhow!("price overflow"))?;
        if current["currency"] != accepted["currency"]
            || current["period_seconds"] != accepted["period_seconds"]
        {
            shortfall = shortfall.max(1);
        }
        Ok((shortfall>0).then(||Error::new("conflict","native price moved",true,json!({"reason":"price_moved","shortfall_native":shortfall,"currency":current["currency"]}))))
    }
}
