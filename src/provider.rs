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
        // PRV-13b/OVR-19: the mutation and its confirming read are separate
        // sequential waits, each bounded by its own priced descriptor term.
        let read = ["/offers", "/search", "/get", "/constraints"].contains(&path);
        let read_bound = Duration::from_secs(d["read_seconds"].as_u64().unwrap_or(1));
        let bound = if read {
            read_bound
        } else {
            Duration::from_secs(d["call_seconds"].as_u64().unwrap_or(1))
        };
        let deletion_read = (path == "/delete").then(|| v.clone());
        let v = http::call(
            self.address,
            if read { "GET" } else { "POST" },
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
            let mut error = wire::redact(v["error"].clone());
            if !matches!(
                error["kind"].as_str(),
                Some(
                    "invalid_request"
                        | "not_found"
                        | "conflict"
                        | "unsupported"
                        | "authentication"
                        | "rate_limited"
                        | "provider"
                        | "network"
                        | "timeout"
                        | "integrity"
                        | "internal"
                )
            ) || !error["retryable"].is_boolean()
                || !error["details"].is_object()
            {
                return Err(Error::new(
                    "internal",
                    "invalid simulator error response",
                    true,
                    json!({"accepted":null}),
                ));
            }
            // Provider prose is never a customer-safe message, even on a failed read.
            if error["kind"] == "authentication" && error["details"]["reason"].is_null() {
                error["details"]["reason"] = json!("token");
            }
            if error["details"].get("accepted").is_none() {
                error["details"]["accepted"] = Value::Null;
            }
            return Err(Error::new(
                error["kind"].as_str().expect("validated kind"),
                "simulator request refused",
                error["retryable"]
                    .as_bool()
                    .expect("validated retryability"),
                error["details"].clone(),
            ));
        }
        if let Some(request) = deletion_read {
            let observed = http::call(self.address, "GET", "/get", &request, read_bound)
                .await
                .map_err(|_| {
                    Error::new(
                        "network",
                        "stop-cost read unavailable",
                        false,
                        json!({"accepted":true}),
                    )
                })?;
            if !observed.is_null() && observed["present"] != false {
                return Err(Error::new(
                    "provider",
                    "stop-cost absence unconfirmed",
                    false,
                    json!({"accepted":true}),
                ));
            }
            return Ok(json!({"gone":true,"confirmed":true}));
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
        use base64::{
            Engine as _,
            engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
        };
        if let Some(keys) = r["ssh_public_keys"].as_array() {
            for (index, key) in keys.iter().enumerate() {
                let valid = key.as_str().is_some_and(|text| {
                    let mut words = text.split_whitespace();
                    words.next() == Some("ssh-ed25519")
                        && !text.contains(['\r', '\n'])
                        && words
                            .next()
                            .and_then(|v| {
                                STANDARD
                                    .decode(v)
                                    .or_else(|_| STANDARD_NO_PAD.decode(v))
                                    .ok()
                            })
                            .is_some_and(|blob| {
                                blob.len() == 51
                                    && blob.starts_with(b"\0\0\0\x0bssh-ed25519\0\0\0\x20")
                            })
                });
                if !valid {
                    bad.push(format!("/ssh_public_keys/{index}"));
                }
            }
        }
        if let Some(labels) = r["labels"].as_object() {
            for (key, value) in labels {
                if key.is_empty()
                    || key.len() > 63
                    || !key
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
                    || !value.as_str().is_some_and(|s| s.len() <= 256)
                {
                    bad.push(format!("/labels{}", wire::pointer(key)));
                }
            }
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
        descriptor: &Value,
        rate: &Value,
        runway: i64,
        admitted_sats: i64,
    ) -> anyhow::Result<Option<Error>> {
        if current["currency"] != accepted["currency"]
            || current["period_seconds"] != accepted["period_seconds"]
        {
            return Ok(Some(Error::new(
                "unsupported",
                "native price terms changed",
                false,
                json!({"provider_account":null,"capability":"native_price_terms"}),
            )));
        }
        let shortfall =
            pricing::native_coverage_shortfall(current, descriptor, rate, runway, admitted_sats)?;
        Ok((shortfall > 0).then(|| Error::new(
            "conflict", "native price moved", true,
            json!({"reason":"price_moved","shortfall_native":shortfall,"currency":current["currency"]}),
        )))
    }
}
