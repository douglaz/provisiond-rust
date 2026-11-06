//! WIR-1a/2/3/9/9a: strict I-JSON, UTF-16 JCS ordering and closed errors.
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fmt;

pub const SAFE_INTEGER: i64 = 9_007_199_254_740_991;
#[derive(Clone, Debug)]
pub struct Error(pub Value);
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0["message"].as_str().unwrap_or("request refused"))
    }
}
impl std::error::Error for Error {}
impl Error {
    pub fn new(kind: &str, message: &str, retryable: bool, details: Value) -> Self {
        Self(json!({"kind":kind,"message":message,"retryable":retryable,"details":details}))
    }
    pub fn field(field: &str) -> Self {
        Self::new(
            "invalid_request",
            "invalid request field",
            false,
            json!({"field":field}),
        )
    }
    pub fn conflict(reason: &str) -> Self {
        Self::new(
            "conflict",
            "request conflicts with stored state",
            false,
            json!({"reason":reason}),
        )
    }
    pub fn missing() -> Self {
        Self::new("not_found", "resource not found", false, json!({}))
    }
    pub fn status(&self) -> u16 {
        match self.0["kind"].as_str().unwrap_or("internal") {
            "invalid_request" => 400,
            "authentication" => 401,
            "insufficient_balance" => 402,
            "not_activated" | "suspended" => 403,
            "not_found" => 404,
            "conflict" => 409,
            "gone" => 410,
            "integrity" => 422,
            "rate_limited" | "ceiling_exceeded" => 429,
            "unsupported" => 501,
            "provider" | "network" | "timeout" => 502,
            "halted" | "overloaded" => 503,
            _ => 500,
        }
    }
}
pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
// Safe integers are exactly representable, but JCS serializes negative zero as zero.
pub fn parse(bytes: &[u8]) -> Result<Value, Error> {
    struct Strict(Value);
    impl<'de> Deserialize<'de> for Strict {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = Strict;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("I-JSON with bounded integers")
                }
                fn visit_bool<E: de::Error>(self, v: bool) -> Result<Strict, E> {
                    Ok(Strict(Value::Bool(v)))
                }
                fn visit_unit<E: de::Error>(self) -> Result<Strict, E> {
                    Ok(Strict(Value::Null))
                }
                fn visit_str<E: de::Error>(self, v: &str) -> Result<Strict, E> {
                    Ok(Strict(json!(v)))
                }
                fn visit_string<E: de::Error>(self, v: String) -> Result<Strict, E> {
                    Ok(Strict(json!(v)))
                }
                fn visit_i64<E: de::Error>(self, v: i64) -> Result<Strict, E> {
                    if (-SAFE_INTEGER..=SAFE_INTEGER).contains(&v) {
                        Ok(Strict(json!(v)))
                    } else {
                        Err(E::custom("integer range"))
                    }
                }
                fn visit_u64<E: de::Error>(self, v: u64) -> Result<Strict, E> {
                    if v <= SAFE_INTEGER as u64 {
                        Ok(Strict(json!(v)))
                    } else {
                        Err(E::custom("integer range"))
                    }
                }
                fn visit_f64<E: de::Error>(self, _: f64) -> Result<Strict, E> {
                    Err(E::custom("fraction or exponent"))
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                    let mut out = Vec::new();
                    while let Some(Strict(v)) = a.next_element()? {
                        out.push(v);
                    }
                    Ok(Strict(Value::Array(out)))
                }
                fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                    let mut out = serde_json::Map::new();
                    while let Some(k) = a.next_key::<String>()? {
                        if out.contains_key(&k) {
                            return Err(de::Error::custom("duplicate member"));
                        }
                        let Strict(v) = a.next_value()?;
                        out.insert(k, v);
                    }
                    Ok(Strict(Value::Object(out)))
                }
            }
            d.deserialize_any(V)
        }
    }
    let mut d = serde_json::Deserializer::from_slice(bytes);
    let value = Strict::deserialize(&mut d).and_then(|v| d.end().map(|()| v.0));
    value.map_err(|_| {
        Error::new(
            "invalid_request",
            "body must be valid bounded I-JSON",
            false,
            json!({}),
        )
    })
}
pub fn canonical(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<_> = m.keys().collect();
            keys.sort_by_cached_key(|k| k.encode_utf16().collect::<Vec<_>>());
            format!(
                "{{{}}}",
                keys.into_iter()
                    .map(|k| format!("{}:{}", json!(k), canonical(&m[k])))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(canonical).collect::<Vec<_>>().join(",")
        ),
        _ => v.to_string(),
    }
}
pub fn fingerprint(method: &str, target: &str, body: &Value) -> String {
    digest(
        format!(
            "{}\n{}\n{}",
            method.to_uppercase(),
            target.trim_end_matches('?'),
            digest(canonical(body).as_bytes())
        )
        .as_bytes(),
    )
}
pub fn pointer(k: &str) -> String {
    format!("/{}", k.replace('~', "~0").replace('/', "~1"))
}
pub fn least(mut fields: Vec<String>) -> Result<(), Error> {
    fields.sort_by_cached_key(|s| s.encode_utf16().collect::<Vec<_>>());
    if let Some(f) = fields.first() {
        Err(Error::field(f))
    } else {
        Ok(())
    }
}
pub fn unknown(v: &Value, allowed: &[&str]) -> Vec<String> {
    match v.as_object() {
        Some(m) => m
            .keys()
            .filter(|k| !allowed.contains(&k.as_str()))
            .map(|k| pointer(k))
            .collect(),
        None => vec![String::new()],
    }
}

/// API-13/15, PRV-8/10: one validator for admission and OPS-23 repetition.
pub fn validate_create(v: &Value, min_runway: i64) -> Result<(), Error> {
    let mut bad = unknown(
        v,
        &[
            "provider_account",
            "offer",
            "region",
            "hostname",
            "image",
            "ssh_public_keys",
            "user_data",
            "labels",
            "runway_seconds",
            "max_commitment_sats",
            "acknowledge_purchase",
            "provider_options",
        ],
    );
    for (k, max) in [("provider_account", 256), ("offer", 256), ("hostname", 253)] {
        if !v[k]
            .as_str()
            .is_some_and(|s| !s.is_empty() && s.chars().count() <= max && !s.contains(['\r', '\n']))
        {
            bad.push(pointer(k));
        }
    }
    if v["acknowledge_purchase"] != true {
        bad.push(pointer("acknowledge_purchase"));
    }
    if !v["runway_seconds"]
        .as_i64()
        .is_some_and(|n| n >= min_runway)
    {
        bad.push(pointer("runway_seconds"));
    }
    if v.get("max_commitment_sats")
        .is_some_and(|v| !v.as_i64().is_some_and(|n| n >= 0))
    {
        bad.push(pointer("max_commitment_sats"));
    }
    match v["ssh_public_keys"].as_array() {
        Some(a) if !a.is_empty() && a.len() <= 64 => {
            for (i, k) in a.iter().enumerate() {
                if !k.as_str().is_some_and(|s| {
                    s.len() <= 16384
                        && (s.starts_with("ssh-ed25519 ")
                            || s.starts_with("ssh-rsa ")
                            || s.starts_with("ecdsa-sha2-"))
                        && s.split_whitespace().nth(1).is_some_and(|s| s.len() >= 8)
                }) {
                    bad.push(format!("/ssh_public_keys/{i}"));
                }
            }
        }
        _ => bad.push(pointer("ssh_public_keys")),
    }
    for (k, max) in [("image", 256), ("user_data", 1_048_576), ("region", 256)] {
        if let Some(x) = v.get(k) {
            if !x.is_null() && !x.as_str().is_some_and(|s| !s.is_empty() && s.len() <= max) {
                bad.push(pointer(k));
            }
        }
    }
    if let Some(labels) = v.get("labels") {
        match labels.as_object() {
            Some(m) => {
                for (k, x) in m {
                    if !x.is_string() || k == "provisiond_operation" {
                        bad.push(format!("/labels{}", pointer(k)));
                    }
                }
            }
            None => bad.push(pointer("labels")),
        }
    }
    if !v["provider_options"].is_object() {
        bad.push(pointer("provider_options"));
    }
    let runway_bad = bad.contains(&pointer("runway_seconds"));
    least(bad).map_err(|mut e| {
        if runway_bad && e.0["details"]["field"] == "/runway_seconds" {
            e.0["details"]["min_runway_seconds"] = json!(min_runway);
        }
        e
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_profile() {
        for s in [
            r#"{"x":{"a":1,"a":2}}"#,
            r#"{"x":[1.0]}"#,
            "1e0",
            "9007199254740992",
        ] {
            assert!(parse(s.as_bytes()).is_err(), "{s}");
        }
        assert!(parse(&[255]).is_err());
    }
    #[test]
    fn jcs_and_fingerprint() {
        let a = parse(br#"{ "z": 2, "a":1 }"#).unwrap();
        let b = parse(br#"{"a":1,"z":2}"#).unwrap();
        assert_eq!(
            fingerprint("POST", "/v1/machines", &a),
            fingerprint("POST", "/v1/machines", &b)
        );
        assert_ne!(
            fingerprint("POST", "/v1/machines", &a),
            fingerprint("POST", "/v1/deposits", &b)
        );
        assert_ne!(
            fingerprint("POST", "/v1/machines", &a),
            fingerprint("GET", "/v1/machines", &a)
        );
        assert_eq!(
            canonical(&json!({"\u{e000}":1,"\u{10000}":2})),
            "{\"𐀀\":2,\"\":1}"
        );
    }
}
