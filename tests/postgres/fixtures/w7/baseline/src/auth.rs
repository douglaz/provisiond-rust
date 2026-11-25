//! API-3/33/36/55, WIR-49: keyed stateless issuance and bounded nonce spending.
use crate::wire::Error;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;
use std::{collections::BTreeMap, io::Read, sync::Mutex};
pub fn secret(prefix: &str) -> anyhow::Result<String> {
    let mut bytes = [0; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(format!(
        "{prefix}{}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    ))
}
pub fn now() -> i64 {
    chrono::Utc::now().timestamp()
}
pub fn time(t: i64) -> String {
    chrono::DateTime::from_timestamp(t, 0)
        .expect("validated timestamp")
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}
pub struct Admission {
    key: Vec<u8>,
    spent: Mutex<BTreeMap<String, i64>>,
    pub delay_ms: u64,
    validity: i64,
    capacity: usize,
}
impl Admission {
    pub fn new(delay_ms: u64, validity: i64, capacity: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            delay_ms > 0 && validity > 0 && capacity > 0,
            "invalid admission parameters"
        );
        Ok(Self {
            key: secret("")?.into_bytes(),
            spent: Mutex::new(BTreeMap::new()),
            delay_ms,
            validity,
            capacity,
        })
    }
    fn mac(&self, data: &str) -> Hmac<Sha256> {
        let mut m = Hmac::<Sha256>::new_from_slice(&self.key).expect("key length");
        m.update(data.as_bytes());
        m
    }
    pub fn issue(&self) -> anyhow::Result<(String, i64)> {
        let t = now();
        let data = format!("{t}.{}", secret("")?);
        let mac = self.mac(&data).finalize().into_bytes();
        Ok((
            format!(
                "pvd_a_{data}.{}",
                mac.iter().map(|b| format!("{b:02x}")).collect::<String>()
            ),
            t + self.validity,
        ))
    }
    pub fn consume(&self, token: &str) -> Result<(), Error> {
        let invalid = || {
            Error::new(
                "invalid_request",
                "invalid admission token",
                false,
                json!({}),
            )
        };
        let token = token.strip_prefix("pvd_a_").ok_or_else(invalid)?;
        let (data, signature) = token.rsplit_once('.').ok_or_else(invalid)?;
        let (t, nonce) = data.split_once('.').ok_or_else(invalid)?;
        let t = t.parse::<i64>().map_err(|_| invalid())?;
        let current = now();
        if t > current
            || current.saturating_sub(t) >= self.validity
            || nonce.len() != 64
            || signature.len() != 64
            || !signature.is_ascii()
        {
            return Err(invalid());
        }
        let bytes = (0..32)
            .map(|i| u8::from_str_radix(&signature[i * 2..i * 2 + 2], 16))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid())?;
        self.mac(data).verify_slice(&bytes).map_err(|_| invalid())?;
        let mut spent = self.spent.lock().expect("nonces");
        spent.retain(|_, expiry| *expiry > current);
        if spent.len() >= self.capacity || spent.contains_key(nonce) {
            return Err(invalid());
        }
        spent.insert(nonce.to_owned(), t + self.validity);
        Ok(())
    }
}

impl Admission {
    pub fn seal(&self, data: &str) -> String {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let sig = self.mac(data).finalize().into_bytes();
        format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(data),
            URL_SAFE_NO_PAD.encode(sig)
        )
    }
    pub fn open(&self, token: &str) -> Option<String> {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let (data, sig) = token.split_once('.')?;
        let data = String::from_utf8(URL_SAFE_NO_PAD.decode(data).ok()?).ok()?;
        let sig = URL_SAFE_NO_PAD.decode(sig).ok()?;
        self.mac(&data).verify_slice(&sig).ok()?;
        Some(data)
    }
}
