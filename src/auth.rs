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
// API-33: only the request that consumed a nonce can release its reservation.
// No Drop release: validation, cancellation and transaction errors keep it spent.
pub struct ConsumedAdmission<'a> {
    admission: &'a Admission,
    nonce: String,
}
impl ConsumedAdmission<'_> {
    /// STO-55/DOM-17: release only for a checkout refusal before request BEGIN.
    pub(crate) fn release_after_checkout_refusal(self) {
        self.admission
            .spent
            .lock()
            .expect("nonces")
            .remove(&self.nonce);
    }
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
    pub fn consume(&self, token: &str) -> Result<ConsumedAdmission<'_>, Error> {
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
        if nonce.len() != 64 || signature.len() != 64 || !signature.is_ascii() {
            return Err(invalid());
        }
        let bytes = (0..32)
            .map(|i| u8::from_str_radix(&signature[i * 2..i * 2 + 2], 16))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid())?;
        self.mac(data).verify_slice(&bytes).map_err(|_| invalid())?;
        let mut spent = self.spent.lock().expect("nonces");
        // API-33: judge expiry after waiting for the spent lock.
        let current = now();
        if t > current || current.saturating_sub(t) >= self.validity {
            return Err(invalid());
        }
        spent.retain(|_, expiry| *expiry > current);
        if spent.len() >= self.capacity || spent.contains_key(nonce) {
            return Err(invalid());
        }
        spent.insert(nonce.to_owned(), t + self.validity);
        Ok(ConsumedAdmission {
            admission: self,
            nonce: nonce.to_owned(),
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, mpsc},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    fn stamp() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs_f64()
    }

    // Start just after a whole second, so a request that starts at once reads the issue second.
    fn align() {
        let fraction = stamp().fract();
        thread::sleep(Duration::from_secs_f64(1.0 - fraction + 0.005));
    }

    fn refusal(admission: &Admission, token: &str) -> Option<serde_json::Value> {
        admission.consume(token).err().map(|e| e.0)
    }

    // One request waits for `spent` while the test holds it; `during` runs on the held records
    // just before release. Returns the request's refusal (None when served), start and release times.
    fn waiting_request(
        admission: &Arc<Admission>,
        token: &str,
        during: impl FnOnce(&mut BTreeMap<String, i64>),
    ) -> (Option<serde_json::Value>, f64, f64) {
        let mut held = admission.spent.lock().expect("nonces");
        let (send, receive) = mpsc::channel();
        let worker = Arc::clone(admission);
        let token = token.to_owned();
        let handle = thread::spawn(move || {
            send.send(stamp()).expect("start");
            refusal(&worker, &token)
        });
        let started = receive.recv_timeout(Duration::from_secs(2)).expect("start");
        thread::sleep(Duration::from_millis(1400));
        assert!(
            !handle.is_finished(),
            "the request must still be waiting for the lock"
        );
        during(&mut held);
        let released = stamp();
        drop(held);
        (handle.join().expect("request"), started, released)
    }

    // API-33/WIR-49: a code is refused once its validity period has ended, including after a wait.
    #[test]
    fn api33_code_expiring_while_waiting_is_refused() {
        let admission = Arc::new(Admission::new(1, 1, 64).unwrap());
        align();
        let (token, expiry) = admission.issue().unwrap();
        let (late, started, released) = waiting_request(&admission, &token, |_| {});
        let normal = admission.issue().unwrap().0;
        let in_time = admission.consume(&normal).is_ok();
        let repeated = refusal(&admission, &normal);
        let unknown = refusal(&admission, "pvd_a_1.2.3");
        println!(
            "TIMING {{\"request_start\":{started},\"expiry\":{expiry},\"released\":{released},\"late_accepted\":{},\"late_refusal_identical\":{},\"in_time_served\":{in_time},\"used_refused\":{}}}",
            late.is_none(),
            late.is_some() && late == unknown,
            repeated.is_some()
        );
        assert!(started < expiry as f64 && released > expiry as f64);
        assert!(in_time && repeated.is_some() && repeated == unknown);
        assert!(
            late.is_some(),
            "a request that waited past the validity period must be refused"
        );
        assert_eq!(late, unknown, "the late refusal must not say why");
    }

    // A request that waits but stays inside the validity period is still served.
    #[test]
    fn api33_code_valid_after_wait_is_served() {
        let admission = Arc::new(Admission::new(1, 3, 64).unwrap());
        align();
        let (token, expiry) = admission.issue().unwrap();
        let (result, started, released) = waiting_request(&admission, &token, |_| {});
        println!(
            "IN_TIME {{\"request_start\":{started},\"expiry\":{expiry},\"released\":{released},\"served\":{}}}",
            result.is_none()
        );
        assert!(released < expiry as f64);
        assert!(
            result.is_none(),
            "a request inside the validity period must be served after a wait"
        );
        assert!(refusal(&admission, &token).is_some());
    }

    // API-33/WIR-49, fixed order: a used code waits; its record is removed as a later request's cleanup
    // would remove it (validity ended by the later reading); the waiting request must still be refused.
    #[test]
    fn api33_used_code_refused_after_its_record_is_removed() {
        let admission = Arc::new(Admission::new(1, 1, 64).unwrap());
        align();
        let (token, expiry) = admission.issue().unwrap();
        assert!(admission.consume(&token).is_ok());
        assert!(refusal(&admission, &token).is_some());
        let mut removed = false;
        let (again, started, released) = waiting_request(&admission, &token, |records| {
            let later = now();
            records.retain(|_, expiry| *expiry > later);
            removed = records.is_empty();
        });
        let unknown = refusal(&admission, "pvd_a_1.2.3");
        println!(
            "REMOVED {{\"request_start\":{started},\"expiry\":{expiry},\"released\":{released},\"record_removed\":{removed},\"used_accepted\":{},\"refusal_identical\":{}}}",
            again.is_none(),
            again.is_some() && again == unknown
        );
        assert!(started < expiry as f64 && released > expiry as f64 && removed);
        assert!(
            again.is_some(),
            "a code used once must stay refused after its record is removed"
        );
        assert_eq!(again, unknown);
    }

    // API-33/WIR-49, real cleanup: a fresh request removes old records as the lock is released.
    #[test]
    fn api33_used_code_stays_refused_during_cleanup() {
        let mut accepted_again = 0;
        for attempt in 0..6 {
            let admission = Arc::new(Admission::new(1, 1, 64).unwrap());
            align();
            let (token, expiry) = admission.issue().unwrap();
            assert!(admission.consume(&token).is_ok());
            assert!(refusal(&admission, &token).is_some());
            let held = admission.spent.lock().expect("nonces");
            let (send, receive) = mpsc::channel();
            let worker = Arc::clone(&admission);
            let used = token.clone();
            let handle = thread::spawn(move || {
                send.send(stamp()).expect("start");
                worker.consume(&used).is_ok()
            });
            let started = receive.recv_timeout(Duration::from_secs(2)).expect("start");
            thread::sleep(Duration::from_millis(1400));
            assert!(
                !handle.is_finished(),
                "the used-code request must wait for the lock"
            );
            let fresh = admission.issue().unwrap().0;
            let released = stamp();
            drop(held);
            // The fresh request removes records whose validity period has ended.
            let fresh_served = admission.consume(&fresh).is_ok();
            let accepted = handle.join().expect("request");
            if accepted {
                accepted_again += 1;
            }
            println!(
                "BOOKKEEPING {{\"attempt\":{attempt},\"request_start\":{started},\"expiry\":{expiry},\"released\":{released},\"fresh_served\":{fresh_served},\"used_accepted\":{accepted}}}"
            );
            assert!(started < expiry as f64 && released > expiry as f64);
            assert!(fresh_served);
        }
        assert_eq!(
            accepted_again, 0,
            "a code used once must stay refused during cleanup"
        );
    }

    // STO-55/DOM-17 (D-71): only a pre-BEGIN checkout refusal releases a code for its resend.
    #[test]
    fn api33_checkout_refusal_resend_still_served() {
        let admission = Admission::new(1, 3, 64).unwrap();
        let (token, _) = admission.issue().unwrap();
        admission
            .consume(&token)
            .expect("first send")
            .release_after_checkout_refusal();
        assert!(
            admission.consume(&token).is_ok(),
            "the resend must be served"
        );
        assert!(
            refusal(&admission, &token).is_some(),
            "then the code is used"
        );
    }
}
