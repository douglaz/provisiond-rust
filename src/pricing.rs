//! PRV-13b/13d, LDG-1/2/3/4/23/24/27/28/29: exact local-product pricing.
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
#[derive(Clone, Copy)]
struct Q {
    n: i128,
    d: i128,
}
impl Q {
    fn new(n: i128, d: i128) -> Result<Self> {
        ensure!(n >= 0 && d > 0, "invalid rational");
        let (mut a, mut b) = (n, d);
        while b != 0 {
            (a, b) = (b, a % b);
        }
        Ok(Self { n: n / a, d: d / a })
    }
    fn add(self, b: Self) -> Result<Self> {
        Self::new(
            self.n
                .checked_mul(b.d)
                .and_then(|n| b.n.checked_mul(self.d).and_then(|b| n.checked_add(b)))
                .context("price overflow")?,
            self.d.checked_mul(b.d).context("price overflow")?,
        )
    }
    fn mul(self, n: i128, d: i128) -> Result<Self> {
        Self::new(
            self.n.checked_mul(n).context("price overflow")?,
            self.d.checked_mul(d).context("price overflow")?,
        )
    }
    fn ceil(self) -> Result<i64> {
        let x = self.n / self.d + i128::from(self.n % self.d != 0);
        let x = i64::try_from(x)?;
        ensure!(x <= crate::wire::SAFE_INTEGER, "wire monetary range");
        Ok(x)
    }
}
pub fn minor(s: &str, scale: u32) -> Result<i64> {
    ensure!(scale <= 6 && !s.is_empty(), "unsupported native precision");
    let (a, b) = s.split_once('.').unwrap_or((s, ""));
    ensure!(
        !a.is_empty()
            && a.bytes().all(|x| x.is_ascii_digit())
            && b.bytes().all(|x| x.is_ascii_digit())
            && b.len() <= scale as usize,
        "invalid exact native price"
    );
    let unit = 10_i64.pow(scale);
    let whole = a
        .parse::<i64>()?
        .checked_mul(unit)
        .context("native overflow")?;
    whole
        .checked_add(if b.is_empty() {
            0
        } else {
            b.parse::<i64>()?
                .checked_mul(10_i64.pow(scale - b.len() as u32))
                .context("native overflow")?
        })
        .context("native overflow")
}
#[derive(Clone)]
pub struct Price {
    pub reserve: i64,
    pub setup: Value,
    pub unit: Option<Value>,
    pub recurring_sats: i64,
    pub wind_sats: i64,
    pub per_second_num: i64,
    pub per_second_den: i64,
    required_native: Q,
}
/// The seven sequential waits are descriptor/configuration inputs, never max().
pub fn wind_seconds(d: &Value) -> Result<i64> {
    let mut n = 0_i64;
    for k in [
        "meter_seconds",
        "call_seconds",
        "visibility_seconds",
        "resolution_seconds",
        "read_seconds",
        "hold_seconds",
        "margin_seconds",
    ] {
        let declared = d[k]
            .as_i64()
            .filter(|n| *n > 0)
            .context("missing positive wind-down term")?;
        let value = if k == "visibility_seconds" {
            d["effective_visibility_seconds"]
                .as_i64()
                .unwrap_or(declared)
                .max(declared)
        } else {
            declared
        };
        n = n.checked_add(value).context("duration overflow")?;
    }
    Ok(n)
}
/// PRV-36/STO-53: declaration stays immutable; effective sizing only widens.
pub(crate) fn with_visibility(d: &Value, seconds: i64) -> Value {
    let mut effective = d.clone();
    if seconds > d["visibility_seconds"].as_i64().unwrap_or(0) {
        effective["effective_visibility_seconds"] = json!(seconds);
    }
    effective
}
pub fn sellable(o: &Value, d: &Value) -> bool {
    let today = chrono::Utc::now().date_naive();
    o["period_seconds"] == 3600
        && o["traffic_bound"] == "unlimited"
        && o["attachments"].as_array().is_some_and(|a| a.is_empty())
        && d["stop_cost"] == true
        && has(d, "delete_machine")
        && o["sellable"] == true
        && [o.get("cancellation_bound"), d.get("cancellation_bound")]
            .iter()
            .flatten()
            .filter(|v| !v.is_null())
            .all(|v| {
                v.as_str()
                    .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                    .is_some_and(|x| x <= today)
            })
        && (o["cancellation_bound"].is_string() || d["cancellation_bound"].is_string())
}
pub fn capability(kind: &str) -> &'static str {
    if kind == "bare_metal" {
        "provision_bare_metal"
    } else {
        "provision_virtual"
    }
}
pub fn has(d: &Value, cap: &str) -> bool {
    d["capabilities"]
        .as_array()
        .is_some_and(|a| a.iter().any(|v| v == cap))
}
pub fn calculate(o: &Value, d: &Value, r: &Value, runway: i64) -> Result<Price> {
    ensure!(o["currency"] == r["currency"], "currency mismatch");
    let scale = r["minor_scale"]
        .as_u64()
        .context("missing currency scale")? as u32;
    let recurring = minor(
        o["recurring_price"]
            .as_str()
            .context("missing native price")?,
        scale,
    )?;
    let setup = minor(o["setup_fee"].as_str().context("missing setup")?, scale)?;
    let period = o["period_seconds"]
        .as_i64()
        .filter(|v| *v > 0)
        .context("invalid period")?;
    let seconds = o["billing_unit"]["seconds"]
        .as_i64()
        .filter(|v| *v >= 0)
        .context("invalid billing unit")?;
    let floor = o["billing_unit"]["native_floor"]
        .as_str()
        .map(|v| minor(v, scale))
        .transpose()?
        .unwrap_or(0);
    let margin = d
        .get("margin_bps")
        .unwrap_or(&r["margin_bps"])
        .as_i64()
        .filter(|v| (0..=10000).contains(v))
        .context("invalid margin")?;
    let haircut = r["haircut_bps"]
        .as_i64()
        .filter(|v| (0..=10000).contains(v))
        .context("invalid haircut")?;
    let num = r["rate_num"]
        .as_i64()
        .filter(|v| *v > 0)
        .context("invalid rate")?;
    let den = r["rate_den"]
        .as_i64()
        .filter(|v| *v > 0)
        .context("invalid rate")?;
    let native_rate = Q::new(recurring.into(), period.into())?;
    let customer = native_rate.mul((10000 + margin).into(), 10000)?;
    let unit_time = native_rate.mul(seconds.into(), 1)?;
    let unit_native = if unit_time.n
        >= i128::from(floor)
            .checked_mul(unit_time.d)
            .context("price overflow")?
    {
        unit_time
    } else {
        Q::new(floor.into(), 1)?
    };
    let unit = unit_native.mul((10000 + margin).into(), 10000)?;
    let wind = wind_seconds(d)?;
    ensure!(runway >= wind, "runway below wind-down floor");
    let convert = |q: Q| {
        q.mul(num.into(), den.into())?
            .mul((10000 + haircut).into(), 10000)?
            .ceil()
    };
    let required_native = Q::new(setup.into(), 1)?.add(unit)?.add(
        customer.mul(
            runway
                .checked_add(wind)
                .context("duration overflow")?
                .into(),
            1,
        )?,
    )?;
    let reserve = convert(required_native)?;
    let evidence = |sats: i64, native: i64| json!({"sats":sats,"native_minor":native,"currency":o["currency"],"rate_num":num,"rate_den":den,"rate_source":r["source"],"rate_observed_at":r["observed_at"],"haircut_bps":haircut,"rounding_rule_version":"ceil-v1"});
    let unit_sats = convert(unit)?;
    let mut unit_evidence = evidence(unit_sats, unit_native.ceil()?);
    unit_evidence["seconds"] = json!(seconds);
    let per_second = customer
        .mul(num.into(), den.into())?
        .mul((10000 + haircut).into(), 10000)?;
    Ok(Price {
        reserve,
        setup: evidence(convert(Q::new(setup.into(), 1)?)?, setup),
        unit: (unit_sats > 0).then_some(unit_evidence),
        recurring_sats: convert(customer.mul(period.into(), 1)?)?,
        wind_sats: convert(customer.mul(wind.into(), 1)?)?,
        per_second_num: i64::try_from(per_second.n)?,
        per_second_den: i64::try_from(per_second.d)?,
        required_native,
    })
}
/// OPS-43: measure coverage at the accepted conversion basis, including offsets
/// between native components and the rounding headroom in the admitted reserve.
pub(crate) fn native_coverage_shortfall(
    offer: &Value,
    descriptor: &Value,
    rate: &Value,
    runway: i64,
    admitted_sats: i64,
) -> Result<i64> {
    ensure!(admitted_sats > 0, "invalid admitted reserve");
    let price = calculate(offer, descriptor, rate, runway)?;
    if price.reserve <= admitted_sats {
        return Ok(0);
    }
    let coverage = Q::new(i128::from(admitted_sats), 1)?
        .mul(
            i128::from(rate["rate_den"].as_i64().context("rate denominator")?),
            i128::from(rate["rate_num"].as_i64().context("rate numerator")?),
        )?
        .mul(
            10000,
            i128::from(10000 + rate["haircut_bps"].as_i64().context("haircut")?),
        )?;
    let required = price.required_native;
    let deficit = required
        .n
        .checked_mul(coverage.d)
        .and_then(|n| {
            coverage
                .n
                .checked_mul(required.d)
                .and_then(|c| n.checked_sub(c))
        })
        .context("native shortfall overflow")?;
    Q::new(
        deficit,
        required
            .d
            .checked_mul(coverage.d)
            .context("native shortfall overflow")?,
    )?
    .ceil()
}
/// PRV-44/OVR-19: explicit simulator ordering channels and immediate destruction.
pub(crate) fn validate_descriptor(d: &Value) -> Result<()> {
    ensure!(
        d["delete_shape"] == "immediate_destroy"
            && d["surviving_attachments"]
                .as_array()
                .is_some_and(|a| a.is_empty()),
        "local deletion requires an explicit no-survivor immediate-destroy declaration"
    );
    ensure!(
        d["negative_seconds"]
            .as_i64()
            .is_some_and(|n| n > 0 && n <= i64::from(i32::MAX)),
        "missing bounded provider negative window"
    );
    ensure!(
        d["kind"] == "local_simulator"
            && d["channels"].as_array().is_some_and(|a| !a.is_empty()
                && a.iter().all(|v| v.as_str().is_some_and(|s| !s.is_empty()))
                && a.contains(&d["channel"])
                && a.iter()
                    .map(|v| v.as_str().expect("validated channel"))
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    == a.len())
            && d["channel"].is_string(),
        "unsupported local ordering-channel declaration"
    );
    ensure!(
        matches!(d["correlator"].as_str(), Some("operation_id" | "none"))
            && d["temporary_keys"].is_boolean()
            && d["offer_is_resource"].is_boolean(),
        "unsupported correlator/key declaration"
    );
    ensure!(
        d["order_budget"]["limit"].as_i64().is_some_and(|n| n > 0)
            && d["order_budget"]["per_seconds"]
                .as_i64()
                .is_some_and(|n| n > 0),
        "missing positive channel order-budget declaration"
    );
    ensure!(
        d["capabilities"]
            .as_array()
            .is_some_and(|a| a.iter().all(|v| matches!(
                v.as_str(),
                Some("provision_virtual" | "list_offers" | "delete_machine")
            )))
            && d["allow_orders"].is_boolean(),
        "unsupported local capability"
    );
    ensure!(
        d["allow_orders"] == true || !has(d, "provision_virtual"),
        "ordering capability requires allow_orders"
    );
    ensure!(
        d["margin_bps"]
            .as_i64()
            .is_some_and(|n| (0..=10000).contains(&n))
            && d["max_rate_outage_seconds"].as_i64().is_some_and(|n| n > 0),
        "invalid margin/outage configuration"
    );
    wind_seconds(d)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_exact() {
        assert_eq!(minor("12.34", 2).unwrap(), 1234);
        for s in ["1e2", "-1", "1.001", "NaN"] {
            assert!(minor(s, 2).is_err());
        }
    }
}
