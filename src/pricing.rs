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
        Ok(Self { n, d })
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
        n = n
            .checked_add(
                d[k].as_i64()
                    .filter(|x| *x > 0)
                    .context("missing positive wind-down term")?,
            )
            .context("duration overflow")?;
    }
    Ok(n)
}
pub fn sellable(o: &Value, d: &Value) -> bool {
    let today = chrono::Utc::now().date_naive();
    o["traffic_bound"] == "unlimited"
        && o["attachments"].as_array().is_some_and(|a| a.is_empty())
        && d["stop_cost"] == true
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
    let reserve = convert(
        Q::new(setup.into(), 1)?.add(unit)?.add(
            customer.mul(
                runway
                    .checked_add(wind)
                    .context("duration overflow")?
                    .into(),
                1,
            )?,
        )?,
    )?;
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
    })
}
/// PRV-44/OVR-19: this profile accepts one explicit simulator ordering channel.
pub(crate) fn validate_descriptor(d: &Value) -> Result<()> {
    ensure!(
        d["kind"] == "local_simulator"
            && d["channels"]
                .as_array()
                .is_some_and(|a| a.len() == 1 && a[0] == d["channel"])
            && d["channel"].is_string(),
        "unsupported local ordering-channel declaration"
    );
    ensure!(
        d["correlator"] == "operation_id"
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
