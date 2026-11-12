//! How many request workers a data plane's proxy runs: its CPU request rounded up, no more
//! than its CPU limit rounded down, and at least one ([05 §5](../../../docs/05-operator.md)).
//! Never left to the process, which without a limit would count the node's CPUs.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

#[cfg(test)]
mod tests;

/// The workers for a pod of these `requests` and `limits`, or the quantity that could not
/// be read. Kubernetes takes a limit alone as the request too, and so does this.
pub(crate) fn workers(
    requests: Option<&BTreeMap<String, IntOrString>>,
    limits: Option<&BTreeMap<String, IntOrString>>,
) -> Result<u64, String> {
    let request = cpu(requests)?;
    let limit = cpu(limits)?;
    let wanted = request.or(limit).map_or(1, |millis| millis.div_ceil(1000));
    let ceiling = limit.map_or(u64::MAX, |millis| millis / 1000);
    Ok(wanted.min(ceiling).max(1))
}

/// The `cpu` of `resources`, in thousandths of a CPU, rounded up.
fn cpu(resources: Option<&BTreeMap<String, IntOrString>>) -> Result<Option<u64>, String> {
    match resources.and_then(|r| r.get("cpu")) {
        None => Ok(None),
        Some(IntOrString::Int(cpus)) => u64::try_from(*cpus)
            .ok()
            .and_then(|cpus| cpus.checked_mul(1000))
            .map(Some)
            .ok_or_else(|| cpus.to_string()),
        Some(IntOrString::String(written)) => {
            millis(written).map(Some).ok_or_else(|| written.clone())
        }
    }
}

/// A quantity written as Kubernetes writes one (`2`, `1.5`, `500m`, `1e3`, `2Ki`), in
/// thousandths, rounded up; `None` for one that is not a quantity, is negative, or is too
/// large to count.
pub(crate) fn millis(written: &str) -> Option<u64> {
    let unsigned = written.strip_prefix('+').unwrap_or(written);
    let number_ends = unsigned
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(unsigned.len());
    let (number, suffix) = unsigned.split_at(number_ends);
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if (whole.is_empty() && fraction.is_empty()) || fraction.contains('.') {
        return None;
    }
    // The value is digits / 10^fraction.len() * numerator / denominator.
    let digits: u128 = format!("{whole}{fraction}").parse().ok()?;
    let mut denominator = 10u128.checked_pow(u32::try_from(fraction.len()).ok()?)?;
    let mut numerator = 1000u128;
    match suffix {
        "" => {}
        "n" => denominator = denominator.checked_mul(1_000_000_000)?,
        "u" => denominator = denominator.checked_mul(1_000_000)?,
        "m" => denominator = denominator.checked_mul(1000)?,
        "k" | "M" | "G" | "T" | "P" | "E" => {
            let power = 1 + "kMGTPE".find(suffix)?;
            numerator = numerator.checked_mul(1000u128.checked_pow(u32::try_from(power).ok()?)?)?;
        }
        "Ki" | "Mi" | "Gi" | "Ti" | "Pi" | "Ei" => {
            let power = 1 + "KMGTPE".find(&suffix[..1])?;
            numerator = numerator.checked_mul(1u128 << (10 * power))?;
        }
        _ => {
            let exponent: i32 = suffix.strip_prefix(['e', 'E'])?.parse().ok()?;
            let scale = 10u128.checked_pow(exponent.unsigned_abs())?;
            if exponent < 0 {
                denominator = denominator.checked_mul(scale)?;
            } else {
                numerator = numerator.checked_mul(scale)?;
            }
        }
    }
    u64::try_from(digits.checked_mul(numerator)?.div_ceil(denominator)).ok()
}
