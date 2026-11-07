//! What is said of the dates of the certificates a data plane is handed
//! ([04 §2](../../../docs/04-control-plane.md), [08 §1](../../../docs/08-observability.md)):
//! a certificate out of date is served as any other, and the control plane is what says so.
//! Every replica shows when each one expires, as a gauge; the leader alone says, once for
//! each version handed out already expired or not yet valid, that clients will refuse it.
//!
//! Nothing here reads a clock: the time is handed in.

use edgerush_certificates::{Identity, Validity, common_name, validity};
use edgerush_config::Certificate;
use edgerush_telemetry::access_log::utc;
use edgerush_telemetry::{Exposition, Kind};
use std::collections::BTreeMap;

/// The gauge of when each certificate expires.
pub const GAUGE: &str = "edgerush_certificate_expiry_timestamp_seconds";

/// The certificates last handed out, by name, each with its dates.
#[derive(Debug, Default)]
pub struct Expiry {
    known: BTreeMap<String, Known>,
}

/// A certificate as it was last handed out.
#[derive(Debug)]
struct Known {
    /// Its chain, which tells a new version from the one held.
    chain: String,
    /// Its dates and common name; none where they cannot be read.
    read: Option<Read>,
}

#[derive(Debug)]
struct Read {
    validity: Validity,
    common_name: Option<String>,
}

impl Expiry {
    /// None held yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes `certificates`, those a config handed out names, at `now`, in seconds since
    /// the Unix epoch, and forgets the rest. What is said, where `leading`: a line for each
    /// version not held before that is out of date at `now`. One held already is not said
    /// again, out of date or not since: its gauge says when it expires.
    pub fn take(
        &mut self,
        certificates: &BTreeMap<String, Certificate>,
        now: i64,
        leading: bool,
    ) -> Vec<String> {
        let mut said = Vec::new();
        let mut known = BTreeMap::new();
        for (name, certificate) in certificates {
            let held = self
                .known
                .remove(name)
                .filter(|held| held.chain == certificate.chain);
            let fresh = held.is_none();
            let held = held.unwrap_or_else(|| Known {
                chain: certificate.chain.clone(),
                read: read(certificate),
            });
            if fresh && leading {
                said.extend(out_of_date(name, &held, now));
            }
            known.insert(name.clone(), held);
        }
        self.known = known;
        said
    }

    /// What a replica that has just come to lead says: a line for each certificate held
    /// that is out of date at `now`, as a control plane that has just started says it.
    #[must_use]
    pub fn led(&self, now: i64) -> Vec<String> {
        self.known
            .iter()
            .filter_map(|(name, held)| out_of_date(name, held, now))
            .collect()
    }

    /// The gauge, in the Prometheus text format: each certificate held whose dates can be
    /// read, by its name and its common name, empty where it has none.
    #[must_use]
    pub fn exposition(&self) -> String {
        let mut exposition = Exposition::new();
        exposition.family(
            GAUGE,
            Kind::Gauge,
            "When each certificate handed to the data plane expires: its notAfter, in seconds since the Unix epoch.",
        );
        for (name, held) in &self.known {
            if let Some(read) = &held.read {
                exposition.sample(
                    GAUGE,
                    &[
                        ("certificate", name),
                        ("common_name", read.common_name.as_deref().unwrap_or("")),
                    ],
                    read.validity.not_after,
                );
            }
        }
        exposition.finish()
    }
}

/// `certificate`'s dates and common name, as its leaf has them; none where it cannot be
/// read, which translation has already said.
fn read(certificate: &Certificate) -> Option<Read> {
    let identity = Identity::read(certificate).ok()?;
    Some(Read {
        validity: validity(&identity.leaf)?,
        common_name: common_name(&identity.leaf),
    })
}

/// What is said of `held`, named `name`, if it is out of date at `now`: expired after its
/// notAfter, which is the last second it is valid in, or not valid before its notBefore.
fn out_of_date(name: &str, held: &Known, now: i64) -> Option<String> {
    let read = held.read.as_ref()?;
    let named = match &read.common_name {
        Some(common_name) => format!("certificate {name} (common name {common_name})"),
        None => format!("certificate {name}"),
    };
    let at = |seconds: i64| utc(u64::try_from(seconds).unwrap_or(0));
    let refused = "it is served, and clients will refuse it";
    if now > read.validity.not_after {
        Some(format!(
            "{named} expired at {}: {refused}",
            at(read.validity.not_after)
        ))
    } else if now < read.validity.not_before {
        Some(format!(
            "{named} is not valid until {}: {refused}",
            at(read.validity.not_before)
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
