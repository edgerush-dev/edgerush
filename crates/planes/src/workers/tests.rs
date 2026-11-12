use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use proptest::prelude::*;

use super::{millis, workers};

fn cpu(quantity: &str) -> BTreeMap<String, IntOrString> {
    BTreeMap::from([("cpu".to_owned(), IntOrString::String(quantity.to_owned()))])
}

#[test]
fn a_request_is_rounded_up() {
    for (request, expected) in [
        ("0.3", 1),
        ("1.5", 2),
        ("3.2", 4),
        ("2", 2),
        ("500m", 1),
        ("2001m", 3),
    ] {
        assert_eq!(
            workers(Some(&cpu(request)), None),
            Ok(expected),
            "{request}"
        );
    }
}

#[test]
fn a_limit_caps_the_workers_rounded_down_and_leaves_at_least_one() {
    assert_eq!(workers(Some(&cpu("3.2")), Some(&cpu("2.9"))), Ok(2));
    assert_eq!(workers(Some(&cpu("0.3")), Some(&cpu("0.5"))), Ok(1));
    assert_eq!(workers(Some(&cpu("2")), Some(&cpu("8"))), Ok(2));
}

#[test]
fn a_limit_alone_is_the_request_as_kubernetes_takes_it() {
    assert_eq!(workers(None, Some(&cpu("3"))), Ok(3));
    assert_eq!(workers(None, Some(&cpu("2.5"))), Ok(2));
}

#[test]
fn neither_runs_one_worker() {
    assert_eq!(workers(None, None), Ok(1));
    let memory_alone =
        BTreeMap::from([("memory".to_owned(), IntOrString::String("1Gi".to_owned()))]);
    assert_eq!(workers(Some(&memory_alone), None), Ok(1));
}

#[test]
fn a_whole_number_of_cpus_may_be_an_integer() {
    let three = BTreeMap::from([("cpu".to_owned(), IntOrString::Int(3))]);
    assert_eq!(workers(Some(&three), None), Ok(3));
    let negative = BTreeMap::from([("cpu".to_owned(), IntOrString::Int(-3))]);
    assert_eq!(workers(Some(&negative), None), Err("-3".to_owned()));
}

#[test]
fn a_quantity_not_read_is_named() {
    assert_eq!(workers(Some(&cpu("two")), None), Err("two".to_owned()));
    assert_eq!(workers(None, Some(&cpu("1x"))), Err("1x".to_owned()));
}

#[test]
fn quantities_are_read_in_every_form_kubernetes_writes() {
    for (written, expected) in [
        ("1", Some(1000)),
        ("+1", Some(1000)),
        ("1.", Some(1000)),
        (".5", Some(500)),
        ("250m", Some(250)),
        ("1500000u", Some(1500)),
        ("1n", Some(1)),
        ("0", Some(0)),
        ("2k", Some(2_000_000)),
        ("1Ki", Some(1_024_000)),
        ("1e3", Some(1_000_000)),
        ("1E+3", Some(1_000_000)),
        ("15e-1", Some(1500)),
        ("1P", Some(1_000_000_000_000_000_000)),
        ("1E", None),
        ("-1", None),
        ("", None),
        (".", None),
        ("1.2.3", None),
        ("1q", None),
        ("1e", None),
        ("20E", None),
        ("1e40", None),
    ] {
        assert_eq!(millis(written), expected, "{written}");
    }
}

proptest! {
    /// Thousandths written plainly, as a decimal with a `m` suffix or as a fraction of a
    /// CPU, read back as they were.
    #[test]
    fn plain_quantities_read_back(thousandths in 0u64..10_000_000) {
        prop_assert_eq!(millis(&format!("{thousandths}m")), Some(thousandths));
        let whole = thousandths / 1000;
        let fraction = thousandths % 1000;
        prop_assert_eq!(millis(&format!("{whole}.{fraction:03}")), Some(thousandths));
    }

    /// Workers are always at least one, never above a limit's whole CPUs where that is one
    /// or more, and never below the request rounded up where no limit is.
    #[test]
    fn workers_stay_within_their_bounds(request in 1u64..100_000, limit in proptest::option::of(1u64..100_000)) {
        let limits = limit.map(|l| cpu(&format!("{l}m")));
        let got = workers(Some(&cpu(&format!("{request}m"))), limits.as_ref());
        let got = got.map_err(TestCaseError::fail)?;
        prop_assert!(got >= 1);
        match limit {
            Some(l) if l >= 1000 => prop_assert!(got <= l / 1000),
            Some(_) => prop_assert_eq!(got, 1),
            None => prop_assert_eq!(got, request.div_ceil(1000)),
        }
    }
}
