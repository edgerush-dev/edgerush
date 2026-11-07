use super::*;
use edgerush_certificates::testing::dated;

/// 2027-01-15T08:00:00Z.
const NOW: i64 = 1_800_000_000;

fn handed(certificates: &[(&str, &Certificate)]) -> BTreeMap<String, Certificate> {
    certificates
        .iter()
        .map(|(name, certificate)| ((*name).to_owned(), (*certificate).clone()))
        .collect()
}

/// The leader says each certificate handed out of date, by its name and common name, and
/// nothing of one in date; its first second and its last are in date.
#[test]
fn the_leader_says_each_certificate_handed_out_of_date() {
    let fine = dated(&["ok.test"], Some("ok.test"), NOW - 100, NOW + 100);
    let edges = dated(&["edge.test"], Some("edge.test"), NOW, NOW);
    let expired = dated(&["old.test"], Some("old.test"), 1_700_000_000, NOW - 1);
    let early = dated(&["new.test"], None, NOW + 1, NOW + 1_000);
    let mut expiry = Expiry::new();
    let said = expiry.take(
        &handed(&[
            ("shop/edges", &edges),
            ("shop/early", &early),
            ("shop/expired", &expired),
            ("shop/fine", &fine),
        ]),
        NOW,
        true,
    );
    assert_eq!(
        said,
        [
            "certificate shop/early is not valid until 2027-01-15T08:00:01Z: it is served, \
             and clients will refuse it",
            "certificate shop/expired (common name old.test) expired at 2027-01-15T07:59:59Z: \
             it is served, and clients will refuse it",
        ]
    );
}

/// Once for each version: the same chain again says nothing, a new one out of date is said
/// again, and one forgotten and handed again is new.
#[test]
fn each_version_is_said_once() {
    let expired = dated(&["old.test"], Some("old.test"), 0, NOW - 1);
    let renewed_badly = dated(&["old.test"], Some("old.test"), 0, NOW - 1);
    let mut expiry = Expiry::new();
    assert_eq!(
        expiry
            .take(&handed(&[("shop/tls", &expired)]), NOW, true)
            .len(),
        1
    );
    assert!(
        expiry
            .take(&handed(&[("shop/tls", &expired)]), NOW + 60, true)
            .is_empty()
    );
    assert_eq!(
        expiry
            .take(&handed(&[("shop/tls", &renewed_badly)]), NOW + 120, true)
            .len(),
        1
    );
    assert!(expiry.take(&BTreeMap::new(), NOW + 180, true).is_empty());
    assert_eq!(
        expiry
            .take(&handed(&[("shop/tls", &renewed_badly)]), NOW + 240, true)
            .len(),
        1
    );
}

/// One that goes out of date while held is not said: its gauge shows it.
#[test]
fn one_that_expires_while_held_is_not_said() {
    let soon = dated(&["a.test"], Some("a.test"), 0, NOW);
    let mut expiry = Expiry::new();
    let certificates = handed(&[("shop/tls", &soon)]);
    assert!(expiry.take(&certificates, NOW, true).is_empty());
    assert!(expiry.take(&certificates, NOW + 1, true).is_empty());
}

/// A replica that does not lead says nothing; once it leads, it says every certificate it
/// holds out of date at that moment, as a control plane just started would.
#[test]
fn a_replica_says_nothing_until_it_leads() {
    let expired = dated(&["old.test"], Some("old.test"), 0, NOW - 1);
    let fine = dated(&["ok.test"], Some("ok.test"), 0, NOW + 1_000);
    let mut expiry = Expiry::new();
    let certificates = handed(&[("shop/old", &expired), ("shop/ok", &fine)]);
    assert!(expiry.take(&certificates, NOW, false).is_empty());
    assert_eq!(
        expiry.led(NOW),
        [
            "certificate shop/old (common name old.test) expired at 2027-01-15T07:59:59Z: it \
          is served, and clients will refuse it"
        ]
    );
    // Taken again as the leader, it is held already, so is not said again.
    assert!(expiry.take(&certificates, NOW, true).is_empty());
    // One that has expired since is said by a new leader, which knows nothing of before.
    assert_eq!(expiry.led(NOW + 1_001).len(), 2);
}

/// The gauge has each certificate held, by name and common name, its notAfter in seconds;
/// one forgotten is gone, and one that cannot be read has none.
#[test]
fn the_gauge_says_when_each_held_expires() {
    let named = dated(&["a.test"], Some("a.test"), 0, 1_900_000_000);
    let unnamed = dated(&["b.test"], None, 0, 1_850_000_000);
    let unreadable = Certificate {
        chain: "not a certificate".to_owned(),
        key: named.key.clone(),
    };
    let mut expiry = Expiry::new();
    let help = "# HELP edgerush_certificate_expiry_timestamp_seconds When each certificate \
                handed to the data plane expires: its notAfter, in seconds since the Unix \
                epoch.\n# TYPE edgerush_certificate_expiry_timestamp_seconds gauge\n";
    assert_eq!(expiry.exposition(), help);
    let _said = expiry.take(
        &handed(&[
            ("shop/a", &named),
            ("shop/b", &unnamed),
            ("shop/broken", &unreadable),
        ]),
        NOW,
        false,
    );
    assert_eq!(
        expiry.exposition(),
        format!(
            "{help}edgerush_certificate_expiry_timestamp_seconds{{certificate=\"shop/a\",\
             common_name=\"a.test\"}} 1900000000\n\
             edgerush_certificate_expiry_timestamp_seconds{{certificate=\"shop/b\",\
             common_name=\"\"}} 1850000000\n"
        )
    );
    let _said = expiry.take(&handed(&[("shop/b", &unnamed)]), NOW, false);
    assert_eq!(
        expiry.exposition(),
        format!(
            "{help}edgerush_certificate_expiry_timestamp_seconds{{certificate=\"shop/b\",\
             common_name=\"\"}} 1850000000\n"
        )
    );
}
