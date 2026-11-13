#![allow(
    clippy::unwrap_used,
    reason = "tests: an authority that cannot be made is a broken test"
)]

use super::*;
use crate::testing::Authority as TestAuthority;
use crate::{Identity, spiffe_id, validity};
use boring::x509::X509StoreContext;
use boring::x509::store::X509StoreBuilder;
use std::time::{SystemTime, UNIX_EPOCH};

/// Now, as the clock says: what verifying a chain checks dates against.
fn now() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
}

/// Whether `leaf`'s chain leads to one of `bundle`'s, at the clock's now.
fn verifies(leaf: &Written, bundle: &str) -> bool {
    let mut chain = X509::stack_from_pem(leaf.certificate.as_bytes())
        .unwrap()
        .into_iter();
    let first = chain.next().unwrap();
    let mut untrusted = boring::stack::Stack::new().unwrap();
    for certificate in chain {
        untrusted.push(certificate).unwrap();
    }
    let mut store = X509StoreBuilder::new().unwrap();
    for root in X509::stack_from_pem(bundle.as_bytes()).unwrap() {
        store.add_cert(root).unwrap();
    }
    let store = store.build();
    let mut context = X509StoreContext::new().unwrap();
    context
        .init(&store, &first, &untrusted, |context| {
            Ok(context.verify_cert()? && context.verify_result().is_ok())
        })
        .unwrap()
}

fn leaf() -> Leaf {
    Leaf {
        common_name: "edgerush-proxy-web".to_owned(),
        dns: vec!["edgerush-proxy-web.edge.svc".to_owned()],
        spiffe_id: "spiffe://edge.cluster.local/ns/edge/sa/edgerush-proxy-web".to_owned(),
    }
}

#[test]
fn an_authority_made_is_its_own_root_for_ten_years() {
    let now = now();
    let authority = Authority::make("edge", now).unwrap();
    let written = authority.written();
    let certificates = X509::stack_from_pem(written.certificate.as_bytes()).unwrap();
    assert_eq!(certificates.len(), 1);
    let dates = validity(&certificates[0]).unwrap();
    assert_eq!(dates.not_before, now - BACKDATE);
    assert_eq!(dates.not_after, now + AUTHORITY_SECONDS);
    // Its bundle is itself, and its key is the one its certificate holds.
    assert_eq!(written.bundle, written.certificate);
    let read = Authority::read(&written, Kind::Made, now).unwrap();
    assert_eq!(read.written(), written);
    // One made again is another: a key of its own.
    assert_ne!(
        Authority::make("edge", now).unwrap().written().key,
        written.key
    );
}

#[test]
fn a_leaf_is_issued_for_seven_days_with_its_names_and_a_key_of_its_own() {
    let now = now();
    let authority = Authority::make("edge", now).unwrap();
    let issued = authority.issue(&leaf(), now).unwrap();
    assert_eq!(issued.bundle, authority.written().bundle);
    assert!(verifies(&issued, &issued.bundle));
    let identity = Identity::read(&edgerush_config::Certificate {
        chain: issued.certificate.clone(),
        key: issued.key.clone(),
    })
    .unwrap();
    assert!(identity.covers("edgerush-proxy-web.edge.svc"));
    assert_eq!(
        spiffe_id(&identity.leaf).as_deref(),
        Some("spiffe://edge.cluster.local/ns/edge/sa/edgerush-proxy-web")
    );
    assert_eq!(
        crate::common_name(&identity.leaf).as_deref(),
        Some("edgerush-proxy-web")
    );
    let dates = validity(&identity.leaf).unwrap();
    assert_eq!(dates.not_before, now - BACKDATE);
    assert_eq!(dates.not_after, now + LEAF_SECONDS);
    // Each leaf is a key and a serial number of its own.
    let again = authority.issue(&leaf(), now).unwrap();
    assert_ne!(again.key, issued.key);
    let serial = |written: &Written| {
        X509::from_pem(written.certificate.as_bytes())
            .unwrap()
            .serial_number()
            .to_bn()
            .unwrap()
            .to_vec()
    };
    assert_ne!(serial(&again), serial(&issued));
    // Another authority's bundle does not trust it.
    let other = Authority::make("other", now).unwrap();
    assert!(!verifies(&issued, &other.written().bundle));
}

#[test]
fn a_leaf_is_due_at_half_its_life_and_an_authority_made_at_a_fifth_left() {
    let made = 1_000_000_000;
    let authority = Authority::make("edge", made).unwrap();
    let issued = authority.issue(&leaf(), made).unwrap();
    let dates = validity(&X509::from_pem(issued.certificate.as_bytes()).unwrap()).unwrap();
    let half = made - BACKDATE + (LEAF_SECONDS + BACKDATE) / 2;
    assert!(!renewal_due(dates, half - 1));
    assert!(renewal_due(dates, half));
    // Long past it, or before it is valid, it is due too: a clock moved is no reason to
    // keep a certificate.
    assert!(renewal_due(dates, made + LEAF_SECONDS * 2));
    assert!(renewal_due(dates, made - BACKDATE - 1));
    let fifth = made + AUTHORITY_SECONDS - (AUTHORITY_SECONDS + BACKDATE) / 5;
    assert!(!authority.due(fifth - 1));
    assert!(authority.due(fifth));
}

#[test]
fn an_authority_signed_again_keeps_its_key_and_trusts_both_meanwhile() {
    let now = now();
    let first = Authority::make("edge", now - AUTHORITY_SECONDS + 3600).unwrap();
    let old_leaf = first.issue(&leaf(), now).unwrap();
    let second = first.resign(now).unwrap();
    let written = second.written();
    assert_eq!(written.key, first.written().key);
    assert_ne!(written.certificate, first.written().certificate);
    // Both in the bundle while the first is valid: what either issued is trusted.
    assert_eq!(
        X509::stack_from_pem(written.bundle.as_bytes())
            .unwrap()
            .len(),
        2
    );
    let new_leaf = second.issue(&leaf(), now).unwrap();
    assert!(verifies(&new_leaf, &written.bundle));
    assert!(verifies(&old_leaf, &written.bundle));
    // Once the first has expired, it is left out.
    let third = second.resign(now + 7200).unwrap();
    assert_eq!(
        X509::stack_from_pem(third.written().bundle.as_bytes())
            .unwrap()
            .len(),
        2,
        "the second and the third"
    );
    assert!(
        !third
            .written()
            .bundle
            .contains(first.written().certificate.trim())
    );
}

#[test]
fn a_provided_authority_issues_with_its_chain_and_is_never_signed_again() {
    let now = now();
    // A platform's root, which signs an intermediate the ControlPlane is given.
    let root = TestAuthority::new("platform root");
    let intermediate = root.issue_authority("payments");
    let provided = Written {
        certificate: intermediate.chain.clone(),
        key: intermediate.key.clone(),
        bundle: root.pem(),
    };
    let authority = Authority::read(&provided, Kind::Provided, now).unwrap();
    assert_eq!(
        authority.written(),
        provided,
        "read as it is, never rewritten"
    );
    let issued = authority.issue(&leaf(), now).unwrap();
    // The leaf, then the intermediate, which leads to the platform's root.
    assert_eq!(
        X509::stack_from_pem(issued.certificate.as_bytes())
            .unwrap()
            .len(),
        2
    );
    assert_eq!(issued.bundle, root.pem());
    assert!(verifies(&issued, &root.pem()));
    assert!(!authority.due(now + AUTHORITY_SECONDS * 2));
    assert_eq!(authority.resign(now).err(), Some(IssueError::Provided));
}

#[test]
fn what_cannot_be_an_authority_is_refused_with_why() {
    let now = now();
    let made = Authority::make("edge", now).unwrap().written();
    let other = Authority::make("other", now).unwrap().written();
    let mismatched = Written {
        key: other.key.clone(),
        ..made.clone()
    };
    assert_eq!(
        Authority::read(&mismatched, Kind::Made, now).err(),
        Some(ReadError::Certificate(crate::CertificateError::Mismatch))
    );
    let no_bundle = Written {
        bundle: String::new(),
        ..made.clone()
    };
    assert!(matches!(
        Authority::read(&no_bundle, Kind::Made, now),
        Err(ReadError::Bundle(_))
    ));
    // A leaf is no authority: it may not sign.
    let leaf = Authority::make("edge", now)
        .unwrap()
        .issue(&leaf(), now)
        .unwrap();
    assert_eq!(
        Authority::read(&leaf, Kind::Provided, now).err(),
        Some(ReadError::NotAnAuthority)
    );
}
