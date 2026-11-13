use super::*;
use k8s_openapi::jiff::Timestamp;

fn time(at: i64) -> Time {
    Time(Timestamp::from_second(1_800_000_000 + at).unwrap())
}

/// Each condition as `(type, status, reason)`.
fn said(conditions: &[Condition]) -> Vec<(&str, &str, &str)> {
    conditions
        .iter()
        .map(|condition| {
            (
                condition.type_.as_str(),
                condition.status.as_str(),
                condition.reason.as_str(),
            )
        })
        .collect()
}

fn of(accepted: Result<(), &Refused>, made: &Made, available: bool) -> Vec<Condition> {
    conditions(
        Some(3),
        accepted,
        made,
        "edgerush-proxy-web",
        available,
        &[],
        &time(0),
    )
}

#[test]
fn programmed_once_applied_and_a_replica_is_available() {
    let made = of(Ok(()), &Made::Applied, true);
    assert_eq!(
        said(&made),
        [
            ("Accepted", "True", "Accepted"),
            ("Programmed", "True", "Programmed")
        ]
    );
    assert!(
        made.iter()
            .all(|condition| condition.observed_generation == Some(3))
    );
    assert!(
        made[1].message.contains("edgerush-proxy-web"),
        "{}",
        made[1].message
    );
    let waiting = of(Ok(()), &Made::Applied, false);
    assert_eq!(said(&waiting)[1], ("Programmed", "False", "Pending"));
    assert!(waiting[1].message.contains("edgerush-proxy-web"));
}

#[test]
fn what_is_not_made_says_why() {
    let pending = of(Ok(()), &Made::Pending("Waiting.".to_owned()), true);
    assert_eq!(said(&pending)[1], ("Programmed", "False", "Pending"));
    assert_eq!(pending[1].message, "Waiting.");
    let not_ours = of(
        Ok(()),
        &Made::NotOurs(vec![
            "Deployment a/edgerush-proxy-web".to_owned(),
            "IngressClass edge.web".to_owned(),
        ]),
        true,
    );
    assert_eq!(said(&not_ours)[1], ("Programmed", "False", "NotOurs"));
    assert!(
        not_ours[1]
            .message
            .contains("Deployment a/edgerush-proxy-web, IngressClass edge.web"),
        "{}",
        not_ours[1].message
    );
    let refused = of(Ok(()), &Made::Refused("422: no".to_owned()), true);
    assert_eq!(said(&refused)[1], ("Programmed", "False", "ApplyRefused"));
    assert_eq!(refused[1].message, "422: no");
}

#[test]
fn a_refused_resource_says_why_and_that_nothing_of_it_is_made() {
    let refused = Refused::ScopeNotAllowed {
        from: "All".to_owned(),
    };
    // Whatever was made of a generation before.
    let made = of(Err(&refused), &Made::Applied, true);
    assert_eq!(
        said(&made),
        [
            ("Accepted", "False", "ScopeNotAllowed"),
            ("Programmed", "False", "Pending")
        ]
    );
    assert_eq!(made[0].message, refused.message());
}

#[test]
fn a_condition_keeps_its_time_until_its_status_changes() {
    let before = conditions(Some(1), Ok(()), &Made::Applied, "w", false, &[], &time(0));
    // A new generation, its reasons the same: the times kept.
    let later = conditions(
        Some(2),
        Ok(()),
        &Made::Pending("Again.".to_owned()),
        "w",
        false,
        &before,
        &time(60),
    );
    assert!(
        later
            .iter()
            .all(|condition| condition.last_transition_time == time(0))
    );
    // Programmed turned true: its time is now, Accepted's kept.
    let turned = conditions(
        Some(2),
        Ok(()),
        &Made::Applied,
        "w",
        true,
        &later,
        &time(90),
    );
    assert_eq!(turned[0].last_transition_time, time(0));
    assert_eq!(turned[1].last_transition_time, time(90));
}

#[test]
fn conditions_differ_by_anything_but_their_time() {
    let now = conditions(Some(1), Ok(()), &Made::Applied, "w", true, &[], &time(0));
    let again = conditions(Some(1), Ok(()), &Made::Applied, "w", true, &[], &time(60));
    assert!(!differs(&now, &again), "only the time differs");
    assert!(differs(&[], &now));
    assert!(differs(&now[..1], &now), "a type missing");
    let next = conditions(Some(2), Ok(()), &Made::Applied, "w", true, &now, &time(60));
    assert!(differs(&now, &next), "another generation");
    let pending = conditions(Some(1), Ok(()), &Made::Applied, "w", false, &now, &time(60));
    assert!(differs(&now, &pending), "another status");
    // One written beside them by another, such as the control plane's, is no difference.
    let mut beside = now.clone();
    beside.push(Condition {
        type_: "Other".to_owned(),
        ..now[0].clone()
    });
    assert!(!differs(&beside, &now));
}
