use super::*;
use edgerush_config::{Certificate, Config};
use proptest::prelude::*;
use prost::Message;
use std::collections::BTreeSet;

/// A config told apart by its port, with these certificates.
fn config(port: u16, certificates: &[(&str, &str)]) -> Config {
    let mut config: Config = serde_saphyr::from_str(&format!(
        "listeners:
  http:
    address: \"[::]:{port}\"
    protocol: http
    proxy_protocol: off
    forwarding: {{ trusted_proxies: [], trusted_only_headers: [Forwarded] }}
    request_id: generate
routes: []
upstreams: {{}}
"
    ))
    .unwrap();
    config.certificates = certificates
        .iter()
        .map(|(name, content)| {
            (
                (*name).to_owned(),
                Certificate {
                    chain: format!("chain {content}"),
                    key: format!("key {content}"),
                },
            )
        })
        .collect();
    config
}

/// Each message sent, through the wire, taken.
fn take(staging: &mut Staging, messages: Vec<wire::ToDataPlane>) -> Vec<Taken> {
    messages
        .into_iter()
        .map(|message| {
            let message = wire::ToDataPlane::decode(message.encode_to_vec().as_slice()).unwrap();
            staging.take(message).unwrap()
        })
        .collect()
}

fn hello(staging: &Staging) -> Session {
    let hello = staging.hello();
    Session::open(wire::FromDataPlane::decode(hello.encode_to_vec().as_slice()).unwrap()).unwrap()
}

/// A stream's first message: what is held, at a config version.
fn opening(
    held: Option<wire::Held>,
    config_version: u32,
    oldest_config_version: u32,
) -> wire::FromDataPlane {
    wire::FromDataPlane {
        said: Some(wire::Said::Hello(wire::Hello {
            held,
            config_version,
            oldest_config_version,
        })),
    }
}

fn served() -> ConfigVersion {
    ConfigVersion::served(CONFIG_VERSION).unwrap()
}

#[test]
fn a_config_is_written_at_the_older_end_and_refused_outside_either_window() {
    let open = |version, oldest| Session::open(opening(None, version, oldest));
    // At this control plane's own, and at each older one it still writes.
    for version in OLDEST_CONFIG_VERSION..=CONFIG_VERSION {
        let session = open(version, OLDEST_CONFIG_VERSION).unwrap();
        assert_eq!(session.version().get(), version);
        assert_eq!(session.reads(), version);
    }
    // A newer data plane that still reads this control plane's is written that.
    let session = open(CONFIG_VERSION + 2, CONFIG_VERSION).unwrap();
    assert_eq!(session.version(), served());
    assert_eq!(session.reads(), CONFIG_VERSION + 2);
    // One that no longer does is refused: this control plane is too old for it.
    assert_eq!(
        open(CONFIG_VERSION + 2, CONFIG_VERSION + 1).unwrap_err(),
        OpenError::ControlPlaneTooOld {
            oldest: CONFIG_VERSION + 1,
            newest: CONFIG_VERSION,
        }
    );
    // One older than this control plane writes for, or that does not say, is refused.
    for version in [0, OLDEST_CONFIG_VERSION - 1] {
        assert_eq!(
            open(version, 0).unwrap_err(),
            OpenError::DataPlaneTooOld {
                version,
                oldest: OLDEST_CONFIG_VERSION,
                newest: CONFIG_VERSION,
            }
        );
        assert_eq!(ConfigVersion::served(version), None);
    }
    // A data plane of this build opens at its own.
    let session = hello(&Staging::new());
    assert_eq!(session.version(), served());
    assert_eq!(session.reads(), CONFIG_VERSION);
}

#[test]
fn an_offer_is_written_for_every_config_version_served() {
    let offer = Offer::new(&config(80, &[]), None, &mut Versions::new(7)).unwrap();
    assert_eq!(offer.json.len(), ConfigVersion::all().count());
    for version in ConfigVersion::all() {
        let sent = Session::holding_nothing(version).send(&offer);
        let Some(wire::Resource::Config(config)) = &sent[0].resource else {
            panic!("no config")
        };
        assert_eq!(config.json, offer.json[version.index()]);
    }
}

fn running(offer: &Offer) -> Held {
    Held {
        config: Some(offer.version().to_owned()),
        certificates: offer
            .certificates()
            .map(|(name, version)| (name.to_owned(), version.to_owned()))
            .collect(),
    }
}

fn kinds(taken: &[Taken]) -> Vec<&'static str> {
    taken
        .iter()
        .map(|taken| match taken {
            Taken::Nothing => "nothing",
            Taken::Run(_) => "run",
            Taken::Say(_) => "say",
        })
        .collect()
}

fn sent(messages: &[wire::ToDataPlane]) -> Vec<String> {
    messages
        .iter()
        .map(|message| match &message.resource {
            Some(wire::Resource::Certificate(certificate)) => {
                format!("certificate {}", certificate.name)
            }
            Some(wire::Resource::Config(_)) => "config".to_owned(),
            Some(wire::Resource::Removed(removed)) => {
                format!("removed {}", removed.names.join(" "))
            }
            None => "none".to_owned(),
        })
        .collect()
}

#[test]
fn a_config_runs_once_its_certificates_are_there() {
    let mut versions = Versions::new(7);
    let whole = config(80, &[("a", "1"), ("b", "1")]);
    let offer = Offer::new(&whole, None, &mut versions).unwrap();
    let mut staging = Staging::new();
    let mut session = hello(&staging);
    let messages = session.send(&offer);
    assert_eq!(
        sent(&messages),
        ["certificate a", "certificate b", "config"]
    );
    let mut taken = take(&mut staging, messages);
    assert_eq!(kinds(&taken), ["nothing", "nothing", "run"]);
    let Some(Taken::Run(run)) = taken.pop() else {
        unreachable!()
    };
    assert_eq!(*run, whole);
    let report = staging.ran().unwrap();
    assert_eq!(report.running, running(&offer));
    assert_eq!(report.refused, None);
    // As the control plane hears it.
    assert_eq!(session.heard((&report).into()).unwrap(), Some(report));
    assert_eq!(session.held(), &running(&offer));
}

#[test]
fn a_config_waits_for_each_certificate_at_the_version_it_names() {
    let mut versions = Versions::new(7);
    let first = Offer::new(&config(80, &[("a", "1")]), None, &mut versions).unwrap();
    let second = Offer::new(&config(80, &[("a", "2")]), Some(&first), &mut versions).unwrap();
    let mut messages = hello(&Staging::new()).send(&second);
    assert_eq!(sent(&messages), ["certificate a", "config"]);
    let mut staging = Staging::new();
    // The config first, and an older version of its certificate.
    let config = messages.pop().unwrap();
    let older = hello(&Staging::new()).send(&first).remove(0);
    let taken = take(&mut staging, vec![config, older]);
    assert_eq!(kinds(&taken), ["nothing", "nothing"]);
    let taken = take(&mut staging, messages);
    assert_eq!(kinds(&taken), ["run"]);
}

#[test]
fn what_a_data_plane_holds_is_not_sent_again() {
    let mut versions = Versions::new(7);
    let whole = config(80, &[("a", "1")]);
    let offer = Offer::new(&whole, None, &mut versions).unwrap();
    let mut session = hello(&Staging::new());
    assert_eq!(session.send(&offer).len(), 2);
    assert!(session.send(&offer).is_empty());
    // The same config offered again keeps its versions.
    let again = Offer::new(&whole, Some(&offer), &mut versions).unwrap();
    assert_eq!(again, offer);
    assert!(session.send(&again).is_empty());
}

#[test]
fn a_certificate_changed_alone_is_sent_with_a_new_config_version() {
    let mut versions = Versions::new(7);
    let first = Offer::new(&config(80, &[("a", "1"), ("b", "1")]), None, &mut versions).unwrap();
    let rotated = config(80, &[("a", "1"), ("b", "2")]);
    let second = Offer::new(&rotated, Some(&first), &mut versions).unwrap();
    assert_ne!(second.version(), first.version());
    let a = |offer: &Offer| offer.certificates().next().unwrap().1.to_owned();
    assert_eq!(a(&second), a(&first));
    let mut staging = Staging::new();
    let mut session = hello(&staging);
    take(&mut staging, session.send(&first));
    staging.ran().unwrap();
    let messages = session.send(&second);
    assert_eq!(sent(&messages), ["certificate b", "config"]);
    let mut taken = take(&mut staging, messages);
    assert_eq!(kinds(&taken), ["nothing", "run"]);
    let Some(Taken::Run(run)) = taken.pop() else {
        unreachable!()
    };
    assert_eq!(*run, rotated);
}

#[test]
fn a_certificate_no_longer_offered_goes_last_and_stays_with_the_config_running() {
    let mut versions = Versions::new(7);
    let first = Offer::new(&config(80, &[("a", "1")]), None, &mut versions).unwrap();
    let second = Offer::new(&config(81, &[]), Some(&first), &mut versions).unwrap();
    let mut staging = Staging::new();
    let mut session = hello(&staging);
    take(&mut staging, session.send(&first));
    staging.ran().unwrap();
    let mut messages = session.send(&second);
    assert_eq!(sent(&messages), ["config", "removed a"]);
    // The new config fails to run, and the one running keeps its certificate.
    let removed = messages.pop().unwrap();
    assert_eq!(kinds(&take(&mut staging, messages)), ["run"]);
    staging.failed("no".to_owned()).unwrap();
    assert_eq!(kinds(&take(&mut staging, vec![removed])), ["nothing"]);
    assert_eq!(staging.report().running, running(&first));
    let mut fresh = hello(&staging);
    assert!(fresh.held().certificates.is_empty());
    assert!(fresh.send(&second).is_empty());
}

#[test]
fn a_config_equal_to_the_one_running_is_kept_under_its_new_versions() {
    let whole = config(80, &[("a", "1")]);
    let before = Offer::new(&whole, None, &mut Versions::new(7)).unwrap();
    let mut staging = Staging::new();
    let messages = hello(&staging).send(&before);
    take(&mut staging, messages);
    staging.ran().unwrap();
    // A control plane started again names the same config anew.
    let after = Offer::new(&whole, None, &mut Versions::new(8)).unwrap();
    assert_ne!(after.version(), before.version());
    let mut session = hello(&staging);
    let mut taken = take(&mut staging, session.send(&after));
    assert_eq!(kinds(&taken), ["nothing", "say"]);
    let Some(Taken::Say(report)) = taken.pop() else {
        unreachable!()
    };
    assert_eq!(report.running, running(&after));
    assert_eq!(staging.ran(), None);
}

#[test]
fn a_config_that_cannot_be_read_is_refused_and_the_last_runs_on() {
    let mut versions = Versions::new(7);
    let first = Offer::new(&config(80, &[]), None, &mut versions).unwrap();
    let mut staging = Staging::new();
    let messages = hello(&staging).send(&first);
    take(&mut staging, messages);
    staging.ran().unwrap();
    let unread = wire::ToDataPlane {
        resource: Some(wire::Resource::Config(wire::Config {
            version: "9.1".to_owned(),
            json: br#"{"listeners": {}, "routes": [], "upstreams": {}, "tomorrow": 1}"#.to_vec(),
            requires: BTreeMap::new(),
        })),
    };
    let Taken::Say(report) = staging.take(unread).unwrap() else {
        panic!("not refused")
    };
    assert_eq!(report.running, running(&first));
    let refusal = report.refused.unwrap();
    assert_eq!(refusal.config, "9.1");
    assert!(refusal.why.contains("tomorrow"), "{}", refusal.why);
    // It is held, so that it is not sent again.
    assert_eq!(hello(&staging).held().config.as_deref(), Some("9.1"));
}

#[test]
fn a_config_that_fails_to_run_is_not_given_again_unchanged() {
    let mut versions = Versions::new(7);
    let first = Offer::new(&config(80, &[]), None, &mut versions).unwrap();
    let second = Offer::new(&config(81, &[("a", "1")]), Some(&first), &mut versions).unwrap();
    let mut staging = Staging::new();
    let mut session = hello(&staging);
    take(&mut staging, session.send(&first));
    staging.ran().unwrap();
    take(&mut staging, session.send(&second));
    let report = staging.failed("the port is taken".to_owned()).unwrap();
    assert_eq!(report.running, running(&first));
    assert_eq!(
        report.refused,
        Some(Refusal {
            config: second.version().to_owned(),
            why: "the port is taken".to_owned(),
        })
    );
    assert_eq!(staging.failed("again".to_owned()), None);
    // Its certificate again, as after a stream dialled again, gives nothing to run.
    let again = hello(&Staging::new()).send(&second).remove(0);
    assert_eq!(kinds(&take(&mut staging, vec![again])), ["nothing"]);
    // A config after it is given to run.
    let third = Offer::new(&config(82, &[("a", "1")]), Some(&second), &mut versions).unwrap();
    let taken = take(&mut staging, session.send(&third));
    assert_eq!(kinds(&taken), ["run"]);
    assert_eq!(staging.ran().unwrap().refused, None);
}

#[test]
fn a_newer_config_takes_the_place_of_one_waiting() {
    let mut versions = Versions::new(7);
    let waiting = Offer::new(&config(80, &[("a", "1")]), None, &mut versions).unwrap();
    let newer = Offer::new(&config(81, &[]), Some(&waiting), &mut versions).unwrap();
    let mut staging = Staging::new();
    let mut session = hello(&staging);
    let mut messages = session.send(&waiting);
    let config_alone = messages.pop().unwrap();
    assert_eq!(kinds(&take(&mut staging, vec![config_alone])), ["nothing"]);
    let messages = session.send(&newer);
    assert_eq!(sent(&messages), ["config", "removed a"]);
    let mut taken = take(&mut staging, messages);
    assert_eq!(kinds(&taken), ["run", "nothing"]);
    let Taken::Run(run) = taken.remove(0) else {
        unreachable!()
    };
    assert_eq!(*run, config(81, &[]));
}

#[test]
fn a_message_the_protocol_does_not_allow_ends_the_stream() {
    let report = wire::FromDataPlane::from(&Report::default());
    assert_eq!(
        Session::open(report.clone()).unwrap_err(),
        Malformed::NoHello.into()
    );
    assert_eq!(
        Session::open(wire::FromDataPlane::default()).unwrap_err(),
        Malformed::NoHello.into()
    );
    let session = Session::open(Staging::new().hello()).unwrap();
    assert_eq!(
        session.heard(Staging::new().hello()).unwrap_err(),
        Malformed::HelloAgain
    );
    let unversioned = wire::Held {
        config: String::new(),
        certificates: [("a".to_owned(), String::new())].into(),
    };
    let unnamed = wire::Held {
        config: String::new(),
        certificates: [(String::new(), "1".to_owned())].into(),
    };
    for (held, expected) in [
        (unversioned, Malformed::Unversioned),
        (unnamed, Malformed::Unnamed),
    ] {
        assert_eq!(
            Session::open(opening(Some(held), CONFIG_VERSION, OLDEST_CONFIG_VERSION)).unwrap_err(),
            expected.into()
        );
    }
    let refused = wire::FromDataPlane {
        said: Some(wire::Said::Report(wire::Report {
            running: None,
            refused: Some(wire::Refusal::default()),
        })),
    };
    assert_eq!(session.heard(refused).unwrap_err(), Malformed::Unversioned);
    let mut staging = Staging::new();
    let certificate = |name: &str, version: &str| wire::ToDataPlane {
        resource: Some(wire::Resource::Certificate(wire::Certificate {
            name: name.to_owned(),
            version: version.to_owned(),
            ..Default::default()
        })),
    };
    assert_eq!(
        staging.take(certificate("", "1")).unwrap_err(),
        Malformed::Unnamed
    );
    assert_eq!(
        staging.take(certificate("a", "")).unwrap_err(),
        Malformed::Unversioned
    );
    let config = wire::ToDataPlane {
        resource: Some(wire::Resource::Config(wire::Config::default())),
    };
    assert_eq!(staging.take(config).unwrap_err(), Malformed::Unversioned);
}

#[test]
fn what_one_side_does_not_know_is_passed_over() {
    let mut staging = Staging::new();
    assert!(matches!(
        staging.take(wire::ToDataPlane::default()).unwrap(),
        Taken::Nothing
    ));
    let session = Session::open(Staging::new().hello()).unwrap();
    assert_eq!(session.heard(wire::FromDataPlane::default()).unwrap(), None);
}

#[test]
fn a_config_with_limits_is_not_offered() {
    let mut limited = config(80, &[]);
    limited.limits = Some(edgerush_config::Limits {
        global: Default::default(),
        limits: Vec::new(),
    });
    assert!(matches!(
        Offer::new(&limited, None, &mut Versions::new(7)),
        Err(OfferError::Limits)
    ));
}

#[test]
fn a_resource_longer_than_a_message_is_not_offered() {
    let whole = config(80, &[("a", "1")]);
    let mut versions = Versions::new(7);
    let lengths: Vec<usize> = Session::holding_nothing(served())
        .send(&Offer::new(&whole, None, &mut versions).unwrap())
        .iter()
        .map(Message::encoded_len)
        .collect();
    let (certificate, config) = (lengths[0], lengths[1]);
    let fits = certificate.max(config);
    assert!(Offer::within(&whole, None, &mut versions, fits).is_ok());
    let too_large = |limit| match Offer::within(&whole, None, &mut versions.clone(), limit) {
        Err(OfferError::TooLarge { what, bytes, .. }) => (what, bytes),
        other => panic!("{other:?}"),
    };
    let smaller = certificate.min(config);
    let (what, bytes) = too_large(fits - 1);
    assert_eq!(bytes, fits);
    let expected = if config > certificate {
        "the config"
    } else {
        "the certificate a"
    };
    assert_eq!(what, expected);
    // The certificate is sent first, so it is the one said.
    assert_eq!(
        too_large(smaller - 1),
        ("the certificate a".to_owned(), certificate)
    );
}

#[test]
fn two_runs_of_a_control_plane_never_name_two_resources_alike() {
    let whole = config(80, &[("a", "1"), ("b", "2")]);
    let names = |start| {
        let offer = Offer::new(&whole, None, &mut Versions::new(start)).unwrap();
        let mut names: BTreeSet<String> = offer
            .certificates()
            .map(|(_, version)| version.to_owned())
            .collect();
        names.insert(offer.version().to_owned());
        names
    };
    let (first, second) = (names(7), names(8));
    assert_eq!(first.len(), 3);
    assert!(first.is_disjoint(&second));
}

#[test]
fn a_certificate_key_is_never_printed() {
    let offer = Offer::new(&config(80, &[("a", "secret")]), None, &mut Versions::new(7)).unwrap();
    let messages = hello(&Staging::new()).send(&offer);
    let printed = format!("{messages:?}{offer:?}");
    assert!(!printed.contains("key secret"), "{printed}");
    assert!(printed.contains("chain secret"), "{printed}");
}

/// Takes `messages`, and runs each config given to run, unless `fail` says it fails: checks
/// that it is an offer's config, whole, with that offer's own certificates.
fn deliver(
    staging: &mut Staging,
    messages: Vec<wire::ToDataPlane>,
    offers: &[(Offer, Config)],
    failed: &mut BTreeSet<String>,
    fail: &mut dyn FnMut() -> bool,
) -> Result<(), TestCaseError> {
    // Each config given to run is said to run, or to have failed, before the next message.
    for message in messages {
        let Taken::Run(run) = take(staging, vec![message]).remove(0) else {
            continue;
        };
        let (version, report) = if fail() {
            let report = staging.failed("no".to_owned()).unwrap();
            let version = report.refused.clone().unwrap().config;
            failed.insert(version.clone());
            (version, report)
        } else {
            let report = staging.ran().unwrap();
            (report.running.config.clone().unwrap(), report)
        };
        let Some((offer, whole)) = offers.iter().find(|(offer, _)| offer.version() == version)
        else {
            return Err(TestCaseError::fail(format!("{version} was never offered")));
        };
        prop_assert_eq!(whole, &*run);
        if report.refused.is_none() {
            prop_assert_eq!(&report.running, &running(offer));
        }
    }
    Ok(())
}

/// What a step of the property test does.
#[derive(Debug, Clone)]
enum Step {
    /// The control plane offers this: a port, and each certificate's content.
    Offer(u16, Vec<(u8, u8)>),
    /// The data plane takes every message sent.
    Deliver,
    /// The stream breaks after this many messages of those not yet taken, and is dialled
    /// again.
    Break(usize),
    /// The data plane starts again, holding nothing.
    Restart,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => (
            80u16..83,
            proptest::collection::vec((0u8..3, 0u8..3), 0..3)
        )
            .prop_map(|(port, certificates)| Step::Offer(port, certificates)),
        2 => Just(Step::Deliver),
        1 => (0usize..4).prop_map(Step::Break),
        1 => Just(Step::Restart),
    ]
}

proptest! {
    /// Whatever is offered, however streams break and data planes start again, and
    /// whichever configs fail to run: a config runs only whole, as it was offered, and the
    /// data plane ends running the last offer, unless that very config failed to run.
    #[test]
    fn a_data_plane_runs_only_what_was_offered_and_ends_on_the_last(
        steps in proptest::collection::vec(step(), 1..24),
        failures in proptest::collection::vec(any::<bool>(), 24),
    ) {
        let mut versions = Versions::new(7);
        let mut offers: Vec<(Offer, Config)> = Vec::new();
        let mut staging = Staging::new();
        let mut session = hello(&staging);
        let mut queue: Vec<wire::ToDataPlane> = Vec::new();
        let mut failed: BTreeSet<String> = BTreeSet::new();
        let mut failures = failures.into_iter().cycle();
        for step in steps {
            match step {
                Step::Offer(port, certificates) => {
                    let names = ["a", "b", "c"];
                    let contents = ["0", "1", "2"];
                    let certificates: Vec<(&str, &str)> = certificates
                        .iter()
                        .map(|&(name, content)| (names[name as usize], contents[content as usize]))
                        .collect();
                    let whole = config(port, &certificates);
                    let offer = Offer::new(&whole, offers.last().map(|(offer, _)| offer), &mut versions).unwrap();
                    queue.extend(session.send(&offer));
                    offers.push((offer, whole));
                }
                Step::Deliver => {
                    let delivered = std::mem::take(&mut queue);
                    deliver(&mut staging, delivered, &offers, &mut failed, &mut || failures.next().unwrap_or(false))?;
                }
                Step::Break(after) => {
                    let delivered: Vec<_> = queue.drain(..after.min(queue.len())).collect();
                    queue.clear();
                    deliver(&mut staging, delivered, &offers, &mut failed, &mut || failures.next().unwrap_or(false))?;
                    session = hello(&staging);
                    if let Some((offer, _)) = offers.last() {
                        queue.extend(session.send(offer));
                    }
                }
                Step::Restart => {
                    queue.clear();
                    staging = Staging::new();
                    failed.clear();
                    session = hello(&staging);
                    if let Some((offer, _)) = offers.last() {
                        queue.extend(session.send(offer));
                    }
                }
            }
        }
        // The stream lasts, and the rest is taken, each config running.
        let rest = std::mem::take(&mut queue);
        deliver(&mut staging, rest, &offers, &mut failed, &mut || false)?;
        let report = staging.report();
        if let Some((offer, _)) = offers.last() {
            if failed.contains(offer.version()) {
                prop_assert_eq!(report.refused.map(|refusal| refusal.config), Some(offer.version().to_owned()));
            } else {
                prop_assert_eq!(report.running, running(offer));
                prop_assert_eq!(report.refused, None);
            }
        } else {
            prop_assert_eq!(report, Report::default());
        }
    }
}
