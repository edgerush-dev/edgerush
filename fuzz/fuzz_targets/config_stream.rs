//! Fuzzes the config stream's decoding ([02 §4](../../../docs/02-architecture.md)): any bytes
//! read as the messages one side of a stream sends the other, decoded and taken as each side
//! takes them.
//!
//! The first byte picks the side. Even: the bytes after it are what a control plane sends,
//! each message a byte and the message, length-delimited; the messages are taken by a data
//! plane's `Staging`, and the byte says whether a config given to run runs or fails. Odd:
//! they are what a data plane sends, each message length-delimited; the first opens a
//! `Session`, and the others are heard. Bytes that do not decode end the stream, as gRPC's
//! decoding would.
//!
//! Neither side may fail. A config given to run is the latest sent, whole, with every
//! certificate and the limits it names at the versions it names, as they last arrived; a
//! config that cannot be read is refused; each refusal is told, of the latest config; a
//! message the protocol does not allow changes nothing, and the data plane opens a stream
//! again with what it holds. What a session hears comes back the same through an encoding
//! of its own.
//!
//! `cargo fuzz run config_stream corpus/config_stream seeds/config_stream`.

#![no_main]

use edgerush_config::{Certificate, Config, Limits};
use edgerush_control_stream::wire::{self, FromDataPlane, Resource, ToDataPlane};
use edgerush_control_stream::{Held, Report, Session, Staging, Taken};
use libfuzzer_sys::fuzz_target;
use prost::Message;
use std::collections::BTreeMap;

fuzz_target!(|bytes: &[u8]| {
    let Some((&side, rest)) = bytes.split_first() else {
        return;
    };
    if side % 2 == 0 {
        data_plane(rest);
    } else {
        control_plane(rest);
    }
});

/// What the data plane was sent, as it arrived: what a config given to run is held to.
#[derive(Default)]
struct Arrived {
    /// Each certificate, by name, at its version.
    certificates: BTreeMap<String, (String, Certificate)>,
    /// The limits last sent, at their version.
    limits: Option<(String, Vec<u8>)>,
    /// The latest config.
    config: Option<wire::Config>,
}

impl Arrived {
    /// What the latest config is, whole, if what it names is here.
    fn whole(&self) -> Option<Config> {
        let sent = self.config.as_ref()?;
        let mut config: Config = serde_json::from_slice(&sent.json).ok()?;
        for (name, version) in &sent.requires {
            let (arrived, certificate) = self.certificates.get(name)?;
            if arrived != version {
                return None;
            }
            config
                .certificates
                .insert(name.clone(), certificate.clone());
        }
        if !sent.limits.is_empty() {
            let (arrived, json) = self.limits.as_ref()?;
            if *arrived != sent.limits {
                return None;
            }
            config.limits = Some(serde_json::from_slice::<Limits>(json).ok()?);
        }
        Some(config)
    }

    /// What a data plane running the latest config runs.
    fn running(&self) -> Option<Held> {
        let sent = self.config.as_ref()?;
        Some(Held {
            config: Some(sent.version.clone()),
            certificates: sent.requires.clone(),
            limits: Some(sent.limits.clone()).filter(|version| !version.is_empty()),
        })
    }

    fn latest(&self) -> Option<&str> {
        self.config.as_ref().map(|config| config.version.as_str())
    }

    fn took(&mut self, message: &ToDataPlane) {
        match &message.resource {
            Some(Resource::Certificate(certificate)) => {
                self.certificates.insert(
                    certificate.name.clone(),
                    (
                        certificate.version.clone(),
                        Certificate {
                            chain: certificate.chain.clone(),
                            key: certificate.key.clone(),
                        },
                    ),
                );
            }
            Some(Resource::Limits(limits)) => {
                self.limits = Some((limits.version.clone(), limits.json.clone()));
            }
            Some(Resource::Config(config)) => self.config = Some(config.clone()),
            Some(Resource::Removed(removed)) => {
                for name in &removed.names {
                    self.certificates.remove(name);
                }
            }
            Some(Resource::Gateway(_)) | None => {}
        }
    }
}

/// Each refusal a report tells is of the latest config.
fn told(report: &Report, arrived: &Arrived) {
    if let Some(refusal) = &report.refused {
        assert_eq!(Some(refusal.config.as_str()), arrived.latest());
    }
}

/// The latest config, if it cannot be read, is refused.
fn refused(staging: &Staging, arrived: &Arrived) {
    if let Some(sent) = &arrived.config
        && serde_json::from_slice::<Config>(&sent.json).is_err()
    {
        let refused = staging.report().refused.map(|refusal| refusal.config);
        assert_eq!(refused.as_ref(), Some(&sent.version));
    }
}

fn data_plane(mut rest: &[u8]) {
    let mut staging = Staging::new();
    let mut arrived = Arrived::default();
    opens(&staging);
    while let Some((&outcome, after)) = rest.split_first() {
        rest = after;
        let Ok(message) = ToDataPlane::decode_length_delimited(&mut rest) else {
            return;
        };
        let before = (staging.hello(), staging.report());
        match staging.take(message.clone()) {
            Err(malformed) => {
                let _told = malformed.to_string();
                assert_eq!((staging.hello(), staging.report()), before);
                opens(&staging);
            }
            Ok(taken) => {
                arrived.took(&message);
                match taken {
                    Taken::Nothing | Taken::Gateway(_) => {}
                    Taken::Say(report) => {
                        told(&report, &arrived);
                        // Kept, or refused.
                        assert!(
                            report.refused.is_some()
                                || report.running.config.as_deref() == arrived.latest()
                        );
                    }
                    Taken::Run(config) => {
                        assert_eq!(Some(*config), arrived.whole());
                        let report = if outcome % 2 == 0 {
                            let report = staging.ran().expect("a config was given");
                            assert_eq!(Some(&report.running), arrived.running().as_ref());
                            report
                        } else {
                            let report = staging
                                .failed("it fails".to_owned())
                                .expect("a config was given");
                            assert!(report.refused.is_some());
                            report
                        };
                        told(&report, &arrived);
                    }
                }
            }
        }
        told(&staging.report(), &arrived);
        refused(&staging, &arrived);
    }
}

/// A data plane opens a stream with what it holds, which a control plane serves.
fn opens(staging: &Staging) {
    let hello = FromDataPlane::decode(staging.hello().encode_to_vec().as_slice())
        .expect("our own encoding decodes");
    if let Err(error) = Session::open(hello) {
        panic!("a data plane's own hello is refused: {error}");
    }
}

fn control_plane(mut rest: &[u8]) {
    let Ok(first) = FromDataPlane::decode_length_delimited(&mut rest) else {
        return;
    };
    let session = match Session::open(first) {
        Ok(session) => session,
        Err(error) => {
            let _told = error.to_string();
            return;
        }
    };
    let held = session.held().clone();
    assert_eq!(Held::try_from(wire::Held::from(&held)), Ok(held));
    while !rest.is_empty() {
        let Ok(message) = FromDataPlane::decode_length_delimited(&mut rest) else {
            return;
        };
        match session.heard(message) {
            Err(malformed) => {
                let _told = malformed.to_string();
            }
            Ok(None) => {}
            Ok(Some(report)) => {
                let again =
                    FromDataPlane::decode(FromDataPlane::from(&report).encode_to_vec().as_slice())
                        .expect("our own encoding decodes");
                assert_eq!(session.heard(again), Ok(Some(report)));
            }
        }
    }
}
