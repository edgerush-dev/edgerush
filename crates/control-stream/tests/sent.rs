//! A config sent arrives the same: every example config, and every config the golden
//! translation cases expect, with certificates of its own, through the wire and run.

use edgerush_config::{Certificate, Config, HarnessFile};
use edgerush_control_stream::{Offer, Session, Staging, Taken, Versions, wire};
use prost::Message;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[test]
fn every_config_sent_arrives_the_same() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let read = |path: &Path| fs::read_to_string(path).unwrap().replace("\r\n", "\n");
    let mut configs: Vec<(String, Config)> = Vec::new();
    for path in [
        "examples/all-features.yaml",
        "examples/dev-harness.yaml",
        "examples/grpc.yaml",
        "examples/docker-compose/config/edgerush.yaml",
    ] {
        let file: HarnessFile = serde_saphyr::from_str(&read(&root.join(path))).unwrap();
        configs.push((path.to_owned(), file.into_config(BTreeMap::new(), None)));
    }
    let golden = root.join("crates/translate/tests/golden");
    let mut cases: Vec<_> = fs::read_dir(&golden)
        .unwrap()
        .map(|entry| entry.unwrap().path().join("config.yaml"))
        .filter(|path| path.exists())
        .collect();
    cases.sort();
    assert!(!cases.is_empty(), "no golden cases");
    for path in cases {
        let config: Config = serde_saphyr::from_str(&read(&path)).unwrap();
        configs.push((path.display().to_string(), config));
    }
    let mut versions = Versions::new(7);
    for (name, mut config) in configs {
        config.certificates = (0..2)
            .map(|n| {
                let certificate = Certificate {
                    chain: format!("chain {n}"),
                    key: format!("key {n}"),
                };
                (format!("shop/secret-{n}"), certificate)
            })
            .collect();
        let offer = Offer::new(&config, None, &mut versions).unwrap();
        let mut staging = Staging::new();
        let mut session = Session::open(staging.hello()).unwrap();
        let mut ran = None;
        for message in session.send(&offer) {
            let message = wire::ToDataPlane::decode(message.encode_to_vec().as_slice()).unwrap();
            match staging.take(message).unwrap() {
                Taken::Run(run) => ran = Some(*run),
                Taken::Nothing => {}
                Taken::Say(report) => panic!("{name}: {report:?}"),
                // No offer sends the gateway's addresses.
                Taken::Gateway(addresses) => panic!("{name}: {addresses:?}"),
            }
        }
        assert_eq!(ran.as_ref(), Some(&config), "{name}");
    }
}
