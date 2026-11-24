//! Fuzzes the reading and checking of the rate-limiting service pod's config file
//! (`edgerush_config::ServiceFile`, [06 §4](../../../docs/06-rate-limiting.md)): any bytes at
//! all, as the file's, read as `edgerush-ratelimit-service --config` reads them
//! (`edgerush_ratelimit_service::service_file`).
//!
//! Whatever it is given, reading must not fail, nor checking what was read, and what either
//! says of a refusal must be told without failing: the pod writes it to its log. A file the
//! pod would run is one the target reads too, with the same options, and the pod runs what
//! it says: where it listens and is reached, its metrics, its keys' cap and its TLS, on a
//! store whose settings the store itself would take. It is given an address of its own, as
//! a pod in Kubernetes is, which it is reached at where the file says none.
//!
//! `cargo fuzz run service_file corpus/service_file seeds/service_file`.

#![no_main]

use edgerush_config::ServiceFile;
use edgerush_ratelimit_service::service_file::{MAX_KEY_BYTES, parsed};
use libfuzzer_sys::fuzz_target;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;

/// The address a pod is given, as Kubernetes gives one in `POD_IP`.
const OWN: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3));

fuzz_target!(|data: &[u8]| {
    let pod = match parsed(data, Path::new("/etc/edgerush"), Some(OWN)) {
        Ok(pod) => pod,
        Err(error) => {
            let _told = error.to_string();
            return;
        }
    };
    let options = serde_saphyr::options! { with_snippet: false };
    let file: ServiceFile =
        serde_saphyr::from_slice_with_options(data, options).expect("a file the pod runs reads");
    assert_eq!(pod.listen, file.pod.listen);
    assert_eq!(pod.metrics, file.pod.metrics);
    assert_eq!(
        pod.tls.map(|tls| tls.trust_domain),
        file.tls.map(|tls| tls.trust_domain)
    );
    let store = pod.store;
    let given = SocketAddr::new(OWN, file.pod.listen.port()).to_string();
    assert_eq!(store.address, file.pod.address.unwrap_or(given));
    assert_eq!(
        store.config.max_key_bytes,
        Some(file.keys.max_bytes.unwrap_or(MAX_KEY_BYTES))
    );
    assert_ne!(store.config.max_key_bytes, Some(0));
    assert_eq!(store.config.check(), Ok(()));
});
