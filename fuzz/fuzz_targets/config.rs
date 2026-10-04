//! Fuzzes the reading of the harness's config file and the compiling of what it reads
//! ([07 §1](../../../docs/07-config-and-dsl.md)): any bytes at all, as a file's.
//!
//! The file is read as `edgerush/src/config_file.rs` reads it, with the same options: the
//! config crate knows no format, so the reading is the harness's, repeated here. Each
//! certificate the file names stands as one the harness would have read from its files.
//!
//! Whatever it is given, reading must not fail, nor compiling what was read, and what
//! either says of a refusal must be told without failing: the harness writes it to its log.
//! A config that compiles has every listener and upstream the file names, and an `https`
//! listener among them presents at least one certificate; one that does not compile is
//! refused with at least one problem.
//!
//! `cargo fuzz run config corpus/config seeds/config`.

#![no_main]

use edgerush_config::{Certificate, HarnessFile, Protocol, compile};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let options = serde_saphyr::options! { with_snippet: false };
    let file: HarnessFile = match serde_saphyr::from_slice_with_options(data, options) {
        Ok(file) => file,
        Err(error) => {
            let _told = error.to_string();
            return;
        }
    };
    let certificates = file
        .certificates
        .keys()
        .map(|name| {
            let certificate = Certificate {
                chain: format!("the chain of {name}"),
                key: format!("the key of {name}"),
            };
            (name.clone(), certificate)
        })
        .collect();
    let config = file.into_config(certificates);
    match compile(&config) {
        Ok(compiled) => {
            assert_eq!(compiled.listeners().len(), config.listeners.len());
            assert_eq!(compiled.upstreams().len(), config.upstreams.len());
            for listener in compiled.listeners() {
                if listener.protocol == Protocol::Https {
                    let tls = listener.tls.as_ref().expect("an https listener has TLS");
                    assert!(!tls.certificates.is_empty());
                }
            }
        }
        Err(problems) => {
            assert!(!problems.is_empty());
            for problem in &problems {
                let _told = problem.to_string();
            }
        }
    }
});
