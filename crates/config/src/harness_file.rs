//! What the development harness reads from its config file: a config as the model states it,
//! and the files its certificates are read from ([07 §1](../../../docs/07-config-and-dsl.md)).
//!
//! Certificates are a resource of their own, which a [`Config`] never reads with the rest.
//! The harness stands in for the control plane, which will read them from Secrets, by
//! reading them from the files named here: the file itself holds no key.

use crate::{Certificate, Config, DataPlane, Listener, Route, TcpRoute, TlsRoute, Upstream};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A data plane's config file, as the development harness (`edgerush proxy --config`) reads
/// it: the config's sections, and the files of the certificates they name. The file and
/// those certificate files are read again every second; a change takes over without
/// dropping a request, and a file that is not valid leaves the config before it running.
/// Listeners are bound at the start: one that is new or has moved, or HTTP/3 turned on or
/// off, takes a restart.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(title = "EdgeRush config file"))]
#[serde(deny_unknown_fields)]
pub struct HarnessFile {
    /// Where requests come in, by name.
    pub listeners: BTreeMap<String, Listener>,
    /// The routes of `http` and `https` listeners, in order of precedence among otherwise
    /// equal matches; empty for none.
    pub routes: Vec<Route>,
    /// The routes of `tcp` listeners: one for each.
    #[serde(default)]
    pub tcp_routes: Vec<TcpRoute>,
    /// The routes of `tls` listeners, in order of precedence among equally specific
    /// hostnames.
    #[serde(default)]
    pub tls_routes: Vec<TlsRoute>,
    /// The upstreams that backends and mirrors name, by name.
    pub upstreams: BTreeMap<String, Upstream>,
    /// The data plane's own settings; left out, each has its value.
    #[serde(default)]
    pub data_plane: DataPlane,
    /// The files of each certificate that listeners and upstreams name, by its name. A key
    /// is never written into the config file itself.
    #[serde(default)]
    pub certificates: BTreeMap<String, CertificateFiles>,
}

/// The two files a certificate is read from, each relative to the config file's directory
/// unless it is absolute.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CertificateFiles {
    /// The certificate, then the intermediates that lead from it towards a root, in PEM.
    pub chain_file: PathBuf,
    /// Its private key, in PEM.
    pub key_file: PathBuf,
}

impl HarnessFile {
    /// The config the file states, with the certificates `read` from their files.
    #[must_use]
    pub fn into_config(self, read: BTreeMap<String, Certificate>) -> Config {
        // Every section by name, so that one the model gains cannot be left out here.
        Config {
            listeners: self.listeners,
            routes: self.routes,
            tcp_routes: self.tcp_routes,
            tls_routes: self.tls_routes,
            upstreams: self.upstreams,
            data_plane: self.data_plane,
            certificates: read,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECTIONS: &str = r#"
listeners:
  web: { address: "[::]:443", protocol: https, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate, tls: { certificates: [shop] } }
routes: []
tcp_routes: []
tls_routes: []
upstreams: { u: { load_balancer: p2c, endpoints: [] } }
data_plane: { set_aside_ms: 7 }
"#;

    /// The file states a config, section for section, and names each certificate's two
    /// files; the config it makes has the certificates the harness read.
    #[test]
    fn a_harness_file_states_a_config_and_names_its_certificates_files() {
        let yaml = format!(
            "{SECTIONS}certificates: {{ shop: {{ chain_file: shop.crt, key_file: /keys/shop.key }} }}\n"
        );
        let file: HarnessFile = serde_saphyr::from_str(&yaml).unwrap();
        assert_eq!(
            file.certificates["shop"],
            CertificateFiles {
                chain_file: "shop.crt".into(),
                key_file: "/keys/shop.key".into(),
            }
        );
        let shop = Certificate {
            chain: "C".to_owned(),
            key: "K".to_owned(),
        };
        let config = file.into_config(BTreeMap::from([("shop".to_owned(), shop.clone())]));
        let mut stated: Config = serde_saphyr::from_str(SECTIONS).unwrap();
        stated.certificates.insert("shop".to_owned(), shop);
        assert_eq!(config, stated);
    }

    /// A certificate is its two files, both named; a key written into the file is refused.
    #[test]
    fn a_harness_file_cannot_hold_a_key() {
        let with = |certificates: &str| {
            let yaml = format!("{SECTIONS}certificates: {{ shop: {certificates} }}\n");
            serde_saphyr::from_str::<HarnessFile>(&yaml).map(|_| ())
        };
        assert!(with("{ chain_file: a, key_file: b }").is_ok());
        assert!(with("{ chain: C, key: K }").is_err());
        assert!(with("{ chain_file: a, key: K }").is_err());
        assert!(with("{ chain_file: a }").is_err());
    }
}
