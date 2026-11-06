//! The authorities the control plane's own system trusts, which a BackendTLSPolicy's
//! `wellKnownCACertificates: System` asks an upstream to trust
//! ([04 §2](../../../docs/04-control-plane.md)): read once, as the control plane starts.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::{fs, io};

/// Where a system keeps the bundle of the authorities it trusts, looked for in this order:
/// Debian's and Ubuntu's, Alpine's, then Red Hat's and Fedora's.
const BUNDLES: [&str; 3] = [
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/ssl/cert.pem",
    "/etc/pki/tls/certs/ca-bundle.crt",
];

/// Why the system's authorities cannot be used.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    /// The bundle cannot be read.
    #[error("{} cannot be read: {error}", path.display())]
    Unreadable { path: PathBuf, error: io::Error },
    /// The bundle is not certificates in PEM.
    #[error("{} is not PEM certificates: {reason}", path.display())]
    NotCertificates { path: PathBuf, reason: String },
}

/// Reads the bundle of the system's authorities: the file `SSL_CERT_FILE` names, as
/// OpenSSL's and BoringSSL's clients read it, else the first of the systems' own bundles
/// there is. None there, and the system trusts none.
///
/// # Errors
///
/// An [`Error`] for a bundle that cannot be read, or that is not certificates in PEM, which
/// the data plane would refuse every config over.
pub(crate) fn read() -> Result<Vec<String>, Error> {
    read_from(std::env::var_os("SSL_CERT_FILE"), &BUNDLES.map(Path::new))
}

/// Reads the bundle `named`, else the first of `bundles` there is.
fn read_from(named: Option<OsString>, bundles: &[&Path]) -> Result<Vec<String>, Error> {
    let path = match named {
        Some(named) => PathBuf::from(named),
        None => match bundles.iter().find(|bundle| bundle.is_file()) {
            Some(bundle) => bundle.to_path_buf(),
            None => return Ok(Vec::new()),
        },
    };
    let pem = fs::read_to_string(&path).map_err(|error| Error::Unreadable {
        path: path.clone(),
        error,
    })?;
    edgerush_certificates::authorities(std::slice::from_ref(&pem)).map_err(|error| {
        Error::NotCertificates {
            path,
            reason: error.reason,
        }
    })?;
    Ok(vec![pem])
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_certificates::testing::certificate;

    /// A file of the tests' own, holding `contents`.
    fn file(name: &str, contents: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "edgerush-system-authorities-{}-{name}",
            std::process::id()
        ));
        fs::write(&path, contents).unwrap();
        path
    }

    /// The bundle named is read, whatever the systems' own say; else the first of theirs
    /// there is; else none.
    #[test]
    fn the_bundle_named_is_read_else_the_first_there_is() {
        let named = certificate(&["named.test"]).chain;
        let first = certificate(&["first.test"]).chain;
        let second = certificate(&["second.test"]).chain;
        let named_path = file("named", &named);
        let first_path = file("first", &first);
        let second_path = file("second", &second);
        let missing = std::env::temp_dir().join("edgerush-system-authorities-not-there");
        let bundles = [
            missing.as_path(),
            first_path.as_path(),
            second_path.as_path(),
        ];
        assert_eq!(
            read_from(Some(named_path.clone().into_os_string()), &bundles).unwrap(),
            [named]
        );
        assert_eq!(read_from(None, &bundles).unwrap(), [first]);
        assert_eq!(read_from(None, &bundles[2..]).unwrap(), [second]);
        assert!(read_from(None, &bundles[..1]).unwrap().is_empty());
        for path in [named_path, first_path, second_path] {
            fs::remove_file(path).unwrap();
        }
    }

    /// A bundle named that is not there, or one found that is not PEM certificates, is
    /// refused, never taken for none.
    #[test]
    fn a_bundle_that_cannot_be_used_is_refused() {
        let missing = std::env::temp_dir().join("edgerush-system-authorities-not-there");
        assert!(matches!(
            read_from(Some(missing.into_os_string()), &[]),
            Err(Error::Unreadable { .. })
        ));
        let malformed = file(
            "malformed",
            "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n",
        );
        assert!(matches!(
            read_from(None, &[malformed.as_path()]),
            Err(Error::NotCertificates { .. })
        ));
        let empty = file("empty", "");
        assert!(matches!(
            read_from(Some(empty.clone().into_os_string()), &[]),
            Err(Error::NotCertificates { .. })
        ));
        for path in [malformed, empty] {
            fs::remove_file(path).unwrap();
        }
    }
}
