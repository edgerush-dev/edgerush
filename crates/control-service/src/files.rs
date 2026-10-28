//! Certificate files, read again as they change: Kubernetes replaces a mounted Secret's
//! files in place when the Secret changes. The stream's, and the admission webhook's.

use crate::Credentials;
use edgerush_config::Certificate;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Files [`reread`] reads again as they change.
pub trait Reread {
    /// What they hold.
    type Held: PartialEq;

    /// What they hold now.
    fn held(&self) -> impl Future<Output = Result<Self::Held, FilesError>> + Send;
}

/// Where an end's credentials are: each a PEM file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Files {
    /// The certificate it presents, then the intermediates.
    pub certificate: PathBuf,
    /// Its private key.
    pub key: PathBuf,
    /// The stream's authorities.
    pub authorities: PathBuf,
}

/// A file that cannot be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{path}: {error}")]
pub struct FilesError {
    /// The file.
    pub path: String,
    /// Why.
    pub error: String,
}

impl Files {
    /// What they hold now.
    ///
    /// # Errors
    ///
    /// A [`FilesError`] for a file that cannot be read. Whether what they hold can be used
    /// is for [`acceptor`](crate::acceptor) or [`connector`](crate::connector) to say.
    pub async fn read(&self) -> Result<Credentials, FilesError> {
        Ok(Credentials {
            certificate: Certificate {
                chain: read(&self.certificate).await?,
                key: read(&self.key).await?,
            },
            authorities: read(&self.authorities).await?,
        })
    }
}

impl Reread for Files {
    type Held = Credentials;

    fn held(&self) -> impl Future<Output = Result<Credentials, FilesError>> + Send {
        self.read()
    }
}

/// Where a server's certificate is, for one that asks for none of its clients': each a PEM
/// file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertificateFiles {
    /// The certificate it presents, then the intermediates.
    pub certificate: PathBuf,
    /// Its private key.
    pub key: PathBuf,
}

impl CertificateFiles {
    /// What they hold now.
    ///
    /// # Errors
    ///
    /// A [`FilesError`] for a file that cannot be read. Whether what they hold can be used
    /// is for [`webhook_acceptor`](crate::webhook_acceptor) to say.
    pub async fn read(&self) -> Result<Certificate, FilesError> {
        Ok(Certificate {
            chain: read(&self.certificate).await?,
            key: read(&self.key).await?,
        })
    }
}

impl Reread for CertificateFiles {
    type Held = Certificate;

    fn held(&self) -> impl Future<Output = Result<Certificate, FilesError>> + Send {
        self.read()
    }
}

async fn read(path: &Path) -> Result<String, FilesError> {
    tokio::fs::read_to_string(path)
        .await
        .map_err(|error| FilesError {
            path: path.display().to_string(),
            error: error.to_string(),
        })
}

/// Reads `files` again every `every`, for as long as it is polled, `last` being what they
/// held, and hands `changed` each change: what they hold differing from the last, or a
/// failure to read them other than the last said.
pub async fn reread<F: Reread>(
    files: F,
    mut last: F::Held,
    every: Duration,
    mut changed: impl FnMut(Result<&F::Held, &FilesError>),
) {
    let mut ticks = tokio::time::interval(every);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first is at once, and they were just read.
    ticks.tick().await;
    let mut failed: Option<FilesError> = None;
    loop {
        ticks.tick().await;
        match files.held().await {
            Ok(now) => {
                failed = None;
                if now != last {
                    last = now;
                    changed(Ok(&last));
                }
            }
            Err(error) => {
                if failed.as_ref() != Some(&error) {
                    changed(Err(&error));
                    failed = Some(error);
                }
            }
        }
    }
}
