//! The stream's certificate files, read again as they change: Kubernetes replaces a
//! mounted Secret's files in place when the Secret changes.

use crate::Credentials;
use edgerush_config::Certificate;
use std::path::{Path, PathBuf};
use std::time::Duration;

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

async fn read(path: &Path) -> Result<String, FilesError> {
    tokio::fs::read_to_string(path)
        .await
        .map_err(|error| FilesError {
            path: path.display().to_string(),
            error: error.to_string(),
        })
}

/// Reads `files` again every `every`, for as long as it is polled, `last` being what they
/// held, and hands `changed` each change: credentials that differ from the last, or a
/// failure to read them other than the last said.
pub async fn reread(
    files: Files,
    mut last: Credentials,
    every: Duration,
    mut changed: impl FnMut(Result<&Credentials, &FilesError>),
) {
    let mut ticks = tokio::time::interval(every);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first is at once, and they were just read.
    ticks.tick().await;
    let mut failed: Option<FilesError> = None;
    loop {
        ticks.tick().await;
        match files.read().await {
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
