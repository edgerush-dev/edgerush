//! A certificate authority: a ControlPlane's, which issues every workload of its planes the
//! certificate it is known by ([05 §2](../../../docs/05-operator.md),
//! [12](../../../docs/12-open-questions.md) item 22).
//!
//! One is made by its control plane, its own root for ten years, signed again with its key
//! when a fifth of that is left, the old and the new both trusted until the old expires;
//! or provided by the platform, a certificate its root signed with the chain to it, which
//! is never signed again here. Either is kept as a Secret of `kubernetes.io/tls`'s shape,
//! as cert-manager writes one: its certificate, its key, and the bundle a workload is to
//! trust, [`Written`]. What it issues, a leaf of seven days on a key of its own, is
//! written so too, its bundle what a workload of it trusts: a made authority's roots, or a
//! provided one's own certificate, never the platform's root, which signs other
//! ControlPlanes' authorities too and would vouch for what they issue (05 §2). A peer
//! anchors trust at that certificate, as [`Authority::trusts`] does.
//!
//! Every time is passed in, in seconds since the Unix epoch: nothing here reads a clock.
//! Keys and serial numbers are BoringSSL's random.

use crate::{CertificateError, Identity, Validity, validity};
use boring::asn1::Asn1Time;
use boring::bn::{BigNum, MsbOption};
use boring::ec::{EcGroup, EcKey};
use boring::error::ErrorStack;
use boring::hash::MessageDigest;
use boring::nid::Nid;
use boring::pkey::{PKey, Private};
use boring::stack::Stack;
use boring::x509::extension::{
    AuthorityKeyIdentifier, BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName,
    SubjectKeyIdentifier,
};
use boring::x509::store::X509StoreBuilder;
use boring::x509::verify::X509VerifyFlags;
use boring::x509::{X509, X509Builder, X509NameBuilder, X509StoreContext};

/// How long a leaf lives: a week (Lucas, 05 §2).
pub const LEAF_SECONDS: i64 = 7 * 86_400;

/// How long an authority made here lives: ten years, as Istio's own (05 §2).
pub const AUTHORITY_SECONDS: i64 = 3_650 * 86_400;

/// How long before it is issued a certificate is valid from, so that a peer whose clock is
/// a little behind takes it at once.
pub const BACKDATE: i64 = 300;

/// A certificate, its key and the bundle to trust, in PEM, as a Secret of
/// `kubernetes.io/tls` holds them: `tls.crt`, `tls.key` and `ca.crt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    /// `tls.crt`: the certificate, then the intermediates that lead from it.
    pub certificate: String,
    /// `tls.key`: its private key.
    pub key: String,
    /// `ca.crt`: the authorities a peer of it trusts.
    pub bundle: String,
}

impl Written {
    /// When its certificate is valid, if it can be read.
    #[must_use]
    pub fn validity(&self) -> Option<Validity> {
        X509::from_pem(self.certificate.as_bytes())
            .ok()
            .and_then(|certificate| validity(&certificate))
    }
}

/// Where an authority came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Made here, and signed again here.
    Made,
    /// The platform's, never written here.
    Provided,
}

/// What a leaf is issued for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Leaf {
    /// Its subject's common name, which says which it is to a person reading it.
    pub common_name: String,
    /// The DNS names a peer that dials it checks.
    pub dns: Vec<String>,
    /// The SPIFFE ID it is known by, its one URI.
    pub spiffe_id: String,
}

/// Why an authority could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    /// Its certificate and key cannot be read, or are not each other's.
    #[error("{0}")]
    Certificate(CertificateError),
    /// Its bundle holds no certificate.
    #[error("its bundle, ca.crt: {0}")]
    Bundle(String),
    /// It cannot issue what its bundle trusts: not an authority, its chain not leading to
    /// the bundle, or out of date.
    #[error(
        "what it issues is not trusted by its bundle: it is no authority, or its chain does not lead there"
    )]
    NotAnAuthority,
}

/// Why a certificate could not be issued.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IssueError {
    /// BoringSSL failed.
    #[error("{0}")]
    Tls(String),
    /// A provided authority is the platform's to sign again.
    #[error("a provided authority is never signed again here")]
    Provided,
}

impl From<ErrorStack> for IssueError {
    fn from(error: ErrorStack) -> Self {
        Self::Tls(error.to_string())
    }
}

/// An authority, ready to issue. Its key is never said.
pub struct Authority {
    certificate: X509,
    /// What leads from it to a root of the bundle, for a provided one.
    chain: Vec<X509>,
    key: PKey<Private>,
    bundle: Vec<X509>,
    kind: Kind,
    /// A provided one's predecessors, still trusted until they expire.
    previous: Vec<X509>,
}

impl std::fmt::Debug for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authority")
            .field("kind", &self.kind)
            .field("subject", &crate::common_name(&self.certificate))
            .finish_non_exhaustive()
    }
}

impl Authority {
    /// An authority made now, `edgerush <name>`, its own root.
    ///
    /// # Errors
    ///
    /// An [`IssueError`] where BoringSSL fails.
    pub fn make(name: &str, now: i64) -> Result<Self, IssueError> {
        let key = key()?;
        let certificate = root(name, &key, now)?;
        Ok(Self {
            bundle: vec![certificate.clone()],
            certificate,
            chain: Vec::new(),
            key,
            kind: Kind::Made,
            previous: Vec::new(),
        })
    }

    /// The authority `written` holds, as of `now`.
    ///
    /// # Errors
    ///
    /// A [`ReadError`] for one that cannot be read, or that cannot issue what its bundle
    /// trusts at `now`.
    pub fn read(written: &Written, kind: Kind, now: i64) -> Result<Self, ReadError> {
        let identity = Identity::read(&edgerush_config::Certificate {
            chain: written.certificate.clone(),
            key: written.key.clone(),
        })
        .map_err(ReadError::Certificate)?;
        let bundle = X509::stack_from_pem(written.bundle.as_bytes())
            .map_err(|error| ReadError::Bundle(error.to_string()))?;
        if bundle.is_empty() {
            return Err(ReadError::Bundle("no certificate in it".to_owned()));
        }
        let authority = Self {
            certificate: identity.leaf,
            chain: identity.intermediates,
            key: identity.key,
            bundle,
            kind,
            previous: Vec::new(),
        };
        // What it would issue, checked as a peer checks it: only then is it an authority.
        let probe = authority
            .issue_at(
                &Leaf {
                    common_name: "probe".to_owned(),
                    dns: Vec::new(),
                    spiffe_id: "spiffe://probe/probe".to_owned(),
                },
                now,
            )
            .map_err(|_| ReadError::NotAnAuthority)?;
        if !verifies(&probe, &authority.bundle, now) {
            return Err(ReadError::NotAnAuthority);
        }
        Ok(authority)
    }

    /// It, as its Secret holds it.
    #[must_use]
    pub fn written(&self) -> Written {
        let mut certificate = pem(&self.certificate);
        for intermediate in &self.chain {
            certificate.push_str(&pem(intermediate));
        }
        Written {
            certificate,
            key: String::from_utf8(self.key.private_key_to_pem_pkcs8().unwrap_or_default())
                .unwrap_or_default(),
            bundle: self.bundle_pem(),
        }
    }

    /// The bundle its Secret holds, in PEM: a made one's roots, or what the platform gave
    /// with a provided one.
    #[must_use]
    pub fn bundle_pem(&self) -> String {
        self.bundle.iter().map(pem).collect()
    }

    /// What a workload of it trusts, in PEM: a made one's roots; a provided one's own
    /// certificate, and its predecessors until they expire, as the platform's root vouches
    /// for other ControlPlanes' too.
    #[must_use]
    pub fn trusted_pem(&self) -> String {
        self.trusted().iter().map(pem).collect()
    }

    /// The certificates a workload of it trusts.
    fn trusted(&self) -> Vec<X509> {
        match self.kind {
            Kind::Made => self.bundle.clone(),
            Kind::Provided => std::iter::once(self.certificate.clone())
                .chain(self.previous.iter().cloned())
                .collect(),
        }
    }

    /// It, a provided one, trusting too each certificate of `bundles` that is a predecessor
    /// of its: of its own subject, signed by the platform's root it is given, valid at
    /// `now`, and not itself. Its predecessors are what its workloads' Secrets remember,
    /// trusted until they expire, so that the platform's rotation of it refuses none of its
    /// workloads that still hold a certificate of the one before (05 §2); anything else
    /// written there is never trusted. A made one is left as it is: its own bundle holds
    /// its predecessors.
    #[must_use]
    pub fn trusting(mut self, bundles: &[&str], now: i64) -> Self {
        if self.kind == Kind::Made {
            return self;
        }
        let own = self.certificate.to_der().ok();
        let subject = self.certificate.subject_name().to_der().ok();
        for bundle in bundles {
            for certificate in X509::stack_from_pem(bundle.as_bytes()).unwrap_or_default() {
                let der = certificate.to_der().ok();
                let valid = validity(&certificate)
                    .is_some_and(|dates| dates.not_before <= now && now < dates.not_after);
                let known = der == own
                    || self
                        .previous
                        .iter()
                        .any(|previous| previous.to_der().ok() == der);
                let named = certificate.subject_name().to_der().ok() == subject;
                let rooted = || {
                    let written = Written {
                        certificate: pem(&certificate),
                        key: String::new(),
                        bundle: String::new(),
                    };
                    verifies(&written, &self.bundle, now)
                };
                if valid && !known && named && rooted() {
                    self.previous.push(certificate);
                }
            }
        }
        self
    }

    /// Whether, made here, it is to be signed again at `now`: once a fifth of its life is
    /// left, or once its clock says it is not valid. A provided one never is.
    #[must_use]
    pub fn due(&self, now: i64) -> bool {
        if self.kind == Kind::Provided {
            return false;
        }
        validity(&self.certificate).is_none_or(|dates| {
            let life = dates.not_after - dates.not_before;
            now < dates.not_before || now >= dates.not_after - life / 5
        })
    }

    /// It signed again at `now` with its key, for ten years more, its bundle the new
    /// certificate and those of before that are still valid.
    ///
    /// # Errors
    ///
    /// [`IssueError::Provided`] for a provided authority; an [`IssueError`] where BoringSSL
    /// fails.
    pub fn resign(&self, now: i64) -> Result<Self, IssueError> {
        if self.kind == Kind::Provided {
            return Err(IssueError::Provided);
        }
        let name = crate::common_name(&self.certificate).unwrap_or_default();
        let name = name.strip_prefix("edgerush ").unwrap_or(&name).to_owned();
        let certificate = root(&name, &self.key, now)?;
        let mut bundle = vec![certificate.clone()];
        bundle.extend(
            self.bundle
                .iter()
                .filter(|old| validity(old).is_some_and(|dates| dates.not_after > now))
                .cloned(),
        );
        Ok(Self {
            certificate,
            chain: Vec::new(),
            key: self.key.clone(),
            bundle,
            kind: Kind::Made,
            previous: Vec::new(),
        })
    }

    /// A leaf for `leaf`, issued at `now`, on a key of its own, for [`LEAF_SECONDS`].
    ///
    /// # Errors
    ///
    /// An [`IssueError`] where BoringSSL fails.
    pub fn issue(&self, leaf: &Leaf, now: i64) -> Result<Written, IssueError> {
        self.issue_at(leaf, now)
    }

    fn issue_at(&self, leaf: &Leaf, now: i64) -> Result<Written, IssueError> {
        let key = key()?;
        let mut builder = builder(&leaf.common_name, &key, now, LEAF_SECONDS)?;
        builder.set_issuer_name(self.certificate.subject_name())?;
        let constraints = BasicConstraints::new().critical().build()?;
        builder.append_extension(&constraints)?;
        let usage = KeyUsage::new()
            .critical()
            .digital_signature()
            .key_encipherment()
            .build()?;
        builder.append_extension(&usage)?;
        let extended = ExtendedKeyUsage::new()
            .server_auth()
            .client_auth()
            .build()?;
        builder.append_extension(&extended)?;
        let mut names = SubjectAlternativeName::new();
        for dns in &leaf.dns {
            names.dns(dns);
        }
        names.uri(&leaf.spiffe_id);
        let names = names.build(&builder.x509v3_context(Some(&self.certificate), None))?;
        builder.append_extension(&names)?;
        // Which key signed it, where its authority says its own: a peer then finds its
        // issuer among several of one name, as a replaced authority and the one before it
        // are.
        if self.certificate.subject_key_id().is_some() {
            let signer = AuthorityKeyIdentifier::new()
                .keyid(true)
                .build(&builder.x509v3_context(Some(&self.certificate), None))?;
            builder.append_extension(&signer)?;
        }
        builder.sign(&self.key, MessageDigest::sha256())?;
        let issued = builder.build();
        let mut certificate = pem(&issued);
        if self.kind == Kind::Provided {
            certificate.push_str(&pem(&self.certificate));
            for intermediate in &self.chain {
                certificate.push_str(&pem(intermediate));
            }
        }
        Ok(Written {
            certificate,
            key: String::from_utf8(key.private_key_to_pem_pkcs8()?)
                .map_err(|error| IssueError::Tls(error.to_string()))?,
            bundle: self.trusted_pem(),
        })
    }

    /// Whether it signed `leaf`, as of `now`: issued under its own certificate, not a
    /// predecessor's it still trusts.
    #[must_use]
    pub fn signed(&self, leaf: &Written, now: i64) -> bool {
        match self.kind {
            Kind::Made => self.trusts(leaf, now),
            Kind::Provided => verifies(leaf, std::slice::from_ref(&self.certificate), now),
        }
    }

    /// Whether a workload of it trusts `leaf` at `now`, as a peer of the stream checks it:
    /// its chain leads to what it trusts, each of those an anchor whether or not it is a
    /// root. A provided authority's sibling, signed by the same platform root, is not.
    #[must_use]
    pub fn trusts(&self, leaf: &Written, now: i64) -> bool {
        verifies(leaf, &self.trusted(), now)
    }
}

/// Whether `leaf`'s chain leads to one of `anchors` at `now`, each an anchor whether or
/// not it is a root (partial chains, as the stream's peers verify).
fn verifies(leaf: &Written, anchors: &[X509], now: i64) -> bool {
    let verified = || -> Result<bool, ErrorStack> {
        let mut chain = X509::stack_from_pem(leaf.certificate.as_bytes())?.into_iter();
        let Some(first) = chain.next() else {
            return Ok(false);
        };
        let mut untrusted = Stack::new()?;
        for certificate in chain {
            untrusted.push(certificate)?;
        }
        let mut store = X509StoreBuilder::new()?;
        for anchor in anchors {
            store.add_cert(anchor.clone())?;
        }
        store.set_flags(X509VerifyFlags::PARTIAL_CHAIN);
        store.verify_param_mut().set_time(now);
        let store = store.build();
        let mut context = X509StoreContext::new()?;
        context.init(&store, &first, &untrusted, |context| {
            Ok(context.verify_cert()? && context.verify_result().is_ok())
        })
    };
    verified().unwrap_or(false)
}

/// Whether a leaf of `dates` is to be issued again at `now`: from half its life on, or
/// whenever the clock says it is not valid.
#[must_use]
pub fn renewal_due(dates: Validity, now: i64) -> bool {
    let half = dates.not_before + (dates.not_after - dates.not_before) / 2;
    now < dates.not_before || now >= half
}

/// A root, `edgerush <name>`, of `key`'s, from `now` for [`AUTHORITY_SECONDS`].
fn root(name: &str, key: &PKey<Private>, now: i64) -> Result<X509, IssueError> {
    let subject = format!("edgerush {name}");
    let mut builder = builder(&subject, key, now, AUTHORITY_SECONDS)?;
    let issuer = common_name(&subject)?;
    builder.set_issuer_name(&issuer)?;
    let constraints = BasicConstraints::new().critical().ca().build()?;
    builder.append_extension(&constraints)?;
    let usage = KeyUsage::new()
        .critical()
        .key_cert_sign()
        .crl_sign()
        .build()?;
    builder.append_extension(&usage)?;
    let identifier = SubjectKeyIdentifier::new().build(&builder.x509v3_context(None, None))?;
    builder.append_extension(&identifier)?;
    builder.sign(key, MessageDigest::sha256())?;
    Ok(builder.build())
}

/// A certificate of `key`'s, `subject` its common name, valid from [`BACKDATE`] before
/// `now` to `lives` after it, of a random serial number, not yet issued or signed.
fn builder(
    subject: &str,
    key: &PKey<Private>,
    now: i64,
    lives: i64,
) -> Result<X509Builder, IssueError> {
    let mut builder = X509::builder()?;
    builder.set_version(2)?;
    let mut serial = BigNum::new()?;
    serial.rand(159, MsbOption::MAYBE_ZERO, false)?;
    let serial = serial.to_asn1_integer()?;
    builder.set_serial_number(&serial)?;
    let name = common_name(subject)?;
    builder.set_subject_name(&name)?;
    builder.set_pubkey(key)?;
    let (from, until) = (
        Asn1Time::from_unix(now - BACKDATE)?,
        Asn1Time::from_unix(now + lives)?,
    );
    builder.set_not_before(&from)?;
    builder.set_not_after(&until)?;
    Ok(builder)
}

fn common_name(name: &str) -> Result<boring::x509::X509Name, ErrorStack> {
    let mut subject = X509NameBuilder::new()?;
    subject.append_entry_by_nid(Nid::COMMONNAME, name)?;
    Ok(subject.build())
}

/// A key on P-256.
fn key() -> Result<PKey<Private>, ErrorStack> {
    let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1)?;
    PKey::from_ec_key(EcKey::generate(&group)?)
}

fn pem(certificate: &X509) -> String {
    String::from_utf8(certificate.to_pem().unwrap_or_default()).unwrap_or_default()
}

#[cfg(test)]
mod tests;
