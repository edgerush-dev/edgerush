//! Fuzzes the ClientHello reader a TLS passthrough listener routes by, against BoringSSL's
//! own server: any bytes at all, as a client's first.
//!
//! BoringSSL checks far more of a ClientHello than the reader does (cipher suites, key
//! shares, versions), so it refuses more. But wherever it reads a name, the reader must
//! read the same one or refuse it as no host name, and never route by a name BoringSSL
//! reads otherwise. The one exception is an empty handshake record before the ClientHello
//! is whole, which BoringSSL skips and the reader refuses as malformed, on purpose
//! (`l4/hello.rs`). And once the reader has decided, more bytes must not change its mind.
//!
//! `cargo fuzz run tls_hello corpus/tls_hello seeds/tls_hello`.

#![no_main]

use boring::asn1::Asn1Time;
use boring::bn::BigNum;
use boring::ec::{EcGroup, EcKey};
use boring::hash::MessageDigest;
use boring::nid::Nid;
use boring::pkey::PKey;
use boring::ssl::{NameType, SslAcceptor, SslMethod, SslVerifyMode};
use boring::x509::{X509, X509NameBuilder};
use edgerush_proxy::l4::hello::{Hello, LIMIT, Refusal, read};
use libfuzzer_sys::fuzz_target;
use std::io::{self, Read, Write};
use std::sync::{Mutex, OnceLock};

/// A client's bytes, then nothing more for now; what the server writes is dropped.
struct Wire(Vec<u8>);

impl Read for Wire {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.0.is_empty() {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = buf.len().min(self.0.len());
        buf[..n].copy_from_slice(&self.0[..n]);
        self.0.drain(..n);
        Ok(n)
    }
}

impl Write for Wire {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The name BoringSSL's server last read, as bytes, if its name callback ran. As bytes:
/// BoringSSL takes a name that is not UTF-8, and `servername` would say it read none.
static READ: Mutex<Option<Option<Vec<u8>>>> = Mutex::new(None);

fn acceptor() -> &'static SslAcceptor {
    static ACCEPTOR: OnceLock<SslAcceptor> = OnceLock::new();
    ACCEPTOR.get_or_init(|| {
        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let key = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_nid(Nid::COMMONNAME, "fuzz.test")
            .unwrap();
        let name = name.build();
        let mut leaf = X509::builder().unwrap();
        leaf.set_version(2).unwrap();
        leaf.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap())
            .unwrap();
        leaf.set_subject_name(&name).unwrap();
        leaf.set_issuer_name(&name).unwrap();
        leaf.set_pubkey(&key).unwrap();
        leaf.set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        leaf.set_not_after(&Asn1Time::days_from_now(30).unwrap())
            .unwrap();
        leaf.sign(&key, MessageDigest::sha256()).unwrap();
        let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();
        acceptor.set_certificate(&leaf.build()).unwrap();
        acceptor.set_private_key(&key).unwrap();
        acceptor.set_verify(SslVerifyMode::NONE);
        acceptor.set_servername_callback(|ssl, _alert| {
            let name = ssl.servername_raw(NameType::HOST_NAME).map(<[u8]>::to_vec);
            *READ.lock().unwrap() = Some(name);
            Ok(())
        });
        acceptor.build()
    })
}

/// Whether an empty handshake record comes before the first handshake message in `bytes` is
/// whole, which the reader refuses and BoringSSL skips.
fn empty_record_first(bytes: &[u8]) -> bool {
    let mut at = 0;
    let mut head = Vec::with_capacity(4);
    let mut gathered = 0;
    while let Some(header) = bytes.get(at..at + 5) {
        if header[0] != 0x16 || header[1] != 3 {
            return false;
        }
        let length = usize::from(u16::from_be_bytes([header[3], header[4]]));
        if length == 0 {
            return true;
        }
        let rest = bytes.get(at + 5..).unwrap_or_default();
        let payload = &rest[..length.min(rest.len())];
        let wanted = 4 - head.len();
        head.extend_from_slice(&payload[..wanted.min(payload.len())]);
        gathered += payload.len();
        if let [_, a, b, c] = head[..] {
            let whole = 4 + (usize::from(a) << 16 | usize::from(b) << 8 | usize::from(c));
            if gathered >= whole {
                return false;
            }
        }
        at += 5 + length;
    }
    false
}

/// What BoringSSL's server reads of `bytes`: `None` if it refused them before it looked
/// for a name.
fn boring_reads(bytes: &[u8]) -> Option<Option<Vec<u8>>> {
    *READ.lock().unwrap() = None;
    let _ = acceptor().accept(Wire(bytes.to_vec()));
    READ.lock().unwrap().take()
}

fuzz_target!(|bytes: &[u8]| {
    let decided = read(bytes);

    if decided != Hello::More {
        let mut more = bytes.to_vec();
        more.extend_from_slice(b"\x16\x03\x01\x00\x01\x01");
        assert_eq!(read(&more), decided, "more bytes changed a decision");
    } else {
        assert!(bytes.len() < LIMIT, "not enough at the limit");
    }

    // The reader never looks past the limit, and BoringSSL reads records whole, so only
    // what fits within it is put to BoringSSL.
    if bytes.len() > LIMIT {
        return;
    }
    let Some(theirs) = boring_reads(bytes) else {
        return;
    };
    match (&decided, theirs) {
        (Hello::Whole(Some(ours)), Some(theirs)) => {
            assert_eq!(
                ours.as_bytes(),
                theirs.to_ascii_lowercase(),
                "read another name"
            );
        }
        (Hello::Refused(Refusal::BadName), Some(_)) | (Hello::Whole(None), None) => {}
        // Whatever BoringSSL makes of what follows: the reader refuses at the empty record.
        (Hello::Refused(Refusal::Malformed), _) if empty_record_first(bytes) => {}
        (ours, theirs) => panic!("read {ours:?} where BoringSSL read {theirs:?}"),
    }
});
