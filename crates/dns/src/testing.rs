//! Answers written as a nameserver writes them, for tests that stand in for one: each the
//! answer to a query [`query`](crate::query) wrote, its id and question copied.

use crate::{A, AAAA, IN, QR, RD, SOA, TC};
use std::net::IpAddr;

/// What a nameserver answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// These addresses, those of the kind asked for, each with this time to live.
    Addresses(Vec<IpAddr>, u32),
    /// No such name, its zone's SOA saying a negative answer may be kept this long.
    NoSuchName(u32),
    /// Too long for UDP: cut short, to be asked for over TCP.
    Truncated,
    /// A server failure.
    Failed,
}

/// The answer `reply` gives `query`, or none for what is not a query.
#[must_use]
pub fn answer(query: &[u8], reply: &Reply) -> Option<Vec<u8>> {
    let question = question(query)?;
    let kind = u16::from_be_bytes([query[question - 4], query[question - 3]]);
    let mut flags = QR | RD | 0x0080;
    let mut records: Vec<Vec<u8>> = Vec::new();
    let mut authorities: Vec<Vec<u8>> = Vec::new();
    match reply {
        Reply::Addresses(addresses, ttl) => {
            for address in addresses {
                let (code, data) = match address {
                    IpAddr::V4(v4) => (A, v4.octets().to_vec()),
                    IpAddr::V6(v6) => (AAAA, v6.octets().to_vec()),
                };
                if code == kind {
                    records.push(record(code, *ttl, &data));
                }
            }
        }
        Reply::NoSuchName(ttl) => {
            flags |= 3;
            // The zone's SOA, at the root: two root names, then five numbers, the last its
            // MINIMUM.
            let mut data = vec![0, 0];
            for value in [1u32, 3600, 600, 86400, *ttl] {
                data.extend_from_slice(&value.to_be_bytes());
            }
            authorities.push(record(SOA, *ttl, &data));
        }
        Reply::Truncated => flags |= TC,
        Reply::Failed => flags |= 2,
    }
    let mut message = query[..2].to_vec();
    message.extend_from_slice(&flags.to_be_bytes());
    for count in [1, records.len(), authorities.len(), 0] {
        message.extend_from_slice(&u16::try_from(count).ok()?.to_be_bytes());
    }
    message.extend_from_slice(&query[12..question]);
    for record in records.iter().chain(&authorities) {
        message.extend_from_slice(record);
    }
    Some(message)
}

/// A record owned by the name asked for, pointed at where the question has it.
fn record(kind: u16, ttl: u32, data: &[u8]) -> Vec<u8> {
    let mut record = vec![0xc0, 12];
    record.extend_from_slice(&kind.to_be_bytes());
    record.extend_from_slice(&IN.to_be_bytes());
    record.extend_from_slice(&ttl.to_be_bytes());
    record.extend_from_slice(&u16::try_from(data.len()).unwrap_or(0).to_be_bytes());
    record.extend_from_slice(data);
    record
}

/// Where the query's one question ends, past its name, kind and class.
fn question(query: &[u8]) -> Option<usize> {
    let mut at = 12;
    loop {
        let length = usize::from(*query.get(at)?);
        at += 1 + length;
        if length == 0 {
            break;
        }
    }
    let end = at + 4;
    (end <= query.len()).then_some(end)
}
