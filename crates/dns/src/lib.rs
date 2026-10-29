//! DNS's wire, as the data plane resolves the names of `dns` upstreams
//! ([03 §6](../../../docs/03-data-plane.md)): a query for a name's A or AAAA records
//! written, the nameserver's answer read, and the nameservers `resolv.conf` lists.
//!
//! A pure crate: the bytes are passed in and out, and the caller sends them, over UDP and,
//! for an answer cut short, again over TCP. No I/O, no clocks, no randomness: a query's id
//! is the caller's.
//!
//! What is read is a nameserver's answer, but whoever can send to the caller's socket can
//! send anything: an answer is read only as one to the query it names, its id and its
//! question the query's, and everything in it is bounded by the message itself. A name's
//! labels are compared without regard to ASCII case, as DNS compares them. An answer's
//! records are followed from the name asked for, through CNAMEs, to the addresses of the
//! last name; its time to live is the least of the records it was read from. A name with
//! no such records, or that does not exist, has the time to live its zone gives such an
//! answer (RFC 2308), if the answer carries the zone's SOA.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[cfg(feature = "testing")]
pub mod testing;

/// Which records are asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// IPv4 addresses.
    A,
    /// IPv6 addresses.
    Aaaa,
}

impl Kind {
    fn code(self) -> u16 {
        match self {
            Self::A => A,
            Self::Aaaa => AAAA,
        }
    }
}

/// What an answer says of the name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// Its addresses, each once and in order, and how long, in seconds, they may be kept.
    Addresses {
        /// The addresses.
        addresses: Vec<IpAddr>,
        /// How long they may be kept.
        ttl: u32,
    },
    /// It has none of the records asked for, or does not exist, and how long that may be
    /// kept, if the answer says.
    Nothing {
        /// How long that may be kept: the zone's SOA's, if the answer carries it.
        ttl: Option<u32>,
    },
    /// It was cut short for UDP, and is to be asked for again over TCP.
    Truncated,
    /// The nameserver could not answer: its response code (2, a server failure; 5,
    /// refused; and so on), for the next nameserver to be asked.
    Failed(u8),
}

/// Why bytes are not an answer to the query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Malformed {
    /// Shorter than what it says it holds.
    #[error("the message is cut short")]
    Short,
    /// A query, or another opcode's message.
    #[error("not an answer to a standard query")]
    NotAnAnswer,
    /// Another query's: its id or its question is not this one's.
    #[error("an answer to another query")]
    NotOurs,
    /// A name that is not one: a pointer that leads nowhere earlier, a label too long, or
    /// longer than a name may be.
    #[error("a name that cannot be read")]
    Name,
    /// A record whose data is not what its type says.
    #[error("a record that cannot be read")]
    Record,
}

/// Why a name cannot be asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("not a name that can be asked for: 1 to 63 bytes a label, 253 in all")]
pub struct NameError;

const A: u16 = 1;
const CNAME: u16 = 5;
const SOA: u16 = 6;
const AAAA: u16 = 28;
const OPT: u16 = 41;
const IN: u16 = 1;
/// Recursion desired, as a stub resolver asks.
const RD: u16 = 0x0100;
const QR: u16 = 0x8000;
const TC: u16 = 0x0200;
const OPCODE: u16 = 0x7800;
const NXDOMAIN: u8 = 3;
/// The UDP answer size asked for: what the DNS flag day of 2020 settled on, below any
/// path's MTU, so that no answer is fragmented.
pub const UDP_SIZE: u16 = 1232;
/// The most CNAMEs followed: past that, a chain is taken as having no end.
const CHAIN: usize = 8;

/// The query `id` for `name`'s records of `kind`, `name` absolute whether or not it ends in
/// a dot, asking for recursion and, through EDNS, for answers of up to [`UDP_SIZE`] bytes.
///
/// # Errors
///
/// A [`NameError`] for a name with an empty label, a label over 63 bytes, or over 253 in
/// all.
pub fn query(id: u16, name: &str, kind: Kind) -> Result<Vec<u8>, NameError> {
    let mut message = Vec::with_capacity(12 + name.len() + 2 + 4 + 11);
    message.extend_from_slice(&id.to_be_bytes());
    message.extend_from_slice(&RD.to_be_bytes());
    // One question, no answers or authorities, one additional record: the OPT.
    for count in [1u16, 0, 0, 1] {
        message.extend_from_slice(&count.to_be_bytes());
    }
    write_name(&mut message, name)?;
    message.extend_from_slice(&kind.code().to_be_bytes());
    message.extend_from_slice(&IN.to_be_bytes());
    // The OPT record: the root's, its class the UDP size, its TTL and data none.
    message.push(0);
    message.extend_from_slice(&OPT.to_be_bytes());
    message.extend_from_slice(&UDP_SIZE.to_be_bytes());
    message.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    Ok(message)
}

fn write_name(message: &mut Vec<u8>, name: &str) -> Result<(), NameError> {
    let name = name.strip_suffix('.').unwrap_or(name);
    if name.is_empty() || name.len() > 253 {
        return Err(NameError);
    }
    for label in name.split('.') {
        let length = u8::try_from(label.len())
            .ok()
            .filter(|length| (1..=63).contains(length))
            .ok_or(NameError)?;
        message.push(length);
        message.extend_from_slice(label.as_bytes());
    }
    message.push(0);
    Ok(())
}

/// Reads `message` as the answer to the query `id` for `name`'s records of `kind`.
///
/// # Errors
///
/// A [`Malformed`] for what is not that answer, or cannot be read.
pub fn read(message: &[u8], id: u16, name: &str, kind: Kind) -> Result<Answer, Malformed> {
    let mut reader = Reader { message, at: 0 };
    if reader.u16()? != id {
        return Err(Malformed::NotOurs);
    }
    let flags = reader.u16()?;
    if flags & QR == 0 || flags & OPCODE != 0 {
        return Err(Malformed::NotAnAnswer);
    }
    let questions = reader.u16()?;
    let answers = reader.u16()?;
    let authorities = reader.u16()?;
    reader.u16()?;
    if questions != 1 {
        return Err(Malformed::NotOurs);
    }
    let asked = reader.name()?;
    let (asked_kind, asked_class) = (reader.u16()?, reader.u16()?);
    if !same_name(&asked, name) || asked_kind != kind.code() || asked_class != IN {
        return Err(Malformed::NotOurs);
    }
    // Cut short, what follows is not to be relied on.
    if flags & TC != 0 {
        return Ok(Answer::Truncated);
    }
    let code = u8::try_from(flags & 0x000f).map_err(|_| Malformed::NotAnAnswer)?;
    if code != 0 && code != NXDOMAIN {
        return Ok(Answer::Failed(code));
    }
    let mut records = Vec::new();
    for _ in 0..answers {
        records.push(reader.record()?);
    }
    let mut negative = None;
    for _ in 0..authorities {
        let record = reader.record()?;
        if record.kind == SOA && record.class == IN {
            let minimum = soa_minimum(message, record.data)?;
            negative = Some(record.ttl.min(minimum));
        }
    }
    if code == NXDOMAIN {
        return Ok(Answer::Nothing { ttl: negative });
    }
    // From the name asked for, through each CNAME of the name reached.
    let mut target = asked;
    let mut ttl = u32::MAX;
    for _ in 0..CHAIN {
        let Some(alias) = records
            .iter()
            .find(|record| record.kind == CNAME && record.class == IN && record.owner == target)
        else {
            break;
        };
        target = Reader {
            message,
            at: alias.data.0,
        }
        .name()?;
        ttl = ttl.min(alias.ttl);
    }
    let mut addresses = Vec::new();
    for record in &records {
        if record.kind != kind.code() || record.class != IN || record.owner != target {
            continue;
        }
        let data = &message[record.data.0..record.data.1];
        let address = match kind {
            Kind::A => IpAddr::V4(Ipv4Addr::from(
                <[u8; 4]>::try_from(data).map_err(|_| Malformed::Record)?,
            )),
            Kind::Aaaa => IpAddr::V6(Ipv6Addr::from(
                <[u8; 16]>::try_from(data).map_err(|_| Malformed::Record)?,
            )),
        };
        addresses.push(address);
        ttl = ttl.min(record.ttl);
    }
    if addresses.is_empty() {
        return Ok(Answer::Nothing { ttl: negative });
    }
    addresses.sort_unstable();
    addresses.dedup();
    Ok(Answer::Addresses { addresses, ttl })
}

/// An SOA's MINIMUM, the last of its data: after two names, five numbers.
fn soa_minimum(message: &[u8], data: (usize, usize)) -> Result<u32, Malformed> {
    let mut reader = Reader {
        message: &message[..data.1],
        at: data.0,
    };
    reader.name()?;
    reader.name()?;
    for _ in 0..4 {
        reader.u32()?;
    }
    let minimum = ttl(reader.u32()?);
    if reader.at != data.1 {
        return Err(Malformed::Record);
    }
    Ok(minimum)
}

/// A time to live as RFC 2181 reads one: one with its top bit set is none.
fn ttl(value: u32) -> u32 {
    if value & 0x8000_0000 == 0 { value } else { 0 }
}

/// A name asked for, as `query` wrote it, and one read, compared as DNS compares them.
fn same_name(read: &[u8], name: &str) -> bool {
    let name = name.strip_suffix('.').unwrap_or(name);
    read.eq_ignore_ascii_case(name.as_bytes())
}

/// A record, its data where it lies in the message.
struct Record {
    /// Its owner's name, its labels joined by dots, in lower case.
    owner: Vec<u8>,
    kind: u16,
    class: u16,
    ttl: u32,
    /// Where its data starts and ends.
    data: (usize, usize),
}

struct Reader<'a> {
    message: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn take(&mut self, count: usize) -> Result<&[u8], Malformed> {
        let end = self.at.checked_add(count).ok_or(Malformed::Short)?;
        let taken = self.message.get(self.at..end).ok_or(Malformed::Short)?;
        self.at = end;
        Ok(taken)
    }

    fn u16(&mut self) -> Result<u16, Malformed> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> Result<u32, Malformed> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// A name, its labels joined by dots, in lower case, the root's empty; its pointers
    /// followed, each only to somewhere before where it is, so that none can loop.
    fn name(&mut self) -> Result<Vec<u8>, Malformed> {
        let mut name = Vec::new();
        let mut at = self.at;
        // Where reading goes on once the name is read: past its first pointer, if it has
        // one, or past its end.
        let mut resume = None;
        loop {
            let length = *self.message.get(at).ok_or(Malformed::Short)?;
            match length & 0xc0 {
                0x00 => {
                    at += 1;
                    if length == 0 {
                        break;
                    }
                    let label = self
                        .message
                        .get(at..at + usize::from(length))
                        .ok_or(Malformed::Short)?;
                    // A dot within a label would read as two.
                    if label.contains(&b'.') {
                        return Err(Malformed::Name);
                    }
                    if !name.is_empty() {
                        name.push(b'.');
                    }
                    name.extend(label.iter().map(u8::to_ascii_lowercase));
                    if name.len() > 253 {
                        return Err(Malformed::Name);
                    }
                    at += usize::from(length);
                }
                0xc0 => {
                    let low = *self.message.get(at + 1).ok_or(Malformed::Short)?;
                    let target = usize::from(u16::from_be_bytes([length & 0x3f, low]));
                    if target >= at {
                        return Err(Malformed::Name);
                    }
                    resume.get_or_insert(at + 2);
                    at = target;
                }
                _ => return Err(Malformed::Name),
            }
        }
        self.at = resume.unwrap_or(at);
        Ok(name)
    }

    fn record(&mut self) -> Result<Record, Malformed> {
        let owner = self.name()?;
        let kind = self.u16()?;
        let class = self.u16()?;
        let ttl = ttl(self.u32()?);
        let length = usize::from(self.u16()?);
        let start = self.at;
        self.take(length)?;
        Ok(Record {
            owner,
            kind,
            class,
            ttl,
            data: (start, self.at),
        })
    }
}

/// The nameservers `resolv.conf`'s text lists, in its order: three at most, as the C
/// library takes, each an address; a line that names something else is passed over.
#[must_use]
pub fn nameservers(text: &str) -> Vec<IpAddr> {
    text.lines()
        .filter_map(|line| {
            let mut words = line.split_whitespace();
            (words.next() == Some("nameserver"))
                .then(|| words.next()?.parse().ok())
                .flatten()
        })
        .take(3)
        .collect()
}

#[cfg(test)]
mod tests;
