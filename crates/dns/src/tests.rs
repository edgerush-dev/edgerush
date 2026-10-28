use super::*;
use proptest::prelude::*;

/// A message written byte by byte, as a nameserver might write it.
#[derive(Default)]
struct Message(Vec<u8>);

impl Message {
    fn u16(mut self, value: u16) -> Self {
        self.0.extend_from_slice(&value.to_be_bytes());
        self
    }

    fn u32(mut self, value: u32) -> Self {
        self.0.extend_from_slice(&value.to_be_bytes());
        self
    }

    fn bytes(mut self, bytes: &[u8]) -> Self {
        self.0.extend_from_slice(bytes);
        self
    }

    /// A name, label by label, the root's after.
    fn name(mut self, name: &str) -> Self {
        for label in name.split('.').filter(|label| !label.is_empty()) {
            self.0.push(u8::try_from(label.len()).unwrap());
            self.0.extend_from_slice(label.as_bytes());
        }
        self.0.push(0);
        self
    }

    /// A pointer to the name at `at`.
    fn pointer(self, at: u16) -> Self {
        self.u16(0xc000 | at)
    }

    /// An answer's header: `id`, `flags`, and its four counts.
    fn header(id: u16, flags: u16, counts: [u16; 4]) -> Self {
        let mut message = Self::default().u16(id).u16(flags);
        for count in counts {
            message = message.u16(count);
        }
        message
    }

    /// The question, `name`'s records of `kind`, after the header.
    fn question(self, name: &str, kind: u16) -> Self {
        self.name(name).u16(kind).u16(IN)
    }

    /// A record's kind, class, TTL and data, after its owner.
    fn record(self, kind: u16, ttl: u32, data: &[u8]) -> Self {
        let length = u16::try_from(data.len()).unwrap();
        self.u16(kind).u16(IN).u32(ttl).u16(length).bytes(data)
    }
}

const ID: u16 = 0x4d2a;
const ANSWER: u16 = QR | RD | 0x0080;
const WEB: &str = "web.shop.svc.cluster.local";

fn read_a(message: &Message) -> Result<Answer, Malformed> {
    read(&message.0, ID, WEB, Kind::A)
}

fn addresses(addresses: &[&str], ttl: u32) -> Answer {
    Answer::Addresses {
        addresses: addresses.iter().map(|a| a.parse().unwrap()).collect(),
        ttl,
    }
}

#[test]
fn a_query_asks_for_a_names_records_with_recursion_and_edns() {
    let written = query(ID, "Web.Shop.svc.cluster.local.", Kind::Aaaa).unwrap();
    let expected = Message::header(ID, RD, [1, 0, 0, 1])
        .question("Web.Shop.svc.cluster.local", AAAA)
        // OPT: the root, its kind, the UDP size, no extended flags, no data.
        .bytes(&[0])
        .u16(OPT)
        .u16(UDP_SIZE)
        .u32(0)
        .u16(0);
    assert_eq!(written, expected.0);
    assert_eq!(
        query(1, "localhost", Kind::A).unwrap()[12..23],
        *b"\x09localhost\x00"
    );
    for name in [
        "",
        ".",
        "a..b",
        &"a".repeat(64),
        &vec!["a".repeat(63); 4].join("."),
    ] {
        assert_eq!(query(1, name, Kind::A), Err(NameError), "{name}");
    }
}

#[test]
fn an_answers_addresses_are_read_in_order_each_once_with_the_least_ttl() {
    let message = Message::header(ID, ANSWER, [1, 3, 0, 0])
        .question(WEB, A)
        .pointer(12)
        .record(A, 30, &[10, 0, 0, 2])
        .pointer(12)
        .record(A, 20, &[10, 0, 0, 1])
        .pointer(12)
        .record(A, 40, &[10, 0, 0, 2]);
    assert_eq!(
        read_a(&message),
        Ok(addresses(&["10.0.0.1", "10.0.0.2"], 20))
    );
    // The name asked for in any case, read as the same.
    let shouted = Message::header(ID, ANSWER, [1, 1, 0, 0])
        .question("WEB.Shop.SVC.cluster.local", A)
        .name("web.SHOP.svc.cluster.local")
        .record(A, 5, &[10, 0, 0, 9]);
    assert_eq!(read_a(&shouted), Ok(addresses(&["10.0.0.9"], 5)));
    let six = Message::header(ID, ANSWER, [1, 1, 0, 0])
        .question(WEB, AAAA)
        .pointer(12)
        .record(
            AAAA,
            7,
            &[0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        );
    assert_eq!(
        read(&six.0, ID, WEB, Kind::Aaaa),
        Ok(addresses(&["fd00::1"], 7))
    );
}

#[test]
fn a_cname_chain_is_followed_to_the_last_names_addresses() {
    // web → edge.example → lb.example, each by a pointer into what came before.
    let question_end = 12 + WEB.len() + 2 + 4;
    // The first CNAME's data, `edge.example`, starts past its owner's pointer and its
    // kind, class, TTL and length; `example` is five bytes on.
    let edge = u16::try_from(question_end + 12).unwrap();
    let example = (edge + 5).to_be_bytes();
    let lb = [2, b'l', b'b', 0xc0 | example[0], example[1]];
    let message = Message::header(ID, ANSWER, [1, 4, 0, 0])
        .question(WEB, A)
        .pointer(12)
        .record(CNAME, 60, b"\x04edge\x07example\x00")
        // Another name's address: not the chain's.
        .name("other.example")
        .record(A, 60, &[192, 0, 2, 99])
        .pointer(edge)
        .record(CNAME, 15, &lb)
        .name("lb.example")
        .record(A, 30, &[192, 0, 2, 1]);
    assert_eq!(read_a(&message), Ok(addresses(&["192.0.2.1"], 15)));
    // A chain with no end, a name its own alias, is no address.
    let looped = Message::header(ID, ANSWER, [1, 1, 0, 0])
        .question(WEB, A)
        .pointer(12)
        .record(CNAME, 60, &[0xc0, 12]);
    assert_eq!(read_a(&looped), Ok(Answer::Nothing { ttl: None }));
}

#[test]
fn no_such_name_or_no_such_record_has_the_zones_negative_ttl() {
    let soa = |ttl: u32, minimum: u32| {
        let mut data = b"\x02ns\x00\x05admin\x00".to_vec();
        for value in [1u32, 3600, 600, 86400, minimum] {
            data.extend_from_slice(&value.to_be_bytes());
        }
        (ttl, data)
    };
    let (ttl, data) = soa(300, 30);
    let nxdomain = Message::header(ID, ANSWER | 3, [1, 0, 1, 0])
        .question(WEB, A)
        .name("cluster.local")
        .record(SOA, ttl, &data);
    assert_eq!(read_a(&nxdomain), Ok(Answer::Nothing { ttl: Some(30) }));
    // No record of the kind: the SOA's TTL where it is the less.
    let (ttl, data) = soa(10, 900);
    let nodata = Message::header(ID, ANSWER, [1, 0, 1, 0])
        .question(WEB, A)
        .name("cluster.local")
        .record(SOA, ttl, &data);
    assert_eq!(read_a(&nodata), Ok(Answer::Nothing { ttl: Some(10) }));
    // Records of another kind only, and no SOA: nothing, for how long unsaid.
    let other = Message::header(ID, ANSWER, [1, 1, 0, 0])
        .question(WEB, A)
        .pointer(12)
        .record(AAAA, 60, &[0; 16]);
    assert_eq!(read_a(&other), Ok(Answer::Nothing { ttl: None }));
}

#[test]
fn a_cut_short_or_failed_answer_says_so() {
    let truncated = Message::header(ID, ANSWER | TC, [1, 0, 0, 0]).question(WEB, A);
    assert_eq!(read_a(&truncated), Ok(Answer::Truncated));
    for code in [1u8, 2, 4, 5, 9] {
        let failed = Message::header(ID, ANSWER | u16::from(code), [1, 0, 0, 0]).question(WEB, A);
        assert_eq!(read_a(&failed), Ok(Answer::Failed(code)), "{code}");
    }
}

#[test]
fn an_answer_to_another_query_or_not_an_answer_is_refused() {
    let answer = |id: u16, flags: u16, name: &str, kind: u16| {
        Message::header(id, flags, [1, 1, 0, 0])
            .question(name, kind)
            .pointer(12)
            .record(A, 60, &[10, 0, 0, 1])
    };
    assert_eq!(
        read_a(&answer(ID + 1, ANSWER, WEB, A)),
        Err(Malformed::NotOurs)
    );
    assert_eq!(
        read_a(&answer(ID, ANSWER, "api.shop.svc.cluster.local", A)),
        Err(Malformed::NotOurs)
    );
    assert_eq!(
        read_a(&answer(ID, ANSWER, WEB, AAAA)),
        Err(Malformed::NotOurs)
    );
    assert_eq!(read_a(&answer(ID, RD, WEB, A)), Err(Malformed::NotAnAnswer));
    assert_eq!(
        read_a(&answer(ID, ANSWER | 0x2800, WEB, A)),
        Err(Malformed::NotAnAnswer)
    );
    let two = Message::header(ID, ANSWER, [2, 0, 0, 0])
        .question(WEB, A)
        .question(WEB, A);
    assert_eq!(read_a(&two), Err(Malformed::NotOurs));
}

#[test]
fn what_cannot_be_read_is_refused_and_never_followed_round_in_circles() {
    let whole = Message::header(ID, ANSWER, [1, 1, 0, 0])
        .question(WEB, A)
        .pointer(12)
        .record(A, 60, &[10, 0, 0, 1]);
    assert!(read_a(&whole).is_ok());
    // Every prefix of it is short of what it says it holds.
    for end in 0..whole.0.len() {
        assert_eq!(
            read(&whole.0[..end], ID, WEB, Kind::A),
            Err(Malformed::Short),
            "{end}"
        );
    }
    // An address of the wrong size.
    let wrong = Message::header(ID, ANSWER, [1, 1, 0, 0])
        .question(WEB, A)
        .pointer(12)
        .record(A, 60, &[10, 0, 0]);
    assert_eq!(read_a(&wrong), Err(Malformed::Record));
    // A pointer to itself, or forward, and a label of the reserved kinds.
    let at = u16::try_from(12 + WEB.len() + 2 + 4).unwrap();
    for owner in [at, at + 2] {
        let pointing = Message::header(ID, ANSWER, [1, 1, 0, 0])
            .question(WEB, A)
            .pointer(owner)
            .record(A, 60, &[10, 0, 0, 1]);
        assert_eq!(read_a(&pointing), Err(Malformed::Name), "{owner}");
    }
    let reserved = Message::header(ID, ANSWER, [1, 1, 0, 0])
        .question(WEB, A)
        .bytes(&[0x40, 0])
        .record(A, 60, &[10, 0, 0, 1]);
    assert_eq!(read_a(&reserved), Err(Malformed::Name));
    // A TTL with its top bit set is none.
    let top = Message::header(ID, ANSWER, [1, 1, 0, 0])
        .question(WEB, A)
        .pointer(12)
        .record(A, 0x8000_0001, &[10, 0, 0, 1]);
    assert_eq!(read_a(&top), Ok(addresses(&["10.0.0.1"], 0)));
}

#[test]
fn resolv_conf_gives_its_first_three_nameservers() {
    let text = "# a comment\nsearch shop.svc.cluster.local svc.cluster.local\nnameserver 10.96.0.10\n\
                options ndots:5\nnameserver  fd00::a \nnameserver not-an-address\n\
                nameserver 10.96.0.11 # trailing\nnameserver 10.96.0.12\n";
    let servers: Vec<String> = nameservers(text).iter().map(ToString::to_string).collect();
    assert_eq!(servers, ["10.96.0.10", "fd00::a", "10.96.0.11"]);
    assert!(nameservers("").is_empty());
}

proptest! {
    /// Whatever bytes it is given, reading ends, and with an answer or why not.
    #[test]
    fn reading_any_bytes_ends(bytes in proptest::collection::vec(any::<u8>(), 0..600)) {
        let _ = read(&bytes, ID, WEB, Kind::A);
    }

    /// Any addresses and TTL a nameserver answers are read as it answered them.
    #[test]
    fn what_is_answered_is_read(
        found in proptest::collection::vec(any::<[u8; 4]>(), 1..20),
        ttl in 0u32..0x8000_0000,
    ) {
        let mut message = Message::header(ID, ANSWER, [1, u16::try_from(found.len()).unwrap(), 0, 0])
            .question(WEB, A);
        for address in &found {
            message = message.pointer(12).record(A, ttl, address);
        }
        let mut expected: Vec<IpAddr> = found.iter().map(|a| IpAddr::from(*a)).collect();
        expected.sort_unstable();
        expected.dedup();
        prop_assert_eq!(read_a(&message), Ok(Answer::Addresses { addresses: expected, ttl }));
    }
}
