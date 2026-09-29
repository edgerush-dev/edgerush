//! A request's ID (08 §3 in the docs): a UUIDv7 (RFC 9562 §5.7), which a listener that
//! generates IDs gives every request it serves, and which the upstream and the client are
//! both told in `X-Request-ID`.
//!
//! Its first 48 bits are the time in milliseconds; the 74 bits that follow the version and
//! the variant are all random, with no fraction of a millisecond and no counter: the ID is
//! for finding a request, not for putting requests in order, which is what a log's own
//! timestamps are for. The time and the random bytes are the caller's to give, since this
//! crate reads no clock and draws no numbers; RFC 9562 §6.9 asks for a cryptographically
//! secure generator, as a client that is shown its own ID must not be able to work out
//! anyone else's.

use http::header::{HeaderName, HeaderValue};

/// The header that carries the ID, to the upstream and back to the client. No rule may
/// change it: the upstream, the client and the gateway are to know a request by one ID.
pub const HEADER: HeaderName = HeaderName::from_static("x-request-id");

/// How many random bytes an ID is made from. Six of their 80 bits are not used: the version
/// and the variant take their place.
pub const RANDOM_BYTES: usize = 10;

/// How long an ID is as text: 32 hexadecimal digits in groups of 8, 4, 4, 4 and 12, joined
/// by hyphens.
pub const LENGTH: usize = 36;

/// The ID made at `unix_ms`, milliseconds since the Unix epoch, from `random`, as text: its
/// hexadecimal digits in lower case, as every generator surveyed writes them (RFC 9562 §4
/// allows either case). Only the low 48 bits of `unix_ms` are used; they last until the
/// year 10889.
#[must_use]
pub fn text(unix_ms: u64, random: [u8; RANDOM_BYTES]) -> [u8; LENGTH] {
    // Where each of the 16 bytes' two digits go: after 4, 6, 8 and 10 bytes, a hyphen.
    const AT: [usize; 16] = [0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34];
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let [_, _, t0, t1, t2, t3, t4, t5] = unix_ms.to_be_bytes();
    let [r0, r1, r2, r3, r4, r5, r6, r7, r8, r9] = random;
    let bytes = [
        t0,
        t1,
        t2,
        t3,
        t4,
        t5,
        // Version 7 in the high four bits.
        0x70 | (r0 & 0x0f),
        r1,
        // The variant, `10`, in the high two bits.
        0x80 | (r2 & 0x3f),
        r3,
        r4,
        r5,
        r6,
        r7,
        r8,
        r9,
    ];
    let mut text = [b'-'; LENGTH];
    for (byte, at) in bytes.into_iter().zip(AT) {
        text[at] = DIGITS[usize::from(byte >> 4)];
        text[at + 1] = DIGITS[usize::from(byte & 0x0f)];
    }
    text
}

/// The same ID as a header's value.
#[must_use]
pub fn value(unix_ms: u64, random: [u8; RANDOM_BYTES]) -> HeaderValue {
    // Hexadecimal digits and hyphens are all a value may hold, so the nil UUID is never
    // given: it is there only because the conversion's type allows a failure.
    HeaderValue::from_bytes(&text(unix_ms, random))
        .unwrap_or_else(|_| HeaderValue::from_static("00000000-0000-0000-0000-000000000000"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// The ID the slow way: the 128 bits put together as one number, printed, then cut into
    /// groups.
    fn reference(unix_ms: u64, random: [u8; RANDOM_BYTES]) -> String {
        let random = random
            .iter()
            .fold(0_u128, |number, &byte| (number << 8) | u128::from(byte));
        let rand_a = (random >> 64) & 0xfff;
        let rand_b = random & ((1 << 62) - 1);
        let number = (u128::from(unix_ms & 0xffff_ffff_ffff) << 80)
            | (0x7 << 76)
            | (rand_a << 64)
            | (0b10 << 62)
            | rand_b;
        let digits = format!("{number:032x}");
        format!(
            "{}-{}-{}-{}-{}",
            &digits[..8],
            &digits[8..12],
            &digits[12..16],
            &digits[16..20],
            &digits[20..]
        )
    }

    fn as_str(text: &[u8; LENGTH]) -> &str {
        std::str::from_utf8(text).unwrap()
    }

    #[test]
    fn the_rfc_example_comes_out_as_the_rfc_writes_it() {
        // RFC 9562 Appendix A.6: 2022-02-22 19:22:22 UTC, rand_a 0xCC3, rand_b
        // 0x18C4DC0C0C07398F.
        let random = [0x0c, 0xc3, 0x18, 0xc4, 0xdc, 0x0c, 0x0c, 0x07, 0x39, 0x8f];
        assert_eq!(
            as_str(&text(0x017f_22e2_79b0, random)),
            "017f22e2-79b0-7cc3-98c4-dc0c0c07398f"
        );
    }

    #[test]
    fn the_version_and_the_variant_are_the_ids_whatever_the_random_bytes_say() {
        let ones = text(0, [0xff; RANDOM_BYTES]);
        assert_eq!(as_str(&ones), "00000000-0000-7fff-bfff-ffffffffffff");
        let zeros = text(0, [0; RANDOM_BYTES]);
        assert_eq!(as_str(&zeros), "00000000-0000-7000-8000-000000000000");
    }

    #[test]
    fn a_time_past_48_bits_keeps_its_low_48() {
        assert_eq!(
            text(0xabcd_0123_4567_89ab, [0; RANDOM_BYTES]),
            text(0x0123_4567_89ab, [0; RANDOM_BYTES])
        );
    }

    #[test]
    fn the_value_is_the_text() {
        let random = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let value = value(1_759_142_400_000, random);
        assert_eq!(value.as_bytes(), text(1_759_142_400_000, random));
    }

    proptest! {
        #[test]
        fn the_id_is_the_references(unix_ms: u64, random: [u8; RANDOM_BYTES]) {
            let id = text(unix_ms, random);
            prop_assert_eq!(as_str(&id), reference(unix_ms, random));
        }

        #[test]
        fn ids_of_different_milliseconds_sort_as_their_times(
            earlier in 0_u64..1 << 48,
            later in 0_u64..1 << 48,
            first: [u8; RANDOM_BYTES],
            second: [u8; RANDOM_BYTES],
        ) {
            prop_assume!(earlier < later);
            prop_assert!(text(earlier, first) < text(later, second));
        }
    }
}
