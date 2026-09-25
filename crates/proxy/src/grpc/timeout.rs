//! `grpc-timeout`, read and written ([gRPC over HTTP/2]).
//!
//! A value is at most eight digits and a unit: `H`, `M`, `S` for hours, minutes and
//! seconds, `m`, `u`, `n` for milli-, micro- and nanoseconds. Anything else is not a
//! timeout. Written, a time goes in the finest unit that holds it in eight digits,
//! rounded up — a deadline sent on is never earlier than the one it stands for — as
//! grpc-go writes it.
//!
//! [gRPC over HTTP/2]: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md

use http::HeaderValue;
use std::time::Duration;

/// The most digits a value may have.
const DIGITS: usize = 8;

/// The largest value eight digits hold.
const MOST: u128 = 99_999_999;

/// The time a `grpc-timeout` value gives, or `None` for one that is not a timeout.
pub(crate) fn parse(value: &[u8]) -> Option<Duration> {
    let (&unit, digits) = value.split_last()?;
    if digits.is_empty() || digits.len() > DIGITS || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    // Eight digits at most, so no overflow here.
    let amount = digits
        .iter()
        .fold(0_u64, |amount, digit| amount * 10 + u64::from(digit - b'0'));
    Some(match unit {
        b'H' => Duration::from_secs(amount * 3600),
        b'M' => Duration::from_secs(amount * 60),
        b'S' => Duration::from_secs(amount),
        b'm' => Duration::from_millis(amount),
        b'u' => Duration::from_micros(amount),
        b'n' => Duration::from_nanos(amount),
        _ => return None,
    })
}

/// `left` as a `grpc-timeout` value: the finest unit that holds it in eight digits,
/// rounded up.
pub(crate) fn format(left: Duration) -> HeaderValue {
    let nanos = left.as_nanos();
    const UNITS: [(u128, &str); 6] = [
        (1, "n"),
        (1_000, "u"),
        (1_000_000, "m"),
        (1_000_000_000, "S"),
        (60_000_000_000, "M"),
        (3_600_000_000_000, "H"),
    ];
    for (size, unit) in UNITS {
        let amount = nanos.div_ceil(size);
        if amount <= MOST {
            return HeaderValue::from_str(&format!("{amount}{unit}"))
                .unwrap_or(HeaderValue::from_static("99999999H"));
        }
    }
    // More than eight digits of hours: as long as can be said.
    HeaderValue::from_static("99999999H")
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn every_unit_is_read() {
        assert_eq!(parse(b"2H"), Some(Duration::from_secs(7200)));
        assert_eq!(parse(b"3M"), Some(Duration::from_secs(180)));
        assert_eq!(parse(b"4S"), Some(Duration::from_secs(4)));
        assert_eq!(parse(b"5m"), Some(Duration::from_millis(5)));
        assert_eq!(parse(b"6u"), Some(Duration::from_micros(6)));
        assert_eq!(parse(b"7n"), Some(Duration::from_nanos(7)));
        assert_eq!(parse(b"0n"), Some(Duration::ZERO));
        assert_eq!(
            parse(b"99999999H"),
            Some(Duration::from_secs(99_999_999 * 3600))
        );
        assert_eq!(parse(b"00000001S"), Some(Duration::from_secs(1)));
    }

    #[test]
    fn what_is_not_a_timeout_is_not_read_as_one() {
        for value in [
            &b""[..],
            b"S",
            b"1",
            b"1s",
            b"1h",
            b"-1S",
            b"+1S",
            b" 1S",
            b"1S ",
            b"1.5S",
            b"123456789S",
            b"1SS",
            b"1 S",
            b"\xff1S",
        ] {
            assert_eq!(parse(value), None, "{:?}", String::from_utf8_lossy(value));
        }
    }

    #[test]
    fn a_time_is_written_in_the_finest_unit_that_holds_it() {
        let written = |duration| format(duration).to_str().unwrap().to_owned();
        assert_eq!(written(Duration::ZERO), "0n");
        assert_eq!(written(Duration::from_nanos(99_999_999)), "99999999n");
        assert_eq!(written(Duration::from_nanos(100_000_000)), "100000u");
        assert_eq!(written(Duration::from_secs(1)), "1000000u");
        assert_eq!(written(Duration::from_secs(100)), "100000m");
        // Rounded up, never down: a microsecond and a nanosecond is two microseconds.
        assert_eq!(written(Duration::from_nanos(100_000_001)), "100001u");
        assert_eq!(written(Duration::from_secs(200_000)), "200000S");
        assert_eq!(written(Duration::from_secs(99_999_999 * 60)), "99999999M");
        assert_eq!(written(Duration::MAX), "99999999H");
    }

    proptest! {
        /// Whatever is written reads back as no less than it stood for, and no more than
        /// one of its unit more.
        #[test]
        fn written_reads_back_no_earlier(nanos in 0_u64..u64::MAX) {
            let left = Duration::from_nanos(nanos);
            let value = format(left);
            let read = parse(value.as_bytes()).unwrap();
            prop_assert!(read >= left, "{left:?} became {read:?}");
            let unit = match value.as_bytes().last() {
                Some(b'n') => Duration::from_nanos(1),
                Some(b'u') => Duration::from_micros(1),
                Some(b'm') => Duration::from_millis(1),
                Some(b'S') => Duration::from_secs(1),
                Some(b'M') => Duration::from_secs(60),
                _ => Duration::from_secs(3600),
            };
            prop_assert!(read - left < unit, "{left:?} became {read:?}");
        }

        /// Nothing that is not a timeout panics on the way to being refused.
        #[test]
        fn anything_is_read_without_harm(value in prop::collection::vec(any::<u8>(), 0..12)) {
            let _read = parse(&value);
        }
    }
}
