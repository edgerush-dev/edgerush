//! The value of a `Date` field, in the one format a sender may use.
//!
//! A worker makes one of these a second, from a clock it reads itself, and hands it to the
//! response writer, which reads no clock ([14 §4](../../../../docs/14-downstream-server.md)).

/// A time in the IMF-fixdate format of
/// [RFC 9110 §5.6.7](https://www.rfc-editor.org/rfc/rfc9110.html#section-5.6.7):
/// `Sun, 06 Nov 1994 08:49:37 GMT`, always 29 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpDate([u8; 29]);

impl HttpDate {
    /// The time `seconds` after the Unix epoch, in UTC.
    ///
    /// Years past 9999 do not fit the format, and are written as the last second it has.
    pub fn from_unix(seconds: u64) -> Self {
        // 9999-12-31T23:59:59Z.
        const LAST: u64 = 253_402_300_799;
        let seconds = seconds.min(LAST);
        let days = seconds / 86_400;
        let of_day = seconds % 86_400;
        let (year, month, day) = civil(days);
        // 1970-01-01 was a Thursday.
        let weekday = (days + 4) % 7;

        const WEEKDAYS: [&[u8; 3]; 7] = [b"Sun", b"Mon", b"Tue", b"Wed", b"Thu", b"Fri", b"Sat"];
        const MONTHS: [&[u8; 3]; 12] = [
            b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
            b"Dec",
        ];
        let mut out = *b"Thu, 01 Jan 1970 00:00:00 GMT";
        // Each index is within its table: a weekday is below 7 and a month from 1 to 12.
        out[..3].copy_from_slice(WEEKDAYS[usize::try_from(weekday).unwrap_or(0)]);
        digits(&mut out[5..7], u64::from(day));
        out[8..11].copy_from_slice(MONTHS[usize::from(month - 1)]);
        digits(&mut out[12..16], year);
        digits(&mut out[17..19], of_day / 3600);
        digits(&mut out[20..22], of_day % 3600 / 60);
        digits(&mut out[23..25], of_day % 60);
        Self(out)
    }

    /// The field value, as it goes on the wire.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Writes `value` into `out` in decimal, as many digits as `out` has, zero-padded.
fn digits(out: &mut [u8], mut value: u64) {
    for place in out.iter_mut().rev() {
        // A remainder of ten is below ten, so it fits a byte.
        *place = b'0' + u8::try_from(value % 10).unwrap_or(0);
        value /= 10;
    }
}

/// The year, month and day of the day `days` after 1970-01-01, by Howard Hinnant's
/// `civil_from_days` (public domain), restricted to days that are not before the epoch.
fn civil(days: u64) -> (u64, u8, u8) {
    // Shifted to 0000-03-01, so that a leap day is the last day of its year.
    let shifted = days + 719_468;
    let era = shifted / 146_097;
    let of_era = shifted % 146_097;
    let year_of_era = (of_era - of_era / 1460 + of_era / 36_524 - of_era / 146_096) / 365;
    let day_of_year = of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let march_based = (5 * day_of_year + 2) / 153;
    // Below 31 and 12 by the arithmetic above, so each fits a byte.
    let day = u8::try_from(day_of_year - (153 * march_based + 2) / 5 + 1).unwrap_or(1);
    let month = u8::try_from(if march_based < 10 {
        march_based + 3
    } else {
        march_based - 9
    })
    .unwrap_or(1);
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn date(seconds: u64) -> String {
        String::from_utf8(HttpDate::from_unix(seconds).as_bytes().to_vec()).unwrap()
    }

    #[test]
    fn known_times_are_written_as_the_rfc_writes_them() {
        assert_eq!(date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        // RFC 9110 §5.6.7's own example.
        assert_eq!(date(784_111_777), "Sun, 06 Nov 1994 08:49:37 GMT");
        // A leap day, and the day after it.
        assert_eq!(date(951_782_400), "Tue, 29 Feb 2000 00:00:00 GMT");
        assert_eq!(date(951_868_800), "Wed, 01 Mar 2000 00:00:00 GMT");
        // The last second of a year, and the first of the next.
        assert_eq!(date(1_798_761_599), "Thu, 31 Dec 2026 23:59:59 GMT");
        assert_eq!(date(1_798_761_600), "Fri, 01 Jan 2027 00:00:00 GMT");
        // 2100 is not a leap year.
        assert_eq!(date(4_107_542_400), "Mon, 01 Mar 2100 00:00:00 GMT");
    }

    #[test]
    fn a_time_past_the_format_is_its_last_second() {
        assert_eq!(date(u64::MAX), "Fri, 31 Dec 9999 23:59:59 GMT");
    }

    /// Against a reference that counts days one at a time, which is too slow for the path
    /// a request takes and too simple to be wrong in the same way.
    fn counted(seconds: u64) -> (u64, u8, u8) {
        let leap = |year: u64| {
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
        };
        let mut days = seconds / 86_400;
        let mut year = 1970;
        loop {
            let length = if leap(year) { 366 } else { 365 };
            if days < length {
                break;
            }
            days -= length;
            year += 1;
        }
        let months = [
            31,
            if leap(year) { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut month = 0;
        while days >= months[month] {
            days -= months[month];
            month += 1;
        }
        (
            year,
            u8::try_from(month + 1).unwrap(),
            u8::try_from(days + 1).unwrap(),
        )
    }

    proptest! {
        #[test]
        fn every_day_is_the_day_counting_gives(seconds in 0_u64..4_102_444_800) {
            prop_assert_eq!(civil(seconds / 86_400), counted(seconds));
        }
    }
}
