//! Numbers as the recording holds them: the decoder's integers (tenths of a yen, shares, microseconds) in
//! `decimal128(38, 16)` and nanosecond columns, and their canonical JSON sizes.
use crate::error::Res;
use crate::require;

/// The inverse of an odd number modulo 2^128 (Newton's iteration), to divide exactly by multiplying.
const fn inverse(m: u128) -> u128 {
    let mut x = m;
    let mut i = 0;
    while i < 7 {
        x = x.wrapping_mul(2u128.wrapping_sub(m.wrapping_mul(x)));
        i += 1;
    }
    x
}

const P15: u128 = 5u128.pow(15);
const P16: u128 = 5u128.pow(16);
const INV15: u128 = inverse(P15);
const INV16: u128 = inverse(P16);
const LIMIT15: u128 = u128::MAX / P15;
const LIMIT16: u128 = u128::MAX / P16;
pub const TWO_53: u64 = 1 << 53;

#[inline(always)]
fn quotient(v: i128, twos: u32, inv: u128, limit: u128) -> Option<u64> {
    if v <= 0 {
        return if v == 0 { Some(0) } else { None };
    }
    let v = v as u128;
    if v.trailing_zeros() < twos {
        return None;
    }
    // v / (2^twos * 5^k) exactly, if v is a multiple of it: a multiple of an odd number times its inverse stays small.
    let q = (v >> twos).wrapping_mul(inv);
    if q > limit || q >= TWO_53 as u128 {
        return None;
    }
    Some(q as u64)
}

/// Tenths of a yen from a `decimal128(38, 16)` raw value; None for what is no whole, non-negative count below 2^53.
#[inline(always)]
pub fn tenths(v: i128) -> Option<u64> {
    quotient(v, 15, INV15, LIMIT15)
}

/// A whole count from a `decimal128(38, 16)` raw value (scale 0).
#[inline(always)]
pub fn whole(v: i128) -> Option<u64> {
    quotient(v, 16, INV16, LIMIT16)
}

/// `decimal128(38, 16)` raw values of tenths and of whole counts.
pub fn tenths_raw(t: u64) -> i128 {
    t as i128 * 10i128.pow(15)
}

pub fn whole_raw(q: u64) -> i128 {
    q as i128 * 10i128.pow(16)
}

/// len(str(v)).
#[inline(always)]
pub fn digits(v: u64) -> u64 {
    v.checked_ilog10().map_or(1, |l| l as u64 + 1)
}

/// Days since 1970-01-01 of a proleptic Gregorian date.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// UTC nanoseconds at JST midnight on a YYYYMMDD trading date.
pub fn midnight_ns(date: &str) -> Res<i64> {
    let bytes = date.as_bytes();
    require!(
        bytes.len() == 8 && bytes.iter().all(u8::is_ascii_digit),
        "Invalid date"
    );
    let number = |range: std::ops::Range<usize>| date[range].parse::<i64>().unwrap_or(0);
    let (y, m, d) = (number(0..4), number(4..6), number(6..8));
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let days = [
        31,
        if leap { 29 } else { 28 },
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
    require!(
        (1970..=2200).contains(&y) && (1..=12).contains(&m) && d >= 1 && d <= days[m as usize - 1],
        "Invalid date"
    );
    Ok((days_from_civil(y, m, d) * 86_400 - 9 * 3_600) * 1_000_000_000)
}

/// A local timing in milliseconds: non-negative, at most `max`, and with at most microsecond precision.
pub fn measurement(v: Option<f64>, max: f64) -> bool {
    match v {
        Some(v) => {
            v >= 0.0
                && v <= max
                && v.is_sign_positive()
                && format!("{v:.3}").parse::<f64>() == Ok(v)
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_are_exact_or_refused() {
        assert_eq!(tenths(tenths_raw(12345)), Some(12345));
        assert_eq!(tenths(0), Some(0));
        assert_eq!(tenths(tenths_raw(12345) + 1), None);
        assert_eq!(tenths(-tenths_raw(1)), None);
        assert_eq!(tenths(tenths_raw(TWO_53 - 1)), Some(TWO_53 - 1));
        assert_eq!(tenths(tenths_raw(TWO_53)), None);
        assert_eq!(whole(whole_raw(7_000_000)), Some(7_000_000));
        assert_eq!(whole(whole_raw(7) / 10), None);
        assert_eq!(whole(tenths_raw(1)), None);
        for q in [1u64, 9, 10, 99_999, TWO_53 - 1] {
            assert_eq!(tenths(tenths_raw(q)), Some(q));
            assert_eq!(whole(whole_raw(q)), Some(q));
        }
    }

    #[test]
    fn digits_are_those_of_the_decimal_text() {
        for v in [0u64, 9, 10, 99, 100, 123_456_789, TWO_53 - 1] {
            assert_eq!(digits(v), v.to_string().len() as u64);
        }
    }

    #[test]
    fn dates_are_japan_standard_midnight() {
        assert_eq!(midnight_ns("19700101").unwrap(), -9 * 3_600 * 1_000_000_000);
        assert_eq!(midnight_ns("20261014").unwrap(), 1_791_903_600_000_000_000);
        assert_eq!(midnight_ns("20240229").unwrap(), 1_709_132_400_000_000_000);
        assert!(midnight_ns("20260230").is_err());
        assert!(midnight_ns("2026-10-14").is_err());
    }

    #[test]
    fn measurements_have_microsecond_precision() {
        assert!(measurement(Some(12.345), 1e6));
        assert!(!measurement(Some(12.3456), 1e6));
        assert!(!measurement(Some(-0.0), 1e6));
        assert!(!measurement(Some(f64::NAN), 1e6));
        assert!(!measurement(None, 1e6));
    }
}
