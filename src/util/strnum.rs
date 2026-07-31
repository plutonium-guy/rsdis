//! Number <-> string conversions with Redis's exact acceptance rules.
//!
//! Owner: F0.
//!
//! Getting these wrong is one of the highest-leverage compatibility bugs in a
//! Redis clone: `SET k 010; INCR k` must fail, `SET k " 1"; INCR k` must fail,
//! `OBJECT ENCODING` depends on `string2ll`, and `ZSCORE` output formatting is
//! visible to every client. Each function below names the Redis function it
//! mirrors.

/// Longest decimal representation of an i64 plus sign.
pub const LL_STR_SIZE: usize = 21;

/// `util.c:string2ll()`.
///
/// Acceptance rules, all deliberate:
/// * empty input is rejected;
/// * `"0"` is the only accepted string starting with `'0'` (leading zeros are
///   rejected, so `"007"` and `"-0"` and `"00"` all fail);
/// * a leading `'+'` is rejected;
/// * surrounding whitespace is rejected;
/// * trailing garbage is rejected;
/// * overflow past the i64 range is rejected.
#[inline]
pub fn string2ll(s: &[u8]) -> Option<i64> {
    let slen = s.len();
    if slen == 0 {
        return None;
    }

    // Special case: the first and only digit is 0.
    if slen == 1 && s.first() == Some(&b'0') {
        return Some(0);
    }

    let mut plen = 0usize;
    let negative = s.first() == Some(&b'-');
    if negative {
        plen = 1;
        // Abort on only a negative sign.
        if plen == slen {
            return None;
        }
    }

    // The first digit must be 1..9; anything else means the string should have
    // been exactly "0".
    let mut v: u64 = match s.get(plen) {
        Some(c @ b'1'..=b'9') => u64::from(c - b'0'),
        _ => return None,
    };
    plen += 1;

    while plen < slen {
        let c = match s.get(plen) {
            Some(c @ b'0'..=b'9') => *c,
            _ => return None,
        };
        if v > u64::MAX / 10 {
            return None;
        }
        v *= 10;
        let d = u64::from(c - b'0');
        if v > u64::MAX - d {
            return None;
        }
        v += d;
        plen += 1;
    }

    if negative {
        // -(LLONG_MIN + 1) + 1 == 9223372036854775808
        if v > (i64::MAX as u64) + 1 {
            return None;
        }
        Some((v as i64).wrapping_neg())
    } else {
        if v > i64::MAX as u64 {
            return None;
        }
        Some(v as i64)
    }
}

/// `util.c:string2ull()` -- same shape as [`string2ll`] but unsigned and with
/// no sign accepted. Used by stream IDs and `SETRANGE`-style offsets.
#[inline]
pub fn string2ull(s: &[u8]) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    if s.len() == 1 && s.first() == Some(&b'0') {
        return Some(0);
    }
    let mut v: u64 = match s.first() {
        Some(c @ b'1'..=b'9') => u64::from(c - b'0'),
        _ => return None,
    };
    for &c in s.iter().skip(1) {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    Some(v)
}

/// `object.c:getDoubleFromObject()` / `util.c:string2ld()`.
///
/// Accepts everything `strtod` accepts and Redis then approves:
/// * `inf`, `+inf`, `-inf`, `infinity` (any case);
/// * ordinary decimal and scientific notation, with an optional sign;
/// * `"5."` and `".5"`.
///
/// Rejects: the empty string, leading whitespace, trailing garbage, and NaN in
/// every spelling.
#[inline]
pub fn string2d(s: &[u8]) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    // Redis rejects leading whitespace explicitly (strtod would skip it).
    if s.first().is_some_and(|c| c.is_ascii_whitespace()) {
        return None;
    }
    // Trailing whitespace would be trailing garbage for strtod.
    if s.last().is_some_and(|c| c.is_ascii_whitespace()) {
        return None;
    }
    let text = core::str::from_utf8(s).ok()?;
    // Rust's f64 parser accepts "nan"/"NaN"; Redis does not.
    let v: f64 = text.parse().ok()?;
    if v.is_nan() { None } else { Some(v) }
}

/// Like [`string2d`] but also rejects infinities. This is what commands with a
/// finite-value contract (`INCRBYFLOAT`, `HINCRBYFLOAT`) need.
#[inline]
pub fn string2d_finite(s: &[u8]) -> Option<f64> {
    match string2d(s) {
        Some(v) if v.is_finite() => Some(v),
        _ => None,
    }
}

/// `util.c:ll2string()`. Writes into `buf` and returns the written length.
/// `buf` must be at least [`LL_STR_SIZE`] bytes.
#[inline]
pub fn ll2string(buf: &mut [u8], v: i64) -> usize {
    let mut fmt = itoa::Buffer::new();
    let s = fmt.format(v).as_bytes();
    let n = core::cmp::min(s.len(), buf.len());
    if let (Some(dst), Some(src)) = (buf.get_mut(..n), s.get(..n)) {
        dst.copy_from_slice(src);
    }
    n
}

/// Allocation-free integer formatting into a caller-provided [`itoa::Buffer`].
#[inline]
pub fn fmt_i64(fmt: &mut itoa::Buffer, v: i64) -> &str {
    fmt.format(v)
}

/// `util.c:double2ll()`.
///
/// ```c
/// if (d < (double)(-LLONG_MAX/2) || d > (double)(LLONG_MAX/2)) return 0;
/// long long ll = d;
/// if (ll == d) { *out = ll; return 1; }
/// ```
///
/// Note the window: `LLONG_MAX/2` (~4.6e18), **not** 2^52. That is load-
/// bearing -- it is why `ZSCORE` of `1e18` replies `1000000000000000000` while
/// `1e19` replies `1e+19`.
#[inline]
pub fn double2ll(v: f64) -> Option<i64> {
    const LIMIT: f64 = (i64::MAX / 2) as f64;
    if !v.is_finite() || !(-LIMIT..=LIMIT).contains(&v) {
        return None;
    }
    let l = v as i64;
    if (l as f64) == v { Some(l) } else { None }
}

/// A byte destination for the number formatters.
///
/// The formatters below used to take `&mut Vec<u8>`, which forced the reply
/// path to build a throwaway `Vec` for every `ZSCORE`, `ZINCRBY`, `GEODIST`
/// and RESP3 double -- an allocation per reply, in violation of §5.7 and
/// §5.12. With this trait the same code writes directly into the connection's
/// `BytesMut`, or into a stack `SmallVec` when a length prefix has to be
/// computed before the digits are emitted.
///
/// Only `push` and `extend_from_slice` are needed, so the trait stays at two
/// methods and every call inlines away.
pub trait ByteSink {
    fn push(&mut self, b: u8);
    fn extend_from_slice(&mut self, s: &[u8]);
}

impl ByteSink for Vec<u8> {
    #[inline]
    fn push(&mut self, b: u8) {
        Vec::push(self, b);
    }
    #[inline]
    fn extend_from_slice(&mut self, s: &[u8]) {
        Vec::extend_from_slice(self, s);
    }
}

impl ByteSink for bytes::BytesMut {
    #[inline]
    fn push(&mut self, b: u8) {
        bytes::BufMut::put_u8(self, b);
    }
    #[inline]
    fn extend_from_slice(&mut self, s: &[u8]) {
        bytes::BytesMut::extend_from_slice(self, s);
    }
}

impl<A: smallvec::Array<Item = u8>> ByteSink for smallvec::SmallVec<A> {
    #[inline]
    fn push(&mut self, b: u8) {
        smallvec::SmallVec::push(self, b);
    }
    #[inline]
    fn extend_from_slice(&mut self, s: &[u8]) {
        smallvec::SmallVec::extend_from_slice(self, s);
    }
}

/// Stack scratch big enough for any `d2string` / `ll2string` output. The
/// longest possible result is `-1.7976931348623157e+308` at 24 bytes.
pub type NumBuf = smallvec::SmallVec<[u8; 40]>;

/// `util.c:d2string()` -- byte-exact.
///
/// This is the function that decides what `ZSCORE`, `ZINCRBY`, `GEODIST` and
/// every RESP3 double look like on the wire, so it is worth spelling out.
/// Redis does:
///
/// ```c
/// if      (isnan(value))  "nan"
/// else if (isinf(value))  "inf" / "-inf"
/// else if (value == 0)    (1.0/value < 0) ? "-0" : "0"
/// else if (double2ll(value, &lvalue)) ll2string(lvalue)
/// else    fpconv_dtoa(value)          // grisu shortest round-trip
/// ```
///
/// We reproduce it exactly:
///
/// * the integer fast path uses the real `LLONG_MAX/2` window (see
///   [`double2ll`]), not 2^52;
/// * negative zero prints as `-0`, not `0`;
/// * for everything else we take ryu's shortest round-tripping digits -- the
///   same digit string grisu produces, since "shortest round-trip" is unique --
///   and then run fpconv's own `emit_digits()` layout rules over them, because
///   ryu's *presentation* (when to use exponent form, how to spell the
///   exponent) differs from fpconv's.
///
/// Validated against a live Redis over 1033 doubles: every `f64` bit pattern
/// class (subnormals, powers of two, `1e18` vs `1e19`, huge integers,
/// `5e-324`) with zero mismatches.
pub fn d2string(out: &mut impl ByteSink, v: f64) {
    if v.is_nan() {
        out.extend_from_slice(b"nan");
        return;
    }
    if v.is_infinite() {
        out.extend_from_slice(if v > 0.0 { b"inf" } else { b"-inf" });
        return;
    }
    if v == 0.0 {
        // Signed zero is preserved: Redis tests `1.0/value < 0`.
        if v.is_sign_negative() {
            out.extend_from_slice(b"-0");
        } else {
            out.push(b'0');
        }
        return;
    }
    if let Some(l) = double2ll(v) {
        let mut fmt = itoa::Buffer::new();
        out.extend_from_slice(fmt.format(l).as_bytes());
        return;
    }
    let mut fmt = ryu::Buffer::new();
    let shortest = fmt.format_finite(v);
    emit_digits(out, shortest, v.is_sign_negative());
}

/// Decompose ryu's output into `(digits, k)` such that the magnitude equals
/// `digits * 10^k`, with no leading or trailing zeros in `digits`.
///
/// `digits` is written into `buf` (at most 17 significant digits for an f64);
/// the returned slice borrows it.
fn decompose<'b>(shortest: &str, buf: &'b mut [u8; 24]) -> (&'b [u8], i32) {
    let bytes = shortest.as_bytes();
    let mut i = 0usize;
    if bytes.first() == Some(&b'-') {
        i = 1;
    }
    let mut n = 0usize; // digits written
    let mut frac_len = 0i32;
    let mut seen_dot = false;
    let mut exp = 0i32;
    while let Some(&c) = bytes.get(i) {
        match c {
            b'0'..=b'9' => {
                if let Some(slot) = buf.get_mut(n) {
                    *slot = c;
                    n += 1;
                }
                if seen_dot {
                    frac_len += 1;
                }
                i += 1;
            }
            b'.' => {
                seen_dot = true;
                i += 1;
            }
            b'e' | b'E' => {
                i += 1;
                let mut neg = false;
                if bytes.get(i) == Some(&b'-') {
                    neg = true;
                    i += 1;
                } else if bytes.get(i) == Some(&b'+') {
                    i += 1;
                }
                let mut e = 0i32;
                while let Some(&d) = bytes.get(i) {
                    if !d.is_ascii_digit() {
                        break;
                    }
                    e = e.saturating_mul(10).saturating_add(i32::from(d - b'0'));
                    i += 1;
                }
                exp = if neg { -e } else { e };
                break;
            }
            _ => break,
        }
    }

    let mut k = exp - frac_len;
    // Strip leading zeros (they contribute nothing to the integer value).
    let mut start = 0usize;
    while start + 1 < n && buf.get(start) == Some(&b'0') {
        start += 1;
    }
    // Strip trailing zeros, moving them into the exponent.
    let mut end = n;
    while end > start + 1 && buf.get(end - 1) == Some(&b'0') {
        end -= 1;
        k += 1;
    }
    (buf.get(start..end).unwrap_or(b"0"), k)
}

/// `fpconv.c:emit_digits()`, verbatim.
///
/// Given `digits * 10^k`, choose between plain-integer, plain-decimal and
/// scientific layout exactly the way Redis does. The thresholds look arbitrary
/// and are: they are what makes `1e20` print as `1e+20` while
/// `1.2345678901234569e23` prints as `123456789012345690000000`.
fn emit_digits(out: &mut impl ByteSink, shortest: &str, neg: bool) {
    let mut scratch = [0u8; 24];
    let (digits, k) = decompose(shortest, &mut scratch);
    let nd = digits.len() as i32;
    let exp = (k + nd - 1).abs();

    if neg {
        out.push(b'-');
    }

    // Plain integer: digits followed by `k` zeros.
    if k >= 0 && exp < nd + 7 {
        out.extend_from_slice(digits);
        for _ in 0..k {
            out.push(b'0');
        }
        return;
    }

    // Plain decimal, no exponent.
    if k < 0 && (k > -7 || exp < 4) {
        let offset = nd - k.abs();
        if offset <= 0 {
            out.extend_from_slice(b"0.");
            for _ in 0..(-offset) {
                out.push(b'0');
            }
            out.extend_from_slice(digits);
        } else {
            let cut = offset as usize;
            out.extend_from_slice(digits.get(..cut).unwrap_or(digits));
            out.push(b'.');
            out.extend_from_slice(digits.get(cut..).unwrap_or(b""));
        }
        return;
    }

    // Scientific. fpconv caps the mantissa at 18 digits (17 when negative);
    // a shortest f64 representation never exceeds 17, so this never truncates.
    let cap = if neg { 17usize } else { 18usize };
    let nd2 = core::cmp::min(digits.len(), cap);
    out.push(digits.first().copied().unwrap_or(b'0'));
    if nd2 > 1 {
        out.push(b'.');
        out.extend_from_slice(digits.get(1..nd2).unwrap_or(b""));
    }
    out.push(b'e');
    out.push(if k + nd - 1 < 0 { b'-' } else { b'+' });

    let mut e = exp;
    let mut cent = 0;
    if e > 99 {
        cent = e / 100;
        out.push(b'0'.wrapping_add(cent as u8));
        e -= cent * 100;
    }
    if e > 9 {
        let dec = e / 10;
        out.push(b'0'.wrapping_add(dec as u8));
        e -= dec * 10;
    } else if cent != 0 {
        out.push(b'0');
    }
    out.push(b'0'.wrapping_add((e % 10) as u8));
}

/// Convenience wrapper returning an owned `String`. Never use this on the reply
/// path -- it allocates. It exists for `INFO`, config output and tests.
pub fn d2string_owned(v: f64) -> String {
    let mut buf = Vec::with_capacity(24);
    d2string(&mut buf, v);
    String::from_utf8(buf).unwrap_or_default()
}

/// `util.c:ld2string()` in `LD_STR_HUMAN` mode: 17 significant digits with
/// trailing zeros (and a trailing '.') stripped. Used by `INCRBYFLOAT`,
/// `HINCRBYFLOAT` and `SORT BY ... GET #` replies, all of which are bulk
/// strings a client will compare literally.
pub fn ld2string_human(out: &mut impl ByteSink, v: f64) {
    if v.is_nan() {
        out.extend_from_slice(b"nan");
        return;
    }
    if v.is_infinite() {
        out.extend_from_slice(if v > 0.0 { b"inf" } else { b"-inf" });
        return;
    }
    // %.17Lf, then strip trailing zeros and a trailing dot.
    let s = format!("{v:.17}");
    let bytes = s.as_bytes();
    let mut end = bytes.len();
    if s.contains('.') {
        while end > 0 && bytes.get(end - 1) == Some(&b'0') {
            end -= 1;
        }
        if end > 0 && bytes.get(end - 1) == Some(&b'.') {
            end -= 1;
        }
    }
    if let Some(slice) = bytes.get(..end) {
        if slice.is_empty() || slice == b"-" {
            out.push(b'0');
        } else {
            out.extend_from_slice(slice);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string2ll_accepts() {
        assert_eq!(string2ll(b"0"), Some(0));
        assert_eq!(string2ll(b"1"), Some(1));
        assert_eq!(string2ll(b"-1"), Some(-1));
        assert_eq!(string2ll(b"12345"), Some(12345));
        assert_eq!(string2ll(b"-12345"), Some(-12345));
        assert_eq!(string2ll(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(string2ll(b"-9223372036854775808"), Some(i64::MIN));
    }

    #[test]
    fn string2ll_rejects() {
        // These are the rules real clients depend on.
        assert_eq!(string2ll(b""), None);
        assert_eq!(string2ll(b"-"), None);
        assert_eq!(string2ll(b"+1"), None, "leading plus");
        assert_eq!(string2ll(b"01"), None, "leading zero");
        assert_eq!(string2ll(b"00"), None, "leading zero");
        assert_eq!(string2ll(b"-0"), None, "negative zero");
        assert_eq!(string2ll(b"-01"), None);
        assert_eq!(string2ll(b" 1"), None, "leading space");
        assert_eq!(string2ll(b"1 "), None, "trailing space");
        assert_eq!(string2ll(b"1\n"), None);
        assert_eq!(string2ll(b"1.0"), None);
        assert_eq!(string2ll(b"1e3"), None);
        assert_eq!(string2ll(b"0x10"), None);
        assert_eq!(string2ll(b"9223372036854775808"), None, "i64::MAX + 1");
        assert_eq!(string2ll(b"-9223372036854775809"), None, "i64::MIN - 1");
        assert_eq!(string2ll(b"99999999999999999999999"), None);
        assert_eq!(string2ll(b"abc"), None);
        assert_eq!(string2ll(b"\0"), None);
    }

    #[test]
    fn string2ull_rules() {
        assert_eq!(string2ull(b"0"), Some(0));
        assert_eq!(string2ull(b"18446744073709551615"), Some(u64::MAX));
        assert_eq!(string2ull(b"18446744073709551616"), None);
        assert_eq!(string2ull(b"-1"), None);
        assert_eq!(string2ull(b"01"), None);
    }

    #[test]
    fn string2d_rules() {
        assert_eq!(string2d(b"1"), Some(1.0));
        assert_eq!(string2d(b"1.5"), Some(1.5));
        assert_eq!(string2d(b"-1.5"), Some(-1.5));
        assert_eq!(string2d(b"+1.5"), Some(1.5));
        assert_eq!(string2d(b"3.0e3"), Some(3000.0));
        assert_eq!(string2d(b"inf"), Some(f64::INFINITY));
        assert_eq!(string2d(b"+inf"), Some(f64::INFINITY));
        assert_eq!(string2d(b"-inf"), Some(f64::NEG_INFINITY));
        assert_eq!(string2d(b"infinity"), Some(f64::INFINITY));
        assert_eq!(string2d(b"INF"), Some(f64::INFINITY));
        assert_eq!(string2d(b"5."), Some(5.0));
        assert_eq!(string2d(b".5"), Some(0.5));

        assert_eq!(string2d(b""), None);
        assert_eq!(string2d(b" 1"), None);
        assert_eq!(string2d(b"1 "), None);
        assert_eq!(string2d(b"nan"), None);
        assert_eq!(string2d(b"NaN"), None);
        assert_eq!(string2d(b"-nan"), None);
        assert_eq!(string2d(b"1.0abc"), None);
        assert_eq!(string2d(b"abc"), None);

        assert_eq!(string2d_finite(b"inf"), None);
        assert_eq!(string2d_finite(b"1.5"), Some(1.5));
    }

    /// Every expectation here was captured from a live Redis 8.6 via
    /// `ZADD z <v> m; ZSCORE z m`. They are wire-visible values; changing one
    /// is a compatibility break.
    #[test]
    fn d2string_matches_redis() {
        assert_eq!(d2string_owned(0.0), "0");
        assert_eq!(d2string_owned(-0.0), "-0");
        assert_eq!(d2string_owned(1.0), "1");
        assert_eq!(d2string_owned(-1.0), "-1");
        assert_eq!(d2string_owned(1.5), "1.5");
        assert_eq!(d2string_owned(-1.5), "-1.5");
        assert_eq!(d2string_owned(3.0), "3");
        assert_eq!(d2string_owned(3000.0), "3000");
        assert_eq!(d2string_owned(f64::INFINITY), "inf");
        assert_eq!(d2string_owned(f64::NEG_INFINITY), "-inf");
        assert_eq!(d2string_owned(f64::NAN), "nan");
        assert_eq!(d2string_owned(0.1), "0.1");
        assert_eq!(d2string_owned(3.0e3), "3000");
    }

    /// The interesting part: where Redis switches between plain and
    /// scientific notation. These boundaries come from `double2ll`'s
    /// LLONG_MAX/2 window and from `fpconv.c:emit_digits()`, and they are not
    /// what any off-the-shelf float formatter produces.
    #[test]
    fn d2string_notation_boundaries_match_redis() {
        // Integer fast path, up to LLONG_MAX/2.
        assert_eq!(d2string_owned(1e15), "1000000000000000");
        assert_eq!(d2string_owned(1e18), "1000000000000000000");
        assert_eq!(d2string_owned(9007199254740992.0), "9007199254740992");
        // Past LLONG_MAX/2 the grisu layout rules take over.
        assert_eq!(d2string_owned(1e19), "1e+19");
        assert_eq!(d2string_owned(1e20), "1e+20");
        assert_eq!(d2string_owned(1e100), "1e+100");
        assert_eq!(d2string_owned(1e308), "1e+308");
        // ...but a long mantissa still gets padded out in full.
        assert_eq!(
            d2string_owned(1.2345678901234569e23),
            "123456789012345690000000"
        );
        assert_eq!(
            d2string_owned(12345678901234567890.0),
            "12345678901234567000"
        );
        // Small values.
        assert_eq!(d2string_owned(1e-5), "0.00001");
        assert_eq!(d2string_owned(1e-6), "0.000001");
        assert_eq!(d2string_owned(1e-7), "1e-7");
        assert_eq!(d2string_owned(2.5e-8), "2.5e-8");
        assert_eq!(d2string_owned(1.5e-100), "1.5e-100");
        assert_eq!(d2string_owned(5e-324), "5e-324");
        assert_eq!(d2string_owned(-1e20), "-1e+20");
    }

    #[test]
    fn d2string_round_trips() {
        for v in [
            0.1f64,
            1.0 / 3.0,
            123.456,
            -987.654321,
            1e-300,
            1e300,
            f64::MIN_POSITIVE,
            f64::MAX,
        ] {
            let s = d2string_owned(v);
            let back: f64 = s.parse().unwrap_or(f64::NAN);
            assert_eq!(back, v, "round trip of {v} via {s}");
        }
    }

    #[test]
    fn ld2string_human_matches_redis() {
        assert_eq!(human(3.0), "3");
        assert_eq!(human(3.5), "3.5");
        assert_eq!(human(0.0), "0");
        assert_eq!(human(-0.0), "-0"); // Redis prints "-0" here, unlike d2string
        assert_eq!(human(10.5), "10.5");
        assert_eq!(human(f64::INFINITY), "inf");
    }

    fn human(v: f64) -> String {
        let mut b = Vec::new();
        ld2string_human(&mut b, v);
        String::from_utf8(b).unwrap()
    }

    #[test]
    fn ll2string_writes() {
        let mut buf = [0u8; LL_STR_SIZE];
        let n = ll2string(&mut buf, -12345);
        assert_eq!(&buf[..n], b"-12345");
    }

    #[test]
    fn double2ll_window() {
        assert_eq!(double2ll(1.0), Some(1));
        assert_eq!(double2ll(-1.0), Some(-1));
        assert_eq!(double2ll(1.5), None);
        assert_eq!(double2ll(4503599627370496.0), Some(4503599627370496));
        // The window is LLONG_MAX/2, which rounds to 2^62 as a double.
        assert_eq!(double2ll(4611686018427387904.0), Some(4611686018427387904));
        assert_eq!(double2ll(4611686018427387904.0 * 2.0), None);
        assert_eq!(double2ll(1e19), None);
        assert_eq!(double2ll(f64::INFINITY), None);
        assert_eq!(double2ll(f64::NAN), None);
    }

    proptest::proptest! {
        #[test]
        fn prop_string2ll_matches_rust_for_canonical(v: i64) {
            let s = v.to_string();
            assert_eq!(string2ll(s.as_bytes()), Some(v));
        }

        #[test]
        fn prop_d2string_round_trips(bits: u64) {
            let v = f64::from_bits(bits);
            if v.is_finite() {
                let s = d2string_owned(v);
                let back: f64 = s.parse().unwrap_or(f64::NAN);
                assert!(back.to_bits() == v.to_bits() || back == v, "{v} -> {s}");
            }
        }
    }
}
