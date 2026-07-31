//! The RESP request parser.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! # Zero copy (§5.2)
//!
//! Parsing happens in two passes over the read buffer:
//!
//! 1. **Scan** -- walk the bytes, find the end of one complete command and
//!    record `(offset, len)` for each argument. Nothing is copied and nothing
//!    is consumed, so an incomplete frame simply leaves the buffer alone.
//! 2. **Split** -- `BytesMut::split_to(total).freeze()` hands back an owned
//!    `Bytes` covering exactly that frame, and each argument becomes a
//!    `Bytes::slice` of it. Every argument therefore shares one refcounted
//!    allocation with the read buffer; a command argument is never a fresh
//!    `Vec<u8>` or `String`.
//!
//! # Limits
//!
//! Taken from Redis, and enforced before any allocation is sized from
//! client-controlled input:
//!
//! * multibulk count <= 1024 * 1024
//! * bulk length <= `proto-max-bulk-len` (512 MB by default)
//! * inline request <= 64 KB
//!
//! Every limit violation is a *protocol* error: the connection must reply and
//! then close, because the stream can no longer be resynchronised.

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

use crate::command::ArgVec;

/// Redis's `PROTO_INLINE_MAX_SIZE`.
pub const INLINE_MAX_SIZE: usize = 64 * 1024;
/// Redis's hard cap on a multibulk element count.
pub const MULTIBULK_MAX: i64 = 1024 * 1024;

/// A fatal protocol error. The connection replies and closes.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtoError {
    #[error("Protocol error: invalid multibulk length")]
    InvalidMultibulkLength,
    #[error("Protocol error: invalid bulk length")]
    InvalidBulkLength,
    #[error("Protocol error: expected '$', got '{0}'")]
    ExpectedDollar(char),
    #[error("Protocol error: too big inline request")]
    TooBigInline,
    #[error("Protocol error: unbalanced quotes in request")]
    UnbalancedQuotes,
    #[error("Protocol error: too big mbulk count string")]
    TooBigMbulkCount,
}

impl ProtoError {
    /// The full error line, as Redis writes it (no leading `-`, no CRLF).
    pub fn wire_message(&self) -> String {
        format!("ERR {self}")
    }
}

/// Outcome of one parse attempt.
///
/// `Command` is large (a `SmallVec<[Bytes; 8]>` is ~264 bytes) and the other
/// variants are empty, which clippy flags. Boxing it would trade one
/// allocation per command for a smaller return slot -- the wrong way round on
/// the hot path (§5.4), and the value is moved out immediately anyway.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Parsed {
    /// A complete command; the bytes have been consumed from the buffer.
    Command(ArgVec),
    /// A complete but empty command (e.g. `*0\r\n`, or a blank inline line).
    /// The bytes were consumed; the caller should just loop.
    Empty,
    /// Not enough bytes yet. Nothing was consumed.
    Incomplete,
}

/// A stateless request parser.
///
/// It holds only limits, so a connection can keep one for its lifetime and it
/// stays trivially `Send`.
#[derive(Debug, Clone)]
pub struct RequestParser {
    pub proto_max_bulk_len: u64,
}

impl Default for RequestParser {
    fn default() -> Self {
        RequestParser {
            proto_max_bulk_len: 512 * 1024 * 1024,
        }
    }
}

impl RequestParser {
    pub fn new(proto_max_bulk_len: u64) -> Self {
        RequestParser { proto_max_bulk_len }
    }

    /// Try to take one command off the front of `buf`.
    pub fn parse(&self, buf: &mut BytesMut) -> Result<Parsed, ProtoError> {
        let Some(&first) = buf.first() else {
            return Ok(Parsed::Incomplete);
        };
        if first == b'*' {
            self.parse_multibulk(buf)
        } else {
            self.parse_inline(buf)
        }
    }

    // ------------------------------------------------------------ multibulk

    fn parse_multibulk(&self, buf: &mut BytesMut) -> Result<Parsed, ProtoError> {
        // Pass 1: scan without consuming.
        let mut pos = 0usize;

        let Some((count, next)) = read_line_int(buf, pos, b'*')? else {
            // Guard against a client dribbling an endless count line.
            if buf.len() > 64 * 1024 {
                return Err(ProtoError::TooBigMbulkCount);
            }
            return Ok(Parsed::Incomplete);
        };
        pos = next;

        if count > MULTIBULK_MAX {
            return Err(ProtoError::InvalidMultibulkLength);
        }
        if count <= 0 {
            // `*0\r\n` and `*-1\r\n` are both "nothing to do" in a request.
            buf.advance_to(pos);
            return Ok(Parsed::Empty);
        }

        // Ranges into the frame, recorded before anything is consumed.
        let mut ranges: SmallVec<[(usize, usize); 8]> = SmallVec::new();
        for _ in 0..count {
            match buf.get(pos) {
                None => return Ok(Parsed::Incomplete),
                Some(&b'$') => {}
                Some(&c) => return Err(ProtoError::ExpectedDollar(char::from(c))),
            }
            let Some((len, next)) = read_line_int(buf, pos, b'$')? else {
                return Ok(Parsed::Incomplete);
            };
            if len < 0 || (len as u64) > self.proto_max_bulk_len {
                return Err(ProtoError::InvalidBulkLength);
            }
            let len = len as usize;
            let start = next;
            let end = start
                .checked_add(len)
                .ok_or(ProtoError::InvalidBulkLength)?;
            // Need the payload plus its trailing CRLF.
            if buf.len() < end + 2 {
                return Ok(Parsed::Incomplete);
            }
            ranges.push((start, end));
            pos = end + 2;
        }

        // Pass 2: take the frame in one piece; every argument is a slice of it.
        let frame = buf.split_to(pos).freeze();
        let mut args = ArgVec::with_capacity(ranges.len());
        for (s, e) in ranges {
            args.push(frame.slice(s..e));
        }
        Ok(Parsed::Command(args))
    }

    // ---------------------------------------------------------------- inline

    fn parse_inline(&self, buf: &mut BytesMut) -> Result<Parsed, ProtoError> {
        let Some(nl) = memchr::memchr(b'\n', buf) else {
            if buf.len() > INLINE_MAX_SIZE {
                return Err(ProtoError::TooBigInline);
            }
            return Ok(Parsed::Incomplete);
        };
        if nl > INLINE_MAX_SIZE {
            return Err(ProtoError::TooBigInline);
        }
        // Strip the newline and an optional preceding carriage return.
        let mut end = nl;
        if end > 0 && buf.get(end - 1) == Some(&b'\r') {
            end -= 1;
        }
        let line = buf.split_to(nl + 1).freeze();
        let text = line.slice(0..end);

        let args = split_inline(&text)?;
        if args.is_empty() {
            return Ok(Parsed::Empty);
        }
        Ok(Parsed::Command(args))
    }
}

/// Split an inline command, honouring quotes the way `sdssplitargs()` does.
///
/// Unlike the multibulk path this cannot be fully zero-copy for quoted or
/// escaped arguments (the bytes change), but the common unquoted case still
/// yields slices of the original frame.
fn split_inline(line: &Bytes) -> Result<ArgVec, ProtoError> {
    let mut out = ArgVec::new();
    let mut i = 0usize;
    let n = line.len();

    while i < n {
        // Skip whitespace.
        while i < n && line.get(i).is_some_and(|c| c.is_ascii_whitespace()) {
            i += 1;
        }
        if i >= n {
            break;
        }
        match line.get(i) {
            Some(&b'"') => {
                i += 1;
                let mut buf: Vec<u8> = Vec::new();
                loop {
                    let Some(&c) = line.get(i) else {
                        return Err(ProtoError::UnbalancedQuotes);
                    };
                    match c {
                        b'\\' => {
                            let Some(&esc) = line.get(i + 1) else {
                                return Err(ProtoError::UnbalancedQuotes);
                            };
                            if esc == b'x'
                                && let (Some(&h1), Some(&h2)) = (line.get(i + 2), line.get(i + 3))
                                && let (Some(a), Some(b)) = (hex_val(h1), hex_val(h2))
                            {
                                buf.push((a << 4) | b);
                                i += 4;
                                continue;
                            }
                            buf.push(match esc {
                                b'n' => b'\n',
                                b'r' => b'\r',
                                b't' => b'\t',
                                b'b' => 0x08,
                                b'a' => 0x07,
                                other => other,
                            });
                            i += 2;
                        }
                        b'"' => {
                            i += 1;
                            // A closing quote must be followed by a space or
                            // the end of the line.
                            if line.get(i).is_some_and(|c| !c.is_ascii_whitespace()) {
                                return Err(ProtoError::UnbalancedQuotes);
                            }
                            break;
                        }
                        other => {
                            buf.push(other);
                            i += 1;
                        }
                    }
                }
                out.push(Bytes::from(buf));
            }
            Some(&b'\'') => {
                i += 1;
                let mut buf: Vec<u8> = Vec::new();
                loop {
                    let Some(&c) = line.get(i) else {
                        return Err(ProtoError::UnbalancedQuotes);
                    };
                    match c {
                        b'\\' if line.get(i + 1) == Some(&b'\'') => {
                            buf.push(b'\'');
                            i += 2;
                        }
                        b'\'' => {
                            i += 1;
                            if line.get(i).is_some_and(|c| !c.is_ascii_whitespace()) {
                                return Err(ProtoError::UnbalancedQuotes);
                            }
                            break;
                        }
                        other => {
                            buf.push(other);
                            i += 1;
                        }
                    }
                }
                out.push(Bytes::from(buf));
            }
            _ => {
                let start = i;
                while i < n && line.get(i).is_some_and(|c| !c.is_ascii_whitespace()) {
                    i += 1;
                }
                // Zero-copy: a slice of the frame the caller already owns.
                out.push(line.slice(start..i));
            }
        }
    }
    Ok(out)
}

#[inline]
fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Read a `<prefix><int>\r\n` header starting at `pos`.
///
/// Returns `Ok(None)` when the line is not complete yet. The parse is
/// deliberately strict: Redis rejects `$0x10` and `$ 1` too.
fn read_line_int(
    buf: &BytesMut,
    pos: usize,
    prefix: u8,
) -> Result<Option<(i64, usize)>, ProtoError> {
    debug_assert_eq!(buf.get(pos), Some(&prefix));
    let after_prefix = pos + 1;
    let Some(rest) = buf.get(after_prefix..) else {
        return Ok(None);
    };
    let Some(rel) = memchr::memchr(b'\r', rest) else {
        return Ok(None);
    };
    // Need the '\n' too.
    if rest.get(rel + 1) != Some(&b'\n') {
        if rest.len() > rel + 1 {
            return Err(bad_length(prefix));
        }
        return Ok(None);
    }
    let digits = rest.get(..rel).unwrap_or(b"");
    let v = parse_i64_strict(digits).ok_or_else(|| bad_length(prefix))?;
    Ok(Some((v, after_prefix + rel + 2)))
}

fn bad_length(prefix: u8) -> ProtoError {
    if prefix == b'*' {
        ProtoError::InvalidMultibulkLength
    } else {
        ProtoError::InvalidBulkLength
    }
}

/// `-?[0-9]+`, no whitespace, no plus sign, no leading-zero rule (the protocol
/// header is not `string2ll`: Redis accepts `$007` here).
fn parse_i64_strict(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let (neg, digits) = match s.first() {
        Some(b'-') => (true, s.get(1..)?),
        _ => (false, s),
    };
    if digits.is_empty() {
        return None;
    }
    let mut v: i64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(i64::from(c - b'0'))?;
    }
    Some(if neg { -v } else { v })
}

/// `BytesMut` has `advance` via the `Buf` trait; this spells out the intent
/// and keeps the trait import local.
trait AdvanceTo {
    fn advance_to(&mut self, n: usize);
}

impl AdvanceTo for BytesMut {
    #[inline]
    fn advance_to(&mut self, n: usize) {
        let _ = self.split_to(n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(input: &[u8]) -> (Result<Parsed, ProtoError>, usize) {
        let mut buf = BytesMut::from(input);
        let p = RequestParser::default();
        let r = p.parse(&mut buf);
        (r, buf.len())
    }

    fn words(p: Parsed) -> Vec<String> {
        match p {
            Parsed::Command(a) => a
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect(),
            other => panic!("expected a command, got {other:?}"),
        }
    }

    #[test]
    fn multibulk_basic() {
        let (r, left) = parse_one(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n");
        assert_eq!(words(r.unwrap()), vec!["ECHO", "hello"]);
        assert_eq!(left, 0);
    }

    #[test]
    fn multibulk_empty_and_binary_args() {
        let (r, _) = parse_one(b"*2\r\n$0\r\n\r\n$4\r\na\r\nb\r\n");
        match r.unwrap() {
            Parsed::Command(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(&a[0][..], b"");
                assert_eq!(&a[1][..], b"a\r\nb");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn multibulk_incomplete_consumes_nothing() {
        for prefix in [
            &b"*"[..],
            b"*2\r\n",
            b"*2\r\n$4\r\n",
            b"*2\r\n$4\r\nECHO\r\n",
            b"*2\r\n$4\r\nECHO\r\n$5\r\nhell",
            b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r",
        ] {
            let mut buf = BytesMut::from(prefix);
            let before = buf.len();
            let p = RequestParser::default();
            assert!(
                matches!(p.parse(&mut buf), Ok(Parsed::Incomplete)),
                "{prefix:?} should be incomplete"
            );
            assert_eq!(buf.len(), before, "{prefix:?} must not consume");
        }
    }

    #[test]
    fn pipelining_yields_each_command_in_order() {
        let mut buf = BytesMut::from(
            &b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n*1\r\n$4\r\nPING\r\n"[..],
        );
        let p = RequestParser::default();
        let mut got = Vec::new();
        loop {
            match p.parse(&mut buf).unwrap() {
                Parsed::Command(a) => got.push(
                    a.iter()
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .collect::<Vec<_>>(),
                ),
                Parsed::Empty => continue,
                Parsed::Incomplete => break,
            }
        }
        assert_eq!(got.len(), 3);
        assert_eq!(got[1], vec!["ECHO", "hi"]);
        assert!(buf.is_empty());
    }

    #[test]
    fn arguments_share_the_read_buffer() {
        // The zero-copy guarantee of §5.2: an argument must be a slice of the
        // frame, not a fresh allocation. `Bytes::is_unique` is false when two
        // handles share one allocation.
        let mut buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"[..]);
        let p = RequestParser::default();
        match p.parse(&mut buf).unwrap() {
            Parsed::Command(a) => {
                assert_eq!(a.len(), 2);
                assert!(
                    !a[0].is_unique() && !a[1].is_unique(),
                    "arguments should share the frame allocation"
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn empty_multibulk_is_consumed() {
        let mut buf = BytesMut::from(&b"*0\r\n*1\r\n$4\r\nPING\r\n"[..]);
        let p = RequestParser::default();
        assert!(matches!(p.parse(&mut buf).unwrap(), Parsed::Empty));
        assert_eq!(words(p.parse(&mut buf).unwrap()), vec!["PING"]);
    }

    #[test]
    fn protocol_errors() {
        assert_eq!(
            parse_one(b"*3\r\n$3\r\nGET\r\nx").0.unwrap_err(),
            ProtoError::ExpectedDollar('x')
        );
        assert_eq!(
            parse_one(b"*abc\r\n").0.unwrap_err(),
            ProtoError::InvalidMultibulkLength
        );
        assert_eq!(
            parse_one(b"*2000000\r\n").0.unwrap_err(),
            ProtoError::InvalidMultibulkLength
        );
        assert_eq!(
            parse_one(b"*1\r\n$abc\r\n").0.unwrap_err(),
            ProtoError::InvalidBulkLength
        );
        assert_eq!(
            parse_one(b"*1\r\n$-1\r\n").0.unwrap_err(),
            ProtoError::InvalidBulkLength
        );
        let p = RequestParser::new(16);
        let mut buf = BytesMut::from(&b"*1\r\n$32\r\n"[..]);
        assert_eq!(
            p.parse(&mut buf).unwrap_err(),
            ProtoError::InvalidBulkLength
        );
    }

    #[test]
    fn error_messages_match_redis() {
        assert_eq!(
            ProtoError::InvalidMultibulkLength.wire_message(),
            "ERR Protocol error: invalid multibulk length"
        );
        assert_eq!(
            ProtoError::InvalidBulkLength.wire_message(),
            "ERR Protocol error: invalid bulk length"
        );
        assert_eq!(
            ProtoError::ExpectedDollar('x').wire_message(),
            "ERR Protocol error: expected '$', got 'x'"
        );
        assert_eq!(
            ProtoError::UnbalancedQuotes.wire_message(),
            "ERR Protocol error: unbalanced quotes in request"
        );
        assert_eq!(
            ProtoError::TooBigInline.wire_message(),
            "ERR Protocol error: too big inline request"
        );
    }

    #[test]
    fn inline_commands() {
        assert_eq!(words(parse_one(b"PING\r\n").0.unwrap()), vec!["PING"]);
        assert_eq!(words(parse_one(b"PING\n").0.unwrap()), vec!["PING"]);
        assert_eq!(
            words(parse_one(b"SET key value\r\n").0.unwrap()),
            vec!["SET", "key", "value"]
        );
        assert_eq!(
            words(parse_one(b"SET k \"hello world\"\r\n").0.unwrap()),
            vec!["SET", "k", "hello world"]
        );
        assert_eq!(
            words(parse_one(b"SET k 'a b'\r\n").0.unwrap()),
            vec!["SET", "k", "a b"]
        );
        assert_eq!(
            words(parse_one(b"ECHO \"a\\x41b\"\r\n").0.unwrap()),
            vec!["ECHO", "aAb"]
        );
        assert_eq!(
            words(parse_one(b"  SET   k   v  \r\n").0.unwrap()),
            vec!["SET", "k", "v"]
        );
    }

    #[test]
    fn inline_blank_line_is_empty_not_an_error() {
        assert!(matches!(parse_one(b"\r\n").0.unwrap(), Parsed::Empty));
        assert!(matches!(parse_one(b"   \r\n").0.unwrap(), Parsed::Empty));
    }

    #[test]
    fn inline_unbalanced_quotes() {
        assert_eq!(
            parse_one(b"SET k \"unterminated\r\n").0.unwrap_err(),
            ProtoError::UnbalancedQuotes
        );
        assert_eq!(
            parse_one(b"SET k \"a\"b\r\n").0.unwrap_err(),
            ProtoError::UnbalancedQuotes
        );
    }

    #[test]
    fn inline_too_big() {
        let mut v = vec![b'a'; INLINE_MAX_SIZE + 10];
        v.push(b'\n');
        assert_eq!(parse_one(&v).0.unwrap_err(), ProtoError::TooBigInline);
    }

    #[test]
    fn inline_incomplete_waits() {
        let mut buf = BytesMut::from(&b"PIN"[..]);
        let p = RequestParser::default();
        assert!(matches!(p.parse(&mut buf), Ok(Parsed::Incomplete)));
        assert_eq!(buf.len(), 3);
    }

    proptest::proptest! {
        /// The parser must never panic and never consume more than it
        /// produced, whatever a hostile client sends.
        #[test]
        fn prop_never_panics(data: Vec<u8>) {
            let mut buf = BytesMut::from(&data[..]);
            let p = RequestParser::default();
            let before = buf.len();
            match p.parse(&mut buf) {
                Ok(Parsed::Incomplete) => assert_eq!(buf.len(), before),
                _ => assert!(buf.len() <= before),
            }
        }

        /// Any well-formed multibulk round-trips.
        #[test]
        fn prop_multibulk_round_trip(args in proptest::collection::vec(
            proptest::collection::vec(proptest::num::u8::ANY, 0..32), 1..8))
        {
            let mut wire = Vec::new();
            wire.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in &args {
                wire.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                wire.extend_from_slice(a);
                wire.extend_from_slice(b"\r\n");
            }
            let mut buf = BytesMut::from(&wire[..]);
            let p = RequestParser::default();
            match p.parse(&mut buf).expect("valid frame") {
                Parsed::Command(got) => {
                    assert_eq!(got.len(), args.len());
                    for (g, a) in got.iter().zip(args.iter()) {
                        assert_eq!(&g[..], &a[..]);
                    }
                    assert!(buf.is_empty());
                }
                other => panic!("{other:?}"),
            }
        }
    }
}
