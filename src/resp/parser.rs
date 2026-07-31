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
//! # Incremental (W1b)
//!
//! The scan is a **resumable state machine**, not a restart. F0's version
//! re-scanned a partial frame from byte zero on every `read()`, which is
//! O(n²) when a large value or a large pipeline dribbles in over many small
//! reads. [`RequestParser`] instead remembers:
//!
//! * which frame element it is in the middle of ([`State`]);
//! * how far it has already searched for the next `\r` (`scanned`), so
//!   `memchr` never re-reads a byte it has already rejected;
//! * the argument ranges collected so far (`ranges`), so completed bulks are
//!   never re-parsed.
//!
//! Resuming is sound because nothing is consumed until a frame is complete:
//! the buffer's logical origin does not move, so every recorded offset stays
//! valid across reads. The invariant is asserted by
//! `byte_at_a_time_matches_one_shot` in `tests/protocol_test.rs`, which feeds
//! every frame type one byte per call and requires an identical parse.
//!
//! # Limits
//!
//! Taken from Redis's `networking.c`, and enforced before any allocation is
//! sized from client-controlled input:
//!
//! * multibulk count <= 1024 * 1024 (`*100000000\r\n` is rejected *before*
//!   anything is reserved -- `ranges` grows only as real arguments arrive, so
//!   even a legal count of 1M costs nothing until 1M arguments are actually
//!   sent);
//! * bulk length <= `proto-max-bulk-len` (512 MB by default);
//! * an unterminated `*`/`$` count line, or an inline request, longer than
//!   `PROTO_INLINE_MAX_SIZE` (64 KB).
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
/// Redis's default `proto-max-bulk-len`.
pub const PROTO_MAX_BULK_LEN: u64 = 512 * 1024 * 1024;

/// A fatal protocol error. The connection replies and closes.
#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
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
    #[error("Protocol error: too big bulk count string")]
    TooBigBulkCount,
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
    /// Not enough bytes yet. Nothing was consumed, and the parser has
    /// remembered how far it got.
    Incomplete,
}

/// Where the scan is inside the frame currently being read.
///
/// Every offset is relative to the **start of the read buffer**, which is also
/// the start of the current frame: a frame is consumed atomically, so no
/// partially-parsed bytes ever sit behind the buffer's origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Between frames. The next byte decides multibulk versus inline.
    Start,
    /// Inside the leading `*<count>\r\n`. `scanned` is where the next search
    /// for `\r` resumes.
    MbulkHeader { scanned: usize },
    /// Between bulks. `pos` is the offset of the expected `$`; `remaining`
    /// counts the bulks still to read, including the one at `pos`.
    BulkHeader {
        pos: usize,
        remaining: usize,
        scanned: usize,
    },
    /// A `$<len>\r\n` header has been read; waiting for `len + 2` payload
    /// bytes at `start`. `remaining` excludes this bulk.
    BulkBody {
        start: usize,
        len: usize,
        remaining: usize,
    },
    /// Inside an inline command. `scanned` is where the next search for `\n`
    /// resumes.
    Inline { scanned: usize },
}

/// A resumable request parser. One per connection, for the connection's life.
#[derive(Debug, Clone)]
pub struct RequestParser {
    proto_max_bulk_len: u64,
    state: State,
    /// `(start, end)` of each argument decoded so far in the current frame.
    /// Cleared when the frame completes; never sized from a client-supplied
    /// count, only grown as real arguments arrive.
    ranges: SmallVec<[(usize, usize); 8]>,
}

impl Default for RequestParser {
    fn default() -> Self {
        RequestParser::new(PROTO_MAX_BULK_LEN)
    }
}

impl RequestParser {
    pub fn new(proto_max_bulk_len: u64) -> Self {
        RequestParser {
            proto_max_bulk_len,
            state: State::Start,
            ranges: SmallVec::new(),
        }
    }

    /// Pick up a changed `proto-max-bulk-len` without discarding parse state.
    ///
    /// The connection loop refreshes this periodically; rebuilding the parser
    /// instead would throw away a half-read frame and desynchronise the
    /// stream.
    #[inline]
    pub fn set_proto_max_bulk_len(&mut self, v: u64) {
        self.proto_max_bulk_len = v;
    }

    #[inline]
    pub fn proto_max_bulk_len(&self) -> u64 {
        self.proto_max_bulk_len
    }

    /// True when a frame is partially read. The connection layer uses this to
    /// tell "idle between commands" from "mid-command", and to refuse to
    /// shrink the read buffer under a partial frame.
    #[inline]
    pub fn is_mid_frame(&self) -> bool {
        self.state != State::Start
    }

    /// Bytes of the current frame already scanned, for diagnostics and for
    /// `CLIENT LIST`'s `qbuf` field.
    pub fn scan_position(&self) -> usize {
        match self.state {
            State::Start => 0,
            State::MbulkHeader { scanned } | State::Inline { scanned } => scanned,
            State::BulkHeader { pos, .. } => pos,
            State::BulkBody { start, len, .. } => start.saturating_add(len),
        }
    }

    #[inline]
    fn reset(&mut self) {
        self.state = State::Start;
        self.ranges.clear();
    }

    /// Try to take one command off the front of `buf`.
    ///
    /// On [`Parsed::Incomplete`] nothing is consumed and the scan position is
    /// remembered, so the next call resumes rather than restarts.
    pub fn parse(&mut self, buf: &mut BytesMut) -> Result<Parsed, ProtoError> {
        loop {
            match self.state {
                // ------------------------------------------------ dispatch
                State::Start => match buf.first() {
                    None => return Ok(Parsed::Incomplete),
                    Some(b'*') => self.state = State::MbulkHeader { scanned: 1 },
                    Some(_) => self.state = State::Inline { scanned: 0 },
                },

                // ------------------------------------------- `*<count>\r\n`
                State::MbulkHeader { scanned } => match line_end(buf, 1, scanned) {
                    Line::Incomplete(next) => {
                        // A client dribbling an endless count line must not be
                        // allowed to buffer without bound.
                        if buf.len() > INLINE_MAX_SIZE {
                            return Err(ProtoError::TooBigMbulkCount);
                        }
                        self.state = State::MbulkHeader { scanned: next };
                        return Ok(Parsed::Incomplete);
                    }
                    Line::Malformed => return Err(ProtoError::InvalidMultibulkLength),
                    Line::Found { start, end, next } => {
                        let digits = buf.get(start..end).unwrap_or(b"");
                        let Some(count) = parse_i64_strict(digits) else {
                            return Err(ProtoError::InvalidMultibulkLength);
                        };
                        // Validated *before* anything is reserved: `*100000000`
                        // never reaches an allocator.
                        if count > MULTIBULK_MAX {
                            return Err(ProtoError::InvalidMultibulkLength);
                        }
                        if count <= 0 {
                            // `*0\r\n` and `*-1\r\n` are both "nothing to do".
                            let _ = buf.split_to(next);
                            self.reset();
                            return Ok(Parsed::Empty);
                        }
                        self.ranges.clear();
                        self.state = State::BulkHeader {
                            pos: next,
                            remaining: count as usize,
                            scanned: next,
                        };
                    }
                },

                // --------------------------------------------- `$<len>\r\n`
                State::BulkHeader {
                    pos,
                    remaining,
                    scanned,
                } => {
                    match buf.get(pos) {
                        None => return Ok(Parsed::Incomplete),
                        Some(&b'$') => {}
                        Some(&c) => return Err(ProtoError::ExpectedDollar(char::from(c))),
                    }
                    match line_end(buf, pos + 1, scanned) {
                        Line::Incomplete(next) => {
                            if buf.len().saturating_sub(pos) > INLINE_MAX_SIZE {
                                return Err(ProtoError::TooBigBulkCount);
                            }
                            self.state = State::BulkHeader {
                                pos,
                                remaining,
                                scanned: next,
                            };
                            return Ok(Parsed::Incomplete);
                        }
                        Line::Malformed => return Err(ProtoError::InvalidBulkLength),
                        Line::Found { start, end, next } => {
                            let digits = buf.get(start..end).unwrap_or(b"");
                            let Some(len) = parse_i64_strict(digits) else {
                                return Err(ProtoError::InvalidBulkLength);
                            };
                            if len < 0 || len as u64 > self.proto_max_bulk_len {
                                return Err(ProtoError::InvalidBulkLength);
                            }
                            self.state = State::BulkBody {
                                start: next,
                                len: len as usize,
                                remaining: remaining - 1,
                            };
                        }
                    }
                }

                // ------------------------------------------- the bulk payload
                State::BulkBody {
                    start,
                    len,
                    remaining,
                } => {
                    // `start + len + 2` on a 64-bit host cannot overflow for a
                    // length already bounded by proto-max-bulk-len, but the
                    // arithmetic is checked anyway: this is network input.
                    let Some(after) = start.checked_add(len).and_then(|e| e.checked_add(2)) else {
                        return Err(ProtoError::InvalidBulkLength);
                    };
                    if buf.len() < after {
                        // O(1) on every subsequent read: no re-scan, just a
                        // length check against the recorded body extent.
                        return Ok(Parsed::Incomplete);
                    }
                    self.ranges.push((start, after - 2));
                    if remaining == 0 {
                        let frame = buf.split_to(after).freeze();
                        let mut args = ArgVec::with_capacity(self.ranges.len());
                        for &(s, e) in &self.ranges {
                            args.push(frame.slice(s..e));
                        }
                        self.reset();
                        return Ok(Parsed::Command(args));
                    }
                    self.state = State::BulkHeader {
                        pos: after,
                        remaining,
                        scanned: after,
                    };
                }

                // ------------------------------------------------ inline
                State::Inline { scanned } => {
                    let rest = buf.get(scanned..).unwrap_or(b"");
                    let Some(rel) = memchr::memchr(b'\n', rest) else {
                        if buf.len() > INLINE_MAX_SIZE {
                            return Err(ProtoError::TooBigInline);
                        }
                        self.state = State::Inline { scanned: buf.len() };
                        return Ok(Parsed::Incomplete);
                    };
                    let nl = scanned + rel;
                    if nl > INLINE_MAX_SIZE {
                        return Err(ProtoError::TooBigInline);
                    }
                    // Strip the newline and an optional preceding CR.
                    let mut end = nl;
                    if end > 0 && buf.get(end - 1) == Some(&b'\r') {
                        end -= 1;
                    }
                    let line = buf.split_to(nl + 1).freeze();
                    self.reset();
                    let args = split_inline(&line.slice(0..end))?;
                    if args.is_empty() {
                        return Ok(Parsed::Empty);
                    }
                    return Ok(Parsed::Command(args));
                }
            }
        }
    }
}

/// Result of looking for the `\r\n` that ends a protocol header line.
enum Line {
    /// The line runs `start..end`, and the next frame element begins at
    /// `next` (just past the `\r\n`).
    Found {
        start: usize,
        end: usize,
        next: usize,
    },
    /// Not terminated yet. The payload is where the next search should
    /// resume, so no byte is ever examined twice.
    Incomplete(usize),
    /// A `\r` not followed by `\n`: the header can never become valid.
    Malformed,
}

/// Find the `\r\n` closing a header line that starts at `from`, resuming the
/// `memchr` at `scanned`.
#[inline]
fn line_end(buf: &[u8], from: usize, scanned: usize) -> Line {
    let scan_from = scanned.max(from);
    let Some(rest) = buf.get(scan_from..) else {
        return Line::Incomplete(buf.len());
    };
    let Some(rel) = memchr::memchr(b'\r', rest) else {
        return Line::Incomplete(buf.len());
    };
    let cr = scan_from + rel;
    match buf.get(cr + 1) {
        // The `\r` is the last byte we have: resume from it, not past it.
        None => Line::Incomplete(cr),
        Some(b'\n') => Line::Found {
            start: from,
            end: cr,
            next: cr + 2,
        },
        Some(_) => Line::Malformed,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(input: &[u8]) -> (Result<Parsed, ProtoError>, usize) {
        let mut buf = BytesMut::from(input);
        let mut p = RequestParser::default();
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
            let mut p = RequestParser::default();
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
        let mut p = RequestParser::default();
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
        let mut p = RequestParser::default();
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
        let mut p = RequestParser::default();
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
        let mut p = RequestParser::new(16);
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
        assert_eq!(
            ProtoError::TooBigMbulkCount.wire_message(),
            "ERR Protocol error: too big mbulk count string"
        );
        assert_eq!(
            ProtoError::TooBigBulkCount.wire_message(),
            "ERR Protocol error: too big bulk count string"
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
        let mut p = RequestParser::default();
        assert!(matches!(p.parse(&mut buf), Ok(Parsed::Incomplete)));
        assert_eq!(buf.len(), 3);
    }

    // ------------------------------------------------------- incrementality

    #[test]
    fn state_survives_a_partial_read() {
        let mut p = RequestParser::default();
        let mut buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n"[..]);
        assert!(matches!(p.parse(&mut buf).unwrap(), Parsed::Incomplete));
        assert!(p.is_mid_frame());
        // The completed first bulk is already recorded, and the scan resumes
        // where it stopped rather than at byte zero.
        assert_eq!(p.ranges.len(), 1);
        assert_eq!(
            p.state,
            State::BulkHeader {
                pos: 13,
                remaining: 1,
                scanned: 13
            }
        );
        buf.extend_from_slice(b"$1\r\nk\r\n");
        assert_eq!(words(p.parse(&mut buf).unwrap()), vec!["GET", "k"]);
        assert!(!p.is_mid_frame());
    }

    #[test]
    fn a_dribbled_body_is_not_rescanned() {
        // The body state records the exact extent, so each further read is an
        // O(1) length check instead of an O(n) re-scan.
        let mut p = RequestParser::default();
        let mut buf = BytesMut::from(&b"*1\r\n$8\r\n"[..]);
        assert!(matches!(p.parse(&mut buf).unwrap(), Parsed::Incomplete));
        assert_eq!(
            p.state,
            State::BulkBody {
                start: 8,
                len: 8,
                remaining: 0
            }
        );
        for b in b"abcdefgh\r\n" {
            buf.extend_from_slice(&[*b]);
            let before = p.state;
            match p.parse(&mut buf).unwrap() {
                Parsed::Incomplete => assert_eq!(p.state, before, "body state must not move"),
                Parsed::Command(a) => {
                    assert_eq!(&a[0][..], b"abcdefgh");
                    return;
                }
                other => panic!("{other:?}"),
            }
        }
        panic!("never completed");
    }

    #[test]
    fn a_dribbled_header_never_rescans_a_byte() {
        let mut p = RequestParser::default();
        let mut buf = BytesMut::new();
        let mut last = 0usize;
        for b in b"*123" {
            buf.extend_from_slice(&[*b]);
            assert!(matches!(p.parse(&mut buf).unwrap(), Parsed::Incomplete));
            let State::MbulkHeader { scanned } = p.state else {
                panic!("{:?}", p.state)
            };
            assert!(scanned >= last, "scan position must never go backwards");
            last = scanned;
        }
        assert_eq!(last, buf.len());
    }

    #[test]
    fn too_big_count_strings() {
        // An unterminated `*` line longer than 64 KB.
        let mut v = vec![b'*'];
        v.extend(std::iter::repeat_n(b'1', INLINE_MAX_SIZE + 1));
        assert_eq!(parse_one(&v).0.unwrap_err(), ProtoError::TooBigMbulkCount);

        // ... and the same for a `$` line.
        let mut v = Vec::from(&b"*1\r\n$"[..]);
        v.extend(std::iter::repeat_n(b'1', INLINE_MAX_SIZE + 1));
        assert_eq!(parse_one(&v).0.unwrap_err(), ProtoError::TooBigBulkCount);
    }

    #[test]
    fn a_hostile_count_allocates_nothing() {
        // 100M elements: rejected on the header, before `ranges` is touched.
        let mut buf = BytesMut::from(&b"*100000000\r\n"[..]);
        let mut p = RequestParser::default();
        assert_eq!(
            p.parse(&mut buf).unwrap_err(),
            ProtoError::InvalidMultibulkLength
        );
        assert_eq!(
            p.ranges.capacity(),
            8,
            "no growth beyond the inline SmallVec"
        );

        // A legal-but-huge count reserves nothing either: `ranges` grows only
        // as real arguments arrive.
        let mut buf = BytesMut::from(&b"*1048576\r\n"[..]);
        let mut p = RequestParser::default();
        assert!(matches!(p.parse(&mut buf).unwrap(), Parsed::Incomplete));
        assert_eq!(p.ranges.capacity(), 8);
    }

    #[test]
    fn bulk_length_beyond_the_limit_is_rejected_before_buffering() {
        let mut p = RequestParser::default();
        let mut buf = BytesMut::from(&b"*1\r\n$536870913\r\n"[..]);
        assert_eq!(
            p.parse(&mut buf).unwrap_err(),
            ProtoError::InvalidBulkLength
        );
        // Exactly at the limit is legal (and simply incomplete).
        let mut p = RequestParser::default();
        let mut buf = BytesMut::from(&b"*1\r\n$536870912\r\n"[..]);
        assert!(matches!(p.parse(&mut buf).unwrap(), Parsed::Incomplete));
    }

    #[test]
    fn cr_without_lf_is_a_protocol_error() {
        assert_eq!(
            parse_one(b"*1\rX").0.unwrap_err(),
            ProtoError::InvalidMultibulkLength
        );
        assert_eq!(
            parse_one(b"*1\r\n$1\rX").0.unwrap_err(),
            ProtoError::InvalidBulkLength
        );
    }

    #[test]
    fn proto_max_bulk_len_can_change_mid_stream() {
        let mut p = RequestParser::new(1024);
        let mut buf = BytesMut::from(&b"*1\r\n$2048\r\n"[..]);
        assert!(p.parse(&mut buf).is_err());
        let mut p = RequestParser::new(1024);
        p.set_proto_max_bulk_len(4096);
        let mut buf = BytesMut::from(&b"*1\r\n$2048\r\n"[..]);
        assert!(matches!(p.parse(&mut buf).unwrap(), Parsed::Incomplete));
    }

    proptest::proptest! {
        /// The parser must never panic and never consume more than it
        /// produced, whatever a hostile client sends.
        #[test]
        fn prop_never_panics(data: Vec<u8>) {
            let mut buf = BytesMut::from(&data[..]);
            let mut p = RequestParser::default();
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
            let mut p = RequestParser::default();
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
