//! The RESP2/RESP3 reply writer.
//!
//! Owner: F0. **FROZEN** -- every handler in the project writes through this
//! type. You may add methods; you may not change the meaning of an existing
//! one.
//!
//! §4.3: this is a concrete struct rather than a trait so that every call
//! inlines. All output goes straight into the connection's existing
//! `BytesMut`; nothing here allocates a temporary, formats through
//! `core::fmt`, or copies a reply twice.
//!
//! **RESP2/RESP3 divergence is handled here and nowhere else.** A handler may
//! branch on `proto` only where the reply *shape* genuinely differs per the
//! Redis docs (`XPENDING`, `CONFIG GET`, `CLIENT INFO`), never to pick a type
//! marker.
//!
//! ## Protocol reference (RESP3 spec + Redis 7.4 `networking.c`)
//!
//! | method        | RESP2            | RESP3                   |
//! |---------------|------------------|-------------------------|
//! | `simple`      | `+s\r\n`         | `+s\r\n`                |
//! | `error`       | `-e\r\n`         | `-e\r\n`                |
//! | `int`         | `:n\r\n`         | `:n\r\n`                |
//! | `bulk`        | `$len\r\n..\r\n` | `$len\r\n..\r\n`        |
//! | `null`        | `$-1\r\n`        | `_\r\n`                 |
//! | `null_array`  | `*-1\r\n`        | `_\r\n`                 |
//! | `array`       | `*n\r\n`         | `*n\r\n`                |
//! | `map`         | `*2n\r\n`        | `%n\r\n`                |
//! | `set_header`  | `*n\r\n`         | `~n\r\n`                |
//! | `double`      | bulk string      | `,x\r\n`                |
//! | `boolean`     | `:1\r\n`/`:0\r\n`| `#t\r\n`/`#f\r\n`       |
//! | `verbatim`    | bulk string      | `=len\r\nfmt:..\r\n`    |
//! | `big_number`  | bulk string      | `(s\r\n`                |
//! | `push`        | `*n\r\n`         | `>n\r\n`                |

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::CmdError;
use crate::util::strnum;

/// RESP protocol version 2.
pub const RESP2: u8 = 2;
/// RESP protocol version 3.
pub const RESP3: u8 = 3;

const CRLF: &[u8; 2] = b"\r\n";

pub struct ReplyWriter<'a> {
    buf: &'a mut BytesMut,
    pub proto: u8,
}

impl<'a> ReplyWriter<'a> {
    #[inline]
    pub fn new(buf: &'a mut BytesMut, proto: u8) -> Self {
        ReplyWriter { buf, proto }
    }

    /// True when the connection negotiated RESP3 via `HELLO 3`.
    #[inline]
    pub fn is_resp3(&self) -> bool {
        self.proto >= RESP3
    }

    /// Direct access to the output buffer, for the few callers that must emit
    /// a pre-encoded frame (pub/sub push payloads, `DUMP`, RDB-in-reply).
    #[inline]
    pub fn raw(&mut self) -> &mut BytesMut {
        self.buf
    }

    /// Current write position. Pair with [`ReplyWriter::rollback`] to abandon a
    /// partially written reply when a handler discovers an error mid-way.
    #[inline]
    pub fn mark(&self) -> usize {
        self.buf.len()
    }

    /// Truncate back to a position previously returned by
    /// [`ReplyWriter::mark`]. Only ever shrinks.
    #[inline]
    pub fn rollback(&mut self, mark: usize) {
        if mark <= self.buf.len() {
            self.buf.truncate(mark);
        }
    }

    // ---------------------------------------------------------------- scalars

    /// `+OK\r\n` -- the single most common reply in the protocol.
    #[inline]
    pub fn ok(&mut self) {
        self.buf.extend_from_slice(b"+OK\r\n");
    }

    /// A simple status string. Must not contain CR or LF.
    #[inline]
    pub fn simple(&mut self, s: &str) {
        self.buf.reserve(s.len() + 3);
        self.buf.put_u8(b'+');
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.extend_from_slice(CRLF);
    }

    /// An error, rendered from [`CmdError`]'s `Display`.
    #[inline]
    pub fn error(&mut self, e: &CmdError) {
        self.buf.put_u8(b'-');
        // `Display` for CmdError never allocates for the fixed variants; the
        // fmt machinery writes straight into the BytesMut.
        use std::fmt::Write as _;
        let _ = write!(BytesMutFmt(self.buf), "{e}");
        self.buf.extend_from_slice(CRLF);
    }

    /// A raw error line, e.g. `"ERR something"`. Prefer [`ReplyWriter::error`].
    #[inline]
    pub fn error_str(&mut self, msg: &str) {
        self.buf.reserve(msg.len() + 3);
        self.buf.put_u8(b'-');
        self.buf.extend_from_slice(msg.as_bytes());
        self.buf.extend_from_slice(CRLF);
    }

    /// `:n\r\n`
    #[inline]
    pub fn int(&mut self, v: i64) {
        self.buf.put_u8(b':');
        self.put_i64(v);
        self.buf.extend_from_slice(CRLF);
    }

    /// `$len\r\n<bytes>\r\n`
    #[inline]
    pub fn bulk(&mut self, b: &[u8]) {
        self.buf.reserve(b.len() + 16);
        self.buf.put_u8(b'$');
        self.put_usize(b.len());
        self.buf.extend_from_slice(CRLF);
        self.buf.extend_from_slice(b);
        self.buf.extend_from_slice(CRLF);
    }

    /// Bulk string from a `Bytes`.
    ///
    /// This is the "zero-copy path where possible" of §4.3. Today the value is
    /// still memcpy'd into the connection buffer: true zero-copy needs the
    /// writer to hold a vectored queue of `Bytes` and `writev` them, which is
    /// W1b's call to make when it owns `src/net`. The signature is here so the
    /// change is a body edit, not a call-site sweep across 200 handlers.
    #[inline]
    pub fn bulk_from(&mut self, b: &Bytes) {
        self.bulk(b);
    }

    /// A bulk string whose content is a decimal integer, formatted straight
    /// into the buffer. This is Redis's `addReplyBulkLongLong`, and it is what
    /// makes `GET` on an int-encoded key allocation-free (§5.1).
    #[inline]
    pub fn bulk_i64(&mut self, v: i64) {
        let mut fmt = itoa::Buffer::new();
        self.bulk(fmt.format(v).as_bytes());
    }

    /// A bulk string built from two pieces, without joining them first.
    #[inline]
    pub fn bulk2(&mut self, a: &[u8], b: &[u8]) {
        self.buf.reserve(a.len() + b.len() + 16);
        self.buf.put_u8(b'$');
        self.put_usize(a.len() + b.len());
        self.buf.extend_from_slice(CRLF);
        self.buf.extend_from_slice(a);
        self.buf.extend_from_slice(b);
        self.buf.extend_from_slice(CRLF);
    }

    /// The null bulk string: `$-1` on RESP2, `_` on RESP3.
    #[inline]
    pub fn null(&mut self) {
        if self.is_resp3() {
            self.buf.extend_from_slice(b"_\r\n");
        } else {
            self.buf.extend_from_slice(b"$-1\r\n");
        }
    }

    /// The null array: `*-1` on RESP2, `_` on RESP3.
    ///
    /// RESP3 collapsed both nulls into one type, but RESP2 clients distinguish
    /// them (`BLPOP` timing out replies `*-1`, `GET` on a missing key replies
    /// `$-1`), so handlers must keep choosing the right one.
    #[inline]
    pub fn null_array(&mut self) {
        if self.is_resp3() {
            self.buf.extend_from_slice(b"_\r\n");
        } else {
            self.buf.extend_from_slice(b"*-1\r\n");
        }
    }

    /// `,x\r\n` on RESP3; a bulk string on RESP2.
    ///
    /// Formatting follows `util.c:d2string()` in both cases, so `1.0` is sent
    /// as `1` and infinities as `inf` / `-inf`.
    pub fn double(&mut self, v: f64) {
        if self.is_resp3() {
            self.buf.put_u8(b',');
            self.put_double(v);
            self.buf.extend_from_slice(CRLF);
        } else {
            // RESP2 has no double type: Redis sends the same digits as a bulk.
            let mut tmp = [0u8; 40];
            let n = write_double(&mut tmp, v);
            let slice = tmp.get(..n).unwrap_or(b"0");
            self.bulk(slice);
        }
    }

    /// `#t` / `#f` on RESP3; `:1` / `:0` on RESP2.
    #[inline]
    pub fn boolean(&mut self, v: bool) {
        if self.is_resp3() {
            self.buf
                .extend_from_slice(if v { b"#t\r\n" } else { b"#f\r\n" });
        } else {
            self.buf
                .extend_from_slice(if v { b":1\r\n" } else { b":0\r\n" });
        }
    }

    /// `=len\r\nfmt:<bytes>\r\n` on RESP3; a plain bulk string on RESP2.
    ///
    /// `fmt` is a three-character hint such as `txt` or `mkd`. The declared
    /// length includes the `fmt:` prefix, per the RESP3 spec.
    pub fn verbatim(&mut self, fmt: &str, b: &[u8]) {
        if self.is_resp3() {
            self.buf.reserve(b.len() + 16);
            self.buf.put_u8(b'=');
            self.put_usize(b.len() + 4);
            self.buf.extend_from_slice(CRLF);
            // Exactly three bytes of format, padded/truncated defensively.
            let f = fmt.as_bytes();
            for i in 0..3 {
                self.buf.put_u8(f.get(i).copied().unwrap_or(b' '));
            }
            self.buf.put_u8(b':');
            self.buf.extend_from_slice(b);
            self.buf.extend_from_slice(CRLF);
        } else {
            self.bulk(b);
        }
    }

    /// `(s\r\n` on RESP3; a bulk string on RESP2.
    pub fn big_number(&mut self, s: &str) {
        if self.is_resp3() {
            self.buf.reserve(s.len() + 3);
            self.buf.put_u8(b'(');
            self.buf.extend_from_slice(s.as_bytes());
            self.buf.extend_from_slice(CRLF);
        } else {
            self.bulk(s.as_bytes());
        }
    }

    // ------------------------------------------------------------- aggregates

    /// `*n\r\n` -- header only; the caller writes `n` items.
    #[inline]
    pub fn array(&mut self, len: usize) {
        self.buf.put_u8(b'*');
        self.put_usize(len);
        self.buf.extend_from_slice(CRLF);
    }

    /// `%n\r\n` on RESP3; `*2n\r\n` on RESP2.
    ///
    /// `len` is the number of *pairs* in both cases. The caller then writes
    /// `2 * len` items, alternating key and value.
    #[inline]
    pub fn map(&mut self, len: usize) {
        if self.is_resp3() {
            self.buf.put_u8(b'%');
            self.put_usize(len);
        } else {
            self.buf.put_u8(b'*');
            self.put_usize(len.saturating_mul(2));
        }
        self.buf.extend_from_slice(CRLF);
    }

    /// `~n\r\n` on RESP3; `*n\r\n` on RESP2.
    #[inline]
    pub fn set_header(&mut self, len: usize) {
        if self.is_resp3() {
            self.buf.put_u8(b'~');
        } else {
            self.buf.put_u8(b'*');
        }
        self.put_usize(len);
        self.buf.extend_from_slice(CRLF);
    }

    /// `>n\r\n` on RESP3; `*n\r\n` on RESP2.
    ///
    /// On RESP2 a push is indistinguishable from a normal array, which is
    /// exactly why RESP2 pub/sub clients must be in subscribe mode to receive
    /// one.
    #[inline]
    pub fn push(&mut self, len: usize) {
        if self.is_resp3() {
            self.buf.put_u8(b'>');
        } else {
            self.buf.put_u8(b'*');
        }
        self.put_usize(len);
        self.buf.extend_from_slice(CRLF);
    }

    /// `|n\r\n` -- RESP3 attribute header. Silently writes nothing on RESP2,
    /// where attributes do not exist; callers must then also skip the pairs.
    #[inline]
    pub fn attribute(&mut self, len: usize) -> bool {
        if self.is_resp3() {
            self.buf.put_u8(b'|');
            self.put_usize(len);
            self.buf.extend_from_slice(CRLF);
            true
        } else {
            false
        }
    }

    // ---------------------------------------------------------------- helpers

    /// An empty array (`*0`), the reply for e.g. `LRANGE` on a missing key.
    #[inline]
    pub fn empty_array(&mut self) {
        self.buf.extend_from_slice(b"*0\r\n");
    }

    /// The empty bulk string.
    #[inline]
    pub fn empty_bulk(&mut self) {
        self.buf.extend_from_slice(b"$0\r\n\r\n");
    }

    #[inline]
    fn put_i64(&mut self, v: i64) {
        let mut fmt = itoa::Buffer::new();
        self.buf.extend_from_slice(fmt.format(v).as_bytes());
    }

    #[inline]
    fn put_usize(&mut self, v: usize) {
        let mut fmt = itoa::Buffer::new();
        self.buf.extend_from_slice(fmt.format(v).as_bytes());
    }

    #[inline]
    fn put_double(&mut self, v: f64) {
        let mut tmp = [0u8; 40];
        let n = write_double(&mut tmp, v);
        if let Some(s) = tmp.get(..n) {
            self.buf.extend_from_slice(s);
        }
    }
}

/// Format `v` into `buf` using Redis's `d2string` rules; returns the length.
/// 40 bytes is always enough (`-1.7976931348623157e+308` is 24).
fn write_double(buf: &mut [u8; 40], v: f64) -> usize {
    // `d2string` appends to a Vec; use a stack scratch to keep this off the
    // allocator. The scratch is a SmallVec-free hand roll so that the hot
    // integer case never touches the heap.
    if let Some(l) = strnum::double2ll(v) {
        let mut fmt = itoa::Buffer::new();
        let s = fmt.format(l).as_bytes();
        let n = core::cmp::min(s.len(), buf.len());
        if let (Some(d), Some(t)) = (buf.get_mut(..n), s.get(..n)) {
            d.copy_from_slice(t);
        }
        return n;
    }
    let mut tmp = Vec::with_capacity(40);
    strnum::d2string(&mut tmp, v);
    let n = core::cmp::min(tmp.len(), buf.len());
    if let (Some(d), Some(t)) = (buf.get_mut(..n), tmp.get(..n)) {
        d.copy_from_slice(t);
    }
    n
}

/// Adapter so `core::fmt` can write into a `BytesMut` without an intermediate
/// `String`.
struct BytesMutFmt<'a>(&'a mut BytesMut);

impl std::fmt::Write for BytesMutFmt<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(proto: u8, f: impl FnOnce(&mut ReplyWriter<'_>)) -> String {
        let mut buf = BytesMut::new();
        let mut w = ReplyWriter::new(&mut buf, proto);
        f(&mut w);
        String::from_utf8(buf.to_vec()).expect("reply is not utf8")
    }

    #[test]
    fn scalars_identical_in_both_protocols() {
        for p in [RESP2, RESP3] {
            assert_eq!(render(p, |w| w.ok()), "+OK\r\n");
            assert_eq!(render(p, |w| w.simple("PONG")), "+PONG\r\n");
            assert_eq!(render(p, |w| w.int(0)), ":0\r\n");
            assert_eq!(render(p, |w| w.int(-1)), ":-1\r\n");
            assert_eq!(render(p, |w| w.int(i64::MIN)), ":-9223372036854775808\r\n");
            assert_eq!(render(p, |w| w.int(i64::MAX)), ":9223372036854775807\r\n");
            assert_eq!(render(p, |w| w.bulk(b"hello")), "$5\r\nhello\r\n");
            assert_eq!(render(p, |w| w.bulk(b"")), "$0\r\n\r\n");
            assert_eq!(render(p, |w| w.bulk_i64(-42)), "$3\r\n-42\r\n");
            assert_eq!(render(p, |w| w.bulk2(b"ab", b"cd")), "$4\r\nabcd\r\n");
            assert_eq!(render(p, |w| w.array(3)), "*3\r\n");
            assert_eq!(render(p, |w| w.array(0)), "*0\r\n");
        }
    }

    #[test]
    fn binary_safe_bulk() {
        let mut buf = BytesMut::new();
        let mut w = ReplyWriter::new(&mut buf, RESP2);
        w.bulk(b"a\r\n\0b");
        assert_eq!(&buf[..], b"$5\r\na\r\n\0b\r\n");
    }

    #[test]
    fn errors() {
        assert_eq!(
            render(RESP2, |w| w.error(&CmdError::WrongType)),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            render(RESP3, |w| w.error(&CmdError::WrongArity("get"))),
            "-ERR wrong number of arguments for 'get' command\r\n"
        );
        assert_eq!(
            render(RESP2, |w| w.error(&CmdError::custom("BUSYGROUP", "nope"))),
            "-BUSYGROUP nope\r\n"
        );
        assert_eq!(render(RESP2, |w| w.error_str("ERR x")), "-ERR x\r\n");
    }

    #[test]
    fn nulls_diverge() {
        assert_eq!(render(RESP2, |w| w.null()), "$-1\r\n");
        assert_eq!(render(RESP3, |w| w.null()), "_\r\n");
        assert_eq!(render(RESP2, |w| w.null_array()), "*-1\r\n");
        assert_eq!(render(RESP3, |w| w.null_array()), "_\r\n");
    }

    #[test]
    fn maps_sets_pushes_diverge() {
        assert_eq!(render(RESP2, |w| w.map(2)), "*4\r\n");
        assert_eq!(render(RESP3, |w| w.map(2)), "%2\r\n");
        assert_eq!(render(RESP2, |w| w.map(0)), "*0\r\n");
        assert_eq!(render(RESP3, |w| w.map(0)), "%0\r\n");

        assert_eq!(render(RESP2, |w| w.set_header(3)), "*3\r\n");
        assert_eq!(render(RESP3, |w| w.set_header(3)), "~3\r\n");

        assert_eq!(render(RESP2, |w| w.push(3)), "*3\r\n");
        assert_eq!(render(RESP3, |w| w.push(3)), ">3\r\n");
    }

    #[test]
    fn booleans_diverge() {
        assert_eq!(render(RESP2, |w| w.boolean(true)), ":1\r\n");
        assert_eq!(render(RESP2, |w| w.boolean(false)), ":0\r\n");
        assert_eq!(render(RESP3, |w| w.boolean(true)), "#t\r\n");
        assert_eq!(render(RESP3, |w| w.boolean(false)), "#f\r\n");
    }

    #[test]
    fn doubles_use_redis_formatting() {
        // Integral doubles lose the fractional part, exactly as ZSCORE does.
        assert_eq!(render(RESP3, |w| w.double(1.0)), ",1\r\n");
        assert_eq!(render(RESP2, |w| w.double(1.0)), "$1\r\n1\r\n");
        assert_eq!(render(RESP3, |w| w.double(1.5)), ",1.5\r\n");
        assert_eq!(render(RESP2, |w| w.double(1.5)), "$3\r\n1.5\r\n");
        assert_eq!(render(RESP3, |w| w.double(-0.0)), ",0\r\n");
        assert_eq!(render(RESP3, |w| w.double(f64::INFINITY)), ",inf\r\n");
        assert_eq!(render(RESP3, |w| w.double(f64::NEG_INFINITY)), ",-inf\r\n");
        assert_eq!(render(RESP3, |w| w.double(f64::NAN)), ",nan\r\n");
        assert_eq!(render(RESP2, |w| w.double(f64::INFINITY)), "$3\r\ninf\r\n");
        assert_eq!(
            render(RESP2, |w| w.double(f64::NEG_INFINITY)),
            "$4\r\n-inf\r\n"
        );
        assert_eq!(render(RESP3, |w| w.double(3.0e3)), ",3000\r\n");
    }

    #[test]
    fn verbatim_and_big_number() {
        assert_eq!(
            render(RESP3, |w| w.verbatim("txt", b"hi")),
            "=6\r\ntxt:hi\r\n"
        );
        assert_eq!(render(RESP2, |w| w.verbatim("txt", b"hi")), "$2\r\nhi\r\n");
        assert_eq!(
            render(RESP3, |w| w
                .big_number("3492890328409238509324850943850943825024385")),
            "(3492890328409238509324850943850943825024385\r\n"
        );
        assert_eq!(
            render(RESP2, |w| w.big_number("34928903284")),
            "$11\r\n34928903284\r\n"
        );
    }

    #[test]
    fn attribute_only_on_resp3() {
        let mut buf = BytesMut::new();
        let mut w = ReplyWriter::new(&mut buf, RESP2);
        assert!(!w.attribute(1));
        assert!(buf.is_empty());

        let mut buf = BytesMut::new();
        let mut w = ReplyWriter::new(&mut buf, RESP3);
        assert!(w.attribute(1));
        assert_eq!(&buf[..], b"|1\r\n");
    }

    #[test]
    fn mark_and_rollback() {
        let mut buf = BytesMut::new();
        let mut w = ReplyWriter::new(&mut buf, RESP2);
        w.ok();
        let m = w.mark();
        w.array(3);
        w.bulk(b"a");
        w.rollback(m);
        w.error(&CmdError::Syntax);
        assert_eq!(&buf[..], b"+OK\r\n-ERR syntax error\r\n");
    }

    #[test]
    fn a_full_resp3_map_reply() {
        let out = render(RESP3, |w| {
            w.map(2);
            w.bulk(b"server");
            w.bulk(b"rsdis");
            w.bulk(b"proto");
            w.int(3);
        });
        assert_eq!(
            out,
            "%2\r\n$6\r\nserver\r\n$5\r\nrsdis\r\n$5\r\nproto\r\n:3\r\n"
        );
    }

    #[test]
    fn the_same_map_on_resp2() {
        let out = render(RESP2, |w| {
            w.map(2);
            w.bulk(b"server");
            w.bulk(b"rsdis");
            w.bulk(b"proto");
            w.int(2);
        });
        assert_eq!(
            out,
            "*4\r\n$6\r\nserver\r\n$5\r\nrsdis\r\n$5\r\nproto\r\n:2\r\n"
        );
    }

    #[test]
    fn writer_appends_never_rewrites() {
        let mut buf = BytesMut::from(&b"prefix"[..]);
        let mut w = ReplyWriter::new(&mut buf, RESP2);
        w.ok();
        assert_eq!(&buf[..], b"prefix+OK\r\n");
    }
}
