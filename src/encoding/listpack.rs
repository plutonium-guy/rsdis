//! Listpack -- Redis's compact, allocation-free-ish sequence encoding.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! The on-disk layout must match `listpack.c` byte for byte, because RDB
//! (W3a) writes listpacks verbatim into the file and real Redis reads them
//! back. Header: 4-byte total-bytes LE, 2-byte num-elements LE, then entries,
//! then the 0xFF terminator.
//!
//! # Layout
//!
//! ```text
//! +--------+--------+ ... +--------+
//! | tot:u32| num:u16| ent | 0xFF   |
//! +--------+--------+ ... +--------+
//! ```
//!
//! Each entry is `<encoding+data><backlen>`, where `backlen` is a 1..5 byte
//! variable-length encoding of the length of `<encoding+data>` written so it
//! can be decoded *backwards*. That is what makes reverse traversal possible
//! without a second index.
//!
//! # Encoding ladder (`lpEncodeGetType`)
//!
//! | first byte      | meaning                       | entry bytes |
//! |-----------------|-------------------------------|-------------|
//! | `0xxxxxxx`      | 7-bit unsigned int (0..=127)  | 1           |
//! | `10xxxxxx`      | 6-bit length string (<64)     | 1 + len     |
//! | `110xxxxx`      | 13-bit signed int             | 2           |
//! | `1110xxxx`      | 12-bit length string (<4096)  | 2 + len     |
//! | `11110000`      | 32-bit length string          | 5 + len     |
//! | `11110001`      | 16-bit signed int             | 3           |
//! | `11110010`      | 24-bit signed int             | 4           |
//! | `11110011`      | 32-bit signed int             | 5           |
//! | `11110100`      | 64-bit signed int             | 9           |
//! | `11111111`      | end of listpack               | 1           |
//!
//! A string that parses as an integer under `string2ll` is stored with the
//! integer encoding, exactly as Redis does -- so `append(Str(b"123"))` reads
//! back as `Int(123)`. Callers that need the textual form use
//! [`ListpackEntry::write_to`] or [`ListpackEntry::eq_bytes`], both of which
//! are allocation-free.
//!
//! # Safety
//!
//! This module is entirely safe Rust. `unsafe` was permitted here (§6) but did
//! not pay for itself: every hot read is a bounds-checked byte load whose
//! branch is perfectly predicted, and the mutating paths are dominated by the
//! `memmove` of splicing. [`Listpack::from_bytes`] fully validates untrusted
//! input (RDB files are untrusted), so nothing downstream has to.

use smallvec::SmallVec;

use crate::util::strnum::{ByteSink, string2ll};

/// Bytes of header before the first entry: 4 (total bytes) + 2 (num elements).
pub const LP_HDR_SIZE: usize = 6;
/// End-of-listpack marker.
pub const LP_EOF: u8 = 0xFF;
/// Sentinel written into the 16-bit element counter when the real count no
/// longer fits. `len()` then falls back to a full scan, exactly as Redis does.
pub const LP_HDR_NUMELE_UNKNOWN: u16 = u16::MAX;
/// `listpack.c: LP_MAX_SAFETY_SIZE` -- refuse to grow a listpack past 1 GB.
pub const LP_MAX_SAFETY_SIZE: usize = 1 << 30;

// Encoding tags.
const ENC_6BIT_STR: u8 = 0x80;
const ENC_13BIT_INT: u8 = 0xC0;
const ENC_12BIT_STR: u8 = 0xE0;
const ENC_32BIT_STR: u8 = 0xF0;
const ENC_16BIT_INT: u8 = 0xF1;
const ENC_24BIT_INT: u8 = 0xF2;
const ENC_32BIT_INT: u8 = 0xF3;
const ENC_64BIT_INT: u8 = 0xF4;

/// One element of a listpack: either an integer or a byte string.
///
/// Borrowed, never owned -- reads off a listpack must not allocate (§5.1).
#[derive(Debug, Clone, Copy)]
pub enum ListpackEntry<'a> {
    Int(i64),
    Str(&'a [u8]),
}

/// Scratch big enough for any integer's textual form, and for most short
/// strings, without touching the allocator.
pub type EntryBuf = SmallVec<[u8; 32]>;

impl<'a> ListpackEntry<'a> {
    /// The integer payload, if this entry is integer-encoded.
    #[inline]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            ListpackEntry::Int(v) => Some(*v),
            ListpackEntry::Str(_) => None,
        }
    }

    /// The raw bytes, if this entry is string-encoded. Integer entries return
    /// `None`; use [`Self::write_to`] when you want the textual form of both.
    #[inline]
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        match self {
            ListpackEntry::Str(s) => Some(s),
            ListpackEntry::Int(_) => None,
        }
    }

    /// Interpret the entry as an integer the way a Redis command would:
    /// integer entries directly, string entries via `string2ll`.
    #[inline]
    pub fn to_int(&self) -> Option<i64> {
        match self {
            ListpackEntry::Int(v) => Some(*v),
            ListpackEntry::Str(s) => string2ll(s),
        }
    }

    /// Write the textual form into `out`. Allocation-free (§5.7): integers go
    /// through `itoa`, never `format!`.
    #[inline]
    pub fn write_to(&self, out: &mut impl ByteSink) {
        match self {
            ListpackEntry::Str(s) => out.extend_from_slice(s),
            ListpackEntry::Int(v) => {
                let mut fmt = itoa::Buffer::new();
                out.extend_from_slice(fmt.format(*v).as_bytes());
            }
        }
    }

    /// Run `f` over the textual form without allocating on the heap for
    /// integers or copying for strings.
    #[inline]
    pub fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        match self {
            ListpackEntry::Str(s) => f(s),
            ListpackEntry::Int(v) => {
                let mut fmt = itoa::Buffer::new();
                f(fmt.format(*v).as_bytes())
            }
        }
    }

    /// Textual length, without materialising the text.
    #[inline]
    pub fn byte_len(&self) -> usize {
        match self {
            ListpackEntry::Str(s) => s.len(),
            ListpackEntry::Int(v) => {
                let mut fmt = itoa::Buffer::new();
                fmt.format(*v).len()
            }
        }
    }

    /// Owned copy of the textual form. Inline for values up to 32 bytes.
    #[inline]
    pub fn to_buf(&self) -> EntryBuf {
        let mut b = EntryBuf::new();
        self.write_to(&mut b);
        b
    }

    /// Compare against a raw byte needle using Redis's rules: an int-encoded
    /// entry equals the needle when the needle's decimal text matches.
    /// Allocation-free.
    #[inline]
    pub fn eq_bytes(&self, needle: &[u8]) -> bool {
        match self {
            ListpackEntry::Str(s) => *s == needle,
            ListpackEntry::Int(v) => match string2ll(needle) {
                Some(n) => n == *v,
                None => false,
            },
        }
    }
}

impl PartialEq for ListpackEntry<'_> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ListpackEntry::Int(a), ListpackEntry::Int(b)) => a == b,
            (ListpackEntry::Str(a), ListpackEntry::Str(b)) => a == b,
            (ListpackEntry::Int(a), ListpackEntry::Str(b))
            | (ListpackEntry::Str(b), ListpackEntry::Int(a)) => string2ll(b) == Some(*a),
        }
    }
}
impl Eq for ListpackEntry<'_> {}

impl<'a> From<&'a [u8]> for ListpackEntry<'a> {
    #[inline]
    fn from(s: &'a [u8]) -> Self {
        ListpackEntry::Str(s)
    }
}
impl From<i64> for ListpackEntry<'_> {
    #[inline]
    fn from(v: i64) -> Self {
        ListpackEntry::Int(v)
    }
}

/// The canonical payload after Redis's string-looks-like-an-integer coercion.
#[derive(Debug, Clone, Copy)]
enum Payload<'a> {
    Int(i64),
    Str(&'a [u8]),
}

#[inline]
fn canonicalize<'a>(v: ListpackEntry<'a>) -> Payload<'a> {
    match v {
        ListpackEntry::Int(i) => Payload::Int(i),
        ListpackEntry::Str(s) => match string2ll(s) {
            Some(i) => Payload::Int(i),
            None => Payload::Str(s),
        },
    }
}

/// Bytes the encoding + data occupy, excluding the trailing backlen.
#[inline]
fn payload_enclen(p: &Payload<'_>) -> usize {
    match p {
        Payload::Int(v) => match *v {
            0..=127 => 1,
            -4096..=4095 => 2,
            -32768..=32767 => 3,
            -8_388_608..=8_388_607 => 4,
            -2_147_483_648..=2_147_483_647 => 5,
            _ => 9,
        },
        Payload::Str(s) => {
            let n = s.len();
            if n < 64 {
                1 + n
            } else if n < 4096 {
                2 + n
            } else {
                5 + n
            }
        }
    }
}

/// `listpack.c: lpEncodeBacklen` -- note the `<` comparisons, which are
/// Redis's own off-by-ones. They are reproduced exactly: a listpack we write
/// must be byte-identical to one Redis would write, or RDB diffs.
#[inline]
pub fn backlen_size(l: usize) -> usize {
    if l <= 127 {
        1
    } else if l < 16383 {
        2
    } else if l < 2_097_151 {
        3
    } else if l < 268_435_455 {
        4
    } else {
        5
    }
}

/// Write the backlen bytes for `l` into `dst`, returning how many were used.
/// `dst` must be at least `backlen_size(l)` long.
fn write_backlen(dst: &mut [u8], l: usize) -> usize {
    let n = backlen_size(l);
    // Big-endian 7-bit groups, continuation bit set on every byte but the
    // first, so that a backwards walk terminates on a clear high bit.
    for i in 0..n {
        let shift = 7 * (n - 1 - i);
        let mut b = ((l >> shift) & 127) as u8;
        if i != 0 {
            b |= 128;
        }
        if let Some(slot) = dst.get_mut(i) {
            *slot = b;
        }
    }
    n
}

/// `listpack.c: lpDecodeBacklen` -- walks backwards from `last`.
/// Returns `(value, bytes_consumed)`.
#[inline]
fn decode_backlen(buf: &[u8], last: usize) -> Option<(usize, usize)> {
    let mut val: usize = 0;
    let mut shift = 0u32;
    let mut p = last;
    let mut n = 0usize;
    loop {
        let b = *buf.get(p)?;
        val |= ((b & 127) as usize) << shift;
        n += 1;
        if b & 128 == 0 {
            return Some((val, n));
        }
        shift += 7;
        if shift > 28 {
            return None;
        }
        p = p.checked_sub(1)?;
    }
}

/// Encode `p` into `dst[..]`, returning the number of bytes written
/// (encoding + data + backlen).
fn write_payload(dst: &mut [u8], p: Payload<'_>) -> usize {
    let enclen = payload_enclen(&p);
    match p {
        Payload::Int(v) => match enclen {
            1 => {
                if let Some(s) = dst.get_mut(0) {
                    *s = (v as u8) & 0x7f;
                }
            }
            2 => {
                let u = if v < 0 { (1i64 << 13) + v } else { v } as u16;
                if let Some(s) = dst.get_mut(..2) {
                    s[0] = ((u >> 8) as u8) | ENC_13BIT_INT;
                    s[1] = (u & 0xff) as u8;
                }
            }
            3 => {
                let u = (v as i16) as u16;
                if let Some(s) = dst.get_mut(..3) {
                    s[0] = ENC_16BIT_INT;
                    s[1..3].copy_from_slice(&u.to_le_bytes());
                }
            }
            4 => {
                let u = (if v < 0 { (1i64 << 24) + v } else { v }) as u32;
                if let Some(s) = dst.get_mut(..4) {
                    s[0] = ENC_24BIT_INT;
                    s[1..4].copy_from_slice(&u.to_le_bytes()[..3]);
                }
            }
            5 => {
                let u = (v as i32) as u32;
                if let Some(s) = dst.get_mut(..5) {
                    s[0] = ENC_32BIT_INT;
                    s[1..5].copy_from_slice(&u.to_le_bytes());
                }
            }
            _ => {
                let u = v as u64;
                if let Some(s) = dst.get_mut(..9) {
                    s[0] = ENC_64BIT_INT;
                    s[1..9].copy_from_slice(&u.to_le_bytes());
                }
            }
        },
        Payload::Str(b) => {
            let n = b.len();
            if n < 64 {
                if let Some(s) = dst.get_mut(..1 + n) {
                    s[0] = ENC_6BIT_STR | (n as u8);
                    s[1..].copy_from_slice(b);
                }
            } else if n < 4096 {
                if let Some(s) = dst.get_mut(..2 + n) {
                    s[0] = ENC_12BIT_STR | ((n >> 8) as u8);
                    s[1] = (n & 0xff) as u8;
                    s[2..].copy_from_slice(b);
                }
            } else if let Some(s) = dst.get_mut(..5 + n) {
                s[0] = ENC_32BIT_STR;
                s[1..5].copy_from_slice(&(n as u32).to_le_bytes());
                s[5..].copy_from_slice(b);
            }
        }
    }
    let bl = backlen_size(enclen);
    if let Some(tail) = dst.get_mut(enclen..enclen + bl) {
        write_backlen(tail, enclen);
    }
    enclen + bl
}

/// Length of `encoding + data` at `off`, without materialising the entry.
///
/// This is the inner step of every forward walk -- `seek`, `next_offset`,
/// `delete_range`, `find_from`'s stride -- so it deliberately skips both the
/// payload bounds check and the `ListpackEntry` construction that
/// [`decode_at`] pays for. Reading an entry still goes through `decode_at`,
/// which validates the payload slice, and untrusted input has already been
/// through [`Listpack::validate`], so nothing here can hand out a bad slice:
/// the worst a corrupt span can do is make a subsequent `decode_at` return
/// `None`.
#[inline]
fn enclen_at(buf: &[u8], off: usize) -> Option<usize> {
    let b0 = *buf.get(off)?;
    if b0 & 0x80 == 0 {
        return Some(1);
    }
    if b0 & 0xC0 == ENC_6BIT_STR {
        return Some(1 + usize::from(b0 & 0x3f));
    }
    if b0 & 0xE0 == ENC_13BIT_INT {
        return Some(2);
    }
    if b0 & 0xF0 == ENC_12BIT_STR {
        let b1 = *buf.get(off + 1)?;
        return Some(2 + ((usize::from(b0 & 0x0f) << 8) | usize::from(b1)));
    }
    match b0 {
        ENC_16BIT_INT => Some(3),
        ENC_24BIT_INT => Some(4),
        ENC_32BIT_INT => Some(5),
        ENC_64BIT_INT => Some(9),
        ENC_32BIT_STR => {
            let h = buf.get(off + 1..off + 5)?;
            Some(5 + u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as usize)
        }
        // 0xFF (EOF) and the unused 0xF5..=0xFE tags.
        _ => None,
    }
}

/// Decode the entry starting at `off`, returning the entry and the length of
/// `encoding + data` (i.e. excluding the backlen).
#[inline]
fn decode_at(buf: &[u8], off: usize) -> Option<(ListpackEntry<'_>, usize)> {
    let b0 = *buf.get(off)?;
    if b0 & 0x80 == 0 {
        return Some((ListpackEntry::Int(i64::from(b0 & 0x7f)), 1));
    }
    if b0 & 0xC0 == ENC_6BIT_STR {
        let n = usize::from(b0 & 0x3f);
        let s = buf.get(off + 1..off + 1 + n)?;
        return Some((ListpackEntry::Str(s), 1 + n));
    }
    if b0 & 0xE0 == ENC_13BIT_INT {
        let b1 = *buf.get(off + 1)?;
        let uval = (u32::from(b0 & 0x1f) << 8) | u32::from(b1);
        // Two's complement over 13 bits.
        let v = if uval >= 1 << 12 {
            i64::from(uval) - 8192
        } else {
            i64::from(uval)
        };
        return Some((ListpackEntry::Int(v), 2));
    }
    if b0 & 0xF0 == ENC_12BIT_STR {
        let b1 = *buf.get(off + 1)?;
        let n = (usize::from(b0 & 0x0f) << 8) | usize::from(b1);
        let s = buf.get(off + 2..off + 2 + n)?;
        return Some((ListpackEntry::Str(s), 2 + n));
    }
    match b0 {
        ENC_16BIT_INT => {
            let s = buf.get(off + 1..off + 3)?;
            let v = i16::from_le_bytes([s[0], s[1]]);
            Some((ListpackEntry::Int(i64::from(v)), 3))
        }
        ENC_24BIT_INT => {
            let s = buf.get(off + 1..off + 4)?;
            // Sign-extend from bit 23 by placing the 3 bytes in the high end
            // of an i32 and arithmetic-shifting back down.
            let v = i32::from_le_bytes([0, s[0], s[1], s[2]]) >> 8;
            Some((ListpackEntry::Int(i64::from(v)), 4))
        }
        ENC_32BIT_INT => {
            let s = buf.get(off + 1..off + 5)?;
            let v = i32::from_le_bytes([s[0], s[1], s[2], s[3]]);
            Some((ListpackEntry::Int(i64::from(v)), 5))
        }
        ENC_64BIT_INT => {
            let s = buf.get(off + 1..off + 9)?;
            let v = i64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]);
            Some((ListpackEntry::Int(v), 9))
        }
        ENC_32BIT_STR => {
            let h = buf.get(off + 1..off + 5)?;
            let n = u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as usize;
            let s = buf.get(off + 5..off + 5 + n)?;
            Some((ListpackEntry::Str(s), 5 + n))
        }
        // 0xFF (EOF) and the unused 0xF5..=0xFE tags.
        _ => None,
    }
}

/// A listpack: one contiguous, RDB-ready `Vec<u8>`.
#[derive(Clone, PartialEq, Eq)]
pub struct Listpack {
    buf: Vec<u8>,
}

impl Default for Listpack {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Listpack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Listpack")
            .field("len", &self.len())
            .field("total_bytes", &self.total_bytes())
            .finish()
    }
}

impl Listpack {
    /// An empty listpack: header plus terminator, 7 bytes.
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// An empty listpack whose buffer has room for `extra` payload bytes.
    pub fn with_capacity(extra: usize) -> Self {
        let mut buf = Vec::with_capacity(LP_HDR_SIZE + 1 + extra);
        buf.extend_from_slice(&7u32.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.push(LP_EOF);
        Listpack { buf }
    }

    /// Build from an iterator of entries.
    pub fn from_entries<'a, I>(items: I) -> Self
    where
        I: IntoIterator<Item = ListpackEntry<'a>>,
    {
        let it = items.into_iter();
        let (lo, _) = it.size_hint();
        let mut lp = Listpack::with_capacity(lo * 8);
        for e in it {
            lp.append(e);
        }
        lp
    }

    /// Adopt a serialized listpack, validating it end to end first.
    ///
    /// RDB input is untrusted (§6), so this is the only way to construct a
    /// `Listpack` from bytes and it rejects anything malformed: a bad header,
    /// a truncated entry, a backlen that disagrees with the forward length, a
    /// missing terminator, or a header element count that does not match.
    pub fn from_bytes(buf: Vec<u8>) -> Option<Self> {
        if !Self::validate(&buf) {
            return None;
        }
        Some(Listpack { buf })
    }

    /// Structural check used by [`Self::from_bytes`]. Exposed so W3a can
    /// validate before deciding what to do with a bad RDB record.
    pub fn validate(buf: &[u8]) -> bool {
        if buf.len() < LP_HDR_SIZE + 1 {
            return false;
        }
        let Some(h) = buf.get(..LP_HDR_SIZE) else {
            return false;
        };
        let total = u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as usize;
        if total != buf.len() {
            return false;
        }
        if buf.last() != Some(&LP_EOF) {
            return false;
        }
        let declared = u16::from_le_bytes([h[4], h[5]]);
        let end = buf.len() - 1;
        let mut off = LP_HDR_SIZE;
        let mut count: usize = 0;
        while off < end {
            let Some((_, enclen)) = decode_at(buf, off) else {
                return false;
            };
            let bl = backlen_size(enclen);
            let next = match off.checked_add(enclen).and_then(|x| x.checked_add(bl)) {
                Some(n) => n,
                None => return false,
            };
            if next > end {
                return false;
            }
            // The backlen must round-trip, otherwise reverse traversal lies.
            match decode_backlen(buf, next - 1) {
                Some((v, n)) if v == enclen && n == bl => {}
                _ => return false,
            }
            off = next;
            count += 1;
            if count > u32::MAX as usize {
                return false;
            }
        }
        if off != end {
            return false;
        }
        if declared != LP_HDR_NUMELE_UNKNOWN && usize::from(declared) != count {
            return false;
        }
        true
    }

    /// The serialized bytes, ready to be written straight into an RDB file.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Consume the listpack, yielding its serialized bytes.
    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// Total serialized size, i.e. what the 4-byte header records.
    #[inline]
    pub fn total_bytes(&self) -> usize {
        self.buf.len()
    }

    /// Approximate heap footprint, for `MEMORY USAGE`.
    #[inline]
    pub fn mem_usage(&self) -> usize {
        core::mem::size_of::<Self>() + self.buf.capacity()
    }

    #[inline]
    fn header_count(&self) -> u16 {
        match self.buf.get(4..6) {
            Some(s) => u16::from_le_bytes([s[0], s[1]]),
            None => 0,
        }
    }

    /// Number of elements. O(1) in the normal case; O(n) only once a listpack
    /// has exceeded 65535 elements, which no configured Redis threshold
    /// allows.
    #[inline]
    pub fn len(&self) -> usize {
        let n = self.header_count();
        if n != LP_HDR_NUMELE_UNKNOWN {
            usize::from(n)
        } else {
            self.iter().count()
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.len() <= LP_HDR_SIZE + 1
    }

    /// Reset to an empty listpack, keeping the allocation.
    pub fn clear(&mut self) {
        self.buf.truncate(LP_HDR_SIZE);
        self.buf.push(LP_EOF);
        self.sync_header(0);
    }

    /// Refresh the two header fields. `delta` adjusts the element count.
    fn sync_header(&mut self, delta: i64) {
        let total = self.buf.len();
        if let Some(h) = self.buf.get_mut(..4) {
            h.copy_from_slice(&(total as u32).to_le_bytes());
        }
        let cur = self.header_count();
        if cur == LP_HDR_NUMELE_UNKNOWN {
            // Already unknown: Redis never updates it again, and `len()`
            // scans. Matching that keeps the two implementations agreeing.
            return;
        }
        let n = (i64::from(cur) + delta).clamp(0, i64::from(LP_HDR_NUMELE_UNKNOWN));
        if let Some(h) = self.buf.get_mut(4..6) {
            h.copy_from_slice(&(n as u16).to_le_bytes());
        }
    }

    /// `listpack.c: lpSafeToAdd`.
    #[inline]
    pub fn safe_to_add(&self, add: usize) -> bool {
        self.buf.len().saturating_add(add) <= LP_MAX_SAFETY_SIZE
    }

    // ---- offsets -------------------------------------------------------

    /// Byte offset of the first entry (== the terminator when empty).
    #[inline]
    pub fn first_offset(&self) -> usize {
        LP_HDR_SIZE
    }

    /// Byte offset of the terminator.
    #[inline]
    pub fn eof_offset(&self) -> usize {
        self.buf.len() - 1
    }

    /// Total bytes of the entry starting at `off`, including its backlen.
    #[inline]
    fn entry_span(&self, off: usize) -> Option<usize> {
        let enclen = enclen_at(&self.buf, off)?;
        Some(enclen + backlen_size(enclen))
    }

    /// `listpack.c: lpNext`.
    #[inline]
    pub fn next_offset(&self, off: usize) -> Option<usize> {
        if off >= self.eof_offset() {
            return None;
        }
        let n = off + self.entry_span(off)?;
        if n > self.eof_offset() { None } else { Some(n) }
    }

    /// `listpack.c: lpPrev` -- the reverse walk the backlen bytes exist for.
    #[inline]
    pub fn prev_offset(&self, off: usize) -> Option<usize> {
        if off <= LP_HDR_SIZE {
            return None;
        }
        let (prevlen, _) = decode_backlen(&self.buf, off - 1)?;
        let start = off.checked_sub(prevlen + backlen_size(prevlen))?;
        if start < LP_HDR_SIZE {
            None
        } else {
            Some(start)
        }
    }

    /// `listpack.c: lpSeek` -- byte offset of element `index`, walking from
    /// whichever end is closer. Negative indices count from the end.
    pub fn seek(&self, index: isize) -> Option<usize> {
        let numele = self.header_count();
        if numele != LP_HDR_NUMELE_UNKNOWN {
            let n = numele as isize;
            let mut i = index;
            if i < 0 {
                i += n;
            }
            if i < 0 || i >= n {
                return None;
            }
            if i > n / 2 {
                // Closer to the tail: walk backwards.
                return self.seek_back(i - n);
            }
            return self.seek_fwd(i as usize);
        }
        // Unknown count: forward for non-negative, backward otherwise.
        if index < 0 {
            self.seek_back(index)
        } else {
            self.seek_fwd(index as usize)
        }
    }

    fn seek_fwd(&self, mut n: usize) -> Option<usize> {
        let mut off = LP_HDR_SIZE;
        let eof = self.eof_offset();
        while n > 0 {
            if off >= eof {
                return None;
            }
            off += self.entry_span(off)?;
            n -= 1;
        }
        if off >= eof { None } else { Some(off) }
    }

    /// `n` is negative: -1 is the last element.
    fn seek_back(&self, mut n: isize) -> Option<usize> {
        let mut off = self.eof_offset();
        while n < 0 {
            off = self.prev_offset(off)?;
            n += 1;
        }
        Some(off)
    }

    // ---- reads ---------------------------------------------------------

    /// The entry at `off`, which must be an entry start.
    #[inline]
    pub fn entry_at(&self, off: usize) -> Option<ListpackEntry<'_>> {
        if off >= self.eof_offset() {
            return None;
        }
        decode_at(&self.buf, off).map(|(e, _)| e)
    }

    /// Element `index`, negative counting from the end. Borrowed, no
    /// allocation (§5.1).
    #[inline]
    pub fn get(&self, index: isize) -> Option<ListpackEntry<'_>> {
        let off = self.seek(index)?;
        self.entry_at(off)
    }

    #[inline]
    pub fn first(&self) -> Option<ListpackEntry<'_>> {
        self.entry_at(LP_HDR_SIZE)
    }

    #[inline]
    pub fn last(&self) -> Option<ListpackEntry<'_>> {
        let off = self.prev_offset(self.eof_offset())?;
        self.entry_at(off)
    }

    /// Forward iterator.
    #[inline]
    pub fn iter(&self) -> ListpackIter<'_> {
        ListpackIter {
            lp: self,
            off: LP_HDR_SIZE,
        }
    }

    /// Forward iterator starting at a byte offset obtained from [`Self::seek`].
    #[inline]
    pub fn iter_from(&self, off: usize) -> ListpackIter<'_> {
        ListpackIter { lp: self, off }
    }

    /// Reverse iterator.
    #[inline]
    pub fn iter_rev(&self) -> ListpackRevIter<'_> {
        ListpackRevIter {
            lp: self,
            off: self.eof_offset(),
        }
    }

    /// Index of the first entry equal to `needle`, comparing with Redis's
    /// rules (an int-encoded entry matches its decimal text).
    #[inline]
    pub fn find(&self, needle: &[u8]) -> Option<usize> {
        self.find_from(0, needle, 0)
    }

    /// `listpack.c: lpFind`. Start at element `start`, compare, then step
    /// `skip + 1` elements. `skip = 1` is the field/value stride a hash uses.
    pub fn find_from(&self, start: usize, needle: &[u8], skip: usize) -> Option<usize> {
        let mut off = self.seek_fwd(start)?;
        let eof = self.eof_offset();
        let mut idx = start;
        let canon = canonicalize(ListpackEntry::Str(needle));
        while off < eof {
            let (e, enclen) = decode_at(&self.buf, off)?;
            let hit = match (&canon, &e) {
                (Payload::Int(a), ListpackEntry::Int(b)) => a == b,
                (Payload::Str(a), ListpackEntry::Str(b)) => a == b,
                _ => false,
            };
            if hit {
                return Some(idx);
            }
            off += enclen + backlen_size(enclen);
            idx += 1;
            for _ in 0..skip {
                if off >= eof {
                    return None;
                }
                off += self.entry_span(off)?;
                idx += 1;
            }
        }
        None
    }

    // ---- writes --------------------------------------------------------

    /// Splice `insert` bytes in at `at`, dropping `remove` bytes. One memmove.
    fn splice(&mut self, at: usize, remove: usize, insert: usize) {
        let old = self.buf.len();
        let tail = at + remove;
        match insert.cmp(&remove) {
            std::cmp::Ordering::Greater => {
                self.buf.resize(old + (insert - remove), 0);
                self.buf.copy_within(tail..old, at + insert);
            }
            std::cmp::Ordering::Less => {
                self.buf.copy_within(tail..old, at + insert);
                self.buf.truncate(old - (remove - insert));
            }
            std::cmp::Ordering::Equal => {}
        }
    }

    /// Insert `v` at byte offset `off`, which must be an entry start or the
    /// terminator offset. Returns false when the listpack would exceed the
    /// 1 GB safety limit.
    fn insert_at_offset(&mut self, off: usize, v: ListpackEntry<'_>) -> bool {
        let p = canonicalize(v);
        let enclen = payload_enclen(&p);
        let total = enclen + backlen_size(enclen);
        if !self.safe_to_add(total) {
            return false;
        }
        self.splice(off, 0, total);
        if let Some(dst) = self.buf.get_mut(off..off + total) {
            write_payload(dst, p);
        }
        self.sync_header(1);
        true
    }

    /// Append to the tail.
    #[inline]
    pub fn append(&mut self, v: ListpackEntry<'_>) -> bool {
        let off = self.eof_offset();
        self.insert_at_offset(off, v)
    }

    /// Insert at the head.
    #[inline]
    pub fn prepend(&mut self, v: ListpackEntry<'_>) -> bool {
        self.insert_at_offset(LP_HDR_SIZE, v)
    }

    /// Insert before element `index`. `index == len()` appends.
    pub fn insert(&mut self, index: usize, v: ListpackEntry<'_>) -> bool {
        let off = if index >= self.len() {
            self.eof_offset()
        } else {
            match self.seek_fwd(index) {
                Some(o) => o,
                None => return false,
            }
        };
        self.insert_at_offset(off, v)
    }

    /// Overwrite element `index` in place (re-encoding as needed).
    pub fn replace(&mut self, index: usize, v: ListpackEntry<'_>) -> bool {
        let Some(off) = self.seek(index as isize) else {
            return false;
        };
        let Some(old_span) = self.entry_span(off) else {
            return false;
        };
        let p = canonicalize(v);
        let enclen = payload_enclen(&p);
        let total = enclen + backlen_size(enclen);
        if total > old_span && !self.safe_to_add(total - old_span) {
            return false;
        }
        self.splice(off, old_span, total);
        if let Some(dst) = self.buf.get_mut(off..off + total) {
            write_payload(dst, p);
        }
        self.sync_header(0);
        true
    }

    /// Delete element `index` (negative counts from the end).
    pub fn delete(&mut self, index: isize) -> bool {
        let Some(off) = self.seek(index) else {
            return false;
        };
        let Some(span) = self.entry_span(off) else {
            return false;
        };
        self.splice(off, span, 0);
        self.sync_header(-1);
        true
    }

    /// Delete `count` elements starting at `index`. Returns how many went.
    pub fn delete_range(&mut self, index: usize, count: usize) -> usize {
        if count == 0 {
            return 0;
        }
        let Some(start) = self.seek_fwd(index) else {
            return 0;
        };
        let eof = self.eof_offset();
        let mut end = start;
        let mut removed = 0usize;
        while removed < count && end < eof {
            match self.entry_span(end) {
                Some(s) => end += s,
                None => break,
            }
            removed += 1;
        }
        if removed == 0 {
            return 0;
        }
        self.splice(start, end - start, 0);
        self.sync_header(-(removed as i64));
        removed
    }

    /// Delete every element from byte offset `from` (inclusive) to `to`
    /// (exclusive), which must both be entry starts or the terminator. Used by
    /// quicklist when splitting a node.
    pub(crate) fn delete_byte_range(&mut self, from: usize, to: usize, count: usize) {
        if to <= from {
            return;
        }
        self.splice(from, to - from, 0);
        self.sync_header(-(count as i64));
    }

    /// Append every entry of `other`. Used by quicklist node merging.
    pub(crate) fn append_all(&mut self, other: &Listpack) -> bool {
        let extra = other.buf.len().saturating_sub(LP_HDR_SIZE + 1);
        if !self.safe_to_add(extra) {
            return false;
        }
        let at = self.eof_offset();
        let n = other.len();
        self.splice(at, 0, extra);
        if let (Some(dst), Some(src)) = (
            self.buf.get_mut(at..at + extra),
            other.buf.get(LP_HDR_SIZE..LP_HDR_SIZE + extra),
        ) {
            dst.copy_from_slice(src);
        }
        self.sync_header(n as i64);
        true
    }
}

impl<'a> FromIterator<ListpackEntry<'a>> for Listpack {
    fn from_iter<T: IntoIterator<Item = ListpackEntry<'a>>>(iter: T) -> Self {
        Listpack::from_entries(iter)
    }
}

/// Forward iterator over listpack entries.
#[derive(Clone)]
pub struct ListpackIter<'a> {
    lp: &'a Listpack,
    off: usize,
}

impl<'a> ListpackIter<'a> {
    /// Byte offset of the entry `next` will return.
    #[inline]
    pub fn offset(&self) -> usize {
        self.off
    }
}

impl<'a> Iterator for ListpackIter<'a> {
    type Item = ListpackEntry<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.off >= self.lp.eof_offset() {
            return None;
        }
        let (e, enclen) = decode_at(&self.lp.buf, self.off)?;
        self.off += enclen + backlen_size(enclen);
        Some(e)
    }
}

/// Reverse iterator over listpack entries.
#[derive(Clone)]
pub struct ListpackRevIter<'a> {
    lp: &'a Listpack,
    off: usize,
}

impl<'a> Iterator for ListpackRevIter<'a> {
    type Item = ListpackEntry<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let prev = self.lp.prev_offset(self.off)?;
        self.off = prev;
        self.lp.entry_at(prev)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(b: &[u8]) -> ListpackEntry<'_> {
        ListpackEntry::Str(b)
    }
    fn i(v: i64) -> ListpackEntry<'static> {
        ListpackEntry::Int(v)
    }

    #[test]
    fn empty_listpack_layout() {
        let lp = Listpack::new();
        assert_eq!(lp.as_bytes(), &[7, 0, 0, 0, 0, 0, 0xFF]);
        assert_eq!(lp.len(), 0);
        assert!(lp.is_empty());
        assert_eq!(lp.total_bytes(), 7);
        assert!(lp.first().is_none());
        assert!(lp.last().is_none());
        assert!(Listpack::validate(lp.as_bytes()));
    }

    #[test]
    fn encoding_ladder_sizes() {
        // (value, expected total_bytes over an empty listpack)
        let cases: &[(ListpackEntry<'_>, usize)] = &[
            (i(0), 7 + 2),     // 7-bit uint + 1 backlen
            (i(127), 7 + 2),   //
            (i(128), 7 + 3),   // 13-bit int
            (i(-1), 7 + 3),    // 13-bit int
            (i(4095), 7 + 3),  //
            (i(-4096), 7 + 3), //
            (i(4096), 7 + 4),  // 16-bit
            (i(32767), 7 + 4), //
            (i(32768), 7 + 5), // 24-bit
            (i(8388607), 7 + 5),
            (i(8388608), 7 + 6), // 32-bit
            (i(i32::MAX as i64), 7 + 6),
            (i(i32::MAX as i64 + 1), 7 + 10), // 64-bit
            (i(i64::MIN), 7 + 10),
        ];
        for (v, want) in cases {
            let mut lp = Listpack::new();
            assert!(lp.append(*v));
            assert_eq!(lp.total_bytes(), *want, "{v:?}");
            assert_eq!(lp.get(0), Some(*v), "{v:?}");
            assert!(Listpack::validate(lp.as_bytes()));
        }
    }

    #[test]
    fn string_encoding_boundaries() {
        for n in [0usize, 1, 63, 64, 4095, 4096] {
            let data = vec![b'x'; n];
            let mut lp = Listpack::new();
            assert!(lp.append(s(&data)));
            let overhead = if n < 64 {
                1
            } else if n < 4096 {
                2
            } else {
                5
            };
            let enclen = overhead + n;
            assert_eq!(lp.total_bytes(), 7 + enclen + backlen_size(enclen), "n={n}");
            assert_eq!(lp.get(0).and_then(|e| e.as_bytes()), Some(&data[..]));
            assert!(Listpack::validate(lp.as_bytes()));
        }
    }

    #[test]
    fn numeric_strings_become_ints() {
        let mut lp = Listpack::new();
        lp.append(s(b"123"));
        lp.append(s(b"0123")); // leading zero -> stays a string
        lp.append(s(b"-0")); // rejected by string2ll -> string
        lp.append(s(b""));
        assert_eq!(lp.get(0), Some(i(123)));
        assert_eq!(lp.get(1).and_then(|e| e.as_bytes()), Some(&b"0123"[..]));
        assert_eq!(lp.get(2).and_then(|e| e.as_bytes()), Some(&b"-0"[..]));
        assert_eq!(lp.get(3).and_then(|e| e.as_bytes()), Some(&b""[..]));
    }

    #[test]
    fn backlen_encoding_round_trips() {
        for l in [
            0usize, 1, 126, 127, 128, 16382, 16383, 16384, 2097150, 2097151, 268435454,
        ] {
            let n = backlen_size(l);
            let mut buf = vec![0u8; n];
            assert_eq!(write_backlen(&mut buf, l), n);
            let (v, got) = decode_backlen(&buf, n - 1).expect("decode");
            assert_eq!((v, got), (l, n), "l={l}");
        }
        // Redis's own off-by-one: 16383 uses three bytes, not two.
        assert_eq!(backlen_size(16382), 2);
        assert_eq!(backlen_size(16383), 3);
        assert_eq!(backlen_size(2097151), 4);
    }

    #[test]
    fn append_prepend_insert_delete() {
        let mut lp = Listpack::new();
        lp.append(s(b"b"));
        lp.append(s(b"c"));
        lp.prepend(s(b"a"));
        lp.insert(3, s(b"d"));
        lp.insert(2, s(b"x"));
        let got: Vec<_> = lp.iter().map(|e| e.to_buf().to_vec()).collect();
        assert_eq!(
            got,
            vec![
                b"a".to_vec(),
                b"b".into(),
                b"x".into(),
                b"c".into(),
                b"d".into()
            ]
        );
        assert_eq!(lp.len(), 5);
        assert!(lp.delete(2));
        assert_eq!(lp.len(), 4);
        assert!(lp.delete(-1));
        let got: Vec<_> = lp.iter().map(|e| e.to_buf().to_vec()).collect();
        assert_eq!(got, vec![b"a".to_vec(), b"b".into(), b"c".into()]);
        assert!(Listpack::validate(lp.as_bytes()));
    }

    #[test]
    fn delete_last_element_leaves_valid_empty() {
        let mut lp = Listpack::new();
        lp.append(i(42));
        assert!(lp.delete(0));
        assert!(lp.is_empty());
        assert_eq!(lp.len(), 0);
        assert_eq!(lp.as_bytes(), &[7, 0, 0, 0, 0, 0, 0xFF]);
        assert!(!lp.delete(0));
    }

    #[test]
    fn replace_grows_and_shrinks() {
        let mut lp = Listpack::new();
        lp.append(i(1));
        lp.append(i(2));
        lp.append(i(3));
        let big = [b'z'; 200];
        assert!(lp.replace(1, s(&big)));
        assert_eq!(lp.len(), 3);
        assert_eq!(
            lp.get(1).and_then(|e| e.as_bytes()).map(<[u8]>::len),
            Some(200)
        );
        assert_eq!(lp.get(2), Some(i(3)));
        assert!(lp.replace(1, i(7)));
        assert_eq!(lp.get(1), Some(i(7)));
        assert!(Listpack::validate(lp.as_bytes()));
    }

    #[test]
    fn delete_range_semantics() {
        let mut lp = Listpack::from_entries((0..10).map(ListpackEntry::Int).collect::<Vec<_>>());
        assert_eq!(lp.delete_range(3, 4), 4);
        let got: Vec<_> = lp.iter().filter_map(|e| e.as_int()).collect();
        assert_eq!(got, vec![0, 1, 2, 7, 8, 9]);
        // Over-long count clamps.
        assert_eq!(lp.delete_range(4, 100), 2);
        let got: Vec<_> = lp.iter().filter_map(|e| e.as_int()).collect();
        assert_eq!(got, vec![0, 1, 2, 7]);
        assert_eq!(lp.delete_range(9, 1), 0);
        assert_eq!(lp.delete_range(0, 0), 0);
        assert!(Listpack::validate(lp.as_bytes()));
    }

    #[test]
    fn reverse_iteration_after_deletion() {
        let mut lp = Listpack::from_entries((0..20).map(ListpackEntry::Int).collect::<Vec<_>>());
        for _ in 0..5 {
            lp.delete(0);
        }
        lp.delete(-1);
        let fwd: Vec<_> = lp.iter().filter_map(|e| e.as_int()).collect();
        let mut rev: Vec<_> = lp.iter_rev().filter_map(|e| e.as_int()).collect();
        rev.reverse();
        assert_eq!(fwd, rev);
        assert_eq!(fwd, (5..19).collect::<Vec<_>>());
    }

    #[test]
    fn seek_negative_and_bounds() {
        let lp = Listpack::from_entries((0..7).map(ListpackEntry::Int).collect::<Vec<_>>());
        for k in 0..7i64 {
            assert_eq!(lp.get(k as isize), Some(i(k)));
            assert_eq!(lp.get(k as isize - 7), Some(i(k)));
        }
        assert_eq!(lp.get(7), None);
        assert_eq!(lp.get(-8), None);
        assert_eq!(lp.first(), Some(i(0)));
        assert_eq!(lp.last(), Some(i(6)));
    }

    #[test]
    fn find_with_stride() {
        let mut lp = Listpack::new();
        for (k, v) in [("a", "1"), ("bb", "2"), ("ccc", "3")] {
            lp.append(s(k.as_bytes()));
            lp.append(s(v.as_bytes()));
        }
        assert_eq!(lp.find_from(0, b"bb", 1), Some(2));
        assert_eq!(lp.find_from(0, b"ccc", 1), Some(4));
        assert_eq!(lp.find_from(0, b"2", 1), None); // values are skipped
        assert_eq!(lp.find_from(1, b"2", 1), Some(3));
        assert_eq!(lp.find(b"3"), Some(5));
        assert_eq!(lp.find(b"nope"), None);
    }

    #[test]
    fn find_matches_int_encoded_entries_by_text() {
        let mut lp = Listpack::new();
        lp.append(i(1000));
        assert_eq!(lp.find(b"1000"), Some(0));
        assert_eq!(lp.find(b"01000"), None);
    }

    #[test]
    fn validate_rejects_corruption() {
        let mut lp = Listpack::new();
        lp.append(s(b"hello"));
        lp.append(i(70000));
        let good = lp.into_bytes();
        assert!(Listpack::validate(&good));

        let mut bad = good.clone();
        bad[0] = bad[0].wrapping_add(1); // total-bytes lies
        assert!(!Listpack::validate(&bad));

        let mut bad = good.clone();
        let n = bad.len();
        bad[n - 1] = 0x00; // no terminator
        assert!(!Listpack::validate(&bad));

        let mut bad = good.clone();
        bad[4] = 9; // element count lies
        assert!(!Listpack::validate(&bad));

        let mut bad = good.clone();
        bad[LP_HDR_SIZE + 6] = 0x00; // corrupt a backlen byte
        assert!(!Listpack::validate(&bad));

        assert!(!Listpack::validate(&[]));
        assert!(!Listpack::validate(&[7, 0, 0, 0, 0, 0]));
        assert!(Listpack::from_bytes(vec![1, 2, 3]).is_none());
        assert!(Listpack::from_bytes(good).is_some());
    }

    #[test]
    fn entry_helpers_are_allocation_shaped() {
        assert!(i(-5).eq_bytes(b"-5"));
        assert!(!i(-5).eq_bytes(b"-05"));
        assert!(s(b"abc").eq_bytes(b"abc"));
        assert_eq!(i(1234).byte_len(), 4);
        assert_eq!(s(b"abcd").byte_len(), 4);
        assert_eq!(i(-7).to_buf().as_slice(), b"-7");
        assert_eq!(i(9).to_int(), Some(9));
        assert_eq!(s(b"9").to_int(), Some(9));
        assert_eq!(s(b"x").to_int(), None);
        assert_eq!(i(1), s(b"1"));
    }

    #[test]
    fn append_all_merges() {
        let a = Listpack::from_entries(vec![i(1), i(2)]);
        let b = Listpack::from_entries(vec![i(3), s(b"four")]);
        let mut m = a.clone();
        assert!(m.append_all(&b));
        assert_eq!(m.len(), 4);
        let got: Vec<_> = m.iter().map(|e| e.to_buf().to_vec()).collect();
        assert_eq!(
            got,
            vec![b"1".to_vec(), b"2".into(), b"3".into(), b"four".into()]
        );
        assert!(Listpack::validate(m.as_bytes()));
        // Merging an empty listpack is a no-op.
        let mut m2 = a.clone();
        assert!(m2.append_all(&Listpack::new()));
        assert_eq!(m2, a);
    }
}
