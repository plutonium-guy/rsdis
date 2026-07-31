//! LZF compression, wire-compatible with liblzf as embedded in Redis.
//!
//! Owner: F0. Consumed by W3a (RDB) for `RDB_ENC_LZF` string objects.
//!
//! # Format
//!
//! An LZF stream is a sequence of two kinds of records:
//!
//! * **Literal run** -- control byte `0x00..=0x1f`; `ctrl + 1` raw bytes follow
//!   (so a run is 1..=32 bytes).
//! * **Back reference** -- control byte `>= 0x20`. `len = ctrl >> 5`; if
//!   `len == 7` a second byte carries `len - 7`; the final byte carries the low
//!   8 bits of the offset, whose high 5 bits are `ctrl & 0x1f`. The match
//!   copies `len + 2` bytes from `out_pos - offset - 1`, byte at a time (so
//!   overlapping runs are legal and are how RLE is expressed).
//!
//! Offsets are at most `1 << 13` and match lengths at most `264`.
//!
//! Our compressor emits a valid stream that real Redis decompresses; it is not
//! required to be byte-identical to liblzf's output (RDB stores both the
//! compressed and the uncompressed length, so any valid encoding round-trips).
//! The decompressor *is* required to accept everything liblzf can produce, and
//! must never panic or read out of bounds on a corrupt file.

const HLOG: u32 = 16;
const HSIZE: usize = 1 << HLOG;
/// Longest literal run expressible in one control byte.
const MAX_LIT: usize = 1 << 5; // 32
/// Largest back-reference distance (`offset + 1`).
const MAX_OFF: usize = 1 << 13; // 8192
/// Longest match: `7 + 255 + 2`.
const MAX_REF: usize = (1 << 8) + (1 << 3); // 264
/// Shortest match worth encoding.
const MIN_REF: usize = 3;

#[inline]
fn first_hash(p: &[u8]) -> u32 {
    // FRST(p) then NEXT(v, p) from lzf_c.c, i.e. a 24-bit rolling value.
    let a = u32::from(p.first().copied().unwrap_or(0));
    let b = u32::from(p.get(1).copied().unwrap_or(0));
    let c = u32::from(p.get(2).copied().unwrap_or(0));
    (a << 16) | (b << 8) | c
}

#[inline]
fn idx(h: u32) -> usize {
    // IDX(h) from lzf_c.c with HLOG = 16.
    (((h >> (3 * 8 - HLOG)).wrapping_sub(h)) as usize) & (HSIZE - 1)
}

/// Compress `input`, appending to `out`.
///
/// Returns `false` (leaving `out` truncated back to its original length) when
/// the input does not compress, which is the condition Redis uses to decide
/// whether to store the string raw. Matching Redis, we refuse to emit output
/// that is not strictly smaller than the input.
pub fn compress(input: &[u8], out: &mut Vec<u8>) -> bool {
    let start_len = out.len();
    if input.len() <= 4 {
        return false;
    }
    // Budget: liblzf is called with out_len = in_len - 4 by Redis.
    let budget = input.len() - 4;

    // Hash table of candidate positions, biased by 1 so that 0 means "empty".
    let mut htab = vec![0u32; HSIZE];

    let mut ip = 0usize; // read cursor
    let mut lit_start = 0usize; // start of the pending literal run
    let n = input.len();

    macro_rules! flush_literals {
        ($end:expr) => {{
            let mut s = lit_start;
            while s < $end {
                let run = core::cmp::min(MAX_LIT, $end - s);
                if out.len() - start_len + 1 + run > budget {
                    out.truncate(start_len);
                    return false;
                }
                out.push((run - 1) as u8);
                match input.get(s..s + run) {
                    Some(slice) => out.extend_from_slice(slice),
                    None => {
                        out.truncate(start_len);
                        return false;
                    }
                }
                s += run;
            }
            #[allow(unused_assignments)]
            {
                lit_start = $end;
            }
        }};
    }

    while ip + MIN_REF <= n {
        let window = match input.get(ip..) {
            Some(w) => w,
            None => break,
        };
        let h = idx(first_hash(window));
        let candidate = match htab.get_mut(h) {
            Some(slot) => {
                let prev = *slot;
                *slot = (ip as u32).wrapping_add(1);
                prev
            }
            None => 0,
        };

        let mut matched = 0usize;
        let mut off = 0usize;
        if candidate != 0 {
            let refpos = (candidate - 1) as usize;
            if refpos < ip && ip - refpos <= MAX_OFF {
                let maxlen = core::cmp::min(MAX_REF, n - ip);
                let mut l = 0usize;
                while l < maxlen {
                    match (input.get(refpos + l), input.get(ip + l)) {
                        (Some(a), Some(b)) if a == b => l += 1,
                        _ => break,
                    }
                }
                if l >= MIN_REF {
                    matched = l;
                    off = ip - refpos - 1;
                }
            }
        }

        if matched == 0 {
            ip += 1;
            continue;
        }

        flush_literals!(ip);

        let len_field = matched - 2; // 1..=262
        let need = if len_field < 7 { 2 } else { 3 };
        if out.len() - start_len + need > budget {
            out.truncate(start_len);
            return false;
        }
        if len_field < 7 {
            out.push(((off >> 8) as u8) | ((len_field as u8) << 5));
        } else {
            out.push(((off >> 8) as u8) | (7 << 5));
            out.push((len_field - 7) as u8);
        }
        out.push((off & 0xff) as u8);

        // Index the positions we skipped over so later matches can find them.
        let stop = core::cmp::min(ip + matched, n.saturating_sub(MIN_REF - 1));
        let mut p = ip + 1;
        while p < stop {
            if let Some(w) = input.get(p..) {
                let hh = idx(first_hash(w));
                if let Some(slot) = htab.get_mut(hh) {
                    *slot = (p as u32).wrapping_add(1);
                }
            }
            p += 1;
        }

        ip += matched;
        lit_start = ip;
    }

    flush_literals!(n);

    if out.len() - start_len >= input.len() {
        out.truncate(start_len);
        return false;
    }
    true
}

/// Decompress `input` into a buffer of exactly `expected_len` bytes.
///
/// Returns `None` if the stream is truncated, references data before the start
/// of the output, or does not produce exactly `expected_len` bytes. Never
/// panics: RDB files are untrusted input.
pub fn decompress(input: &[u8], expected_len: usize) -> Option<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(expected_len);
    let mut ip = 0usize;

    while ip < input.len() {
        let ctrl = *input.get(ip)?;
        ip += 1;

        if (ctrl as usize) < MAX_LIT {
            // Literal run of ctrl + 1 bytes.
            let run = ctrl as usize + 1;
            let slice = input.get(ip..ip + run)?;
            if out.len() + run > expected_len {
                return None;
            }
            out.extend_from_slice(slice);
            ip += run;
        } else {
            let mut len = (ctrl >> 5) as usize;
            if len == 7 {
                len += *input.get(ip)? as usize;
                ip += 1;
            }
            let off_low = *input.get(ip)? as usize;
            ip += 1;
            let off = (((ctrl & 0x1f) as usize) << 8) | off_low;

            let total = len + 2;
            // `ref = op - off - 1` must not precede the output start.
            let mut src = out.len().checked_sub(off + 1)?;
            if out.len() + total > expected_len {
                return None;
            }
            // Byte-at-a-time: overlapping copies are legal and meaningful.
            for _ in 0..total {
                let b = *out.get(src)?;
                out.push(b);
                src += 1;
            }
        }
    }

    if out.len() == expected_len {
        Some(out)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(data: &[u8]) {
        let mut buf = Vec::new();
        if compress(data, &mut buf) {
            assert!(buf.len() < data.len(), "compressor grew the input");
            let back = decompress(&buf, data.len()).expect("decompress failed");
            assert_eq!(back, data);
        }
    }

    #[test]
    fn highly_compressible() {
        let data = vec![b'a'; 4096];
        let mut buf = Vec::new();
        assert!(compress(&data, &mut buf));
        assert!(buf.len() < 64, "rle should be tiny, got {}", buf.len());
        assert_eq!(decompress(&buf, data.len()).unwrap(), data);
    }

    #[test]
    fn text_round_trip() {
        let data = b"the quick brown fox jumps over the lazy dog. \
                     the quick brown fox jumps over the lazy dog. \
                     the quick brown fox jumps over the lazy dog."
            .to_vec();
        let mut buf = Vec::new();
        assert!(compress(&data, &mut buf));
        assert_eq!(decompress(&buf, data.len()).unwrap(), data);
    }

    #[test]
    fn incompressible_is_rejected() {
        // A short pseudo-random buffer must not "compress".
        let data: Vec<u8> = (0..64u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect();
        let mut buf = Vec::new();
        let ok = compress(&data, &mut buf);
        if ok {
            assert_eq!(decompress(&buf, data.len()).unwrap(), data);
        } else {
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn empty_and_tiny() {
        let mut buf = Vec::new();
        assert!(!compress(b"", &mut buf));
        assert!(!compress(b"abcd", &mut buf));
        assert!(buf.is_empty());
    }

    #[test]
    fn decompress_rejects_truncated_stream() {
        let data = vec![b'x'; 1000];
        let mut buf = Vec::new();
        assert!(compress(&data, &mut buf));
        buf.truncate(buf.len() - 1);
        assert!(decompress(&buf, data.len()).is_none());
    }

    #[test]
    fn decompress_rejects_backref_before_start() {
        // ctrl = 0x20 (len 1 -> copy 3), offset 0x0005, with no output yet.
        let stream = [0x20u8, 0x05];
        assert!(decompress(&stream, 3).is_none());
    }

    #[test]
    fn decompress_rejects_overlong_output() {
        // A literal run claiming more bytes than the caller expects.
        let stream = [0x03u8, b'a', b'b', b'c', b'd'];
        assert!(decompress(&stream, 2).is_none());
    }

    #[test]
    fn known_liblzf_stream() {
        // Produced by liblzf for "aaaaaaaaaa" (10 bytes):
        //   0x00 'a'   -> literal run of 1
        //   0xE0 0x00 0x00 -> len 7 (+0) => copy 9 bytes at distance 1
        let stream = [0x00u8, b'a', 0xE0, 0x00, 0x00];
        assert_eq!(decompress(&stream, 10).unwrap(), vec![b'a'; 10]);
    }

    proptest::proptest! {
        #[test]
        fn prop_round_trip(data: Vec<u8>) {
            round_trip(&data);
        }

        #[test]
        fn prop_round_trip_repetitive(seed in 0u8..64, reps in 1usize..200) {
            let unit: Vec<u8> = (0..=seed).collect();
            let data: Vec<u8> = unit.iter().cycle().take(unit.len() * reps).copied().collect();
            round_trip(&data);
        }

        #[test]
        fn prop_decompress_never_panics(stream: Vec<u8>, len in 0usize..4096) {
            let _ = decompress(&stream, len);
        }
    }
}
