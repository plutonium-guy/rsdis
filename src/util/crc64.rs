//! CRC64 as used in the RDB file trailer.
//!
//! Owner: F0.
//!
//! Redis uses the "Jones" CRC64 variant (`src/crc64.c`):
//!
//! ```text
//! Name   : CRC-64/JONES  (Redis calls it crc64)
//! Width  : 64 bits
//! Poly   : 0xad93d23594c935a9
//! Init   : 0x0000000000000000
//! RefIn  : true
//! RefOut : true
//! XorOut : 0x0000000000000000
//! Check  : 0xe9c6d914c4b8d9ca   ("123456789")
//! ```
//!
//! Because both input and output are reflected, the table is built from the
//! bit-reversed polynomial and the register shifts right. That is exactly what
//! `crcspeed64native_init()` produces in Redis.

const POLY: u64 = 0xad93d23594c935a9;

const fn reverse_u64(mut v: u64) -> u64 {
    let mut out = 0u64;
    let mut i = 0;
    while i < 64 {
        out = (out << 1) | (v & 1);
        v >>= 1;
        i += 1;
    }
    out
}

const REFLECTED_POLY: u64 = reverse_u64(POLY);

const fn build_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u64;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ REFLECTED_POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static TABLE: [u64; 256] = build_table();

/// Incremental CRC64. `crc` is the running value (start with `0`).
///
/// This is the signature RDB wants: the checksum accumulates across every
/// buffer written to (or read from) the file.
#[inline]
pub fn crc64(mut crc: u64, buf: &[u8]) -> u64 {
    for &b in buf {
        let idx = ((crc ^ u64::from(b)) & 0xff) as usize;
        // Masked to 8 bits: always in bounds.
        let entry = TABLE[idx];
        crc = (crc >> 8) ^ entry;
    }
    crc
}

/// One-shot CRC64 of a buffer.
#[inline]
pub fn digest(buf: &[u8]) -> u64 {
    crc64(0, buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jones_check_value() {
        // The vector asserted by Redis's own `crc64Test()`.
        assert_eq!(digest(b"123456789"), 0xe9c6d914c4b8d9ca);
    }

    #[test]
    fn redis_selftest_lorem_vector() {
        // The second vector asserted by `crc64Test()`. Note that Redis hashes
        // `sizeof(li)` bytes, i.e. the trailing NUL terminator is included.
        const LOREM: &[u8] = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, \
sed do eiusmod tempor incididunt ut labore et dolore magna \
aliqua. Ut enim ad minim veniam, quis nostrud exercitation \
ullamco laboris nisi ut aliquip ex ea commodo consequat. \
Duis aute irure dolor in reprehenderit in voluptate velit \
esse cillum dolore eu fugiat nulla pariatur. Excepteur sint \
occaecat cupidatat non proident, sunt in culpa qui officia \
deserunt mollit anim id est laborum.\0";
        assert_eq!(digest(LOREM), 0xc779_4709_e696_83b3);
    }

    #[test]
    fn empty_is_zero() {
        assert_eq!(digest(b""), 0);
    }

    #[test]
    fn incremental_matches_one_shot() {
        let data: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        let one_shot = digest(&data);
        let mut crc = 0u64;
        for chunk in data.chunks(7) {
            crc = crc64(crc, chunk);
        }
        assert_eq!(crc, one_shot);
    }
}
