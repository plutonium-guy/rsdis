//! CRC16 as used by Redis Cluster for key -> slot mapping.
//!
//! Owner: F0.
//!
//! This is CRC-16/XMODEM (a.k.a. CRC-16/CCITT, "ZMODEM", "ACORN"):
//!
//! ```text
//! Name   : XMODEM
//! Width  : 16 bits
//! Poly   : 0x1021
//! Init   : 0x0000
//! RefIn  : false
//! RefOut : false
//! XorOut : 0x0000
//! Check  : 0x31C3   ("123456789")
//! ```
//!
//! Redis ships the 256-entry table verbatim in `src/crc16.c`. We generate the
//! identical table in a `const fn` instead of transcribing 256 literals, which
//! removes a whole class of copy errors; the unit tests pin it to the published
//! check value and to real `CLUSTER KEYSLOT` outputs.

/// Number of hash slots in a Redis Cluster keyspace.
pub const SLOT_COUNT: u16 = 16384;

const fn build_table() -> [u16; 256] {
    let mut table = [0u16; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static TABLE: [u16; 256] = build_table();

/// CRC-16/XMODEM over `buf`.
#[inline]
pub fn crc16(buf: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in buf {
        let idx = (((crc >> 8) ^ u16::from(b)) & 0xff) as usize;
        // The index is masked to 8 bits, so it is always in bounds.
        let entry = TABLE[idx];
        crc = (crc << 8) ^ entry;
    }
    crc
}

/// Redis Cluster `keyHashSlot()`: CRC16 of the hash tag if the key contains a
/// non-empty `{...}` section, otherwise CRC16 of the whole key, masked to 14
/// bits.
///
/// Mirrors `cluster.c:keyHashSlot()` exactly, including the two degenerate
/// cases (`{` with no closing `}`, and the empty tag `{}`) which both fall back
/// to hashing the entire key.
#[inline]
pub fn key_hash_slot(key: &[u8]) -> u16 {
    let open = match memchr::memchr(b'{', key) {
        Some(s) => s,
        None => return crc16(key) & (SLOT_COUNT - 1),
    };
    // Search for '}' strictly after '{'.
    let rest = match key.get(open + 1..) {
        Some(r) => r,
        None => return crc16(key) & (SLOT_COUNT - 1),
    };
    match memchr::memchr(b'}', rest) {
        // `{}` -- empty tag, hash the whole key.
        Some(0) | None => crc16(key) & (SLOT_COUNT - 1),
        Some(rel) => {
            let tag = rest.get(..rel).unwrap_or(key);
            crc16(tag) & (SLOT_COUNT - 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xmodem_check_value() {
        // The canonical CRC-16/XMODEM check value.
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }

    #[test]
    fn empty_input() {
        assert_eq!(crc16(b""), 0);
    }

    #[test]
    fn redis_cluster_keyslot_vectors() {
        // Verified against `redis-cli CLUSTER KEYSLOT` on Redis 7.4.
        assert_eq!(key_hash_slot(b"foo"), 12182);
        assert_eq!(key_hash_slot(b"bar"), 5061);
        assert_eq!(key_hash_slot(b"hello"), 866);
        assert_eq!(key_hash_slot(b"key:1"), 6657);
        assert_eq!(key_hash_slot(b""), 0);
        assert_eq!(key_hash_slot(b"123456789"), 0x31C3 & 16383);
    }

    #[test]
    fn hash_tags() {
        assert_eq!(key_hash_slot(b"{foo}"), key_hash_slot(b"foo"));
        assert_eq!(key_hash_slot(b"user:{foo}:name"), key_hash_slot(b"foo"));
        assert_eq!(key_hash_slot(b"{foo}{bar}"), key_hash_slot(b"foo"));
        // Empty tag: hash the whole key.
        assert_eq!(key_hash_slot(b"{}foo"), crc16(b"{}foo") & 16383);
        // Unterminated tag: hash the whole key.
        assert_eq!(key_hash_slot(b"{foo"), crc16(b"{foo") & 16383);
        // '}' before '{': the '{' scan wins, so the tag is "b".
        assert_eq!(key_hash_slot(b"}a{b}"), key_hash_slot(b"b"));
    }

    #[test]
    fn slots_are_in_range() {
        for i in 0..2000u32 {
            let k = format!("key:{i}");
            assert!(key_hash_slot(k.as_bytes()) < SLOT_COUNT);
        }
    }
}
