//! Randomness helpers.
//!
//! Owner: F0.
//!
//! Everything goes through `fastrand`'s thread-local generator, which is a
//! per-thread WyRand: no locks, no syscalls, and good enough for LFU sampling,
//! eviction sampling, `SPOP`/`SRANDMEMBER` and run IDs. It is explicitly *not*
//! a CSPRNG; nothing security-relevant may use it.

/// Uniform `u64`.
#[inline]
pub fn u64_() -> u64 {
    fastrand::u64(..)
}

/// Uniform `u32`.
#[inline]
pub fn u32_() -> u32 {
    fastrand::u32(..)
}

/// Uniform value in `0..n`. Returns 0 when `n == 0`.
#[inline]
pub fn below(n: usize) -> usize {
    if n == 0 { 0 } else { fastrand::usize(..n) }
}

/// Uniform `f64` in `[0, 1)`.
#[inline]
pub fn unit() -> f64 {
    fastrand::f64()
}

/// A biased coin: true with probability `p`.
#[inline]
pub fn chance(p: f64) -> bool {
    unit() < p
}

/// Fill `buf` with random bytes.
pub fn fill(buf: &mut [u8]) {
    let mut chunks = buf.chunks_exact_mut(8);
    for c in chunks.by_ref() {
        c.copy_from_slice(&u64_().to_le_bytes());
    }
    let rest = chunks.into_remainder();
    if !rest.is_empty() {
        let r = u64_().to_le_bytes();
        for (dst, src) in rest.iter_mut().zip(r.iter()) {
            *dst = *src;
        }
    }
}

/// A lowercase hex string of `n` characters, the shape Redis uses for
/// `run_id`, `replid` and `CLIENT ID`-adjacent identifiers.
pub fn hex_string(n: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(n);
    for _ in 0..n {
        let nibble = (u32_() & 0xf) as usize;
        s.push(char::from(HEX.get(nibble).copied().unwrap_or(b'0')));
    }
    s
}

/// Redis's 40-character run ID.
#[inline]
pub fn run_id() -> String {
    hex_string(40)
}

/// Seed the thread-local generator. Only tests should need this.
pub fn seed(v: u64) {
    fastrand::seed(v);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn below_is_in_range() {
        for n in [1usize, 2, 7, 1000] {
            for _ in 0..1000 {
                assert!(below(n) < n);
            }
        }
        assert_eq!(below(0), 0);
    }

    #[test]
    fn fill_touches_every_byte() {
        // With 4096 bytes, the probability of an all-zero buffer is nil.
        let mut buf = [0u8; 4096];
        fill(&mut buf);
        assert!(buf.iter().any(|&b| b != 0));
        let mut odd = [0u8; 13];
        fill(&mut odd);
        assert!(odd.iter().any(|&b| b != 0));
    }

    #[test]
    fn run_id_shape() {
        let id = run_id();
        assert_eq!(id.len(), 40);
        assert!(
            id.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
    }
}
