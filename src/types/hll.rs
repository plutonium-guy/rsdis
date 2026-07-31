//! HyperLogLog, stored inside a string object.
//!
//! Owned by W2a; do not edit if you are not that agent.
//!
//! There is no `Robj::Hll` variant and there must never be one: to Redis a
//! HyperLogLog is a `string` whose bytes happen to begin with the `HYLL`
//! magic. `TYPE` must report `string` and `GET` must return the raw registers.
//!
//! W2a implements the sparse and dense representations here, plus the
//! `hll-sparse-max-bytes` promotion rule and the bias-corrected cardinality
//! estimator, and exposes them as free functions over `StrObj`.

/// The 16-byte HyperLogLog header magic Redis writes at the start of the
/// string. Declared here so that RDB (W3a) and `DEBUG` can recognise it.
pub const HLL_MAGIC: &[u8; 4] = b"HYLL";

/// `HLL_DENSE` / `HLL_SPARSE` encoding byte, offset 4 of the header.
pub const HLL_DENSE: u8 = 0;
pub const HLL_SPARSE: u8 = 1;

/// Number of registers (2^14).
pub const HLL_REGISTERS: usize = 16384;
