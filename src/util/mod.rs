//! Low-level helpers shared by every layer of the server.
//!
//! Ownership: F0 (frozen foundation) owns `crc16`, `crc64`, `lzf`, `strnum`
//! and `rand`. `geohash` is owned by W2c.

pub mod crc16;
pub mod crc64;
pub mod geohash;
pub mod lzf;
pub mod rand;
pub mod strnum;

use hashbrown::{HashMap, HashSet};

/// The project's canonical hash builder. §5.3: SipHash is banned internally.
pub type FoldBuildHasher = foldhash::fast::RandomState;

/// `hashbrown::HashMap` with foldhash. Never use `std::collections::HashMap`
/// on a hot path.
pub type FoldMap<K, V> = HashMap<K, V, FoldBuildHasher>;

/// `hashbrown::HashSet` with foldhash.
pub type FoldSet<T> = HashSet<T, FoldBuildHasher>;

/// Construct an empty [`FoldMap`].
#[inline]
pub fn fold_map<K, V>() -> FoldMap<K, V> {
    FoldMap::with_hasher(FoldBuildHasher::default())
}

/// Construct a [`FoldMap`] with capacity for `n` entries.
#[inline]
pub fn fold_map_with_capacity<K, V>(n: usize) -> FoldMap<K, V> {
    FoldMap::with_capacity_and_hasher(n, FoldBuildHasher::default())
}

/// Construct an empty [`FoldSet`].
#[inline]
pub fn fold_set<T>() -> FoldSet<T> {
    FoldSet::with_hasher(FoldBuildHasher::default())
}

/// ASCII-case-insensitive byte comparison. Command keywords are matched with
/// this; it never allocates and never touches the locale.
#[inline]
pub fn eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}
