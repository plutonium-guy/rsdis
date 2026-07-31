//! `maxmemory` eviction.
//!
//! Owned by W1c; do not edit if you are not that agent.
//!
//! All eight policies are in scope:
//! `noeviction`, `allkeys-lru`, `allkeys-lfu`, `allkeys-random`,
//! `volatile-lru`, `volatile-lfu`, `volatile-random`, `volatile-ttl`.
//!
//! The LRU/LFU metadata the sampler needs is already on `Entry::lru`; see
//! `crate::object::lru` for the encoding and the decay/increment rules. Per-
//! shard memory accounting is mirrored on `ShardHandle::mem`.

/// Redis's default `maxmemory-samples`.
pub const DEFAULT_SAMPLES: u32 = 5;

/// Size of the eviction candidate pool Redis keeps between sweeps.
pub const EVICTION_POOL_SIZE: usize = 16;
