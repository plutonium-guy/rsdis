//! The value object and its per-key metadata.
//!
//! Owner: F0. **FROZEN** -- `Robj`, `Entry` and `Key` are the spine of the
//! keyspace. The `*Obj` payload types are declared in `src/types/` and are
//! owned by their wave agents; the enum that wraps them never changes.
//!
//! §4.1 of the architecture places `Entry` under "Object" while §3 lists it
//! under `src/shard/mod.rs`. It is defined here (next to `Robj`, which it
//! owns) and re-exported from `crate::shard`, so both spellings resolve.

use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;

pub use crate::types::hash::HashObj;
pub use crate::types::list::ListObj;
pub use crate::types::set::SetObj;
pub use crate::types::stream::StreamObj;
pub use crate::types::string::StrObj;
pub use crate::types::zset::ZSetObj;

/// A keyspace key. Always a `Bytes` slice of a buffer that already exists
/// (§5.2), never a freshly allocated `Vec<u8>`.
pub type Key = Bytes;

/// The value stored under a key, plus everything the keyspace needs to know
/// about it.
#[derive(Debug)]
pub struct Entry {
    pub obj: Robj,
    /// Absolute expiry in milliseconds since the Unix epoch. `None` = no TTL.
    pub expire_at_ms: Option<u64>,
    /// LRU clock, or the packed LFU (16-bit decay minute + 8-bit counter),
    /// depending on `maxmemory-policy`. See [`lru`].
    pub lru: AtomicU32,
}

impl Entry {
    #[inline]
    pub fn new(obj: Robj, expire_at_ms: Option<u64>, now_ms: u64, lfu: bool) -> Self {
        Entry {
            obj,
            expire_at_ms,
            lru: AtomicU32::new(lru::initial(now_ms, lfu)),
        }
    }

    /// True when the entry is logically gone at `now_ms`. Note that a
    /// logically-expired entry may still be physically present -- see
    /// `Ctx::lookup_read` for the lazy-deletion path.
    #[inline]
    pub fn is_expired(&self, now_ms: u64) -> bool {
        matches!(self.expire_at_ms, Some(at) if at <= now_ms)
    }

    /// Remaining TTL in milliseconds, or `None` when the key is persistent.
    #[inline]
    pub fn ttl_ms(&self, now_ms: u64) -> Option<u64> {
        self.expire_at_ms.map(|at| at.saturating_sub(now_ms))
    }
}

/// The six Redis value types.
///
/// `Str` also backs bitmaps and HyperLogLogs; `ZSet` also backs geo. There is
/// deliberately no `Bitmap`/`Hll`/`Geo` variant -- those are interpretations of
/// an existing type, exactly as in Redis, and `TYPE` must report the
/// underlying type.
#[derive(Debug)]
pub enum Robj {
    Str(StrObj),
    List(ListObj),
    Hash(HashObj),
    Set(SetObj),
    ZSet(ZSetObj),
    Stream(StreamObj),
}

impl Robj {
    /// What `TYPE` replies.
    #[inline]
    pub fn type_name(&self) -> &'static str {
        match self {
            Robj::Str(_) => "string",
            Robj::List(_) => "list",
            Robj::Hash(_) => "hash",
            Robj::Set(_) => "set",
            Robj::ZSet(_) => "zset",
            Robj::Stream(_) => "stream",
        }
    }

    /// What `OBJECT ENCODING` replies. The test suites of real clients assert
    /// on these, so they must track the actual in-memory representation.
    #[inline]
    pub fn encoding(&self) -> &'static str {
        match self {
            Robj::Str(o) => o.encoding(),
            Robj::List(o) => o.encoding(),
            Robj::Hash(o) => o.encoding(),
            Robj::Set(o) => o.encoding(),
            Robj::ZSet(o) => o.encoding(),
            Robj::Stream(o) => o.encoding(),
        }
    }

    /// Approximate heap footprint in bytes, for `MEMORY USAGE` and for
    /// `maxmemory` accounting. Includes the object's own inline size.
    #[inline]
    pub fn mem_usage(&self) -> usize {
        match self {
            Robj::Str(o) => o.mem_usage(),
            Robj::List(o) => o.mem_usage(),
            Robj::Hash(o) => o.mem_usage(),
            Robj::Set(o) => o.mem_usage(),
            Robj::ZSet(o) => o.mem_usage(),
            Robj::Stream(o) => o.mem_usage(),
        }
    }

    /// A stable numeric discriminant, matching Redis's `OBJ_*` constants. Used
    /// by RDB (W3a) and by `SCAN TYPE`.
    #[inline]
    pub fn type_id(&self) -> u8 {
        match self {
            Robj::Str(_) => 0,
            Robj::List(_) => 1,
            Robj::Set(_) => 2,
            Robj::ZSet(_) => 3,
            Robj::Hash(_) => 4,
            Robj::Stream(_) => 6,
        }
    }

    /// True when the collection holds no elements and therefore the key must
    /// be deleted (Redis never keeps an empty aggregate key).
    #[inline]
    pub fn is_empty_collection(&self) -> bool {
        match self {
            Robj::Str(_) => false,
            Robj::List(o) => o.is_empty(),
            Robj::Hash(o) => o.is_empty(),
            Robj::Set(o) => o.is_empty(),
            Robj::ZSet(o) => o.is_empty(),
            // A stream survives becoming empty: consumer groups live on it.
            Robj::Stream(_) => false,
        }
    }
}

/// LRU / LFU bookkeeping, packed into the single `AtomicU32` on [`Entry`].
///
/// Both encodings live in 24 bits, exactly as in Redis:
///
/// * **LRU** -- a coarse clock in seconds, wrapping at 2^24 (194 days).
/// * **LFU** -- 16 bits of "last decrement time" in minutes, then 8 bits of a
///   logarithmic access counter.
pub mod lru {
    use super::{AtomicU32, Ordering};

    pub const LRU_BITS: u32 = 24;
    pub const LRU_CLOCK_MAX: u32 = (1 << LRU_BITS) - 1;
    /// A new key starts at 5 so that it survives the first eviction sweep.
    pub const LFU_INIT_VAL: u32 = 5;

    /// The coarse LRU clock: seconds since the epoch, truncated to 24 bits.
    #[inline]
    pub fn clock(now_ms: u64) -> u32 {
        ((now_ms / 1000) as u32) & LRU_CLOCK_MAX
    }

    /// Minutes since the epoch, truncated to 16 bits (the LFU decay clock).
    #[inline]
    pub fn lfu_clock(now_ms: u64) -> u32 {
        ((now_ms / 60_000) as u32) & 0xffff
    }

    /// Value for a freshly created key.
    #[inline]
    pub fn initial(now_ms: u64, lfu: bool) -> u32 {
        if lfu {
            (lfu_clock(now_ms) << 8) | LFU_INIT_VAL
        } else {
            clock(now_ms)
        }
    }

    /// Idle time in milliseconds, for `OBJECT IDLETIME` and LRU eviction.
    #[inline]
    pub fn idle_ms(value: u32, now_ms: u64) -> u64 {
        let now = clock(now_ms);
        let then = value & LRU_CLOCK_MAX;
        let secs = if now >= then {
            u64::from(now - then)
        } else {
            u64::from(LRU_CLOCK_MAX - then + now + 1)
        };
        secs * 1000
    }

    /// The 8-bit LFU counter, for `OBJECT FREQ`.
    #[inline]
    pub fn lfu_counter(value: u32) -> u8 {
        (value & 0xff) as u8
    }

    /// Redis's `LFUDecrAndReturn` + `LFULogIncr`, fused.
    ///
    /// The counter saturates at 255 and increments with probability
    /// `1 / (baseval * lfu_log_factor + 1)`, so it behaves like a logarithm of
    /// the access count rather than a raw count.
    #[inline]
    fn lfu_bump(value: u32, now_ms: u64, log_factor: u32, decay_minutes: u32) -> u32 {
        let ldt = (value >> 8) & 0xffff;
        let mut counter = value & 0xff;
        let now = lfu_clock(now_ms);

        // Decay first.
        let elapsed_min = if now >= ldt {
            now - ldt
        } else {
            65535 - ldt + now
        };
        if decay_minutes > 0 {
            let periods = elapsed_min / decay_minutes;
            counter = counter.saturating_sub(periods);
        }

        // Then increment, logarithmically.
        if counter < 255 {
            let baseval = counter.saturating_sub(LFU_INIT_VAL);
            let p = 1.0 / (f64::from(baseval) * f64::from(log_factor) + 1.0);
            if crate::util::rand::unit() < p {
                counter += 1;
            }
        }
        (now << 8) | counter
    }

    /// Called on every key access. `Relaxed` is deliberate: this is a
    /// statistical hint, and the shard lock already orders the access.
    #[inline]
    pub fn touch(cell: &AtomicU32, now_ms: u64, lfu: bool, log_factor: u32, decay_minutes: u32) {
        if lfu {
            let old = cell.load(Ordering::Relaxed);
            cell.store(
                lfu_bump(old, now_ms, log_factor, decay_minutes),
                Ordering::Relaxed,
            );
        } else {
            cell.store(clock(now_ms), Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names_match_redis() {
        assert_eq!(Robj::Str(StrObj::default()).type_name(), "string");
        assert_eq!(Robj::List(ListObj::default()).type_name(), "list");
        assert_eq!(Robj::Hash(HashObj::default()).type_name(), "hash");
        assert_eq!(Robj::Set(SetObj::default()).type_name(), "set");
        assert_eq!(Robj::ZSet(ZSetObj::default()).type_name(), "zset");
        assert_eq!(Robj::Stream(StreamObj::default()).type_name(), "stream");
    }

    #[test]
    fn type_ids_match_redis_obj_constants() {
        assert_eq!(Robj::Str(StrObj::default()).type_id(), 0);
        assert_eq!(Robj::List(ListObj::default()).type_id(), 1);
        assert_eq!(Robj::Set(SetObj::default()).type_id(), 2);
        assert_eq!(Robj::ZSet(ZSetObj::default()).type_id(), 3);
        assert_eq!(Robj::Hash(HashObj::default()).type_id(), 4);
        assert_eq!(Robj::Stream(StreamObj::default()).type_id(), 6);
    }

    #[test]
    fn expiry_logic() {
        let e = Entry::new(Robj::Str(StrObj::default()), Some(1_000), 0, false);
        assert!(!e.is_expired(999));
        assert!(e.is_expired(1_000));
        assert!(e.is_expired(1_001));
        assert_eq!(e.ttl_ms(400), Some(600));
        assert_eq!(e.ttl_ms(5_000), Some(0));

        let p = Entry::new(Robj::Str(StrObj::default()), None, 0, false);
        assert!(!p.is_expired(u64::MAX));
        assert_eq!(p.ttl_ms(0), None);
    }

    #[test]
    fn lru_clock_and_idle() {
        let v = lru::initial(10_000, false);
        assert_eq!(v, 10);
        assert_eq!(lru::idle_ms(v, 10_000), 0);
        assert_eq!(lru::idle_ms(v, 70_000), 60_000);
    }

    #[test]
    fn lfu_initial_and_counter() {
        let v = lru::initial(120_000, true);
        assert_eq!(lru::lfu_counter(v), 5);
        assert_eq!(v >> 8, 2); // 2 minutes
    }

    #[test]
    fn lfu_saturates_and_decays() {
        let cell = AtomicU32::new(lru::initial(0, true));
        for _ in 0..100_000 {
            lru::touch(&cell, 0, true, 10, 1);
        }
        let hot = lru::lfu_counter(cell.load(Ordering::Relaxed));
        assert!(hot > 100, "counter should climb, got {hot}");

        // 10000 minutes later everything has decayed away.
        lru::touch(&cell, 10_000 * 60_000, true, 10, 1);
        assert!(lru::lfu_counter(cell.load(Ordering::Relaxed)) <= 1);
    }
}
