//! `maxmemory` eviction.
//!
//! Owned by W1c; do not edit if you are not that agent.
//!
//! All eight policies are implemented: `noeviction`, `allkeys-lru`,
//! `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lfu`,
//! `volatile-random`, `volatile-ttl`.
//!
//! # Approximated LRU
//!
//! Like Redis, this does **not** keep a true LRU list. Maintaining exact
//! recency ordering costs a doubly-linked list pointer pair per key plus a
//! list splice on every read -- more than the eviction quality is worth. What
//! happens instead is Redis's sampled-pool algorithm: draw
//! `maxmemory-samples` (default 5, [`DEFAULT_SAMPLES`]) candidates, score them,
//! keep the best [`EVICTION_POOL_SIZE`] in a pool, and evict from the top of
//! the pool. The scoring metadata is already packed into `Entry::lru` by
//! `crate::object::lru`; nothing is reinvented here.
//!
//! Deviation from Redis, stated deliberately: Redis's pool persists between
//! calls, so a candidate sampled during one eviction can still be chosen
//! during the next. Here the pool is per sweep. That costs a little quality
//! under trickle pressure and buys a much simpler concurrency story -- the
//! pool never has to be invalidated when another thread deletes a key that is
//! sitting in it.
//!
//! # What the memory accounting does and does not count
//!
//! [`entry_cost`] is an **estimate** built from `Robj::mem_usage`, which is
//! itself documented as approximate. It counts:
//!
//! * the dict slot (`Key` + `Entry`, i.e. the value inline, the TTL and the
//!   LRU word) plus [`DICT_ENTRY_OVERHEAD`] for hashbrown's control byte and
//!   load-factor slack;
//! * the key bytes;
//! * whatever `Robj::mem_usage` reports beyond the inline `Robj`, which for a
//!   string is the payload and for the aggregate types is whatever W2 makes it;
//! * a second slot in the expiry index when the key has a TTL.
//!
//! It does **not** count: allocator bookkeeping and size-class rounding, the
//! `watch` map, per-connection input/output buffers, the AOF and replication
//! buffers, the command table, or anything outside the keyspace. Real Redis
//! measures `zmalloc_used_memory()`, which is the whole process; a fair
//! comparison of `maxmemory` numbers between the two is therefore not
//! meaningful, and `maxmemory` here should be read as "bytes of keyspace".
//!
//! # How the estimate is maintained (contract gap)
//!
//! `ShardHandle::mem` exists for this and is documented as W1c's, but nothing
//! on the write path maintains it: `Ctx::insert`/`Ctx::remove` and
//! `ShardGuards::release`/`sync_stats` are frozen and do not touch it. The
//! estimate is therefore refreshed by sampling from the background cycle
//! ([`refresh_shard_estimate`]), which means it lags a burst of writes by up to
//! one [`CYCLE_PERIOD`]. Fixing this properly needs `sync_stats` to accumulate
//! a per-shard byte counter maintained by `set_entry`/`remove_entry`; see the
//! W1c handover note.
//!
//! # OOM rejection (contract gap)
//!
//! §4.4 gives commands a `DENYOOM` flag and §4.2 gives `CmdError::Oom`, but
//! `engine::dispatch` never checks either, and it is frozen. [`check_oom`] is
//! the sanctioned check; handlers that allocate call it themselves until the
//! engine does it centrally.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use smallvec::SmallVec;

use crate::command::ArgVec;
use crate::config::{Config, MaxmemoryPolicy};
use crate::ctx::ServerShared;
use crate::error::CmdError;
use crate::info::Stats;
use crate::notify::{self, NotifyClass};
use crate::object::{Entry, Key, Robj, lru};
use crate::shard::expire::CYCLE_PERIOD;
use crate::shard::{Db, ShardHandle};

/// Redis's default `maxmemory-samples`.
pub const DEFAULT_SAMPLES: u32 = 5;

/// Size of the eviction candidate pool Redis keeps between sweeps.
pub const EVICTION_POOL_SIZE: usize = 16;

/// hashbrown's per-entry overhead: one control byte plus the slack implied by
/// the 7/8 load factor, rounded up to something that survives a rehash.
pub const DICT_ENTRY_OVERHEAD: usize = 16;

/// Entries examined per database when re-estimating a shard's footprint.
pub const ESTIMATE_SAMPLE: usize = 128;

/// Keys evicted per lock acquisition. Bounds how long one eviction batch can
/// stall a client hashing to the same shard.
pub const MAX_EVICTIONS_PER_LOCK: usize = 16;

/// Sampling rounds per eviction sweep, so a sweep is always bounded even when
/// the estimate says the shard is enormous.
pub const MAX_ROUNDS_PER_CYCLE: usize = 512;

/// How long one eviction sweep may run before yielding, before the
/// `maxmemory-eviction-tenacity` multiplier.
pub const BASE_EVICT_BUDGET: Duration = Duration::from_millis(10);

// ---------------------------------------------------------------------------
// Accounting
// ---------------------------------------------------------------------------

/// Estimated bytes held by one keyspace entry. See the module header for what
/// this counts and what it ignores.
#[inline]
pub fn entry_cost(key: &Key, entry: &Entry) -> usize {
    let slot = core::mem::size_of::<Key>() + core::mem::size_of::<Entry>() + DICT_ENTRY_OVERHEAD;
    // `Robj::mem_usage` includes the inline object, which is already inside
    // `size_of::<Entry>()`; subtract it so it is not counted twice.
    let payload = entry
        .obj
        .mem_usage()
        .saturating_sub(core::mem::size_of::<Robj>());
    let expires_slot = if entry.expire_at_ms.is_some() {
        core::mem::size_of::<Key>() + core::mem::size_of::<u64>() + DICT_ENTRY_OVERHEAD + key.len()
    } else {
        0
    };
    slot + key.len() + payload + expires_slot
}

/// Estimated bytes held by one database slice.
///
/// Exact while the slice has at most [`ESTIMATE_SAMPLE`] keys; above that it
/// is `mean(sample) * len`. The sample is the head of the iteration order,
/// which is hash order and therefore uncorrelated with value size -- an
/// unbiased sample that costs `O(ESTIMATE_SAMPLE)` rather than the `O(len)`
/// walk a random offset would need.
pub fn db_cost(db: &Db) -> u64 {
    let len = db.dict.len();
    if len == 0 {
        return 0;
    }
    let mut sampled = 0usize;
    let mut total = 0u64;
    for (key, entry) in db.dict.iter().take(ESTIMATE_SAMPLE) {
        sampled += 1;
        total += entry_cost(key, entry) as u64;
    }
    if sampled == 0 {
        return 0;
    }
    if sampled >= len {
        return total;
    }
    // mean * len, in integer arithmetic.
    total.saturating_mul(len as u64) / (sampled as u64)
}

/// Re-estimate one shard's footprint and publish it on `ShardHandle::mem`.
///
/// Takes the shard lock for the duration of the sample (at most
/// `databases * ESTIMATE_SAMPLE` entries), then releases it.
pub fn refresh_shard_estimate(handle: &ShardHandle) -> u64 {
    let shard = handle.lock();
    let mut total = 0u64;
    for db in shard.dbs.iter() {
        total += db_cost(db);
    }
    drop(shard);
    handle.mem.store(total, Ordering::Relaxed);
    total
}

/// Re-estimate every shard. Returns the new total.
pub fn refresh_all(server: &ServerShared) -> u64 {
    server.shards.iter().map(refresh_shard_estimate).sum()
}

/// The current estimate, without taking a single lock.
#[inline]
pub fn used_memory(server: &ServerShared) -> u64 {
    server
        .shards
        .iter()
        .map(|h| h.mem.load(Ordering::Relaxed))
        .sum()
}

/// Keys carrying a TTL, across every shard. Lock-free.
#[inline]
pub fn volatile_keys(server: &ServerShared) -> u64 {
    server
        .shards
        .iter()
        .map(|h| h.expires.load(Ordering::Relaxed))
        .sum()
}

/// True when the estimate is above `maxmemory`. Always false when `maxmemory`
/// is 0, which means "no limit".
#[inline]
pub fn over_limit(server: &ServerShared, cfg: &Config) -> bool {
    cfg.maxmemory != 0 && used_memory(server) > cfg.maxmemory
}

/// The `DENYOOM` gate (§4.4).
///
/// Call this at the top of any handler that can grow the keyspace. It reads
/// atomics only, so it is safe to call while shard locks are held.
///
/// The rule mirrors Redis: being over the limit is not by itself fatal, it is
/// being over the limit *with no way back* that is. That means
/// `noeviction`, or a `volatile-*` policy with nothing volatile to evict.
/// Under a working eviction policy the background sweep is expected to make
/// room, exactly as `performEvictions()` does in Redis.
pub fn check_oom(server: &ServerShared, cfg: &Config) -> Result<(), CmdError> {
    if !over_limit(server, cfg) {
        return Ok(());
    }
    let hopeless = match cfg.maxmemory_policy {
        MaxmemoryPolicy::NoEviction => true,
        MaxmemoryPolicy::VolatileLru
        | MaxmemoryPolicy::VolatileLfu
        | MaxmemoryPolicy::VolatileRandom
        | MaxmemoryPolicy::VolatileTtl => volatile_keys(server) == 0,
        _ => false,
    };
    if hopeless { Err(CmdError::Oom) } else { Ok(()) }
}

// ---------------------------------------------------------------------------
// Candidate selection
// ---------------------------------------------------------------------------

/// A sampled eviction candidate. `score` is "how much we want this gone":
/// larger is a better victim, whatever the policy.
#[derive(Debug, Clone)]
struct Candidate {
    key: Key,
    score: u64,
}

/// The bounded best-of pool. Kept sorted descending, truncated to
/// [`EVICTION_POOL_SIZE`], so the front is always the best victim seen.
#[derive(Debug, Default)]
struct Pool {
    v: SmallVec<[Candidate; EVICTION_POOL_SIZE]>,
}

impl Pool {
    fn push(&mut self, c: Candidate) {
        let at = self
            .v
            .iter()
            .position(|e| e.score < c.score)
            .unwrap_or(self.v.len());
        if at >= EVICTION_POOL_SIZE {
            return;
        }
        self.v.insert(at, c);
        self.v.truncate(EVICTION_POOL_SIZE);
    }
}

/// Policy-specific victim score. Larger evicts first.
#[inline]
fn score(policy: MaxmemoryPolicy, entry: &Entry, now_ms: u64) -> u64 {
    match policy {
        MaxmemoryPolicy::AllkeysLru | MaxmemoryPolicy::VolatileLru => {
            lru::idle_ms(entry.lru.load(Ordering::Relaxed), now_ms)
        }
        MaxmemoryPolicy::AllkeysLfu | MaxmemoryPolicy::VolatileLfu => {
            // The rarely-used key is the good victim, so invert the counter.
            255 - u64::from(lru::lfu_counter(entry.lru.load(Ordering::Relaxed)))
        }
        MaxmemoryPolicy::VolatileTtl => {
            // The soonest deadline is the best victim.
            u64::MAX - entry.expire_at_ms.unwrap_or(u64::MAX)
        }
        MaxmemoryPolicy::AllkeysRandom
        | MaxmemoryPolicy::VolatileRandom
        | MaxmemoryPolicy::NoEviction => crate::util::rand::u64_(),
    }
}

#[inline]
fn is_volatile_policy(policy: MaxmemoryPolicy) -> bool {
    matches!(
        policy,
        MaxmemoryPolicy::VolatileLru
            | MaxmemoryPolicy::VolatileLfu
            | MaxmemoryPolicy::VolatileRandom
            | MaxmemoryPolicy::VolatileTtl
    )
}

/// Draw `want` candidates from one database slice into `pool`.
///
/// Volatile policies sample the expiry index, which is both smaller and
/// exactly the eligible set; the others sample the keyspace.
fn fill_pool(db: &Db, policy: MaxmemoryPolicy, now_ms: u64, want: usize, pool: &mut Pool) -> usize {
    let volatile = is_volatile_policy(policy);
    let n = if volatile {
        db.expires.len()
    } else {
        db.dict.len()
    };
    if n == 0 || want == 0 {
        return 0;
    }
    let want = want.min(n);
    let start = crate::util::rand::below(n - want + 1);

    let mut sampled = 0usize;
    if volatile {
        let keys: SmallVec<[Key; 32]> = db
            .expires
            .iter()
            .skip(start)
            .take(want)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            if let Some(entry) = db.dict.get(&key) {
                sampled += 1;
                pool.push(Candidate {
                    score: score(policy, entry, now_ms),
                    key,
                });
            }
        }
    } else {
        for (key, entry) in db.dict.iter().skip(start).take(want) {
            sampled += 1;
            pool.push(Candidate {
                score: score(policy, entry, now_ms),
                key: key.clone(),
            });
        }
    }
    sampled
}

// ---------------------------------------------------------------------------
// The sweep
// ---------------------------------------------------------------------------

/// What one sweep did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EvictStats {
    pub evicted: u64,
    pub freed_bytes: u64,
    /// Sampling rounds, i.e. lock acquisitions.
    pub rounds: u64,
    /// The sweep stopped with the keyspace still over `maxmemory`.
    pub over_limit: bool,
    /// Nothing was evictable: `noeviction`, or a `volatile-*` policy with no
    /// volatile keys left. This is the state in which `DENYOOM` commands are
    /// rejected.
    pub exhausted: bool,
}

/// Evict at most `max_keys` from one shard, under one lock acquisition.
///
/// Returns the victims so the caller can notify after the lock is dropped.
fn evict_batch(
    server: &ServerShared,
    handle: &ShardHandle,
    cfg: &Config,
    max_keys: usize,
) -> (u64, SmallVec<[(usize, Key); MAX_EVICTIONS_PER_LOCK]>) {
    let now_ms = server.clock.now_ms();
    let databases = server.shards.databases();
    let propagating = server.is_propagating();
    let del_verb: &'static [u8] = if cfg.lazyfree_lazy_eviction {
        b"UNLINK"
    } else {
        b"DEL"
    };
    let samples = (cfg.maxmemory_samples.max(1) as usize).min(64);

    let mut victims: SmallVec<[(usize, Key); MAX_EVICTIONS_PER_LOCK]> = SmallVec::new();
    let mut freed = 0u64;

    let mut shard = handle.lock();
    for db_idx in 0..databases {
        if victims.len() >= max_keys {
            break;
        }
        let Some(db) = shard.db(db_idx) else {
            continue;
        };
        if db.dict.is_empty() {
            continue;
        }
        let mut pool = Pool::default();
        // Draw a wider sample than `maxmemory-samples` in one walk: the walk
        // is the expensive part, the scoring is not.
        fill_pool(db, cfg.maxmemory_policy, now_ms, samples * 4, &mut pool);

        for cand in pool.v.iter() {
            if victims.len() >= max_keys {
                break;
            }
            let Some(entry) = db.dict.get(&cand.key) else {
                continue;
            };
            freed += entry_cost(&cand.key, entry) as u64;
            db.remove_entry(&cand.key);
            db.signal_watch(&cand.key);
            victims.push((db_idx, cand.key.clone()));
        }
    }

    if !victims.is_empty() {
        shard.dirty += victims.len() as u64;
        if propagating {
            for (db_idx, key) in &victims {
                let mut argv = ArgVec::new();
                argv.push(Bytes::from_static(del_verb));
                argv.push(key.clone());
                shard.propagate(*db_idx, argv);
            }
        }
        handle.sync_stats(&shard);
    }
    drop(shard);

    handle
        .mem
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |m| {
            Some(m.saturating_sub(freed))
        })
        .ok();
    (freed, victims)
}

/// Index of the shard with the largest estimated footprint.
fn fattest_shard(server: &ServerShared) -> Option<usize> {
    let mut best: Option<(usize, u64)> = None;
    for (i, h) in server.shards.iter().enumerate() {
        let m = h.mem.load(Ordering::Relaxed);
        if m > 0 && best.is_none_or(|(_, b)| m > b) {
            best = Some((i, m));
        }
    }
    best.map(|(i, _)| i)
}

/// Bring the keyspace back under `maxmemory`, or report that it cannot be
/// done.
///
/// **Never call this from a command handler.** It takes shard locks one at a
/// time, which is only safe from a thread that holds none; a handler already
/// holds its command's shards and would violate the ordering discipline of
/// §2.1. Handlers use [`check_oom`], which only reads atomics.
pub fn evict_cycle(server: &ServerShared) -> EvictStats {
    let cfg = server.config();
    let mut stats = EvictStats::default();
    if cfg.maxmemory == 0 {
        return stats;
    }
    refresh_all(server);

    if cfg.maxmemory_policy == MaxmemoryPolicy::NoEviction {
        stats.over_limit = used_memory(server) > cfg.maxmemory;
        stats.exhausted = stats.over_limit;
        return stats;
    }

    let budget = BASE_EVICT_BUDGET * (1 + cfg.maxmemory_eviction_tenacity.min(100) / 10);
    let deadline = Instant::now() + budget;

    for _ in 0..MAX_ROUNDS_PER_CYCLE {
        if used_memory(server) <= cfg.maxmemory {
            return stats;
        }
        if Instant::now() >= deadline {
            break;
        }
        let Some(idx) = fattest_shard(server) else {
            stats.exhausted = true;
            break;
        };
        let Some(handle) = server.shards.get(idx) else {
            break;
        };

        let (freed, victims) = evict_batch(server, handle, &cfg, MAX_EVICTIONS_PER_LOCK);
        stats.rounds += 1;
        if victims.is_empty() {
            // Nothing evictable on the fattest shard. With a volatile policy
            // that usually means no volatile keys anywhere; re-estimate so the
            // next round picks a different shard, and give up if it does not
            // help.
            refresh_shard_estimate(handle);
            if is_volatile_policy(cfg.maxmemory_policy) && volatile_keys(server) == 0 {
                stats.exhausted = true;
                break;
            }
            if stats.rounds > server.shards.len() as u64 {
                stats.exhausted = true;
                break;
            }
            continue;
        }

        stats.evicted += victims.len() as u64;
        stats.freed_bytes += freed;
        Stats::add(&server.stats.evicted_keys, victims.len() as u64);
        server
            .dirty
            .fetch_add(victims.len() as u64, Ordering::Relaxed);
        for (db_idx, key) in &victims {
            notify::dispatch(
                server,
                cfg.notify_keyspace_events,
                NotifyClass::EVICTED,
                "evicted",
                *db_idx,
                key,
            );
        }
    }

    stats.over_limit = used_memory(server) > cfg.maxmemory;
    stats
}

/// Spawn the memory cron: re-estimate every shard and evict when needed.
///
/// One task, not one per shard: eviction has to compare shards against each
/// other to pick a victim, and the work is proportional to the overshoot
/// rather than to the shard count.
///
/// Not wired up yet -- `src/main.rs` is F0's. See the W1c handover note.
pub fn spawn(server: &Arc<ServerShared>) -> tokio::task::JoinHandle<()> {
    let server = Arc::clone(server);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(CYCLE_PERIOD);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if server.shutting_down.load(Ordering::Relaxed) {
                break;
            }
            evict_cycle(&server);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::Robj;
    use crate::types::string::StrObj;

    fn server_with(policy: MaxmemoryPolicy, maxmemory: u64) -> Arc<ServerShared> {
        ServerShared::new(Config {
            shard_count: 4,
            maxmemory,
            maxmemory_policy: policy,
            ..Default::default()
        })
    }

    fn insert(server: &ServerShared, key: &str, value_len: usize, expire_at_ms: Option<u64>) {
        let key = Key::copy_from_slice(key.as_bytes());
        let idx = server.shards.shard_index(&key);
        let handle = server.shards.get(idx).expect("shard");
        let mut shard = handle.lock();
        let db = shard.db(0).expect("db");
        db.set_entry(
            key,
            Entry::new(
                Robj::Str(StrObj::raw(Bytes::from(vec![b'x'; value_len]))),
                expire_at_ms,
                server.clock.now_ms(),
                false,
            ),
        );
        handle.sync_stats(&shard);
    }

    fn live_keys(server: &ServerShared) -> usize {
        server.shards.iter().map(|h| h.lock().key_count()).sum()
    }

    fn has_key(server: &ServerShared, key: &str) -> bool {
        let key = Key::copy_from_slice(key.as_bytes());
        let idx = server.shards.shard_index(&key);
        server
            .shards
            .get(idx)
            .map(|h| {
                h.lock()
                    .db_ref(0)
                    .is_some_and(|d| d.dict.contains_key(&key))
            })
            .unwrap_or(false)
    }

    #[test]
    fn cost_grows_with_the_payload_and_the_ttl() {
        let small = Entry::new(
            Robj::Str(StrObj::raw(Bytes::from_static(b"x"))),
            None,
            0,
            false,
        );
        let big = Entry::new(
            Robj::Str(StrObj::raw(Bytes::from(vec![b'x'; 1000]))),
            None,
            0,
            false,
        );
        let k = Key::from_static(b"k");
        assert!(entry_cost(&k, &big) > entry_cost(&k, &small) + 900);

        let volatile = Entry::new(
            Robj::Str(StrObj::raw(Bytes::from_static(b"x"))),
            Some(1),
            0,
            false,
        );
        assert!(entry_cost(&k, &volatile) > entry_cost(&k, &small));

        let long_key = Key::from_static(b"a-very-much-longer-key-name-here");
        assert!(entry_cost(&long_key, &small) > entry_cost(&k, &small));
    }

    #[test]
    fn estimate_is_exact_for_small_slices_and_scales_for_large_ones() {
        let s = server_with(MaxmemoryPolicy::NoEviction, 0);
        for i in 0..10 {
            insert(&s, &format!("k:{i}"), 100, None);
        }
        let total = refresh_all(&s);
        assert!(total > 1000, "10 x 100 bytes should be visible: {total}");
        assert_eq!(total, used_memory(&s));

        // Above the sample size the estimate is a projection, but it must stay
        // in the right ballpark.
        let s = server_with(MaxmemoryPolicy::NoEviction, 0);
        for i in 0..2_000 {
            insert(&s, &format!("k:{i}"), 100, None);
        }
        let total = refresh_all(&s);
        let lower = 2_000 * 100;
        assert!(total > lower, "estimate {total} below the payload {lower}");
        assert!(total < lower * 4, "estimate {total} implausibly high");
    }

    #[test]
    fn noeviction_reports_oom_and_evicts_nothing() {
        let s = server_with(MaxmemoryPolicy::NoEviction, 1);
        for i in 0..50 {
            insert(&s, &format!("k:{i}"), 100, None);
        }
        let stats = evict_cycle(&s);
        assert_eq!(stats.evicted, 0);
        assert!(stats.over_limit && stats.exhausted);
        assert_eq!(live_keys(&s), 50);

        let cfg = s.config();
        assert!(matches!(check_oom(&s, &cfg), Err(CmdError::Oom)));
    }

    #[test]
    fn allkeys_policies_evict_until_under_the_limit() {
        for policy in [
            MaxmemoryPolicy::AllkeysLru,
            MaxmemoryPolicy::AllkeysLfu,
            MaxmemoryPolicy::AllkeysRandom,
        ] {
            let s = server_with(policy, 0);
            for i in 0..400 {
                insert(&s, &format!("k:{i}"), 200, None);
            }
            let used = refresh_all(&s);
            // Ask for half of it back.
            s.config
                .update(|c| {
                    c.maxmemory = used / 2;
                    Ok(())
                })
                .expect("config");

            let stats = evict_cycle(&s);
            assert!(stats.evicted > 0, "{policy:?} evicted nothing");
            assert!(
                !stats.over_limit,
                "{policy:?} left the keyspace over the limit: {stats:?}"
            );
            assert!(live_keys(&s) < 400);
            assert_eq!(Stats::get(&s.stats.evicted_keys), stats.evicted);
        }
    }

    #[test]
    fn volatile_policies_only_evict_keys_with_a_ttl() {
        for policy in [
            MaxmemoryPolicy::VolatileLru,
            MaxmemoryPolicy::VolatileLfu,
            MaxmemoryPolicy::VolatileRandom,
            MaxmemoryPolicy::VolatileTtl,
        ] {
            let s = server_with(policy, 0);
            let future = s.clock.now_ms() + 1_000_000;
            for i in 0..200 {
                insert(&s, &format!("vol:{i}"), 200, Some(future + i));
            }
            for i in 0..200 {
                insert(&s, &format!("perm:{i}"), 200, None);
            }
            let used = refresh_all(&s);
            s.config
                .update(|c| {
                    c.maxmemory = used / 2;
                    Ok(())
                })
                .expect("config");

            let stats = evict_cycle(&s);
            assert!(stats.evicted > 0, "{policy:?} evicted nothing");
            for i in 0..200 {
                assert!(
                    has_key(&s, &format!("perm:{i}")),
                    "{policy:?} evicted a key with no TTL"
                );
            }
        }
    }

    #[test]
    fn volatile_with_nothing_volatile_is_oom() {
        let s = server_with(MaxmemoryPolicy::VolatileLru, 1);
        for i in 0..50 {
            insert(&s, &format!("k:{i}"), 100, None);
        }
        let stats = evict_cycle(&s);
        assert_eq!(stats.evicted, 0);
        assert!(stats.exhausted);
        assert_eq!(live_keys(&s), 50);
        let cfg = s.config();
        assert!(matches!(check_oom(&s, &cfg), Err(CmdError::Oom)));
    }

    #[test]
    fn a_working_eviction_policy_does_not_reject_commands() {
        let s = server_with(MaxmemoryPolicy::AllkeysLru, 1);
        for i in 0..50 {
            insert(&s, &format!("k:{i}"), 100, None);
        }
        refresh_all(&s);
        let cfg = s.config();
        assert!(over_limit(&s, &cfg));
        // Over the limit, but eviction can make room, so the command runs.
        assert!(check_oom(&s, &cfg).is_ok());
    }

    #[test]
    fn no_limit_means_no_work() {
        let s = server_with(MaxmemoryPolicy::AllkeysLru, 0);
        for i in 0..100 {
            insert(&s, &format!("k:{i}"), 100, None);
        }
        assert_eq!(evict_cycle(&s), EvictStats::default());
        assert_eq!(live_keys(&s), 100);
        let cfg = s.config();
        assert!(check_oom(&s, &cfg).is_ok());
    }

    #[test]
    fn volatile_ttl_prefers_the_soonest_deadline() {
        let s = server_with(MaxmemoryPolicy::VolatileTtl, 0);
        let now = s.clock.now_ms();
        // One shard so that every candidate competes in the same pool.
        let s = ServerShared::new(Config {
            shard_count: 1,
            maxmemory: 0,
            maxmemory_policy: MaxmemoryPolicy::VolatileTtl,
            ..Default::default()
        });
        for i in 0..64u64 {
            insert(&s, &format!("k:{i}"), 200, Some(now + 1_000_000 + i * 1000));
        }
        let used = refresh_all(&s);
        s.config
            .update(|c| {
                c.maxmemory = used * 3 / 4;
                Ok(())
            })
            .expect("config");
        evict_cycle(&s);

        // The very last key (longest TTL) must survive; the first (shortest)
        // is the one the pool should have picked.
        assert!(has_key(&s, "k:63"), "the longest TTL was evicted first");
    }

    #[test]
    fn evicted_keys_propagate_and_notify() {
        let _guard = notify::SINK_TEST_LOCK.lock();
        let s = ServerShared::new(Config {
            shard_count: 1,
            maxmemory: 0,
            maxmemory_policy: MaxmemoryPolicy::AllkeysRandom,
            notify_keyspace_events: NotifyClass::parse("KEe").expect("flags"),
            ..Default::default()
        });
        s.propagation_enabled.store(true, Ordering::Relaxed);
        let cap = Arc::new(notify::CaptureSink::new());
        notify::install_sink(cap.clone());

        for i in 0..100 {
            insert(&s, &format!("k:{i}"), 200, None);
        }
        let used = refresh_all(&s);
        s.config
            .update(|c| {
                c.maxmemory = used / 2;
                Ok(())
            })
            .expect("config");
        let stats = evict_cycle(&s);
        let events = cap.take_strings();
        notify::clear_sink();

        assert!(stats.evicted > 0);
        let handle = s.shards.get(0).expect("shard");
        let drained = handle.lock().drain_propagation();
        assert_eq!(drained.len() as u64, stats.evicted);
        assert!(
            drained
                .iter()
                .all(|p| p.argv.first().map(|b| &b[..]) == Some(&b"DEL"[..]))
        );
        assert!(
            events
                .iter()
                .any(|(c, _)| c == "__keyevent@0__:evicted" || c.starts_with("__keyspace@0__:")),
            "{events:?}"
        );
    }

    #[test]
    fn the_pool_keeps_the_best_candidates() {
        let mut pool = Pool::default();
        for i in 0..100u64 {
            pool.push(Candidate {
                key: Key::from_static(b"k"),
                score: i,
            });
        }
        assert_eq!(pool.v.len(), EVICTION_POOL_SIZE);
        assert_eq!(pool.v.first().map(|c| c.score), Some(99));
        assert_eq!(pool.v.last().map(|c| c.score), Some(99 - 15));
        // Sorted descending.
        assert!(pool.v.windows(2).all(|w| match w {
            [a, b] => a.score >= b.score,
            _ => true,
        }));
    }
}
