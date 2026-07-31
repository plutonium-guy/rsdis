//! Active expiry cycle.
//!
//! Owned by W1c; do not edit if you are not that agent.
//!
//! Lazy expiry -- a logically-expired key reading as missing and being reaped
//! on access -- lives in `Ctx::lookup_*` and is F0's. This is the *active*
//! half: the background sweep that reclaims memory for keys nobody looks at,
//! modelled on Redis's `activeExpireCycle`.
//!
//! # Shape of the cycle
//!
//! One task per shard, waking every [`CYCLE_PERIOD`]. Per wake, and per
//! database slice inside the shard:
//!
//! 1. take the shard lock,
//! 2. sample [`KEYS_PER_LOOP`] keys from `Db::expires`,
//! 3. delete the ones that have passed, buffering their names,
//! 4. **release the lock**,
//! 5. fire `expired` keyspace notifications for the buffer,
//! 6. repeat while more than 25% of the sample turned out to be expired and
//!    the CPU budget is not spent.
//!
//! Step 4 is the whole point. A sweep never holds a shard lock across more
//! than one 20-key sample, so a client hashing to that shard waits microseconds
//! at worst. Redis, being single threaded, blocks *everything* for up to 25% of
//! a cycle; we spend the same CPU budget but the stall is bounded by one
//! sample.
//!
//! # Budget
//!
//! `ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC` is 25% of a cycle in Redis, measured
//! against a single-threaded server. Here the shards sweep in parallel, so the
//! aggregate budget is divided across them ([`shard_budget`]): the total CPU
//! spent expiring keys is still 25% of one core, not 25% of every core.
//!
//! # Deletion vs. propagation (§7, §9.5)
//!
//! An actively-expired key is a write that the AOF and replicas must see, so
//! each victim propagates a synthetic `DEL` (or `UNLINK` under
//! `lazyfree-lazy-expire`) into the shard's own `repl_buf`. The shard holding
//! the key is the only one locked, so §9.5's "anchor on the lowest-indexed
//! locked shard" is satisfied trivially.
//!
//! # Sampling cost (contract gap)
//!
//! Redis samples with `dictGetSomeKeys`, which jumps to a random *bucket* in
//! O(1). `Dict` is a `hashbrown::HashMap`, which exposes no bucket index, so a
//! random sample here costs one `Iterator::skip` from a random offset -- O(n)
//! pointer-cheap steps over control bytes, ~1-2 ns each. See the module note in
//! `evict.rs`; the fix is for `Dict` to become a `hashbrown::HashTable`, which
//! does have `num_buckets`/`get_bucket`.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use smallvec::SmallVec;

use crate::command::ArgVec;
use crate::ctx::ServerShared;
use crate::info::Stats;
use crate::notify::{self, NotifyClass};
use crate::object::Key;
use crate::shard::Db;

/// Redis's `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP`.
pub const KEYS_PER_LOOP: usize = 20;

/// Redis keeps looping over a database while more than this fraction of the
/// sampled keys turned out to be expired.
pub const CYCLE_ACCEPTABLE_STALE_PCT: f64 = 0.10;

/// Redis's `ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC`: the share of a cycle the slow
/// sweep may spend burning CPU.
pub const ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC: u32 = 25;

/// Keep sweeping a database while the expired fraction of the sample is above
/// this. Redis's `ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE` equivalent for the
/// slow cycle is 25%.
pub const CYCLE_CONTINUE_PCT: u32 = 25;

/// One cycle, i.e. Redis's `1000/server.hz` with the default `hz 10`.
pub const CYCLE_PERIOD: Duration = Duration::from_millis(100);

/// A sweep never gets less than this, however many shards there are.
pub const MIN_SHARD_BUDGET: Duration = Duration::from_micros(100);

/// What one sweep did. Returned so tests and benchmarks can assert on it
/// instead of on wall-clock side effects.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CycleStats {
    /// Keys looked at.
    pub sampled: u64,
    /// Keys found expired and deleted.
    pub expired: u64,
    /// Lock acquisitions, i.e. sampling rounds.
    pub rounds: u64,
    /// True when the sweep stopped because the budget ran out rather than
    /// because it ran out of expired keys.
    pub timed_out: bool,
}

impl CycleStats {
    fn merge(&mut self, other: CycleStats) {
        self.sampled += other.sampled;
        self.expired += other.expired;
        self.rounds += other.rounds;
        self.timed_out |= other.timed_out;
    }
}

/// The slice of the aggregate 25% budget one shard's task gets.
///
/// Dividing rather than replicating is deliberate: `n` shards sweeping in
/// parallel with Redis's full per-server budget would spend 25% of *every*
/// core on expiry.
pub fn shard_budget(shard_count: usize) -> Duration {
    let total = CYCLE_PERIOD * ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC / 100;
    let each = total / (shard_count.max(1) as u32);
    if each < MIN_SHARD_BUDGET {
        MIN_SHARD_BUDGET
    } else {
        each
    }
}

/// Victims from one sampling round: cloned key handles (a `Bytes` clone is a
/// refcount bump, not a copy), carried out of the lock so the notifications
/// can be published without holding it.
type Victims = SmallVec<[Key; KEYS_PER_LOOP]>;

/// One sampling round over one database slice, under the shard lock.
///
/// Returns `(sampled, victims)`. Everything that needs the lock happens here
/// and the lock is dropped by the caller immediately afterwards.
fn sample_round(db: &mut Db, now_ms: u64, want: usize) -> (usize, Victims) {
    let n = db.expires.len();
    if n == 0 {
        return (0, Victims::new());
    }
    let want = want.min(n);
    // A contiguous window from a random offset. This is also what Redis's
    // `dictGetSomeKeys` returns -- consecutive buckets from a random start --
    // so the statistical behaviour matches; only the cost of finding the start
    // differs (see the module header).
    let start = crate::util::rand::below(n - want + 1);

    let mut victims = Victims::new();
    let mut sampled = 0usize;
    for (key, at) in db.expires.iter().skip(start).take(want) {
        sampled += 1;
        if *at <= now_ms {
            victims.push(key.clone());
        }
    }

    for key in &victims {
        db.remove_entry(key);
        db.signal_watch(key);
    }
    (sampled, victims)
}

/// Sweep one shard for up to `budget`.
///
/// Synchronous and self-contained: it takes and releases the shard lock itself,
/// so it is equally usable from the background task, from a test, and from a
/// benchmark. It must **not** be called by a command handler, which already
/// holds shard locks (see the deadlock argument in `engine.rs`).
pub fn cycle_shard(server: &ServerShared, shard_index: usize, budget: Duration) -> CycleStats {
    let mut total = CycleStats::default();
    let cfg = server.config();
    if !cfg.activeexpire {
        return total;
    }
    let Some(handle) = server.shards.get(shard_index) else {
        return total;
    };
    // Nothing with a TTL on this shard: the common case, and it costs one
    // relaxed load rather than a lock.
    if handle.expires.load(Ordering::Relaxed) == 0 {
        return total;
    }

    let deadline = Instant::now() + budget;
    let databases = server.shards.databases();
    let propagating = server.is_propagating();
    let del_verb: &'static [u8] = if cfg.lazyfree_lazy_expire {
        b"UNLINK"
    } else {
        b"DEL"
    };
    let configured = cfg.notify_keyspace_events;

    for db_idx in 0..databases {
        loop {
            if Instant::now() >= deadline {
                total.timed_out = true;
                return total;
            }
            let now_ms = server.clock.now_ms();

            // ---- under the lock, and only for one sample ------------------
            let (sampled, victims) = {
                let mut shard = handle.lock();
                let Some(db) = shard.db(db_idx) else {
                    break;
                };
                let (sampled, victims) = sample_round(db, now_ms, KEYS_PER_LOOP);
                if !victims.is_empty() {
                    shard.dirty += victims.len() as u64;
                    if propagating {
                        for key in &victims {
                            let mut argv = ArgVec::new();
                            argv.push(Bytes::from_static(del_verb));
                            argv.push(key.clone());
                            shard.propagate(db_idx, argv);
                        }
                    }
                    handle.sync_stats(&shard);
                }
                (sampled, victims)
            };
            // ---- lock released --------------------------------------------

            let expired = victims.len();
            if expired > 0 {
                Stats::add(&server.stats.expired_keys, expired as u64);
                server.dirty.fetch_add(expired as u64, Ordering::Relaxed);
                for key in &victims {
                    notify::dispatch(
                        server,
                        configured,
                        NotifyClass::EXPIRED,
                        "expired",
                        db_idx,
                        key,
                    );
                }
            }

            total.rounds += 1;
            total.sampled += sampled as u64;
            total.expired += expired as u64;

            if sampled == 0 {
                break;
            }
            // Redis's rule: keep going only while the sample is still mostly
            // stale, otherwise move on and come back next cycle.
            if (expired as u64) * 100 <= (sampled as u64) * u64::from(CYCLE_CONTINUE_PCT) {
                break;
            }
        }
    }
    total
}

/// Sweep every shard once, sequentially. Test and benchmark entry point; the
/// server uses [`spawn`].
pub fn cycle_all(server: &ServerShared, budget_per_shard: Duration) -> CycleStats {
    let mut total = CycleStats::default();
    for i in 0..server.shards.len() {
        total.merge(cycle_shard(server, i, budget_per_shard));
    }
    total
}

/// Sweep every shard repeatedly until nothing expires. Only for tests and
/// benchmarks: unbounded work by construction.
pub fn drain_expired(server: &ServerShared) -> u64 {
    let mut total = 0u64;
    loop {
        let stats = cycle_all(server, Duration::from_secs(1));
        total += stats.expired;
        if stats.expired == 0 {
            return total;
        }
    }
}

/// Spawn one active-expire task per shard.
///
/// Call from `main` after `ServerShared::new`. Each task is a plain interval
/// loop; `cycle_shard` is synchronous and bounded by [`shard_budget`], so it
/// never blocks a tokio worker for longer than that.
///
/// Not wired up yet: `src/main.rs` is F0's and currently spawns only the clock
/// ticker. See the W1c handover note.
pub fn spawn(server: &Arc<ServerShared>) -> Vec<tokio::task::JoinHandle<()>> {
    let budget = shard_budget(server.shards.len());
    (0..server.shards.len())
        .map(|index| {
            let server = Arc::clone(server);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(CYCLE_PERIOD);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    if server.shutting_down.load(Ordering::Relaxed) {
                        break;
                    }
                    cycle_shard(&server, index, budget);
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::object::{Entry, Robj};
    use crate::types::string::StrObj;

    fn server(shards: usize) -> Arc<ServerShared> {
        ServerShared::new(Config {
            shard_count: shards,
            ..Default::default()
        })
    }

    fn insert(server: &ServerShared, key: &str, expire_at_ms: Option<u64>) {
        let key = Key::copy_from_slice(key.as_bytes());
        let idx = server.shards.shard_index(&key);
        let handle = server.shards.get(idx).expect("shard");
        let mut shard = handle.lock();
        let db = shard.db(0).expect("db");
        db.set_entry(
            key,
            Entry::new(
                Robj::Str(StrObj::from_bytes(Bytes::from_static(b"v"))),
                expire_at_ms,
                0,
                false,
            ),
        );
        handle.sync_stats(&shard);
    }

    fn live_keys(server: &ServerShared) -> usize {
        server.shards.iter().map(|h| h.lock().key_count()).sum()
    }

    #[test]
    fn sweeps_expired_keys_and_leaves_the_rest() {
        let s = server(4);
        let future = s.clock.now_ms() + 10 * 60 * 1000;
        for i in 0..200 {
            insert(&s, &format!("gone:{i}"), Some(1));
        }
        for i in 0..50 {
            insert(&s, &format!("stays:{i}"), Some(future));
        }
        for i in 0..50 {
            insert(&s, &format!("forever:{i}"), None);
        }
        assert_eq!(live_keys(&s), 300);

        let reaped = drain_expired(&s);
        assert_eq!(reaped, 200);
        assert_eq!(live_keys(&s), 100);
    }

    #[test]
    fn a_shard_with_no_ttls_costs_nothing() {
        let s = server(4);
        for i in 0..100 {
            insert(&s, &format!("k:{i}"), None);
        }
        let stats = cycle_all(&s, Duration::from_millis(50));
        assert_eq!(stats.sampled, 0);
        assert_eq!(stats.expired, 0);
        assert_eq!(live_keys(&s), 100);
    }

    #[test]
    fn the_budget_is_respected() {
        let s = server(1);
        for i in 0..20_000 {
            insert(&s, &format!("gone:{i}"), Some(1));
        }
        let start = Instant::now();
        let stats = cycle_shard(&s, 0, Duration::from_millis(5));
        let elapsed = start.elapsed();
        assert!(stats.timed_out, "expected the budget to bind: {stats:?}");
        // Generous slack: one sampling round past the deadline is allowed.
        assert!(
            elapsed < Duration::from_millis(200),
            "sweep overran its budget: {elapsed:?}"
        );
        assert!(stats.expired > 0);
        assert!(live_keys(&s) > 0, "a bounded sweep must not finish the job");
    }

    #[test]
    fn expiry_is_counted_and_propagated() {
        let s = server(1);
        s.propagation_enabled.store(true, Ordering::Relaxed);
        for i in 0..30 {
            insert(&s, &format!("gone:{i}"), Some(1));
        }
        let reaped = drain_expired(&s);
        assert_eq!(reaped, 30);
        assert_eq!(Stats::get(&s.stats.expired_keys), 30);

        let handle = s.shards.get(0).expect("shard");
        let drained = handle.lock().drain_propagation();
        assert_eq!(drained.len(), 30);
        assert!(
            drained
                .iter()
                .all(|p| p.argv.first().map(|b| &b[..]) == Some(&b"DEL"[..])),
            "every victim must propagate a DEL"
        );
    }

    #[test]
    fn lazyfree_propagates_unlink() {
        let s = ServerShared::new(Config {
            shard_count: 1,
            lazyfree_lazy_expire: true,
            ..Default::default()
        });
        s.propagation_enabled.store(true, Ordering::Relaxed);
        insert(&s, "gone", Some(1));
        drain_expired(&s);
        let handle = s.shards.get(0).expect("shard");
        let drained = handle.lock().drain_propagation();
        assert_eq!(
            drained.first().and_then(|p| p.argv.first()).map(|b| &b[..]),
            Some(&b"UNLINK"[..])
        );
    }

    #[test]
    fn fires_expired_notifications_after_releasing_the_lock() {
        let _guard = notify::SINK_TEST_LOCK.lock();
        let s = ServerShared::new(Config {
            shard_count: 1,
            notify_keyspace_events: NotifyClass::parse("KEx").expect("flags"),
            ..Default::default()
        });
        let cap = Arc::new(notify::CaptureSink::new());
        notify::install_sink(cap.clone());

        insert(&s, "gone", Some(1));
        drain_expired(&s);

        let events = cap.take_strings();
        notify::clear_sink();
        assert!(
            events.contains(&("__keyspace@0__:gone".into(), "expired".into())),
            "{events:?}"
        );
        assert!(
            events.contains(&("__keyevent@0__:expired".into(), "gone".into())),
            "{events:?}"
        );
    }

    #[test]
    fn activeexpire_no_disables_the_sweep() {
        let s = ServerShared::new(Config {
            shard_count: 1,
            activeexpire: false,
            ..Default::default()
        });
        insert(&s, "gone", Some(1));
        assert_eq!(cycle_all(&s, Duration::from_millis(10)).expired, 0);
        assert_eq!(live_keys(&s), 1);
    }

    #[test]
    fn every_database_slice_is_swept() {
        let s = server(1);
        let handle = s.shards.get(0).expect("shard");
        for db_idx in [0usize, 5, 15] {
            let mut shard = handle.lock();
            let db = shard.db(db_idx).expect("db");
            db.set_entry(
                Key::from_static(b"gone"),
                Entry::new(
                    Robj::Str(StrObj::from_bytes(Bytes::from_static(b"v"))),
                    Some(1),
                    0,
                    false,
                ),
            );
            handle.sync_stats(&shard);
        }
        assert_eq!(drain_expired(&s), 3);
        assert_eq!(live_keys(&s), 0);
    }

    #[test]
    fn budget_is_shared_across_shards() {
        assert!(shard_budget(1) > shard_budget(16));
        assert!(shard_budget(1024) >= MIN_SHARD_BUDGET);
        // Aggregate never exceeds 25% of a cycle while shards are few enough
        // for the floor not to bite.
        assert_eq!(shard_budget(1), CYCLE_PERIOD / 4);
    }
}
