//! The sharded keyspace.
//!
//! Owner: F0. **FROZEN** -- `Shard`, `Db`, `ShardSet`, `ShardMap` and the
//! slot function are the concurrency contract. `src/shard/expire.rs` and
//! `src/shard/evict.rs` are W1c's.
//!
//! # Model (§2)
//!
//! `N` shards, `N` a power of two >= core count. Each shard owns an
//! independent keyspace (16 databases), its own expiry index, its own dirty
//! counter and its own propagation buffer. Nothing on the hot path is shared
//! between shards.
//!
//! ```text
//! slot  = crc16(key_or_hashtag) & 16383     // identical to Redis Cluster
//! shard = slot & (N - 1)
//! ```
//!
//! The database index is part of the *lookup inside a shard*, never part of
//! the shard hash: `SELECT` must not move a key between shards.

pub mod evict;
pub mod expire;

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use crossbeam_utils::CachePadded;
use parking_lot::{Mutex, MutexGuard};
use smallvec::SmallVec;

pub use crate::object::{Entry, Key, Robj};
pub use crate::util::crc16::{SLOT_COUNT, key_hash_slot};
use crate::util::{FoldMap, fold_map};

/// `SELECT 0..15`. Overridable in config, but 16 is the Redis default and the
/// number every client assumes.
pub const DEFAULT_DATABASES: usize = 16;

/// The keyspace dictionary. `hashbrown` + `foldhash` (§5.3).
pub type Dict = FoldMap<Key, Entry>;
/// Key -> absolute expiry in ms. Mirrors `Entry::expire_at_ms` so that the
/// active-expire cycle (W1c) can sample TTL'd keys without walking the dict.
pub type ExpireIndex = FoldMap<Key, u64>;

/// One command's worth of propagation, destined for AOF and (later)
/// replication. See §7.
#[derive(Debug, Clone)]
pub struct Propagation {
    /// The database the command applied to.
    pub db: usize,
    /// The command, already in its deterministic form.
    pub argv: SmallVec<[Bytes; 8]>,
}

/// A single logical database inside a shard.
///
/// Note that this is *a slice of* database `n`, not the whole of it: database
/// `n` is the union of `shard[i].dbs[n]` over every shard.
pub struct Db {
    /// Database index, 0..databases.
    pub index: usize,
    /// The keyspace.
    pub dict: Dict,
    /// Keys that carry a TTL.
    pub expires: ExpireIndex,
    /// Watched keys -> a monotonically increasing touch counter.
    ///
    /// `WATCH` (W3b) records the current counter for a key; `EXEC` re-reads it
    /// and aborts if it moved. Keeping the map here means the cost of WATCH
    /// bookkeeping on the write path is a single `is_empty()` check when
    /// nobody is watching, which is the normal case.
    pub watch: FoldMap<Key, u64>,
    /// Bumped on every `signal_watch`, so the counter is globally increasing
    /// within the shard and cannot ABA.
    watch_seq: u64,
}

impl Db {
    fn new(index: usize) -> Self {
        Db {
            index,
            dict: fold_map(),
            expires: fold_map(),
            watch: fold_map(),
            watch_seq: 0,
        }
    }

    /// `DBSIZE` for this slice. Includes logically-expired-but-not-yet-reaped
    /// keys, matching Redis, which also reports them until the active expire
    /// cycle catches up.
    #[inline]
    pub fn len(&self) -> usize {
        self.dict.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }

    /// Number of keys carrying a TTL.
    #[inline]
    pub fn expires_len(&self) -> usize {
        self.expires.len()
    }

    /// Insert or replace, keeping the expiry index in sync.
    pub fn set_entry(&mut self, key: Key, entry: Entry) {
        match entry.expire_at_ms {
            Some(at) => {
                self.expires.insert(key.clone(), at);
            }
            None => {
                self.expires.remove(&key);
            }
        }
        self.dict.insert(key, entry);
    }

    /// Remove a key from both the dict and the expiry index.
    pub fn remove_entry(&mut self, key: &Key) -> Option<Entry> {
        self.expires.remove(key);
        self.dict.remove(key)
    }

    /// Set or clear a key's TTL, keeping both structures in sync. Returns
    /// false if the key does not exist.
    pub fn set_expire(&mut self, key: &Key, at_ms: Option<u64>) -> bool {
        let Some(entry) = self.dict.get_mut(key) else {
            return false;
        };
        entry.expire_at_ms = at_ms;
        match at_ms {
            Some(at) => {
                self.expires.insert(key.clone(), at);
            }
            None => {
                self.expires.remove(key);
            }
        }
        true
    }

    /// True when the key exists but is logically expired at `now_ms`.
    #[inline]
    pub fn is_expired(&self, key: &Key, now_ms: u64) -> bool {
        self.dict.get(key).is_some_and(|e| e.is_expired(now_ms))
    }

    /// Bump the WATCH counter for `key`, if anybody is watching it.
    ///
    /// Cheap by design: one branch when the watch map is empty.
    #[inline]
    pub fn signal_watch(&mut self, key: &Key) {
        if self.watch.is_empty() {
            return;
        }
        self.watch_seq += 1;
        let seq = self.watch_seq;
        if let Some(v) = self.watch.get_mut(key) {
            *v = seq;
        }
    }

    /// Register interest in `key` and return the current counter. `WATCH`.
    pub fn watch_key(&mut self, key: &Key) -> u64 {
        *self.watch.entry(key.clone()).or_insert(0)
    }

    /// Current counter for `key`, or `None` when nobody registered it.
    #[inline]
    pub fn watch_version(&self, key: &Key) -> Option<u64> {
        self.watch.get(key).copied()
    }

    /// Drop a watch registration. `UNWATCH` / client teardown.
    pub fn unwatch_key(&mut self, key: &Key) {
        self.watch.remove(key);
    }

    /// `FLUSHDB` for this slice. Every watcher is invalidated.
    pub fn clear(&mut self) {
        if !self.watch.is_empty() {
            self.watch_seq += 1;
            let seq = self.watch_seq;
            for v in self.watch.values_mut() {
                *v = seq;
            }
        }
        self.dict.clear();
        self.expires.clear();
    }
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("index", &self.index)
            .field("keys", &self.dict.len())
            .field("expires", &self.expires.len())
            .finish()
    }
}

/// A shard: everything behind one mutex.
#[derive(Debug)]
pub struct Shard {
    /// This shard's index in the shard array.
    pub index: usize,
    /// One slice per logical database.
    pub dbs: Box<[Db]>,
    /// Modifications since startup. Read by `INFO`'s `rdb_changes_since_...`
    /// and by the AOF/RDB triggers.
    pub dirty: u64,
    /// Commands awaiting a drain by the propagation aggregator (§7).
    pub repl_buf: Vec<Propagation>,
    /// Per-shard sequence number stamped on propagated commands. Ordering is
    /// total within a shard; across shards it is causally consistent, which is
    /// all AOF needs because cross-shard commands hold both locks.
    pub seq: u64,
}

impl Shard {
    fn new(index: usize, databases: usize) -> Self {
        let dbs = (0..databases).map(Db::new).collect::<Vec<_>>();
        Shard {
            index,
            dbs: dbs.into_boxed_slice(),
            dirty: 0,
            repl_buf: Vec::new(),
            seq: 0,
        }
    }

    /// Database slice `n`, or `None` if `n` is out of range. Handlers should
    /// never see `None`: `SELECT` validates the index.
    #[inline]
    pub fn db(&mut self, n: usize) -> Option<&mut Db> {
        self.dbs.get_mut(n)
    }

    #[inline]
    pub fn db_ref(&self, n: usize) -> Option<&Db> {
        self.dbs.get(n)
    }

    /// Append to the propagation buffer and stamp a shard-local sequence.
    pub fn propagate(&mut self, db: usize, argv: SmallVec<[Bytes; 8]>) {
        self.seq += 1;
        self.repl_buf.push(Propagation { db, argv });
    }

    /// Take everything buffered so far. Called by the aggregator task.
    pub fn drain_propagation(&mut self) -> Vec<Propagation> {
        core::mem::take(&mut self.repl_buf)
    }

    /// Total keys across every database in this shard.
    pub fn key_count(&self) -> usize {
        self.dbs.iter().map(Db::len).sum()
    }
}

/// The lock plus the lock-free statistics that go with it.
///
/// `dirty` and `keys` are mirrored outside the mutex so that `INFO` and the
/// eviction/expiry background tasks can read them without taking a lock and
/// perturbing the hot path.
#[derive(Debug)]
pub struct ShardHandle {
    lock: Mutex<Shard>,
    pub dirty: AtomicU64,
    pub keys: AtomicU64,
    pub expires: AtomicU64,
    /// Bytes of value data, for `maxmemory` accounting (W1c).
    pub mem: AtomicU64,
}

impl ShardHandle {
    /// Lock this shard. **Callers must respect ascending index order** -- see
    /// the deadlock argument in `engine.rs`. Prefer `ShardMap::lock_set`,
    /// which enforces it for you.
    #[inline]
    pub fn lock(&self) -> MutexGuard<'_, Shard> {
        self.lock.lock()
    }

    #[inline]
    pub fn try_lock(&self) -> Option<MutexGuard<'_, Shard>> {
        self.lock.try_lock()
    }

    /// Refresh the lock-free mirrors from a guard the caller already holds.
    #[inline]
    pub fn sync_stats(&self, shard: &Shard) {
        self.dirty.store(shard.dirty, Ordering::Relaxed);
        let mut keys = 0u64;
        let mut expires = 0u64;
        for db in shard.dbs.iter() {
            keys += db.len() as u64;
            expires += db.expires_len() as u64;
        }
        self.keys.store(keys, Ordering::Relaxed);
        self.expires.store(expires, Ordering::Relaxed);
    }
}

/// The shard array.
#[derive(Debug)]
pub struct ShardMap {
    shards: Box<[CachePadded<ShardHandle>]>,
    /// `count - 1`; `count` is always a power of two so this is a mask.
    mask: usize,
    databases: usize,
}

impl ShardMap {
    /// Build `count` shards, each with `databases` database slices.
    ///
    /// `count` is rounded up to a power of two: the `slot & (N - 1)` mapping
    /// depends on it, and a non-power-of-two would bias the distribution.
    pub fn new(count: usize, databases: usize) -> Self {
        let n = count.max(1).next_power_of_two();
        let dbs = databases.max(1);
        let shards: Vec<CachePadded<ShardHandle>> = (0..n)
            .map(|i| {
                CachePadded::new(ShardHandle {
                    lock: Mutex::new(Shard::new(i, dbs)),
                    dirty: AtomicU64::new(0),
                    keys: AtomicU64::new(0),
                    expires: AtomicU64::new(0),
                    mem: AtomicU64::new(0),
                })
            })
            .collect();
        ShardMap {
            shards: shards.into_boxed_slice(),
            mask: n - 1,
            databases: dbs,
        }
    }

    /// Number of shards. Always a power of two.
    #[inline]
    pub fn len(&self) -> usize {
        self.shards.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Number of logical databases.
    #[inline]
    pub fn databases(&self) -> usize {
        self.databases
    }

    #[inline]
    pub fn get(&self, index: usize) -> Option<&ShardHandle> {
        self.shards.get(index).map(|p| &**p)
    }

    pub fn iter(&self) -> impl Iterator<Item = &ShardHandle> {
        self.shards.iter().map(|p| &**p)
    }

    /// `slot & (N - 1)`.
    #[inline]
    pub fn shard_index(&self, key: &[u8]) -> usize {
        (key_hash_slot(key) as usize) & self.mask
    }

    /// Convenience for single-key commands.
    #[inline]
    pub fn shard_for(&self, key: &[u8]) -> Option<&ShardHandle> {
        self.get(self.shard_index(key))
    }

    /// Lock every shard in `set`, in ascending index order.
    ///
    /// This is *the* lock-acquisition primitive. See the deadlock-freedom
    /// argument at the top of `engine.rs`; it is only valid because `set` is
    /// sorted, which [`ShardSet`] guarantees by construction.
    pub fn lock_set(&self, set: &ShardSet) -> ShardGuards<'_> {
        debug_assert!(set.is_sorted_and_unique(), "ShardSet must be sorted+unique");
        let mut guards: SmallVec<[(usize, MutexGuard<'_, Shard>); 4]> = SmallVec::new();
        for idx in set.iter() {
            if let Some(h) = self.get(idx) {
                guards.push((idx, h.lock()));
            }
        }
        ShardGuards {
            map: self,
            guards,
            mask: self.mask,
        }
    }

    /// Lock every shard, ascending. Used by keyspace-wide commands
    /// (`FLUSHALL`, `DBSIZE`, `SWAPDB`).
    pub fn lock_all(&self) -> ShardGuards<'_> {
        let mut guards: SmallVec<[(usize, MutexGuard<'_, Shard>); 4]> = SmallVec::new();
        for (i, h) in self.shards.iter().enumerate() {
            guards.push((i, h.lock()));
        }
        ShardGuards {
            map: self,
            guards,
            mask: self.mask,
        }
    }

    /// Total keys in database `n` across every shard. Takes every lock; not
    /// for the hot path.
    pub fn db_size(&self, n: usize) -> usize {
        self.shards
            .iter()
            .map(|h| h.lock().db_ref(n).map_or(0, Db::len))
            .sum()
    }
}

/// A sorted, de-duplicated set of shard indices.
///
/// The invariant "sorted and unique" is what makes `lock_set` deadlock-free,
/// so it is established once, in [`ShardSet::finish`], and asserted on use.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardSet {
    v: SmallVec<[u16; 8]>,
}

impl ShardSet {
    #[inline]
    pub fn new() -> Self {
        ShardSet::default()
    }

    /// Every shard in the map, already sorted.
    pub fn all(map: &ShardMap) -> Self {
        ShardSet {
            v: (0..map.len() as u16).collect(),
        }
    }

    /// A single shard.
    pub fn single(index: usize) -> Self {
        let mut v = SmallVec::new();
        v.push(index as u16);
        ShardSet { v }
    }

    /// Add a shard index. Call [`ShardSet::finish`] before locking.
    #[inline]
    pub fn insert(&mut self, index: usize) {
        self.v.push(index as u16);
    }

    /// Establish the sorted+unique invariant. Idempotent.
    #[inline]
    pub fn finish(&mut self) {
        // Typical N here is 1-3; sort_unstable on a SmallVec of that size is
        // a couple of compares and never allocates.
        self.v.sort_unstable();
        self.v.dedup();
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.v.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.v.is_empty()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.v.iter().map(|&i| i as usize)
    }

    #[inline]
    pub fn contains(&self, index: usize) -> bool {
        self.v.binary_search(&(index as u16)).is_ok()
    }

    /// Invariant check used by `debug_assert!` in `lock_set` and by tests.
    pub fn is_sorted_and_unique(&self) -> bool {
        self.v.windows(2).all(|w| match w {
            [a, b] => a < b,
            _ => true,
        })
    }
}

/// The shards a command has locked, in the order they were locked.
///
/// A handler reaches its data through this and only through this; it has no
/// way to take another lock, which is what makes the ordering discipline
/// unbreakable from handler code.
pub struct ShardGuards<'a> {
    map: &'a ShardMap,
    guards: SmallVec<[(usize, MutexGuard<'a, Shard>); 4]>,
    mask: usize,
}

impl<'a> ShardGuards<'a> {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.guards.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.guards.len()
    }

    #[inline]
    pub fn map(&self) -> &'a ShardMap {
        self.map
    }

    /// The lowest-indexed locked shard. Propagation for a multi-shard command
    /// is anchored here so that a command appears exactly once in the AOF
    /// stream.
    #[inline]
    pub fn primary_index(&self) -> Option<usize> {
        self.guards.first().map(|(i, _)| *i)
    }

    #[inline]
    pub fn primary(&mut self) -> Option<&mut Shard> {
        self.guards.first_mut().map(|(_, g)| &mut **g)
    }

    /// The locked shard with the given index, if it is in the set.
    #[inline]
    pub fn shard(&mut self, index: usize) -> Option<&mut Shard> {
        self.guards
            .iter_mut()
            .find(|(i, _)| *i == index)
            .map(|(_, g)| &mut **g)
    }

    /// The locked shard that owns `key`.
    ///
    /// Returns `None` when the command failed to declare the key -- which is a
    /// bug in the `CommandSpec`, not a client error. Handlers must treat
    /// `None` as "this key is not mine" and never fall back to locking it.
    #[inline]
    pub fn for_key(&mut self, key: &[u8]) -> Option<&mut Shard> {
        let idx = (key_hash_slot(key) as usize) & self.mask;
        self.shard(idx)
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (usize, &mut Shard)> {
        self.guards.iter_mut().map(|(i, g)| (*i, &mut **g))
    }

    /// Publish per-shard statistics and release. Called by the engine after
    /// the handler returns.
    pub fn release(self) {
        for (i, g) in self.guards.iter() {
            if let Some(h) = self.map.get(*i) {
                h.sync_stats(g);
            }
        }
        // Guards drop here, in reverse acquisition order. Release order is
        // irrelevant to deadlock freedom; only acquisition order matters.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{Entry, Robj};
    use crate::types::string::StrObj;

    fn str_entry(v: &'static str) -> Entry {
        Entry::new(
            Robj::Str(StrObj::from_bytes(Bytes::from_static(v.as_bytes()))),
            None,
            0,
            false,
        )
    }

    #[test]
    fn shard_count_rounds_to_power_of_two() {
        assert_eq!(ShardMap::new(10, 16).len(), 16);
        assert_eq!(ShardMap::new(1, 16).len(), 1);
        assert_eq!(ShardMap::new(0, 16).len(), 1);
        assert_eq!(ShardMap::new(64, 16).len(), 64);
        assert_eq!(ShardMap::new(33, 16).len(), 64);
    }

    #[test]
    fn hash_tags_land_on_one_shard() {
        let map = ShardMap::new(16, 16);
        let a = map.shard_index(b"{user:1}:name");
        let b = map.shard_index(b"{user:1}:email");
        let c = map.shard_index(b"user:1");
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn keys_spread_across_shards() {
        let map = ShardMap::new(16, 16);
        let mut seen = [0usize; 16];
        for i in 0..10_000u32 {
            let k = format!("key:{i}");
            let idx = map.shard_index(k.as_bytes());
            if let Some(c) = seen.get_mut(idx) {
                *c += 1;
            }
        }
        // Uniformity: no shard should be starved or dominant.
        for c in seen {
            assert!(c > 400 && c < 900, "skewed distribution: {seen:?}");
        }
    }

    #[test]
    fn shard_set_is_sorted_and_unique() {
        let mut s = ShardSet::new();
        s.insert(5);
        s.insert(1);
        s.insert(5);
        s.insert(3);
        s.finish();
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![1, 3, 5]);
        assert!(s.is_sorted_and_unique());
        assert!(s.contains(3));
        assert!(!s.contains(4));
    }

    #[test]
    fn db_keeps_expiry_index_in_sync() {
        let mut db = Db::new(0);
        let k = Key::from_static(b"k");
        db.set_entry(k.clone(), str_entry("v"));
        assert_eq!(db.len(), 1);
        assert_eq!(db.expires_len(), 0);

        assert!(db.set_expire(&k, Some(1000)));
        assert_eq!(db.expires_len(), 1);
        assert!(db.is_expired(&k, 1000));
        assert!(!db.is_expired(&k, 999));

        assert!(db.set_expire(&k, None));
        assert_eq!(db.expires_len(), 0);

        db.set_expire(&k, Some(5));
        db.remove_entry(&k);
        assert_eq!(db.len(), 0);
        assert_eq!(db.expires_len(), 0);

        assert!(!db.set_expire(&k, Some(1)));
    }

    #[test]
    fn watch_counter_moves_only_when_watched() {
        let mut db = Db::new(0);
        let k = Key::from_static(b"k");
        // Nobody watching: no bookkeeping at all.
        db.signal_watch(&k);
        assert_eq!(db.watch_version(&k), None);

        let v0 = db.watch_key(&k);
        db.signal_watch(&k);
        let v1 = db.watch_version(&k).unwrap();
        assert_ne!(v0, v1);

        // A different key does not disturb ours.
        db.signal_watch(&Key::from_static(b"other"));
        assert_eq!(db.watch_version(&k), Some(v1));

        db.unwatch_key(&k);
        assert_eq!(db.watch_version(&k), None);
    }

    #[test]
    fn flush_invalidates_watchers() {
        let mut db = Db::new(0);
        let k = Key::from_static(b"k");
        db.set_entry(k.clone(), str_entry("v"));
        let v0 = db.watch_key(&k);
        db.clear();
        assert_eq!(db.len(), 0);
        assert_ne!(db.watch_version(&k), Some(v0));
    }

    #[test]
    fn propagation_buffer_drains() {
        let map = ShardMap::new(4, 16);
        {
            let h = map.get(0).unwrap();
            let mut s = h.lock();
            let mut argv = SmallVec::new();
            argv.push(Bytes::from_static(b"SET"));
            s.propagate(0, argv);
            assert_eq!(s.seq, 1);
        }
        let h = map.get(0).unwrap();
        let mut s = h.lock();
        assert_eq!(s.drain_propagation().len(), 1);
        assert_eq!(s.drain_propagation().len(), 0);
    }

    #[test]
    fn guards_route_keys_to_the_right_shard() {
        let map = ShardMap::new(16, 16);
        let key = Bytes::from_static(b"hello");
        let idx = map.shard_index(&key);
        let mut set = ShardSet::single(idx);
        set.finish();
        let mut guards = map.lock_set(&set);
        assert_eq!(guards.len(), 1);
        assert_eq!(guards.primary_index(), Some(idx));
        let shard = guards
            .for_key(&key)
            .expect("key must route to a held shard");
        assert_eq!(shard.index, idx);
        // A key on another shard is not reachable from here.
        let mut other = 0;
        while other == idx {
            other += 1;
        }
        assert!(guards.shard(other).is_none());
        guards.release();
    }

    #[test]
    fn lock_all_covers_everything() {
        let map = ShardMap::new(8, 16);
        let mut g = map.lock_all();
        assert_eq!(g.len(), 8);
        for i in 0..8 {
            assert!(g.shard(i).is_some());
        }
        g.release();
    }

    #[test]
    fn stats_mirror_the_locked_state() {
        let map = ShardMap::new(4, 16);
        let key = Key::from_static(b"hello");
        let idx = map.shard_index(&key);
        let mut set = ShardSet::single(idx);
        set.finish();
        {
            let mut g = map.lock_set(&set);
            let shard = g.for_key(&key).unwrap();
            shard.db(0).unwrap().set_entry(key.clone(), str_entry("v"));
            shard.dirty += 1;
            g.release();
        }
        let h = map.get(idx).unwrap();
        assert_eq!(h.keys.load(Ordering::Relaxed), 1);
        assert_eq!(h.dirty.load(Ordering::Relaxed), 1);
        assert_eq!(map.db_size(0), 1);
    }

    #[test]
    fn databases_are_independent() {
        let map = ShardMap::new(2, 16);
        let key = Key::from_static(b"k");
        let idx = map.shard_index(&key);
        let h = map.get(idx).unwrap();
        let mut s = h.lock();
        s.db(0).unwrap().set_entry(key.clone(), str_entry("a"));
        assert_eq!(s.db(0).unwrap().len(), 1);
        assert_eq!(s.db(1).unwrap().len(), 0);
        assert!(s.db(16).is_none());
    }

    #[test]
    fn fold_map_helper_is_usable() {
        let mut m: FoldMap<u32, u32> = crate::util::fold_map_with_capacity(8);
        m.insert(1, 2);
        assert_eq!(m.get(&1), Some(&2));
    }
}
