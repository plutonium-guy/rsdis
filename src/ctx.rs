//! The handler-facing API, plus the two structures it hangs off:
//! [`ServerShared`] (process-wide) and [`ClientState`] (per connection).
//!
//! Owner: F0. **FROZEN.**
//!
//! §3 gives no home for `ServerShared` and `ClientState` even though §4.5
//! names both, so they live here, next to the `Ctx` that borrows them. Because
//! this file is frozen, every field a later wave needs must already exist --
//! which is why the wave-specific state is reached through a type that wave
//! owns:
//!
//! | field                       | defined in            | owner |
//! |-----------------------------|-----------------------|-------|
//! | `ServerShared::pubsub`      | `src/pubsub.rs`       | W3b   |
//! | `ServerShared::blocking`    | `src/blocking.rs`     | W3b   |
//! | `ServerShared::slowlog`     | `src/slowlog.rs`      | W3c   |
//! | `ServerShared::stats`       | `src/info.rs`         | W3c   |
//! | `ClientState::subs`         | `src/pubsub.rs`       | W3b   |
//! | `ClientState::block`        | `src/blocking.rs`     | W3b   |
//! | `ClientState::multi`        | `src/command/transaction.rs` | W3b |
//!
//! Add fields to *those* types. Nobody needs to edit this file.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bitflags::bitflags;
use bytes::Bytes;
use parking_lot::Mutex;
use smallvec::SmallVec;

use crate::blocking::{BlockKind, BlockRequest, BlockingRegistry, ClientBlockState};
use crate::command::{ArgVec, CommandTable};
use crate::config::{Config, ConfigHandle};
use crate::error::CmdError;
use crate::info::Stats;
use crate::notify::NotifyClass;
use crate::object::{Entry, Key, Robj, lru};
use crate::pubsub::{ClientSubs, PubSub};
use crate::reply::{RESP2, ReplyWriter};
use crate::shard::{Db, ShardGuards, ShardMap};
use crate::slowlog::SlowLog;
use crate::types::hash::HashObj;
use crate::types::list::ListObj;
use crate::types::set::SetObj;
use crate::types::stream::StreamObj;
use crate::types::string::StrObj;
use crate::types::zset::ZSetObj;
use crate::util::{FoldMap, fold_map};

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

/// A coarse clock, updated by a 1 ms background ticker (§5.6).
///
/// Handlers never call `SystemTime::now()`; they read `Ctx::now_ms`, which is
/// sampled once per command. `refresh()` exists so the connection loop can
/// re-anchor at the top of each read batch, which keeps expiry correct even if
/// the ticker task is starved.
#[derive(Debug)]
pub struct Clock {
    ms: AtomicU64,
}

impl Clock {
    pub fn new() -> Self {
        let c = Clock {
            ms: AtomicU64::new(0),
        };
        c.refresh();
        c
    }

    /// The cached time. One relaxed atomic load.
    #[inline]
    pub fn now_ms(&self) -> u64 {
        self.ms.load(Ordering::Relaxed)
    }

    /// Re-read the system clock and publish it.
    #[inline]
    pub fn refresh(&self) -> u64 {
        let now = Self::precise_ms();
        self.ms.store(now, Ordering::Relaxed);
        now
    }

    /// An uncached read. Only the ticker, `TIME` and tests should need this.
    #[inline]
    pub fn precise_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Client registry
// ---------------------------------------------------------------------------

/// A message delivered to a connection from outside its own read loop.
#[derive(Debug, Clone)]
pub enum OutOfBand {
    /// A pre-encoded frame to write verbatim (pub/sub delivery, keyspace
    /// notifications, invalidation pushes).
    Frame(Bytes),
    /// Wake a blocked client. `error` distinguishes `CLIENT UNBLOCK ... ERROR`
    /// from `... TIMEOUT`.
    Unblock { error: bool },
    /// `CLIENT KILL`.
    Kill,
}

/// The registry's view of a live connection. `CLIENT LIST`/`CLIENT KILL`
/// (W3c) and pub/sub delivery (W3b) both go through this.
#[derive(Debug)]
pub struct ClientHandle {
    pub id: u64,
    pub addr: String,
    pub laddr: String,
    pub name: Mutex<Bytes>,
    pub tx: tokio::sync::mpsc::UnboundedSender<OutOfBand>,
    pub created_ms: u64,
}

/// All live connections, by id.
#[derive(Debug, Default)]
pub struct ClientRegistry {
    inner: Mutex<FoldMap<u64, Arc<ClientHandle>>>,
}

impl ClientRegistry {
    pub fn new() -> Self {
        ClientRegistry {
            inner: Mutex::new(fold_map()),
        }
    }

    pub fn insert(&self, h: Arc<ClientHandle>) {
        self.inner.lock().insert(h.id, h);
    }

    pub fn remove(&self, id: u64) {
        self.inner.lock().remove(&id);
    }

    pub fn get(&self, id: u64) -> Option<Arc<ClientHandle>> {
        self.inner.lock().get(&id).cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of every handle. Allocates; admin path only.
    pub fn snapshot(&self) -> Vec<Arc<ClientHandle>> {
        self.inner.lock().values().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// ServerShared
// ---------------------------------------------------------------------------

/// Everything shared by every connection. Held as an `Arc`.
#[derive(Debug)]
pub struct ServerShared {
    /// Hot-swappable configuration (§5: an `ArcSwap`, never a lock).
    pub config: ConfigHandle,
    /// The sharded keyspace.
    pub shards: ShardMap,
    /// The coarse clock.
    pub clock: Clock,
    /// The command table, built once at startup.
    pub commands: CommandTable,
    /// Counters for `INFO`. Owner: W3c.
    pub stats: Stats,
    /// Pub/Sub registry. Owner: W3b.
    pub pubsub: PubSub,
    /// Blocked-client registry. Owner: W3b.
    pub blocking: BlockingRegistry,
    /// `SLOWLOG` ring. Owner: W3c.
    pub slowlog: Mutex<SlowLog>,
    /// Live connections.
    pub clients: ClientRegistry,

    /// `INFO server: run_id`.
    pub run_id: String,
    pub pid: u32,
    pub start_time_ms: u64,

    /// Monotonic client id source. `CLIENT ID`.
    next_client_id: AtomicU64,
    /// Total modifications, mirrored across shards for
    /// `rdb_changes_since_last_save`.
    pub dirty: AtomicU64,
    /// When false, `Ctx::propagate` and default propagation do no work at all.
    /// Set by W3a when AOF is on, and later by replication.
    pub propagation_enabled: AtomicBool,
    /// Set by `SHUTDOWN` and by SIGTERM.
    pub shutting_down: AtomicBool,
}

impl ServerShared {
    /// Build the shared state from a validated configuration.
    pub fn new(cfg: Config) -> Arc<Self> {
        let shard_count = cfg.effective_shard_count();
        let databases = cfg.databases;
        let propagate = cfg.appendonly;
        let clock = Clock::new();
        let now = clock.now_ms();
        Arc::new(ServerShared {
            config: ConfigHandle::new(cfg),
            shards: ShardMap::new(shard_count, databases),
            clock,
            commands: crate::command::build_table(),
            stats: Stats::new(),
            pubsub: PubSub::new(),
            blocking: BlockingRegistry::new(),
            slowlog: Mutex::new(SlowLog::new()),
            clients: ClientRegistry::new(),
            run_id: crate::util::rand::run_id(),
            pid: std::process::id(),
            start_time_ms: now,
            next_client_id: AtomicU64::new(1),
            dirty: AtomicU64::new(0),
            propagation_enabled: AtomicBool::new(propagate),
            shutting_down: AtomicBool::new(false),
        })
    }

    /// A configuration snapshot. Cheap: two relaxed atomics.
    #[inline]
    pub fn config(&self) -> arc_swap::Guard<Arc<Config>> {
        self.config.load()
    }

    #[inline]
    pub fn next_client_id(&self) -> u64 {
        self.next_client_id.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    pub fn is_propagating(&self) -> bool {
        self.propagation_enabled.load(Ordering::Relaxed)
    }

    /// Seconds since startup. `INFO server: uptime_in_seconds`.
    pub fn uptime_secs(&self) -> u64 {
        self.clock.now_ms().saturating_sub(self.start_time_ms) / 1000
    }

    /// Drive the coarse clock. Spawned by `main`; also useful in tests.
    pub fn spawn_clock_ticker(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let server = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                server.clock.refresh();
                if server.shutting_down.load(Ordering::Relaxed) {
                    break;
                }
            }
        })
    }

    /// Spawn every background cycle the server needs: the coarse clock, the
    /// per-shard active-expire tasks, and the eviction task.
    ///
    /// This exists as one call rather than three because forgetting one is
    /// silent and expensive. Without the expire tasks, lazy expiry in
    /// [`Ctx::lookup_read`] is the only reclamation path, so a key that
    /// expires and is never touched again is never freed; without the evict
    /// task, `maxmemory` is not enforced at all. Both failures look exactly
    /// like a working server until memory runs out.
    ///
    /// **Any test harness that cares about expiry or eviction must call this**
    /// rather than `spawn_clock_ticker` alone. The returned guard aborts every
    /// task on drop, so a test does not leak cycles into its neighbours.
    #[must_use = "dropping the guard immediately aborts every background task"]
    pub fn spawn_cron(self: &Arc<Self>) -> CronTasks {
        let mut tasks = vec![self.spawn_clock_ticker()];
        tasks.extend(crate::shard::expire::spawn(self));
        tasks.push(crate::shard::evict::spawn(self));
        CronTasks { tasks }
    }
}

/// Handle to the server's background tasks. Aborts them all when dropped.
pub struct CronTasks {
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl CronTasks {
    /// Number of running background tasks (1 clock + one per shard + 1 evict).
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl Drop for CronTasks {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// ClientState
// ---------------------------------------------------------------------------

bitflags! {
    /// Per-connection flags. Mirrors the `CLIENT_*` flags that matter to us.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct ClientFlags: u32 {
        /// `QUIT`, or a protocol error: flush and hang up.
        const CLOSE_AFTER_REPLY = 1 << 0;
        /// Inside `MULTI`.
        const MULTI             = 1 << 1;
        /// A watched key changed; `EXEC` must abort with a null array.
        const DIRTY_CAS         = 1 << 2;
        /// A queued command failed to queue; `EXEC` must abort with EXECABORT.
        const DIRTY_EXEC        = 1 << 3;
        /// `MONITOR`.
        const MONITOR           = 1 << 4;
        /// `CLIENT REPLY OFF`.
        const REPLY_OFF         = 1 << 5;
        /// `CLIENT REPLY SKIP` armed for the next command.
        const REPLY_SKIP_NEXT   = 1 << 6;
        /// `CLIENT REPLY SKIP` active for this command.
        const REPLY_SKIP        = 1 << 7;
        /// `CLIENT NO-EVICT`.
        const NO_EVICT          = 1 << 8;
        /// `CLIENT NO-TOUCH`: do not bump LRU/LFU on access.
        const NO_TOUCH          = 1 << 9;
        /// Connection came in over the unix socket.
        const UNIX_SOCKET       = 1 << 10;
    }
}

/// Per-connection state that a handler may read and write.
#[derive(Debug)]
pub struct ClientState {
    pub id: u64,
    /// `CLIENT SETNAME`.
    pub name: Bytes,
    /// Currently selected database (`SELECT`).
    pub db: usize,
    /// Negotiated RESP version: 2 or 3.
    pub proto: u8,
    pub flags: ClientFlags,
    pub authenticated: bool,
    pub addr: String,
    pub laddr: String,
    pub fd: i32,
    pub created_ms: u64,
    pub last_interaction_ms: u64,
    /// `CLIENT SETINFO LIB-NAME` / `LIB-VER`.
    pub lib_name: Bytes,
    pub lib_ver: Bytes,
    /// The command currently executing, for `CLIENT LIST`'s `cmd=` field.
    pub last_command: &'static str,

    /// Pub/Sub subscriptions. Owner: W3b -- extend `ClientSubs`, not this.
    pub subs: ClientSubs,
    /// Blocking state. Owner: W3b -- extend `ClientBlockState`, not this.
    pub block: ClientBlockState,
    /// `MULTI` queue and `WATCH` list. Owner: W3b -- extend `MultiState`.
    pub multi: crate::command::transaction::MultiState,

    /// Out-of-band channel back to this connection's writer.
    pub oob: Option<tokio::sync::mpsc::UnboundedSender<OutOfBand>>,
}

impl ClientState {
    pub fn new(id: u64, addr: String, laddr: String, now_ms: u64, requires_auth: bool) -> Self {
        ClientState {
            id,
            name: Bytes::new(),
            db: 0,
            proto: RESP2,
            flags: ClientFlags::empty(),
            authenticated: !requires_auth,
            addr,
            laddr,
            fd: -1,
            created_ms: now_ms,
            last_interaction_ms: now_ms,
            lib_name: Bytes::new(),
            lib_ver: Bytes::new(),
            last_command: "",
            subs: ClientSubs::new(),
            block: ClientBlockState::new(),
            multi: crate::command::transaction::MultiState::default(),
            oob: None,
        }
    }

    #[inline]
    pub fn should_close(&self) -> bool {
        self.flags.contains(ClientFlags::CLOSE_AFTER_REPLY)
    }

    #[inline]
    pub fn close_after_reply(&mut self) {
        self.flags |= ClientFlags::CLOSE_AFTER_REPLY;
    }

    /// True when this command's reply must be suppressed
    /// (`CLIENT REPLY OFF|SKIP`).
    #[inline]
    pub fn reply_suppressed(&self) -> bool {
        self.flags
            .intersects(ClientFlags::REPLY_OFF | ClientFlags::REPLY_SKIP)
    }
}

// ---------------------------------------------------------------------------
// Ctx
// ---------------------------------------------------------------------------

/// How the current command should be propagated to AOF / replicas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PropMode {
    /// Propagate verbatim if the command is a WRITE and made the shard dirty.
    Default,
    /// The handler called `propagate()`; send exactly what it queued.
    Explicit,
    /// The handler called `propagate_none()`.
    Suppressed,
}

/// Everything a handler is allowed to touch.
///
/// The locked shards are private: a handler physically cannot take another
/// lock, which is what makes the ascending-order discipline of §2.1
/// unbreakable from handler code.
pub struct Ctx<'a> {
    pub out: ReplyWriter<'a>,
    pub client: &'a mut ClientState,
    pub server: &'a ServerShared,
    shards: ShardGuards<'a>,
    /// One clock read per command (§5.6).
    pub now_ms: u64,

    // ---- F0 additions beyond the five fields listed in §4.5. All private;
    // the handler-facing API is unchanged. Reported in the W0 handover.
    /// The config snapshot this command runs against.
    pub(crate) cfg: &'a Config,
    /// Modifications made by this command. Drives default propagation.
    pub(crate) dirty: u64,
    pub(crate) prop: PropMode,
    pub(crate) prop_buf: SmallVec<[ArgVec; 2]>,
    pub(crate) block: Option<BlockRequest>,
    /// Cached `maxmemory-policy.is_lfu()`.
    lfu: bool,
}

impl<'a> Ctx<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        out: ReplyWriter<'a>,
        client: &'a mut ClientState,
        server: &'a ServerShared,
        shards: ShardGuards<'a>,
        cfg: &'a Config,
        now_ms: u64,
    ) -> Self {
        Ctx {
            out,
            client,
            server,
            shards,
            now_ms,
            cfg,
            dirty: 0,
            prop: PropMode::Default,
            prop_buf: SmallVec::new(),
            block: None,
            lfu: cfg.maxmemory_policy.is_lfu(),
        }
    }

    /// The configuration snapshot for this command. Prefer this over
    /// `server.config()` inside a handler: it is already loaded and stable for
    /// the whole command.
    #[inline]
    pub fn config(&self) -> &Config {
        self.cfg
    }

    /// The currently selected database index.
    #[inline]
    pub fn db_index(&self) -> usize {
        self.client.db
    }

    /// The locked shards. For keyspace-wide commands only; single-key handlers
    /// should use the lookup helpers.
    #[inline]
    pub fn shards(&mut self) -> &mut ShardGuards<'a> {
        &mut self.shards
    }

    /// Run `f` over the current database's slice in every locked shard.
    /// `FLUSHDB`, `DBSIZE`, `KEYS` and `RANDOMKEY` are the intended callers,
    /// and they must declare [`CmdFlags::ALL_SHARDS`].
    ///
    /// [`CmdFlags::ALL_SHARDS`]: crate::command::CmdFlags::ALL_SHARDS
    pub fn for_each_db<F: FnMut(&mut Db)>(&mut self, mut f: F) {
        let idx = self.client.db;
        for (_, shard) in self.shards.iter_mut() {
            if let Some(db) = shard.db(idx) {
                f(db);
            }
        }
    }

    // ----------------------------------------------------------- key lookup

    /// Reap `key` if it is logically expired. Returns true if it was reaped.
    ///
    /// This is the lazy half of expiry (§4.5). It runs before every lookup, so
    /// an expired key is indistinguishable from a missing one to a handler.
    fn expire_if_needed(&mut self, key: &Key) -> bool {
        let now = self.now_ms;
        let db_idx = self.client.db;
        let propagating = self.server.is_propagating();

        let Some(shard) = self.shards.for_key(key) else {
            return false;
        };
        let reaped = {
            let Some(db) = shard.db(db_idx) else {
                return false;
            };
            if !db.is_expired(key, now) {
                false
            } else {
                db.remove_entry(key);
                db.signal_watch(key);
                true
            }
        };
        if reaped {
            shard.dirty += 1;
            if propagating {
                let mut argv = ArgVec::new();
                argv.push(Bytes::from_static(b"DEL"));
                argv.push(key.clone());
                shard.propagate(db_idx, argv);
            }
            self.server.dirty.fetch_add(1, Ordering::Relaxed);
            Stats::bump(&self.server.stats.expired_keys);
            // Delivery of the `expired` keyspace event is W1c's; the hook is
            // `crate::notify::dispatch`.
            crate::notify::dispatch(
                self.server,
                self.cfg.notify_keyspace_events,
                NotifyClass::EXPIRED,
                "expired",
                db_idx,
                key,
            );
        }
        reaped
    }

    /// Expiry-aware read. `None` when the key is missing or logically expired.
    /// Bumps LRU/LFU and the keyspace hit/miss counters.
    pub fn lookup_read(&mut self, key: &Key) -> Option<&Robj> {
        self.expire_if_needed(key);
        let now = self.now_ms;
        let lfu = self.lfu;
        let no_touch = self.client.flags.contains(ClientFlags::NO_TOUCH);
        let (log_factor, decay) = (self.cfg.lfu_log_factor, self.cfg.lfu_decay_time);
        let db_idx = self.client.db;

        let hits = &self.server.stats.keyspace_hits;
        let misses = &self.server.stats.keyspace_misses;

        let Some(shard) = self.shards.for_key(key) else {
            Stats::bump(misses);
            return None;
        };
        let Some(db) = shard.db(db_idx) else {
            Stats::bump(misses);
            return None;
        };
        match db.dict.get(key) {
            None => {
                Stats::bump(misses);
                None
            }
            Some(e) => {
                Stats::bump(hits);
                if !no_touch {
                    lru::touch(&e.lru, now, lfu, log_factor, decay);
                }
                Some(&e.obj)
            }
        }
    }

    /// Expiry-aware write access. Bumps dirty (and therefore invalidates
    /// `WATCH`) and touches LRU/LFU, per §4.5.
    ///
    /// Note the consequence: calling this and then *not* modifying anything
    /// still marks the command dirty. Handlers that only read must use
    /// [`Ctx::lookup_read`] or a `*_read` typed helper.
    pub fn lookup_write(&mut self, key: &Key) -> Option<&mut Robj> {
        self.expire_if_needed(key);
        if !self.exists_now(key) {
            return None;
        }
        self.signal_modified(key);

        let now = self.now_ms;
        let lfu = self.lfu;
        let no_touch = self.client.flags.contains(ClientFlags::NO_TOUCH);
        let (log_factor, decay) = (self.cfg.lfu_log_factor, self.cfg.lfu_decay_time);
        let db_idx = self.client.db;
        let db = self.shards.for_key(key)?.db(db_idx)?;
        let e = db.dict.get_mut(key)?;
        if !no_touch {
            lru::touch(&e.lru, now, lfu, log_factor, decay);
        }
        Some(&mut e.obj)
    }

    /// Existence check with no side effects beyond lazy expiry. `EXISTS`.
    pub fn exists(&mut self, key: &Key) -> bool {
        self.expire_if_needed(key);
        self.exists_now(key)
    }

    /// Existence, assuming expiry has already been handled.
    fn exists_now(&mut self, key: &Key) -> bool {
        let db_idx = self.client.db;
        self.shards
            .for_key(key)
            .and_then(|s| s.db(db_idx))
            .is_some_and(|db| db.dict.contains_key(key))
    }

    /// `TYPE`, expiry-aware.
    pub fn type_name(&mut self, key: &Key) -> Option<&'static str> {
        self.lookup_read(key).map(Robj::type_name)
    }

    /// The key's absolute expiry, if any. Expiry-aware.
    pub fn expire_at(&mut self, key: &Key) -> Option<u64> {
        self.expire_if_needed(key);
        let db_idx = self.client.db;
        self.shards
            .for_key(key)
            .and_then(|s| s.db(db_idx))
            .and_then(|db| db.dict.get(key))
            .and_then(|e| e.expire_at_ms)
    }

    /// Set or clear a key's TTL. Returns false when the key is missing.
    pub fn set_expire(&mut self, key: &Key, at_ms: Option<u64>) -> bool {
        self.expire_if_needed(key);
        let db_idx = self.client.db;
        let ok = self
            .shards
            .for_key(key)
            .and_then(|s| s.db(db_idx))
            .is_some_and(|db| db.set_expire(key, at_ms));
        if ok {
            self.signal_modified(key);
        }
        ok
    }

    /// Create or replace a key.
    pub fn insert(&mut self, key: Key, obj: Robj, expire_at_ms: Option<u64>) {
        let now = self.now_ms;
        let lfu = self.lfu;
        let db_idx = self.client.db;
        self.dirty += 1;
        self.server.dirty.fetch_add(1, Ordering::Relaxed);
        if let Some(shard) = self.shards.for_key(&key) {
            shard.dirty += 1;
            if let Some(db) = shard.db(db_idx) {
                db.signal_watch(&key);
                db.set_entry(key, Entry::new(obj, expire_at_ms, now, lfu));
            }
        }
    }

    /// Delete a key. Returns true if a live key was removed.
    pub fn remove(&mut self, key: &Key) -> bool {
        self.expire_if_needed(key);
        let db_idx = self.client.db;
        let removed = {
            let Some(shard) = self.shards.for_key(key) else {
                return false;
            };
            let gone = shard
                .db(db_idx)
                .is_some_and(|db| db.remove_entry(key).is_some());
            if gone {
                shard.dirty += 1;
                if let Some(db) = shard.db(db_idx) {
                    db.signal_watch(key);
                }
            }
            gone
        };
        if removed {
            self.dirty += 1;
            self.server.dirty.fetch_add(1, Ordering::Relaxed);
        }
        removed
    }

    // -------------------------------------------------- typed accessors

    /// Peek at a key's type discriminant without touching LRU or dirty.
    fn peek_type(&mut self, key: &Key) -> Option<u8> {
        let db_idx = self.client.db;
        self.shards
            .for_key(key)
            .and_then(|s| s.db(db_idx))
            .and_then(|db| db.dict.get(key))
            .map(|e| e.obj.type_id())
    }

    fn touch_for_write(&mut self, key: &Key) {
        let now = self.now_ms;
        let lfu = self.lfu;
        let no_touch = self.client.flags.contains(ClientFlags::NO_TOUCH);
        let (log_factor, decay) = (self.cfg.lfu_log_factor, self.cfg.lfu_decay_time);
        let db_idx = self.client.db;
        if let Some(e) = self
            .shards
            .for_key(key)
            .and_then(|s| s.db(db_idx))
            .and_then(|db| db.dict.get_mut(key))
            && !no_touch
        {
            lru::touch(&e.lru, now, lfu, log_factor, decay);
        }
    }
}

/// Generates the type-checked accessors.
///
/// These are the ONLY sanctioned way to obtain a typed object: they return
/// `CmdError::WrongType`, so no handler ever hand-rolls the check and no
/// handler ever gets the error string wrong.
///
/// Two variants per type:
/// * `get_<t>` -- mutable, marks the key dirty and invalidates `WATCH`;
/// * `get_<t>_read` -- shared, no dirty, no `WATCH` invalidation.
///
/// §4.5 only lists the mutable form. The read form is an F0 addition, and a
/// necessary one: without it `LRANGE` would dirty the keys it reads and abort
/// concurrent transactions.
macro_rules! typed_accessors {
    ($( $get:ident / $get_read:ident => $variant:ident($obj:ty) = $id:expr; )*) => {
        impl Ctx<'_> {
            $(
                #[doc = concat!("Mutable, type-checked access. Marks the key dirty.")]
                pub fn $get(&mut self, key: &Key) -> Result<Option<&mut $obj>, CmdError> {
                    self.expire_if_needed(key);
                    match self.peek_type(key) {
                        None => return Ok(None),
                        Some(t) if t == $id => {}
                        Some(_) => return Err(CmdError::WrongType),
                    }
                    self.signal_modified(key);
                    self.touch_for_write(key);
                    let db_idx = self.client.db;
                    match self
                        .shards
                        .for_key(key)
                        .and_then(|s| s.db(db_idx))
                        .and_then(|db| db.dict.get_mut(key))
                        .map(|e| &mut e.obj)
                    {
                        Some(Robj::$variant(o)) => Ok(Some(o)),
                        Some(_) => Err(CmdError::WrongType),
                        None => Ok(None),
                    }
                }

                #[doc = concat!("Shared, type-checked access. Does not dirty the key.")]
                pub fn $get_read(&mut self, key: &Key) -> Result<Option<&$obj>, CmdError> {
                    match self.lookup_read(key) {
                        None => Ok(None),
                        Some(Robj::$variant(o)) => Ok(Some(o)),
                        Some(_) => Err(CmdError::WrongType),
                    }
                }
            )*
        }
    };
}

typed_accessors! {
    get_str    / get_str_read    => Str(StrObj)       = 0;
    get_list   / get_list_read   => List(ListObj)     = 1;
    get_set    / get_set_read    => Set(SetObj)       = 2;
    get_zset   / get_zset_read   => ZSet(ZSetObj)     = 3;
    get_hash   / get_hash_read   => Hash(HashObj)     = 4;
    get_stream / get_stream_read => Stream(StreamObj) = 6;
}

impl<'a> Ctx<'a> {
    // ------------------------------------------------------ side effects

    /// Mark `key` modified: bump the dirty counters and invalidate any
    /// `WATCH` on it.
    pub fn signal_modified(&mut self, key: &Key) {
        self.dirty += 1;
        self.server.dirty.fetch_add(1, Ordering::Relaxed);
        let db_idx = self.client.db;
        if let Some(shard) = self.shards.for_key(key) {
            shard.dirty += 1;
            if let Some(db) = shard.db(db_idx) {
                db.signal_watch(key);
            }
        }
    }

    /// Fire a keyspace notification, subject to `notify-keyspace-events`.
    ///
    /// The gate lives here so that handlers can call `notify()` unconditionally
    /// from day one; delivery is W1c's.
    pub fn notify(&mut self, class: NotifyClass, event: &str, key: &Key) {
        let configured = self.cfg.notify_keyspace_events;
        if !class.is_enabled_by(configured) {
            return;
        }
        let db = self.client.db;
        crate::notify::dispatch(self.server, configured, class, event, db, key);
    }

    /// Override what reaches the AOF and (later) replicas.
    ///
    /// **Every non-deterministic write command must call this** (§4.5):
    /// `SPOP` -> `SREM`, `EXPIRE` -> `PEXPIREAT`, `INCRBYFLOAT` -> `SET`,
    /// `SETEX` -> `SET ... PXAT`, `GETEX` -> `PEXPIREAT`/`PERSIST`. Replaying
    /// the verbatim command would diverge.
    ///
    /// May be called more than once; the calls are propagated in order,
    /// wrapped in a `MULTI`/`EXEC` by the aggregator when there are several.
    pub fn propagate(&mut self, args: &[&[u8]]) {
        if !self.server.is_propagating() {
            self.prop = PropMode::Explicit;
            return;
        }
        let mut argv = ArgVec::new();
        for a in args {
            argv.push(Bytes::copy_from_slice(a));
        }
        self.prop = PropMode::Explicit;
        self.prop_buf.push(argv);
    }

    /// Like [`Ctx::propagate`] but takes arguments that are already `Bytes`,
    /// so nothing is copied.
    pub fn propagate_argv(&mut self, argv: ArgVec) {
        if !self.server.is_propagating() {
            self.prop = PropMode::Explicit;
            return;
        }
        self.prop = PropMode::Explicit;
        self.prop_buf.push(argv);
    }

    /// Suppress propagation of this command entirely.
    pub fn propagate_none(&mut self) {
        self.prop = PropMode::Suppressed;
        self.prop_buf.clear();
    }

    /// Park this client until one of `keys` becomes ready or the timeout
    /// elapses.
    ///
    /// The handler must write **no** reply after calling this. The engine
    /// releases the shard locks and hands the request to the connection layer
    /// (see `crate::blocking`).
    pub fn block_on(&mut self, keys: &[Key], timeout_ms: u64, on: BlockKind) {
        let deadline_ms = if timeout_ms == 0 {
            None
        } else {
            Some(self.now_ms.saturating_add(timeout_ms))
        };
        self.block = Some(BlockRequest {
            kind: on,
            keys: keys.iter().cloned().collect(),
            deadline_ms,
            db: self.client.db,
            extra: SmallVec::new(),
        });
    }

    /// True when the handler asked to block.
    #[inline]
    pub fn is_blocking(&self) -> bool {
        self.block.is_some()
    }

    /// Number of modifications this command made.
    #[inline]
    pub fn dirty_count(&self) -> u64 {
        self.dirty
    }

    /// Consume the context, returning what the engine needs to finish up.
    pub(crate) fn finish(self) -> CtxOutcome<'a> {
        CtxOutcome {
            shards: self.shards,
            dirty: self.dirty,
            prop: self.prop,
            prop_buf: self.prop_buf,
            block: self.block,
        }
    }
}

/// What `engine::dispatch` gets back once the handler has returned.
pub(crate) struct CtxOutcome<'a> {
    pub shards: ShardGuards<'a>,
    pub dirty: u64,
    pub prop: PropMode,
    pub prop_buf: SmallVec<[ArgVec; 2]>,
    pub block: Option<BlockRequest>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MaxmemoryPolicy;
    use crate::shard::ShardSet;
    use crate::types::string::StrObj;
    use bytes::BytesMut;

    /// A harness that locks whatever shards the given keys need, ascending,
    /// and hands back a `Ctx`.
    struct Harness {
        server: Arc<ServerShared>,
        client: ClientState,
        buf: BytesMut,
    }

    impl Harness {
        fn new() -> Self {
            Self::with_config(Config::default())
        }

        fn with_config(cfg: Config) -> Self {
            let cfg = Config {
                shard_count: 4,
                ..cfg
            };
            let server = ServerShared::new(cfg);
            let client = ClientState::new(1, "t".into(), "t".into(), 0, false);
            Harness {
                server,
                client,
                buf: BytesMut::new(),
            }
        }

        fn run<R>(&mut self, keys: &[&[u8]], now_ms: u64, f: impl FnOnce(&mut Ctx<'_>) -> R) -> R {
            let mut set = ShardSet::new();
            for k in keys {
                set.insert(self.server.shards.shard_index(k));
            }
            set.finish();
            // `cfg` must outlive `guards`: both are borrowed by the Ctx and
            // locals drop in reverse declaration order.
            let cfg = self.server.config.snapshot();
            let guards = self.server.shards.lock_set(&set);
            let out = ReplyWriter::new(&mut self.buf, RESP2);
            let mut ctx = Ctx::new(out, &mut self.client, &self.server, guards, &cfg, now_ms);
            let r = f(&mut ctx);
            ctx.finish().shards.release();
            r
        }
    }

    fn s(v: &'static [u8]) -> Robj {
        Robj::Str(StrObj::from_bytes(Bytes::from_static(v)))
    }

    #[test]
    fn insert_lookup_remove() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| {
            assert!(c.lookup_read(&k).is_none());
            c.insert(k.clone(), s(b"v"), None);
            assert_eq!(c.lookup_read(&k).map(Robj::type_name), Some("string"));
            assert!(c.exists(&k));
            assert!(c.remove(&k));
            assert!(!c.remove(&k));
            assert!(!c.exists(&k));
        });
    }

    #[test]
    fn lazy_expiry_hides_and_reaps() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 1_000, |c| {
            c.insert(k.clone(), s(b"v"), Some(2_000));
            assert!(c.lookup_read(&k).is_some());
        });
        // Now past the deadline: the key reads as missing...
        h.run(&[b"k"], 2_000, |c| {
            assert!(c.lookup_read(&k).is_none());
        });
        // ...and was physically removed on that access.
        let idx = h.server.shards.shard_index(b"k");
        let handle = h.server.shards.get(idx).unwrap();
        assert_eq!(handle.lock().db_ref(0).unwrap().len(), 0);
    }

    #[test]
    fn expiry_boundary_is_inclusive() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| c.insert(k.clone(), s(b"v"), Some(1_000)));
        h.run(&[b"k"], 999, |c| assert!(c.lookup_read(&k).is_some()));
        h.run(&[b"k"], 1_000, |c| assert!(c.lookup_read(&k).is_none()));
    }

    #[test]
    fn expired_key_is_invisible_to_every_accessor() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| c.insert(k.clone(), s(b"v"), Some(10)));
        h.run(&[b"k"], 100, |c| {
            assert!(!c.exists(&k));
            assert!(c.type_name(&k).is_none());
            assert!(c.expire_at(&k).is_none());
            assert!(c.get_str_read(&k).unwrap().is_none());
            assert!(c.lookup_write(&k).is_none());
            assert!(!c.set_expire(&k, Some(9999)));
        });
    }

    #[test]
    fn wrong_type_is_reported_by_typed_accessors() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| {
            c.insert(k.clone(), s(b"v"), None);
            assert!(c.get_str_read(&k).unwrap().is_some());
            assert!(matches!(c.get_list_read(&k), Err(CmdError::WrongType)));
            assert!(matches!(c.get_hash(&k), Err(CmdError::WrongType)));
            assert!(matches!(c.get_zset(&k), Err(CmdError::WrongType)));
            assert!(matches!(c.get_set(&k), Err(CmdError::WrongType)));
            assert!(matches!(c.get_stream(&k), Err(CmdError::WrongType)));
        });
    }

    #[test]
    fn wrong_type_does_not_dirty_the_key() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| {
            c.insert(k.clone(), s(b"v"), None);
            let before = c.dirty_count();
            assert!(c.get_list(&k).is_err());
            assert_eq!(c.dirty_count(), before, "a WRONGTYPE must not dirty");
        });
    }

    #[test]
    fn read_accessor_does_not_dirty_but_write_accessor_does() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| {
            c.insert(k.clone(), s(b"v"), None);
            let after_insert = c.dirty_count();
            assert!(c.get_str_read(&k).unwrap().is_some());
            assert_eq!(c.dirty_count(), after_insert);
            assert!(c.get_str(&k).unwrap().is_some());
            assert_eq!(c.dirty_count(), after_insert + 1);
        });
    }

    #[test]
    fn lru_is_bumped_on_read() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| c.insert(k.clone(), s(b"v"), None));
        let idx = h.server.shards.shard_index(b"k");
        let before = {
            let handle = h.server.shards.get(idx).unwrap();
            let g = handle.lock();
            g.db_ref(0)
                .unwrap()
                .dict
                .get(&k)
                .unwrap()
                .lru
                .load(Ordering::Relaxed)
        };
        h.run(&[b"k"], 60_000, |c| {
            assert!(c.lookup_read(&k).is_some());
        });
        let after = {
            let handle = h.server.shards.get(idx).unwrap();
            let g = handle.lock();
            g.db_ref(0)
                .unwrap()
                .dict
                .get(&k)
                .unwrap()
                .lru
                .load(Ordering::Relaxed)
        };
        assert_ne!(before, after, "lru clock must advance on access");
    }

    #[test]
    fn lfu_counter_is_bumped_on_read() {
        let cfg = Config {
            maxmemory_policy: MaxmemoryPolicy::AllkeysLfu,
            ..Default::default()
        };
        let mut h = Harness::with_config(cfg);
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| {
            c.insert(k.clone(), s(b"v"), None);
            for _ in 0..10_000 {
                let _ = c.lookup_read(&k);
            }
        });
        let idx = h.server.shards.shard_index(b"k");
        let handle = h.server.shards.get(idx).unwrap();
        let g = handle.lock();
        let raw = g
            .db_ref(0)
            .unwrap()
            .dict
            .get(&k)
            .unwrap()
            .lru
            .load(Ordering::Relaxed);
        assert!(lru::lfu_counter(raw) > 5, "lfu counter must climb");
    }

    #[test]
    fn no_touch_flag_freezes_lru() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| c.insert(k.clone(), s(b"v"), None));
        h.client.flags |= ClientFlags::NO_TOUCH;
        let idx = h.server.shards.shard_index(b"k");
        let read = |h: &Harness| {
            let handle = h.server.shards.get(idx).unwrap();
            let g = handle.lock();
            g.db_ref(0)
                .unwrap()
                .dict
                .get(&Key::from_static(b"k"))
                .unwrap()
                .lru
                .load(Ordering::Relaxed)
        };
        let before = read(&h);
        h.run(&[b"k"], 600_000, |c| {
            let _ = c.lookup_read(&k);
        });
        assert_eq!(before, read(&h));
    }

    #[test]
    fn databases_are_isolated() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| c.insert(k.clone(), s(b"a"), None));
        h.client.db = 1;
        h.run(&[b"k"], 0, |c| {
            assert!(c.lookup_read(&k).is_none());
            c.insert(k.clone(), s(b"b"), None);
        });
        h.client.db = 0;
        h.run(&[b"k"], 0, |c| {
            assert_eq!(
                c.get_str_read(&k).unwrap().and_then(|o| o.as_slice()),
                Some(&b"a"[..])
            );
        });
    }

    #[test]
    fn watch_is_invalidated_by_writes() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        let idx = h.server.shards.shard_index(b"k");
        let v0 = {
            let handle = h.server.shards.get(idx).unwrap();
            let mut g = handle.lock();
            g.db(0).unwrap().watch_key(&k)
        };
        h.run(&[b"k"], 0, |c| c.insert(k.clone(), s(b"v"), None));
        let v1 = {
            let handle = h.server.shards.get(idx).unwrap();
            let g = handle.lock();
            g.db_ref(0).unwrap().watch_version(&k).unwrap()
        };
        assert_ne!(v0, v1);
    }

    #[test]
    fn propagation_modes() {
        let mut h = Harness::new();
        h.server.propagation_enabled.store(true, Ordering::Relaxed);
        let k = Key::from_static(b"k");

        // Default: nothing queued; the engine propagates verbatim.
        let out = h.run(&[b"k"], 0, |c| {
            c.insert(k.clone(), s(b"v"), None);
            (c.prop, c.prop_buf.len(), c.dirty)
        });
        assert_eq!(out, (PropMode::Default, 0, 1));

        // Explicit.
        let out = h.run(&[b"k"], 0, |c| {
            c.propagate(&[b"SET", b"k", b"v"]);
            (c.prop, c.prop_buf.len())
        });
        assert_eq!(out, (PropMode::Explicit, 1));

        // Suppressed.
        let out = h.run(&[b"k"], 0, |c| {
            c.propagate(&[b"SET", b"k", b"v"]);
            c.propagate_none();
            (c.prop, c.prop_buf.len())
        });
        assert_eq!(out, (PropMode::Suppressed, 0));
    }

    #[test]
    fn propagation_is_free_when_disabled() {
        let mut h = Harness::new();
        assert!(!h.server.is_propagating());
        let out = h.run(&[b"k"], 0, |c| {
            c.propagate(&[b"SET", b"k", b"v"]);
            c.prop_buf.len()
        });
        assert_eq!(out, 0, "must not allocate when nothing consumes the stream");
    }

    #[test]
    fn lazy_expiry_propagates_a_del() {
        let mut h = Harness::new();
        h.server.propagation_enabled.store(true, Ordering::Relaxed);
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| c.insert(k.clone(), s(b"v"), Some(10)));
        h.run(&[b"k"], 100, |c| assert!(c.lookup_read(&k).is_none()));
        let idx = h.server.shards.shard_index(b"k");
        let handle = h.server.shards.get(idx).unwrap();
        let mut g = handle.lock();
        let drained = g.drain_propagation();
        assert!(
            drained
                .iter()
                .any(|p| p.argv.first().map(|b| &b[..]) == Some(&b"DEL"[..])),
            "expected a synthetic DEL, got {drained:?}"
        );
    }

    #[test]
    fn block_on_records_a_request() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        let req = h.run(&[b"k"], 1_000, |c| {
            c.block_on(std::slice::from_ref(&k), 500, BlockKind::List);
            assert!(c.is_blocking());
            c.block.clone()
        });
        let req = req.unwrap();
        assert_eq!(req.kind, BlockKind::List);
        assert_eq!(req.deadline_ms, Some(1_500));
        assert_eq!(req.db, 0);

        // timeout 0 means "forever".
        let req = h.run(&[b"k"], 1_000, |c| {
            c.block_on(std::slice::from_ref(&k), 0, BlockKind::ZSet);
            c.block.clone()
        });
        assert_eq!(req.unwrap().deadline_ms, None);
    }

    #[test]
    fn keyspace_hit_and_miss_counters() {
        let mut h = Harness::new();
        let k = Key::from_static(b"k");
        h.run(&[b"k"], 0, |c| {
            let _ = c.lookup_read(&k); // miss
            c.insert(k.clone(), s(b"v"), None);
            let _ = c.lookup_read(&k); // hit
        });
        assert_eq!(Stats::get(&h.server.stats.keyspace_misses), 1);
        assert_eq!(Stats::get(&h.server.stats.keyspace_hits), 1);
    }

    #[test]
    fn for_each_db_visits_every_locked_shard() {
        let mut h = Harness::new();
        let mut set = ShardSet::all(&h.server.shards);
        set.finish();
        let cfg = h.server.config.snapshot();
        let guards = h.server.shards.lock_set(&set);
        let out = ReplyWriter::new(&mut h.buf, RESP2);
        let mut ctx = Ctx::new(out, &mut h.client, &h.server, guards, &cfg, 0);
        let mut n = 0;
        ctx.for_each_db(|_| n += 1);
        assert_eq!(n, 4);
        ctx.finish().shards.release();
    }

    #[test]
    fn server_shared_basics() {
        let s = ServerShared::new(Config::default());
        assert_eq!(s.run_id.len(), 40);
        assert!(s.next_client_id() < s.next_client_id());
        assert_eq!(s.shards.databases(), 16);
        assert!(s.shards.len().is_power_of_two());
        assert!(!s.is_propagating());
    }

    #[test]
    fn clock_moves_forward() {
        let c = Clock::new();
        let a = c.now_ms();
        assert!(a > 1_600_000_000_000, "clock looks unset: {a}");
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(c.refresh() >= a);
    }
}
