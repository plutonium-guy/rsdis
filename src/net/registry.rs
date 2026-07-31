//! The connection registry behind `CLIENT LIST`, `CLIENT INFO` and
//! `CLIENT KILL`.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! # Why this exists
//!
//! `ClientHandle` in the frozen `src/ctx.rs` carries `id`, `addr`, `laddr`,
//! `name`, `created_ms` and the out-of-band sender -- and nothing else. That
//! is enough for pub/sub delivery, which is what F0 needed it for, but
//! `CLIENT LIST` has to report `db`, `resp`, `flags`, `sub`, `multi`, `qbuf`,
//! `obl`, `oll`, `omem`, `tot-net-in`, `tot-net-out`, `cmd`, `lib-name` and
//! `lib-ver` for **every** connection, not just the one asking. None of that
//! is reachable from a `ClientHandle`, and `ctx.rs` cannot gain a field.
//!
//! Rather than emit a `CLIENT LIST` full of zeroes for every peer -- which
//! real tooling (`redis-cli --stat`, RedisInsight, `redis_exporter`) parses
//! and would silently misreport -- each connection publishes a
//! [`ConnSnapshot`] here, refreshed once per read batch. One uncontended
//! `parking_lot` lock per batch is far below the cost of the syscalls in the
//! same batch.
//!
//! Reported as a contract gap: the right fix is a wave-owned slot on
//! `ClientHandle`, exactly like §9.2 gives `ClientState` and `ServerShared`.
//!
//! # Keying
//!
//! The table is process-wide because it cannot hang off `ServerShared`. Keys
//! are therefore `(server identity, client id)`: client ids restart at 1 for
//! each `ServerShared`, and the test suite runs many servers in one process.
//! The server's identity is its address, which is stable for the lifetime of
//! the `Arc` and is only ever compared, never dereferenced.

use std::sync::LazyLock;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::ctx::{ClientFlags, ClientState, OutOfBand, ServerShared};
use crate::util::{FoldMap, fold_map};

/// A connection's publicly visible state, as `CLIENT LIST` reports it.
#[derive(Debug, Clone)]
pub struct ConnSnapshot {
    pub id: u64,
    pub addr: String,
    pub laddr: String,
    pub fd: i32,
    pub name: Bytes,
    pub created_ms: u64,
    pub last_interaction_ms: u64,
    pub db: usize,
    pub resp: u8,
    pub flags: ClientFlags,
    /// Channel / pattern / shard-channel subscription counts (W3b fills these
    /// in through `ClientSubs`; today `ClientSubs::count()` is the only signal
    /// available and it reports the total).
    pub sub: usize,
    pub psub: usize,
    pub ssub: usize,
    /// Queued commands inside `MULTI`, or -1 when not in a transaction.
    pub multi: i64,
    pub watch: usize,
    /// Unparsed bytes in the read buffer, and its spare capacity.
    pub qbuf: usize,
    pub qbuf_free: usize,
    pub argv_mem: usize,
    pub multi_mem: usize,
    pub tot_net_in: u64,
    pub tot_net_out: u64,
    /// Read buffer size and peak, matching Redis's `rbs`/`rbp`.
    pub rbs: usize,
    pub rbp: usize,
    /// Output list length and memory, matching Redis's `obl`/`oll`/`omem`.
    pub obl: usize,
    pub oll: usize,
    pub omem: usize,
    pub tot_mem: usize,
    pub last_command: &'static str,
    pub lib_name: Bytes,
    pub lib_ver: Bytes,
}

impl ConnSnapshot {
    /// Seed a snapshot from a freshly created connection.
    pub fn new(client: &ClientState, fd: i32) -> Self {
        ConnSnapshot {
            id: client.id,
            addr: client.addr.clone(),
            laddr: client.laddr.clone(),
            fd,
            name: client.name.clone(),
            created_ms: client.created_ms,
            last_interaction_ms: client.last_interaction_ms,
            db: client.db,
            resp: client.proto,
            flags: client.flags,
            sub: 0,
            psub: 0,
            ssub: 0,
            multi: -1,
            watch: 0,
            qbuf: 0,
            qbuf_free: 0,
            argv_mem: 0,
            multi_mem: 0,
            tot_net_in: 0,
            tot_net_out: 0,
            rbs: 0,
            rbp: 0,
            obl: 0,
            oll: 0,
            omem: 0,
            tot_mem: 0,
            last_command: client.last_command,
            lib_name: client.lib_name.clone(),
            lib_ver: client.lib_ver.clone(),
        }
    }

    /// Copy across everything that can change while the connection runs.
    pub fn refresh_from(&mut self, client: &ClientState) {
        self.name = client.name.clone();
        self.last_interaction_ms = client.last_interaction_ms;
        self.db = client.db;
        self.resp = client.proto;
        self.flags = client.flags;
        self.sub = client.subs.count();
        self.last_command = client.last_command;
        self.lib_name = client.lib_name.clone();
        self.lib_ver = client.lib_ver.clone();
    }

    /// Redis's `catClientInfoString()` flag field. `N` when nothing is set.
    pub fn flag_string(&self) -> String {
        let mut s = String::new();
        if self.flags.contains(ClientFlags::UNIX_SOCKET) {
            s.push('U');
        }
        if self.flags.contains(ClientFlags::MULTI) {
            s.push('x');
        }
        if self.flags.contains(ClientFlags::DIRTY_CAS) {
            s.push('d');
        }
        if self.flags.contains(ClientFlags::CLOSE_AFTER_REPLY) {
            s.push('c');
        }
        if self.flags.contains(ClientFlags::MONITOR) {
            s.push('O');
        }
        if self.flags.contains(ClientFlags::NO_EVICT) {
            s.push('e');
        }
        if self.flags.contains(ClientFlags::NO_TOUCH) {
            s.push('T');
        }
        if s.is_empty() {
            s.push('N');
        }
        s
    }

    /// Redis's `events=` field: `r` when readable, `rw` while output is
    /// pending and the write handler is installed.
    pub fn events(&self) -> &'static str {
        if self.oll > 0 || self.obl > 0 {
            "rw"
        } else {
            "r"
        }
    }
}

/// A live connection as the registry sees it.
#[derive(Debug)]
pub struct ConnEntry {
    pub id: u64,
    pub tx: tokio::sync::mpsc::UnboundedSender<OutOfBand>,
    snapshot: Mutex<ConnSnapshot>,
}

impl ConnEntry {
    pub fn new(snapshot: ConnSnapshot, tx: tokio::sync::mpsc::UnboundedSender<OutOfBand>) -> Self {
        ConnEntry {
            id: snapshot.id,
            tx,
            snapshot: Mutex::new(snapshot),
        }
    }

    /// Publish an update. Called once per read batch, not per command.
    pub fn update<F: FnOnce(&mut ConnSnapshot)>(&self, f: F) {
        f(&mut self.snapshot.lock());
    }

    /// A consistent copy for rendering.
    pub fn snapshot(&self) -> ConnSnapshot {
        self.snapshot.lock().clone()
    }

    /// Ask the connection to close. False when it has already gone away.
    pub fn kill(&self) -> bool {
        self.tx.send(OutOfBand::Kill).is_ok()
    }
}

/// `(server identity, client id)`.
type Key = (usize, u64);

static REGISTRY: LazyLock<Mutex<FoldMap<Key, std::sync::Arc<ConnEntry>>>> =
    LazyLock::new(|| Mutex::new(fold_map()));

/// A stable identity for a `ServerShared`, used only for equality.
#[inline]
fn server_key(server: &ServerShared) -> usize {
    std::ptr::from_ref(server) as usize
}

pub fn register(server: &ServerShared, entry: std::sync::Arc<ConnEntry>) {
    REGISTRY
        .lock()
        .insert((server_key(server), entry.id), entry);
}

pub fn unregister(server: &ServerShared, id: u64) {
    REGISTRY.lock().remove(&(server_key(server), id));
}

pub fn get(server: &ServerShared, id: u64) -> Option<std::sync::Arc<ConnEntry>> {
    REGISTRY.lock().get(&(server_key(server), id)).cloned()
}

/// Every live connection on this server, oldest first.
///
/// Allocates; this is the admin path (`CLIENT LIST`, `CLIENT KILL`), never the
/// command hot path.
pub fn snapshot(server: &ServerShared) -> Vec<std::sync::Arc<ConnEntry>> {
    let key = server_key(server);
    let mut v: Vec<_> = REGISTRY
        .lock()
        .iter()
        .filter(|((s, _), _)| *s == key)
        .map(|(_, e)| std::sync::Arc::clone(e))
        .collect();
    v.sort_unstable_by_key(|e| e.id);
    v
}

/// Number of live connections on this server.
pub fn count(server: &ServerShared) -> usize {
    let key = server_key(server);
    REGISTRY.lock().keys().filter(|(s, _)| *s == key).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn entry(
        id: u64,
    ) -> (
        std::sync::Arc<ConnEntry>,
        tokio::sync::mpsc::UnboundedReceiver<OutOfBand>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let client = ClientState::new(id, "127.0.0.1:1".into(), "127.0.0.1:6379".into(), 0, false);
        (
            std::sync::Arc::new(ConnEntry::new(ConnSnapshot::new(&client, 7), tx)),
            rx,
        )
    }

    #[test]
    fn registration_is_scoped_to_one_server() {
        let a = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        let b = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        let (e1, _r1) = entry(1);
        let (e2, _r2) = entry(1);
        register(&a, e1);
        register(&b, e2);
        assert_eq!(count(&a), 1);
        assert_eq!(count(&b), 1);
        assert!(get(&a, 1).is_some());
        unregister(&a, 1);
        assert_eq!(count(&a), 0);
        assert_eq!(count(&b), 1, "the other server must be untouched");
        unregister(&b, 1);
    }

    #[test]
    fn snapshots_are_ordered_by_id() {
        let s = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        let mut keep = Vec::new();
        for id in [3u64, 1, 2] {
            let (e, r) = entry(id);
            keep.push(r);
            register(&s, e);
        }
        let ids: Vec<u64> = snapshot(&s).iter().map(|e| e.id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        for id in [1, 2, 3] {
            unregister(&s, id);
        }
    }

    #[test]
    fn kill_reaches_the_connection() {
        let (e, mut rx) = entry(1);
        assert!(e.kill());
        assert!(matches!(rx.try_recv(), Ok(OutOfBand::Kill)));
    }

    #[test]
    fn flag_string_matches_redis_spelling() {
        let client = ClientState::new(1, "a".into(), "b".into(), 0, false);
        let mut snap = ConnSnapshot::new(&client, 1);
        assert_eq!(snap.flag_string(), "N");
        snap.flags |= ClientFlags::MULTI;
        assert_eq!(snap.flag_string(), "x");
        snap.flags |= ClientFlags::DIRTY_CAS;
        assert_eq!(snap.flag_string(), "xd");
        snap.flags |= ClientFlags::NO_TOUCH;
        assert_eq!(snap.flag_string(), "xdT");
    }
}
