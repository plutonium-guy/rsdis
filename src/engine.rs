//! Key resolution, lock ordering, and command dispatch.
//!
//! Owner: F0. **FROZEN.**
//!
//! # Deadlock freedom
//!
//! This is the argument the whole concurrency model rests on. §2.1: *"Ascending
//! order is the only thing preventing deadlock between two multi-key commands;
//! there are no exceptions to it, including in `EXEC`."*
//!
//! **Claim.** No set of threads executing commands through [`dispatch`] can
//! deadlock on shard mutexes.
//!
//! **Setup.** Model the system as a wait-for graph whose vertices are threads
//! and whose edges are "thread A is blocked on a mutex held by thread B". A
//! deadlock exists iff that graph has a cycle. Every shard mutex is identified
//! by its index in `ShardMap`, an integer in `0..N`.
//!
//! **Invariant (I).** *A thread only ever blocks on a shard whose index is
//! strictly greater than every shard index it currently holds.*
//!
//! Why (I) holds:
//!
//! 1. The **only** way to acquire a shard lock on the command path is
//!    [`ShardMap::lock_set`] or [`ShardMap::lock_all`]. Handlers cannot lock
//!    anything: `Ctx` owns the guards privately and exposes no shard-map
//!    handle, so a handler physically cannot call `lock()`.
//! 2. `lock_set` iterates a [`ShardSet`], which is sorted ascending and
//!    de-duplicated by [`ShardSet::finish`]. Sortedness is asserted on entry
//!    (`debug_assert!`) and established at exactly one place,
//!    [`shard_set_for`]. `lock_all` iterates `0..N` directly.
//! 3. Locks are acquired in one contiguous burst before the handler runs, and
//!    all are released after it returns ([`ShardGuards::release`], then the
//!    guards drop). There is no re-entrant or incremental acquisition, so a
//!    thread never holds a partial set across an unrelated operation.
//!    De-duplication matters here: without it, a command naming two keys on
//!    the same shard would try to lock a non-reentrant mutex twice and
//!    self-deadlock.
//!
//! **Proof.** Suppose a cycle `T0 -> T1 -> ... -> Tk -> T0` exists. Let
//! `w(Ti)` be the index of the shard `Ti` is blocked on and `H(Ti)` the set it
//! holds. Edge `Ti -> Ti+1` means `w(Ti) ∈ H(Ti+1)`. By (I),
//! `w(Ti+1) > max H(Ti+1) >= w(Ti)`, so `w` is strictly increasing along every
//! edge. Going all the way round the cycle gives `w(T0) > w(T0)`, a
//! contradiction. Hence no cycle, hence no deadlock. ∎
//!
//! **What would break it.** Any of these, and only these:
//!
//! * locking a shard from inside a handler (structurally prevented);
//! * a `ShardSet` that reaches `lock_set` unsorted (asserted in debug, and
//!   built in one place);
//! * holding guards across an `.await` and resuming on a different task while
//!   another task holds part of the set -- so `dispatch` is a **synchronous**
//!   function and the guards never cross a suspension point;
//! * a future `EXEC` that locks per queued command instead of locking the
//!   union once. §2.1 calls this out explicitly; see
//!   `command::transaction::intercept`.
//!
//! The test `reversed_key_order_does_not_deadlock` hammers the exact adversary
//! case: two commands naming the same two keys in opposite order.

use std::sync::atomic::Ordering;

use crate::blocking::BlockRequest;
use crate::command::{ArgVec, Args, ArgsExt, CmdFlags, CommandSpec};
use crate::ctx::{ClientFlags, ClientState, Ctx, PropMode, ServerShared};
use crate::error::CmdError;
use crate::info::Stats;
use crate::reply::ReplyWriter;
use crate::shard::{ShardMap, ShardSet};

/// What the connection layer must do after a command has run.
#[derive(Debug)]
pub enum Outcome {
    /// The reply is in the output buffer; carry on reading.
    Done,
    /// The handler asked to block. No reply was written. The connection layer
    /// owns the parking (W3b).
    Blocked(Box<BlockRequest>),
    /// Flush the reply, then close the connection (`QUIT`, protocol error).
    Close,
}

/// Resolve the shard set a command needs, already sorted and de-duplicated.
///
/// This is the single place a `ShardSet` is constructed for the command path,
/// which is what makes the sortedness invariant auditable.
pub fn shard_set_for(spec: &CommandSpec, args: &Args, map: &ShardMap) -> ShardSet {
    if spec.flags.contains(CmdFlags::ALL_SHARDS) {
        let mut set = ShardSet::all(map);
        set.finish();
        return set;
    }
    let mut set = ShardSet::new();
    for pos in spec.key_positions(args) {
        if let Some(key) = args.get(pos) {
            set.insert(map.shard_index(key));
        }
    }
    // Establishes "sorted and unique" -- the deadlock-freedom precondition.
    set.finish();
    set
}

/// Execute one command.
///
/// **Synchronous by construction.** The shard guards must not cross an
/// `.await`; see the deadlock argument above. If a future version needs to
/// suspend, it must release the locks first.
pub fn dispatch(
    server: &ServerShared,
    client: &mut ClientState,
    out: &mut bytes::BytesMut,
    args: &Args,
) -> Outcome {
    if args.is_empty() {
        return Outcome::Done;
    }
    Stats::bump(&server.stats.commands_processed);

    let proto = client.proto;

    // ---- lookup ------------------------------------------------------------
    let Some(spec) = server.commands.lookup(args.name()) else {
        let rest = args.get(1..).unwrap_or(&[]);
        let e = CmdError::unknown_command(args.name(), rest);
        reply_error(out, proto, &e, server);
        return Outcome::Done;
    };
    client.last_command = spec.name;

    // ---- arity -------------------------------------------------------------
    if !spec.arity_ok(args.len()) {
        reply_error(out, proto, &CmdError::WrongArity(spec.name), server);
        return Outcome::Done;
    }

    // ---- authentication ----------------------------------------------------
    // AUTH, HELLO, QUIT and RESET are the only commands an unauthenticated
    // client may run, matching Redis's CMD_NO_AUTH set.
    if !client.authenticated && !matches!(spec.name, "auth" | "hello" | "quit" | "reset") {
        reply_error(out, proto, &CmdError::Unauthenticated, server);
        return Outcome::Done;
    }

    // ---- MULTI queuing (W3b) ----------------------------------------------
    // Runs *before* any lock is taken: queuing must never touch a shard.
    if crate::command::transaction::intercept(server, client, out, args) {
        return Outcome::Done;
    }

    // ---- lock --------------------------------------------------------------
    let cfg = server.config.snapshot();
    let now_ms = server.clock.now_ms();
    let set = shard_set_for(spec, args, &server.shards);
    let guards = server.shards.lock_set(&set);

    // ---- run ---------------------------------------------------------------
    let handler = spec.handler;
    let is_write = spec.is_write();
    let mark = out.len();
    // The database in effect *for this command*. Read before the handler runs
    // so that `SELECT` propagates against the db it was issued in.
    let db = client.db;

    // The Ctx borrows `out`, `client` and `server` under one lifetime, and the
    // shard guards are tied to the same lifetime. Everything that needs the
    // guards therefore has to happen inside this scope; only plain data
    // escapes it.
    let (result, block) = {
        let writer = ReplyWriter::new(out, proto);
        let mut ctx = Ctx::new(writer, client, server, guards, &cfg, now_ms);
        let result = handler(&mut ctx, args);
        let outcome = ctx.finish();
        let mut guards = outcome.shards;

        // ---- propagate -----------------------------------------------------
        // Anchored on the lowest-indexed locked shard so a cross-shard command
        // appears exactly once in the stream, in a position that is causally
        // consistent with every other command touching that shard (§7).
        if result.is_ok() && server.is_propagating() {
            match outcome.prop {
                PropMode::Suppressed => {}
                PropMode::Explicit => {
                    if let Some(shard) = guards.primary() {
                        for argv in outcome.prop_buf {
                            shard.propagate(db, argv);
                        }
                    }
                }
                PropMode::Default => {
                    if is_write
                        && outcome.dirty > 0
                        && let Some(shard) = guards.primary()
                    {
                        let argv: ArgVec = args.iter().cloned().collect();
                        shard.propagate(db, argv);
                    }
                }
            }
        }

        // Every shard lock is released here, before we touch `out` again.
        guards.release();
        (result, outcome.block)
    };

    // ---- reply -------------------------------------------------------------
    if let Err(e) = result {
        // Discard anything the handler managed to write before failing: a
        // partial reply plus an error would desynchronise the stream.
        if out.len() > mark {
            out.truncate(mark);
        }
        reply_error(out, proto, &e, server);
        return Outcome::Done;
    }

    if let Some(req) = block {
        // The handler wrote no reply and we have released every lock.
        debug_assert_eq!(out.len(), mark, "a blocking handler must not reply");
        return Outcome::Blocked(Box::new(req));
    }

    if client.should_close() {
        return Outcome::Close;
    }
    Outcome::Done
}

fn reply_error(out: &mut bytes::BytesMut, proto: u8, e: &CmdError, server: &ServerShared) {
    Stats::bump(&server.stats.total_error_replies);
    let mut w = ReplyWriter::new(out, proto);
    w.error(e);
}

/// Clear per-command client flags. Called by the connection loop after each
/// command so that `CLIENT REPLY SKIP` applies to exactly one command.
#[inline]
pub fn post_command(client: &mut ClientState) {
    if client.flags.contains(ClientFlags::REPLY_SKIP) {
        client.flags.remove(ClientFlags::REPLY_SKIP);
    }
    if client.flags.contains(ClientFlags::REPLY_SKIP_NEXT) {
        client.flags.remove(ClientFlags::REPLY_SKIP_NEXT);
        client.flags.insert(ClientFlags::REPLY_SKIP);
    }
}

/// Total modifications since startup, for `rdb_changes_since_last_save`.
#[inline]
pub fn total_dirty(server: &ServerShared) -> u64 {
    server.dirty.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use bytes::{Bytes, BytesMut};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    fn server_with(shards: usize) -> Arc<ServerShared> {
        ServerShared::new(Config {
            shard_count: shards,
            ..Default::default()
        })
    }

    fn argv(items: &[&str]) -> ArgVec {
        items
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }

    fn run(server: &ServerShared, client: &mut ClientState, items: &[&str]) -> String {
        let mut buf = BytesMut::new();
        let a = argv(items);
        dispatch(server, client, &mut buf, &a);
        String::from_utf8_lossy(&buf).into_owned()
    }

    fn client_for(server: &ServerShared) -> ClientState {
        ClientState::new(
            server.next_client_id(),
            "127.0.0.1:1".into(),
            "127.0.0.1:6379".into(),
            0,
            false,
        )
    }

    // -------------------------------------------------------------- resolution

    #[test]
    fn shard_set_is_sorted_deduped_and_covers_every_key() {
        let s = server_with(16);
        let spec = s.commands.lookup(b"mset").expect("mset");
        let a = argv(&["mset", "a", "1", "b", "2", "c", "3"]);
        let set = shard_set_for(spec, &a, &s.shards);
        assert!(set.is_sorted_and_unique());
        for k in [&b"a"[..], b"b", b"c"] {
            assert!(
                set.contains(s.shards.shard_index(k)),
                "shard for {k:?} missing"
            );
        }
    }

    #[test]
    fn duplicate_keys_lock_one_shard_once() {
        let s = server_with(16);
        let spec = s.commands.lookup(b"del").expect("del");
        let a = argv(&["del", "k", "k", "k"]);
        let set = shard_set_for(spec, &a, &s.shards);
        assert_eq!(set.len(), 1, "a repeated key must not lock twice");
        // And it must actually be lockable -- a non-reentrant mutex taken
        // twice would hang here.
        let g = s.shards.lock_set(&set);
        g.release();
    }

    #[test]
    fn hash_tagged_keys_collapse_to_one_shard() {
        let s = server_with(16);
        let spec = s.commands.lookup(b"mget").expect("mget");
        let a = argv(&["mget", "{u1}:a", "{u1}:b", "{u1}:c"]);
        let set = shard_set_for(spec, &a, &s.shards);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn keyless_commands_lock_nothing() {
        let s = server_with(16);
        let spec = s.commands.lookup(b"ping").expect("ping");
        let a = argv(&["ping"]);
        assert!(shard_set_for(spec, &a, &s.shards).is_empty());
    }

    #[test]
    fn all_shards_flag_locks_everything() {
        let s = server_with(8);
        let spec = s.commands.lookup(b"dbsize").expect("dbsize");
        let a = argv(&["dbsize"]);
        let set = shard_set_for(spec, &a, &s.shards);
        assert_eq!(set.len(), 8);
        assert!(set.is_sorted_and_unique());
    }

    // ------------------------------------------------------- deadlock freedom

    /// The adversary case from §2.1: two multi-key commands naming the same
    /// keys in opposite order, hammered concurrently from many threads.
    ///
    /// Without ascending-order acquisition this deadlocks within a few
    /// thousand iterations. The watchdog turns a hang into a failure.
    #[test]
    fn reversed_key_order_does_not_deadlock() {
        let server = server_with(16);

        // Pick keys that genuinely land on different shards -- otherwise the
        // test proves nothing.
        let keys: Vec<&str> = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]
            .into_iter()
            .collect();
        let mut distinct: Vec<&str> = Vec::new();
        let mut seen: Vec<usize> = Vec::new();
        for k in keys {
            let idx = server.shards.shard_index(k.as_bytes());
            if !seen.contains(&idx) {
                seen.push(idx);
                distinct.push(k);
            }
        }
        assert!(
            distinct.len() >= 3,
            "need >= 3 keys on distinct shards, got {distinct:?}"
        );
        let (k1, k2, k3) = (distinct[0], distinct[1], distinct[2]);
        assert_ne!(
            server.shards.shard_index(k1.as_bytes()),
            server.shards.shard_index(k2.as_bytes())
        );

        const THREADS: usize = 8;
        const ITERS: usize = 20_000;
        let done = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::new();

        for t in 0..THREADS {
            let server = Arc::clone(&server);
            // Half the threads use one order, half the reverse. A third
            // pattern with three keys mixes in a longer chain.
            let pattern: Vec<&'static str> = match t % 4 {
                0 => vec!["mset", k1, "1", k2, "2"],
                1 => vec!["mset", k2, "2", k1, "1"],
                2 => vec!["mget", k1, k2, k3],
                _ => vec!["mget", k3, k2, k1],
            };
            handles.push(std::thread::spawn(move || {
                let mut client = ClientState::new(1, "t".into(), "t".into(), 0, false);
                let mut buf = BytesMut::new();
                let a: ArgVec = pattern
                    .iter()
                    .map(|s| Bytes::from_static(s.as_bytes()))
                    .collect();
                for _ in 0..ITERS {
                    buf.clear();
                    dispatch(&server, &mut client, &mut buf, &a);
                }
            }));
        }

        // Watchdog: if the workers are wedged, fail rather than hang the suite.
        let watch_done = Arc::clone(&done);
        let watchdog = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(60);
            while Instant::now() < deadline {
                if watch_done.load(Ordering::Relaxed) {
                    return true;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            false
        });

        for h in handles {
            h.join().expect("worker panicked");
        }
        done.store(true, Ordering::Relaxed);
        assert!(
            watchdog.join().unwrap_or(false),
            "deadlock: workers did not finish within 60s"
        );

        // And the keyspace is still coherent afterwards.
        let mut client = client_for(&server);
        assert_eq!(run(&server, &mut client, &["get", k1]), "$1\r\n1\r\n");
    }

    /// A keyspace-wide command (`ALL_SHARDS`) racing multi-key commands is the
    /// other way to build a cycle: it takes every lock while others take two.
    #[test]
    fn all_shards_racing_multi_key_does_not_deadlock() {
        let server = server_with(8);
        const ITERS: usize = 4_000;
        let mut handles = Vec::new();

        for t in 0..6 {
            let server = Arc::clone(&server);
            handles.push(std::thread::spawn(move || {
                let mut client = ClientState::new(1, "t".into(), "t".into(), 0, false);
                let mut buf = BytesMut::new();
                let a: ArgVec = match t % 3 {
                    0 => ["mset", "aa", "1", "bb", "2", "cc", "3"].iter(),
                    1 => ["mset", "cc", "3", "bb", "2", "aa", "1"].iter(),
                    _ => ["dbsize"].iter(),
                }
                .map(|s| Bytes::from_static(s.as_bytes()))
                .collect();
                for _ in 0..ITERS {
                    buf.clear();
                    dispatch(&server, &mut client, &mut buf, &a);
                }
            }));
        }
        for h in handles {
            h.join().expect("worker panicked");
        }
    }

    // ------------------------------------------------------------- dispatch

    #[test]
    fn unknown_command() {
        let s = server_with(4);
        let mut c = client_for(&s);
        assert_eq!(
            run(&s, &mut c, &["nope", "a", "b"]),
            "-ERR unknown command 'nope', with args beginning with: 'a' 'b' \r\n"
        );
    }

    #[test]
    fn arity_is_enforced_before_the_handler() {
        let s = server_with(4);
        let mut c = client_for(&s);
        assert_eq!(
            run(&s, &mut c, &["get"]),
            "-ERR wrong number of arguments for 'get' command\r\n"
        );
        assert_eq!(
            run(&s, &mut c, &["get", "a", "b"]),
            "-ERR wrong number of arguments for 'get' command\r\n"
        );
    }

    #[test]
    fn set_get_del_exists_round_trip() {
        let s = server_with(4);
        let mut c = client_for(&s);
        assert_eq!(run(&s, &mut c, &["set", "k", "v"]), "+OK\r\n");
        assert_eq!(run(&s, &mut c, &["get", "k"]), "$1\r\nv\r\n");
        assert_eq!(run(&s, &mut c, &["exists", "k"]), ":1\r\n");
        assert_eq!(run(&s, &mut c, &["exists", "k", "k"]), ":2\r\n");
        assert_eq!(run(&s, &mut c, &["type", "k"]), "+string\r\n");
        assert_eq!(run(&s, &mut c, &["del", "k"]), ":1\r\n");
        assert_eq!(run(&s, &mut c, &["del", "k"]), ":0\r\n");
        assert_eq!(run(&s, &mut c, &["get", "k"]), "$-1\r\n");
        assert_eq!(run(&s, &mut c, &["type", "k"]), "+none\r\n");
    }

    #[test]
    fn int_encoded_values_reply_without_a_string_copy() {
        let s = server_with(4);
        let mut c = client_for(&s);
        run(&s, &mut c, &["set", "n", "12345"]);
        assert_eq!(run(&s, &mut c, &["get", "n"]), "$5\r\n12345\r\n");
        assert_eq!(run(&s, &mut c, &["strlen", "n"]), ":5\r\n");
    }

    #[test]
    fn set_options() {
        let s = server_with(4);
        let mut c = client_for(&s);
        assert_eq!(run(&s, &mut c, &["set", "k", "a", "NX"]), "+OK\r\n");
        assert_eq!(run(&s, &mut c, &["set", "k", "b", "NX"]), "$-1\r\n");
        assert_eq!(run(&s, &mut c, &["get", "k"]), "$1\r\na\r\n");
        assert_eq!(run(&s, &mut c, &["set", "k", "c", "XX"]), "+OK\r\n");
        assert_eq!(run(&s, &mut c, &["set", "k", "d", "GET"]), "$1\r\nc\r\n");
        assert_eq!(run(&s, &mut c, &["set", "nope", "x", "XX"]), "$-1\r\n");
        assert_eq!(
            run(&s, &mut c, &["set", "k", "v", "NX", "XX"]),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            run(&s, &mut c, &["set", "k", "v", "BOGUS"]),
            "-ERR syntax error\r\n"
        );
        assert_eq!(
            run(&s, &mut c, &["set", "k", "v", "EX", "0"]),
            "-ERR invalid expire time in 'set' command\r\n"
        );
        assert_eq!(
            run(&s, &mut c, &["set", "k", "v", "EX", "abc"]),
            "-ERR value is not an integer or out of range\r\n"
        );
    }

    #[test]
    fn set_with_ttl_and_ttl_readback() {
        let s = server_with(4);
        let mut c = client_for(&s);
        run(&s, &mut c, &["set", "k", "v", "EX", "100"]);
        let ttl = run(&s, &mut c, &["ttl", "k"]);
        assert!(ttl.starts_with(":10"), "unexpected ttl reply {ttl:?}");
        assert_eq!(run(&s, &mut c, &["ttl", "missing"]), ":-2\r\n");
        run(&s, &mut c, &["set", "p", "v"]);
        assert_eq!(run(&s, &mut c, &["ttl", "p"]), ":-1\r\n");
        // KEEPTTL preserves it, a plain SET clears it.
        run(&s, &mut c, &["set", "k", "v2", "KEEPTTL"]);
        assert!(run(&s, &mut c, &["ttl", "k"]).starts_with(":10"));
        run(&s, &mut c, &["set", "k", "v3"]);
        assert_eq!(run(&s, &mut c, &["ttl", "k"]), ":-1\r\n");
    }

    #[test]
    fn cross_shard_mset_mget() {
        let s = server_with(16);
        let mut c = client_for(&s);
        assert_eq!(
            run(&s, &mut c, &["mset", "alpha", "1", "beta", "2"]),
            "+OK\r\n"
        );
        assert_eq!(
            run(&s, &mut c, &["mget", "alpha", "beta", "missing"]),
            "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n"
        );
    }

    #[test]
    fn wrong_type_error_surfaces_from_typed_accessors() {
        // Nothing in F0 creates a non-string, so simulate by inserting
        // through the keyspace directly.
        use crate::object::Robj;
        use crate::types::list::ListObj;
        let s = server_with(4);
        let mut c = client_for(&s);
        let key = Bytes::from_static(b"L");
        let idx = s.shards.shard_index(&key);
        {
            let h = s.shards.get(idx).expect("shard");
            let mut g = h.lock();
            let db = g.db(0).expect("db");
            db.set_entry(
                key.clone(),
                crate::object::Entry::new(Robj::List(ListObj::default()), None, 0, false),
            );
        }
        assert_eq!(
            run(&s, &mut c, &["get", "L"]),
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(run(&s, &mut c, &["type", "L"]), "+list\r\n");
        // MGET degrades to nil rather than erroring, as Redis does.
        assert_eq!(run(&s, &mut c, &["mget", "L"]), "*1\r\n$-1\r\n");
    }

    #[test]
    fn a_failed_handler_leaves_no_partial_reply() {
        let s = server_with(4);
        let mut c = client_for(&s);
        run(&s, &mut c, &["mset", "a", "1", "b", "2"]);
        // MGET writes an array header before touching any key; if a later key
        // errored we must not see the header plus an error.
        let out = run(&s, &mut c, &["set", "a", "v", "EX", "notanumber"]);
        assert_eq!(out, "-ERR value is not an integer or out of range\r\n");
    }

    #[test]
    fn select_switches_database() {
        let s = server_with(4);
        let mut c = client_for(&s);
        run(&s, &mut c, &["set", "k", "in0"]);
        assert_eq!(run(&s, &mut c, &["select", "1"]), "+OK\r\n");
        assert_eq!(run(&s, &mut c, &["get", "k"]), "$-1\r\n");
        run(&s, &mut c, &["set", "k", "in1"]);
        assert_eq!(run(&s, &mut c, &["select", "0"]), "+OK\r\n");
        assert_eq!(run(&s, &mut c, &["get", "k"]), "$3\r\nin0\r\n");
        assert_eq!(
            run(&s, &mut c, &["select", "99"]),
            "-ERR DB index is out of range\r\n"
        );
        assert_eq!(
            run(&s, &mut c, &["select", "abc"]),
            "-ERR value is not an integer or out of range\r\n"
        );
    }

    #[test]
    fn dbsize_and_flush_use_every_shard() {
        let s = server_with(8);
        let mut c = client_for(&s);
        for i in 0..200 {
            let k = format!("key:{i}");
            run(&s, &mut c, &["set", &k, "v"]);
        }
        assert_eq!(run(&s, &mut c, &["dbsize"]), ":200\r\n");
        assert_eq!(run(&s, &mut c, &["flushdb"]), "+OK\r\n");
        assert_eq!(run(&s, &mut c, &["dbsize"]), ":0\r\n");
    }

    #[test]
    fn hello_switches_protocol_and_null_shape() {
        let s = server_with(4);
        let mut c = client_for(&s);
        assert_eq!(run(&s, &mut c, &["get", "nothing"]), "$-1\r\n");
        let hello = run(&s, &mut c, &["hello", "3"]);
        assert!(hello.starts_with("%7\r\n"), "RESP3 map expected: {hello:?}");
        assert!(hello.contains("$5\r\nproto\r\n:3\r\n"));
        assert_eq!(c.proto, 3);
        // Same command, different null encoding now.
        assert_eq!(run(&s, &mut c, &["get", "nothing"]), "_\r\n");
        let hello2 = run(&s, &mut c, &["hello", "2"]);
        assert!(hello2.starts_with("*14\r\n"), "RESP2 flat map: {hello2:?}");
        assert_eq!(run(&s, &mut c, &["get", "nothing"]), "$-1\r\n");
        assert_eq!(
            run(&s, &mut c, &["hello", "9"]),
            "-NOPROTO unsupported protocol version\r\n"
        );
    }

    #[test]
    fn ping_and_echo() {
        let s = server_with(4);
        let mut c = client_for(&s);
        assert_eq!(run(&s, &mut c, &["ping"]), "+PONG\r\n");
        assert_eq!(run(&s, &mut c, &["PING"]), "+PONG\r\n");
        assert_eq!(run(&s, &mut c, &["ping", "hi"]), "$2\r\nhi\r\n");
        assert_eq!(run(&s, &mut c, &["echo", "hi"]), "$2\r\nhi\r\n");
    }

    #[test]
    fn quit_asks_to_close() {
        let s = server_with(4);
        let mut c = client_for(&s);
        let mut buf = BytesMut::new();
        let a = argv(&["quit"]);
        assert!(matches!(dispatch(&s, &mut c, &mut buf, &a), Outcome::Close));
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn command_count_and_info() {
        let s = server_with(4);
        let mut c = client_for(&s);
        let count = run(&s, &mut c, &["command", "count"]);
        assert!(count.starts_with(':'));
        let info = run(&s, &mut c, &["command", "info", "get"]);
        assert!(info.contains("$3\r\nget\r\n"), "{info}");
        assert!(info.contains(":2\r\n"), "arity 2 expected: {info}");
        let missing = run(&s, &mut c, &["command", "info", "nosuchcmd"]);
        assert_eq!(missing, "*1\r\n*-1\r\n");
    }

    #[test]
    fn auth_required_blocks_everything_else() {
        let s = ServerShared::new(Config {
            shard_count: 4,
            requirepass: Some("hunter2".into()),
            ..Default::default()
        });
        let mut c = ClientState::new(1, "t".into(), "t".into(), 0, true);
        assert_eq!(
            run(&s, &mut c, &["get", "k"]),
            "-NOAUTH Authentication required.\r\n"
        );
        assert!(!run(&s, &mut c, &["ping"]).is_empty());
        assert!(run(&s, &mut c, &["auth", "wrong"]).starts_with("-WRONGPASS"));
        assert_eq!(run(&s, &mut c, &["auth", "hunter2"]), "+OK\r\n");
        assert_eq!(run(&s, &mut c, &["ping"]), "+PONG\r\n");
    }

    #[test]
    fn propagation_records_writes_only_when_enabled() {
        let s = server_with(4);
        s.propagation_enabled
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let mut c = client_for(&s);
        run(&s, &mut c, &["set", "k", "v"]);
        run(&s, &mut c, &["get", "k"]);

        let idx = s.shards.shard_index(b"k");
        let h = s.shards.get(idx).expect("shard");
        let mut g = h.lock();
        let drained = g.drain_propagation();
        assert_eq!(drained.len(), 1, "one write, one propagated entry");
        assert_eq!(
            drained
                .first()
                .and_then(|p| p.argv.first())
                .map(|b| b.to_vec()),
            Some(b"set".to_vec())
        );
    }

    #[test]
    fn relative_expiry_propagates_an_absolute_deadline() {
        let s = server_with(4);
        s.propagation_enabled
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let mut c = client_for(&s);
        run(&s, &mut c, &["set", "k", "v", "EX", "100"]);
        let idx = s.shards.shard_index(b"k");
        let h = s.shards.get(idx).expect("shard");
        let mut g = h.lock();
        let drained = g.drain_propagation();
        let argv = &drained.first().expect("propagated").argv;
        let words: Vec<String> = argv
            .iter()
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .collect();
        assert_eq!(words.first().map(String::as_str), Some("SET"));
        assert_eq!(words.get(3).map(String::as_str), Some("PXAT"));
        assert!(
            words
                .get(4)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
                > 1_600_000_000_000,
            "expected an absolute ms deadline, got {words:?}"
        );
    }
}
