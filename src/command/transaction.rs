//! `MULTI` / `EXEC` / `DISCARD` / `WATCH` / `UNWATCH`.
//!
//! Owned by W3b; do not edit if you are not that agent.
//!
//! F0 seeds three things here because the frozen files reference them:
//!
//! * [`MultiState`] -- hangs off `ClientState` (`src/ctx.rs` is frozen, so the
//!   transaction fields have to live in a type W3b owns);
//! * [`intercept`] -- called by `engine::dispatch` *before* any shard is
//!   locked, so that queuing a command inside `MULTI` never takes a lock;
//! * [`register`] -- the registration hook, already wired into
//!   `command::build_table()`.
//!
//! ## Notes for the implementation
//!
//! `WATCH` support already exists in the keyspace: `Db::watch_key` returns a
//! version counter and `Db::signal_watch` bumps it on every modification, with
//! a zero-cost fast path when nothing is watched. `EXEC` re-reads the versions
//! for every watched key and aborts (null array) if any moved. The client-side
//! bookkeeping -- which keys, in which db, at which version -- goes in
//! [`MultiState`].
//!
//! §2.1 has no exception for `EXEC`: it must resolve the union of every queued
//! command's keys, lock that shard set once in ascending order, and run the
//! whole transaction under it.

use crate::command::{ArgVec, CommandTable};
use crate::ctx::{ClientState, ServerShared};
use crate::object::Key;

/// Per-client transaction state.
#[derive(Debug, Default)]
pub struct MultiState {
    /// Commands queued since `MULTI`.
    pub queue: Vec<ArgVec>,
    /// `(db, key, version)` triples recorded by `WATCH`.
    pub watched: Vec<(usize, Key, u64)>,
}

impl MultiState {
    /// True while the client is inside a `MULTI`. The authoritative flag is
    /// `ClientFlags::MULTI`; this is the storage that goes with it.
    pub fn is_queuing(&self) -> bool {
        !self.queue.is_empty()
    }

    /// `DISCARD` / `RESET` / connection teardown.
    pub fn reset(&mut self) {
        self.queue.clear();
        self.watched.clear();
    }
}

/// Hook called by `engine::dispatch` before the command is executed.
///
/// Returning `true` means "I handled this; do not dispatch". W3b uses it to
/// queue commands while `ClientFlags::MULTI` is set, and to reject a command
/// that failed to queue with `DIRTY_EXEC`.
///
/// F0's implementation returns `false` unconditionally, so the foundation
/// behaves as if transactions do not exist.
#[inline]
pub fn intercept(
    _server: &ServerShared,
    _client: &mut ClientState,
    _out: &mut bytes::BytesMut,
    _args: &crate::command::Args,
) -> bool {
    false
}

/// Owner: W3b. Register `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`.
pub fn register(_t: &mut CommandTable) {}
