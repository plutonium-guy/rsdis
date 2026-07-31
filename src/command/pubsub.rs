//! Pub/Sub commands.
//!
//! Owned by W3b; do not edit if you are not that agent.
//!
//! `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`,
//! `SSUBSCRIBE`, `SUNSUBSCRIBE`, `PUBLISH`, `SPUBLISH`, `PUBSUB`.
//!
//! These touch no keys, so they must declare no keys and take no shard lock.
//! Deliver through `ClientHandle::tx` (`OutOfBand::Frame`) so a publisher
//! never blocks on a slow subscriber's socket. Use `ReplyWriter::push`, which
//! already emits `>` on RESP3 and `*` on RESP2.
//!
//! Register your commands inside `register()`. It is already wired into
//! `command::build_table()`, so you never touch `src/command/mod.rs`.

use crate::command::CommandTable;

/// Owner: W3b.
pub fn register(_t: &mut CommandTable) {}
