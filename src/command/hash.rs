//! Hash commands.
//!
//! Owned by W2b; do not edit if you are not that agent.
//!
//! `HSET`, `HSETNX`, `HGET`, `HMGET`, `HDEL`, `HLEN`, `HEXISTS`,
//! `HKEYS`, `HVALS`, `HGETALL`, `HINCRBY`, `HINCRBYFLOAT`, `HSTRLEN`,
//! `HRANDFIELD`, `HSCAN`.
//!
//! `HRANDFIELD` and `HSCAN` are non-deterministic: propagate a deterministic
//! equivalent or nothing at all. `HGETALL` replies with a map on RESP3 and a
//! flat array on RESP2 -- that divergence is `ReplyWriter::map`'s job, not the
//! handler's.
//!
//! Register your commands inside `register()`. It is already wired into
//! `command::build_table()`, so you never touch `src/command/mod.rs`.

use crate::command::CommandTable;

/// Owner: W2b.
pub fn register(_t: &mut CommandTable) {}
