//! List commands.
//!
//! Owned by W2b; do not edit if you are not that agent.
//!
//! `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LMPOP`,
//! `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LLEN`, `LREM`, `LTRIM`,
//! `LPOS`, `LMOVE`, `RPOPLPUSH`, and the blocking forms `BLPOP`, `BRPOP`,
//! `BLMOVE`, `BRPOPLPUSH`, `BLMPOP`.
//!
//! Blocking handlers call `Ctx::block_on` and write no reply; the engine
//! unwinds the locks and hands the request to the connection layer.
//! `LMPOP`/`BLMPOP` are MOVABLE_KEYS (numkeys-prefixed).
//!
//! Register your commands inside `register()`. It is already wired into
//! `command::build_table()`, so you never touch `src/command/mod.rs`.

use crate::command::CommandTable;

/// Owner: W2b.
pub fn register(_t: &mut CommandTable) {}
