//! Stream commands.
//!
//! Owned by W2d; do not edit if you are not that agent.
//!
//! `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`, `XDEL`, `XTRIM`,
//! `XSETID`, `XINFO`, and the consumer-group family `XGROUP`,
//! `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XAUTOCLAIM`.
//!
//! `XADD *` and `XTRIM MAXLEN ~` are non-deterministic: propagate the
//! resolved ID and the exact trim. `XREAD`/`XREADGROUP` are MOVABLE_KEYS
//! (keys follow `STREAMS`) and blocking. `XPENDING` is one of the few places
//! §4.3 permits a handler to branch on `proto`.
//!
//! Register your commands inside `register()`. It is already wired into
//! `command::build_table()`, so you never touch `src/command/mod.rs`.

use crate::command::CommandTable;

/// Owner: W2d.
pub fn register(_t: &mut CommandTable) {}
