//! Set commands.
//!
//! Owned by W2b; do not edit if you are not that agent.
//!
//! `SADD`, `SREM`, `SMOVE`, `SISMEMBER`, `SMISMEMBER`, `SCARD`,
//! `SPOP`, `SRANDMEMBER`, `SINTER`, `SINTERCARD`, `SUNION`, `SDIFF`,
//! and the `*STORE` variants, plus `SSCAN`.
//!
//! `SPOP` must propagate as `SREM` with the members it actually removed
//! (§4.5) -- this is the canonical example of the propagation bug the
//! architecture calls out. `SINTERCARD` is MOVABLE_KEYS.
//!
//! Register your commands inside `register()`. It is already wired into
//! `command::build_table()`, so you never touch `src/command/mod.rs`.

use crate::command::CommandTable;

/// Owner: W2b.
pub fn register(_t: &mut CommandTable) {}
