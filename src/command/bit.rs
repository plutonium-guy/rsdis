//! Bit operations over string values.
//!
//! Owned by W2a; do not edit if you are not that agent.
//!
//! `SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS`, `BITOP`, `BITFIELD`,
//! `BITFIELD_RO`.
//!
//! Bitmaps are string objects (`Robj::Str`); there is no separate type and
//! `TYPE` must report `string`. `BITCOUNT`/`BITPOS` take the `BYTE`/`BIT`
//! range unit added in Redis 7.0.
//!
//! Register your commands inside `register()`. It is already wired into
//! `command::build_table()`, so you never touch `src/command/mod.rs`.

use crate::command::CommandTable;

/// Owner: W2a.
pub fn register(_t: &mut CommandTable) {}
