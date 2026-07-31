//! HyperLogLog commands.
//!
//! Owned by W2a; do not edit if you are not that agent.
//!
//! `PFADD`, `PFCOUNT`, `PFMERGE`, `PFDEBUG`, `PFSELFTEST`.
//!
//! The registers live in a string object; see `src/types/hll.rs`. `PFCOUNT`
//! is flagged WRITE in real Redis because it caches the estimate back into the
//! header -- match that, or replicas diverge.
//!
//! Register your commands inside `register()`. It is already wired into
//! `command::build_table()`, so you never touch `src/command/mod.rs`.

use crate::command::CommandTable;

/// Owner: W2a.
pub fn register(_t: &mut CommandTable) {}
