//! Geo commands.
//!
//! Owned by W2c; do not edit if you are not that agent.
//!
//! `GEOADD`, `GEOPOS`, `GEODIST`, `GEOHASH`, `GEOSEARCH`,
//! `GEOSEARCHSTORE`, and the deprecated `GEORADIUS*` family.
//!
//! Geo is a sorted set whose scores are 52-bit interleaved geohashes; see
//! `crate::util::geohash`. `TYPE` reports `zset`.
//! `GEORADIUS`/`GEOSEARCHSTORE` are MOVABLE_KEYS (the STORE key moves).
//!
//! Register your commands inside `register()`. It is already wired into
//! `command::build_table()`, so you never touch `src/command/mod.rs`.

use crate::command::CommandTable;

/// Owner: W2c.
pub fn register(_t: &mut CommandTable) {}
