//! Sorted-set commands.
//!
//! Owned by W2c; do not edit if you are not that agent.
//!
//! `ZADD`, `ZINCRBY`, `ZREM`, `ZSCORE`, `ZMSCORE`, `ZCARD`, `ZCOUNT`,
//! `ZRANGE` and the by-score/by-lex/rev/store variants, `ZRANK`,
//! `ZREVRANK`, `ZRANDMEMBER`, `ZPOPMIN`, `ZPOPMAX`, `ZMPOP`,
//! `ZUNION`/`ZINTER`/`ZDIFF` and their `*STORE` forms, `ZSCAN`, plus the
//! blocking `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`.
//!
//! Scores are doubles and must be formatted with `ReplyWriter::double`, which
//! already implements Redis's `d2string` rules (`1.0` replies as `1`).
//! `+inf`/`-inf` are valid scores; `nan` never is.
//! `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFF`/`ZMPOP` are MOVABLE_KEYS.
//!
//! Register your commands inside `register()`. It is already wired into
//! `command::build_table()`, so you never touch `src/command/mod.rs`.

use crate::command::CommandTable;

/// Owner: W2c.
pub fn register(_t: &mut CommandTable) {}
