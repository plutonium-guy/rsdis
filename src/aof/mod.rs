//! AOF -- append-only file, with rewrite and all three fsync policies.
//!
//! Owned by W3a; do not edit if you are not that agent.
//!
//! The producer side already exists and is F0's: handlers call
//! `Ctx::propagate`, which lands commands in the per-shard
//! `Shard::repl_buf`. §7 describes the rest:
//!
//! ```text
//! shard.repl_buf --> aggregator (background, assigns a global seq)
//!                        --> AOF buffer --> fsync per policy
//!                        --> (later) replication backlog
//! ```
//!
//! Build the aggregator so replication can subscribe to the same stream
//! later; do not give AOF a private path to the shards.

/// Redis 7's multi-part AOF manifest name, inside `appenddirname`.
pub const MANIFEST_SUFFIX: &str = ".manifest";
