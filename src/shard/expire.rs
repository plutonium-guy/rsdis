//! Active expiry cycle.
//!
//! Owned by W1c; do not edit if you are not that agent.
//!
//! Lazy expiry (a logically-expired key reading as missing and being reaped on
//! access) already lives in `Ctx::lookup_*` and is F0's. What belongs here is
//! the *active* half: a background task that samples `Db::expires` per shard,
//! deletes what has passed, fires `expired` keyspace notifications and
//! propagates a synthetic `DEL`/`UNLINK`.
//!
//! Constraints from §5.8: the cycle must never hold a shard lock across the
//! whole keyspace. Sample a bounded number of keys per shard per tick,
//! release, and come back.

/// Redis's `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP`.
pub const KEYS_PER_LOOP: usize = 20;

/// Redis keeps looping over a database while more than this fraction of the
/// sampled keys turned out to be expired.
pub const CYCLE_ACCEPTABLE_STALE_PCT: f64 = 0.10;
