//! Skiplist -- the sorted-set index above `zset-max-listpack-entries`.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! Paired with a member -> score dict, exactly as in `t_zset.c`, so that
//! `ZSCORE` is O(1) while `ZRANGEBYSCORE` and `ZRANK` stay O(log n). Level
//! generation uses p = 0.25 and a 32-level cap, matching Redis, so that
//! `DEBUG JMAP`-style structural comparisons line up.
