//! `INFO` and the server statistics behind it.
//!
//! Owned by W3c; do not edit if you are not that agent.
//!
//! F0 declares [`Stats`] because it hangs off `ServerShared`, which lives in
//! the frozen `src/ctx.rs`. Every counter is an atomic so that the hot path
//! can bump it without a lock and `INFO` can read it without one either.
//! Add counters here; do not move the struct.

use std::sync::atomic::{AtomicU64, Ordering};

/// Server-wide counters. All `Relaxed`: these are statistics, not
/// synchronisation.
#[derive(Debug, Default)]
pub struct Stats {
    pub connections_received: AtomicU64,
    pub commands_processed: AtomicU64,
    pub net_input_bytes: AtomicU64,
    pub net_output_bytes: AtomicU64,
    pub rejected_connections: AtomicU64,
    pub expired_keys: AtomicU64,
    pub evicted_keys: AtomicU64,
    pub keyspace_hits: AtomicU64,
    pub keyspace_misses: AtomicU64,
    pub pubsub_channels: AtomicU64,
    pub pubsub_patterns: AtomicU64,
    pub total_reads_processed: AtomicU64,
    pub total_writes_processed: AtomicU64,
    pub unexpected_error_replies: AtomicU64,
    pub total_error_replies: AtomicU64,
}

impl Stats {
    pub fn new() -> Self {
        Stats::default()
    }

    #[inline]
    pub fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}
