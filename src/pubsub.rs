//! Pub/Sub.
//!
//! Owned by W3b; do not edit if you are not that agent.
//!
//! F0 declares two types here because they are *extension points* embedded in
//! frozen structs:
//!
//! * [`PubSub`] hangs off `ServerShared`,
//! * [`ClientSubs`] hangs off `ClientState`.
//!
//! That indirection is deliberate: it lets W3b add every field it needs
//! without ever editing `src/ctx.rs`, which is frozen and shared by all six
//! agents. Fill these in; do not move them.

/// Server-wide pub/sub registry: channels, patterns and RESP3 shard channels,
/// plus the keyspace-notification fan-out W1c drives.
#[derive(Debug, Default)]
pub struct PubSub {
    _placeholder: (),
}

impl PubSub {
    pub fn new() -> Self {
        PubSub::default()
    }

    /// Number of channels with at least one subscriber. `PUBSUB CHANNELS`.
    pub fn channel_count(&self) -> usize {
        0
    }

    /// Number of distinct patterns. `PUBSUB NUMPAT`.
    pub fn pattern_count(&self) -> usize {
        0
    }
}

/// Per-client subscription state.
#[derive(Debug, Default)]
pub struct ClientSubs {
    _placeholder: (),
}

impl ClientSubs {
    pub fn new() -> Self {
        ClientSubs::default()
    }

    /// Channels + patterns + shard channels this client holds.
    pub fn count(&self) -> usize {
        0
    }

    /// True while a RESP2 client is in subscribe mode, during which only
    /// `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`/`SSUBSCRIBE`/
    /// `SUNSUBSCRIBE`/`PING`/`QUIT`/`RESET` are accepted.
    pub fn in_subscribe_mode(&self) -> bool {
        false
    }
}
