//! Blocking commands.
//!
//! Owned by W3b; do not edit if you are not that agent.
//!
//! F0 declares [`BlockKind`], [`BlockRequest`] and [`BlockingRegistry`]
//! because `Ctx::block_on` is a frozen signature that mentions them, and
//! because `engine::dispatch` has to be able to hand a block request back to
//! the connection loop. The mechanism -- ready-key lists, timeout wheels,
//! serving a blocked client from the pushing client's thread -- is W3b's.
//!
//! ## How a block flows today
//!
//! 1. A handler calls `Ctx::block_on(keys, timeout_ms, kind)`.
//! 2. That records a [`BlockRequest`] on the `Ctx` and returns; the handler
//!    must write **no** reply.
//! 3. The engine releases the shard locks (this is the important part: a
//!    blocked client holds nothing) and returns
//!    `engine::Outcome::Blocked(req)`.
//! 4. `src/net/conn.rs` is responsible for parking the connection. Today it
//!    replies with the timeout value immediately, because nothing can wake it
//!    yet; W3b replaces that with a real park/unpark.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::object::Key;

/// What a client is blocked on, and therefore how it must be woken and what
/// it must reply with on timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    /// `BLPOP` / `BRPOP` -- wake on a list push.
    List,
    /// `BLMOVE` / `BRPOPLPUSH` -- wake on a push to the source list.
    ListMove,
    /// `BLMPOP`
    ListMPop,
    /// `BZPOPMIN` / `BZPOPMAX` -- wake on a zset insert.
    ZSet,
    /// `BZMPOP`
    ZSetMPop,
    /// `XREAD BLOCK` / `XREADGROUP BLOCK` -- wake on a stream append.
    Stream,
    /// `WAIT` / `WAITAOF` -- wake on replication or fsync progress.
    Wait,
}

impl BlockKind {
    /// The reply a timed-out block produces. RESP2 and RESP3 differ for the
    /// list and zset families, which is why this is a shape and not a value.
    pub fn timeout_is_null_array(self) -> bool {
        !matches!(self, BlockKind::Wait)
    }
}

/// A pending block, produced by `Ctx::block_on`.
#[derive(Debug, Clone)]
pub struct BlockRequest {
    pub kind: BlockKind,
    pub keys: SmallVec<[Key; 4]>,
    /// Absolute deadline in ms, or `None` for "block forever" (`timeout 0`).
    pub deadline_ms: Option<u64>,
    /// The database the keys live in.
    pub db: usize,
    /// Extra arguments the wake-up path needs (`BLMOVE`'s destination and
    /// directions, `XREAD`'s last-seen IDs, ...).
    pub extra: SmallVec<[Bytes; 4]>,
}

/// Server-wide registry of blocked clients, keyed by (db, key).
///
/// Hangs off `ServerShared`. Extend freely; do not move.
#[derive(Debug, Default)]
pub struct BlockingRegistry {
    _placeholder: (),
}

impl BlockingRegistry {
    pub fn new() -> Self {
        BlockingRegistry::default()
    }

    /// `INFO clients: blocked_clients`.
    pub fn blocked_count(&self) -> usize {
        0
    }
}

/// Per-client blocking state. Hangs off `ClientState`.
#[derive(Debug, Default)]
pub struct ClientBlockState {
    _placeholder: (),
}

impl ClientBlockState {
    pub fn new() -> Self {
        ClientBlockState::default()
    }

    pub fn is_blocked(&self) -> bool {
        false
    }
}
