//! The sorted-set object. Also backs the geo commands.
//!
//! Owned by W2c; do not edit if you are not that agent.
//!
//! F0 declares this as a placeholder. W2c replaces the body with a listpack
//! below `zset-max-listpack-entries` / `zset-max-listpack-value` and a
//! skiplist (W1a) plus member->score dict above it.

/// Placeholder. Replace, do not extend.
#[derive(Debug, Default)]
pub struct ZSetObj {
    _placeholder: (),
}

impl ZSetObj {
    /// `ZCARD`.
    pub fn len(&self) -> usize {
        0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `OBJECT ENCODING`: `listpack` | `skiplist`.
    pub fn encoding(&self) -> &'static str {
        "listpack"
    }

    pub fn mem_usage(&self) -> usize {
        core::mem::size_of::<Self>()
    }
}
