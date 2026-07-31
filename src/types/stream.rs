//! The stream object.
//!
//! Owned by W2d; do not edit if you are not that agent.
//!
//! F0 declares this as a placeholder. W2d replaces the body with a rax (W1a)
//! of listpack-packed entry batches, plus consumer groups, PELs and the
//! last/max-deleted ID bookkeeping that `XINFO STREAM` reports.
//!
//! Note: unlike the other aggregates, an empty stream is NOT deleted -- its
//! consumer groups outlive its entries. `Robj::is_empty_collection` encodes
//! that.

/// Placeholder. Replace, do not extend.
#[derive(Debug, Default)]
pub struct StreamObj {
    _placeholder: (),
}

impl StreamObj {
    /// `XLEN`.
    pub fn len(&self) -> usize {
        0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `OBJECT ENCODING` for a stream is always `stream`.
    pub fn encoding(&self) -> &'static str {
        "stream"
    }

    pub fn mem_usage(&self) -> usize {
        core::mem::size_of::<Self>()
    }
}
