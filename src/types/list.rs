//! The list object.
//!
//! Owned by W2b; do not edit if you are not that agent.
//!
//! F0 declares this as a placeholder so that `Robj::List` type-checks and the
//! rest of the tree compiles. W2b replaces the body with the real thing: a
//! listpack for small lists and a quicklist (W1a) above
//! `list-max-listpack-size`, with `OBJECT ENCODING` reporting `listpack` or
//! `quicklist` accordingly.

/// Placeholder. Replace, do not extend.
#[derive(Debug, Default)]
pub struct ListObj {
    _placeholder: (),
}

impl ListObj {
    /// `LLEN`.
    pub fn len(&self) -> usize {
        0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `OBJECT ENCODING`: `listpack` | `quicklist`.
    pub fn encoding(&self) -> &'static str {
        "listpack"
    }

    pub fn mem_usage(&self) -> usize {
        core::mem::size_of::<Self>()
    }
}
