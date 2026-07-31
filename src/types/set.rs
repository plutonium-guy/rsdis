//! The set object.
//!
//! Owned by W2b; do not edit if you are not that agent.
//!
//! F0 declares this as a placeholder. W2b replaces the body with the three
//! real encodings: `intset` while every member is an integer and there are at
//! most `set-max-intset-entries`, `listpack` for small mixed sets, and
//! `hashtable` above `set-max-listpack-entries`.

/// Placeholder. Replace, do not extend.
#[derive(Debug, Default)]
pub struct SetObj {
    _placeholder: (),
}

impl SetObj {
    /// `SCARD`.
    pub fn len(&self) -> usize {
        0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `OBJECT ENCODING`: `intset` | `listpack` | `hashtable`.
    pub fn encoding(&self) -> &'static str {
        "listpack"
    }

    pub fn mem_usage(&self) -> usize {
        core::mem::size_of::<Self>()
    }
}
