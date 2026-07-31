//! The hash object.
//!
//! Owned by W2b; do not edit if you are not that agent.
//!
//! F0 declares this as a placeholder. W2b replaces the body: a listpack below
//! `hash-max-listpack-entries` / `hash-max-listpack-value`, a `hashbrown` dict
//! above it, plus the per-field TTL support (`HEXPIRE` family) that makes
//! `OBJECT ENCODING` report `listpackex` / `hashtable`.

/// Placeholder. Replace, do not extend.
#[derive(Debug, Default)]
pub struct HashObj {
    _placeholder: (),
}

impl HashObj {
    /// `HLEN`.
    pub fn len(&self) -> usize {
        0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `OBJECT ENCODING`: `listpack` | `listpackex` | `hashtable`.
    pub fn encoding(&self) -> &'static str {
        "listpack"
    }

    pub fn mem_usage(&self) -> usize {
        core::mem::size_of::<Self>()
    }
}
