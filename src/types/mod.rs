//! Value-type payloads.
//!
//! Each submodule is owned by exactly one wave agent (see §3). This `mod.rs`
//! itself is owned by F0 and must not be edited: it exists so that no agent
//! ever has to touch a shared declaration list.

pub mod hash;
pub mod hll;
pub mod list;
pub mod set;
pub mod stream;
pub mod string;
pub mod zset;
