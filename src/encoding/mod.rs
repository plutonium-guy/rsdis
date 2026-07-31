//! Compact in-memory encodings.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! §5.5 makes these mandatory, not optional: listpack for small
//! hash/list/zset, intset for small all-integer sets. They are 5-10x memory
//! wins on realistic workloads and `OBJECT ENCODING` must report them
//! correctly. Conversion thresholds come from `crate::config::Config`
//! (`hash_max_listpack_entries` and friends).
//!
//! §6 expects `unsafe` here, and only here plus `rax`: every block needs a
//! `// SAFETY:` comment stating the invariant and why it holds.

pub mod intset;
pub mod listpack;
pub mod quicklist;
pub mod rax;
pub mod skiplist;
