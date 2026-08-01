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

//! # What each structure is for
//!
//! | module | Redis source | used by |
//! |---|---|---|
//! | [`listpack`] | `listpack.c` | small hash / list / zset / set (W2b, W2c), stream entries (W2d), RDB (W3a) |
//! | [`intset`] | `intset.c` | all-integer sets below `set-max-intset-entries` (W2b) |
//! | [`quicklist`] | `quicklist.c` | lists above `list-max-listpack-size` (W2b) |
//! | [`skiplist`] | `t_zset.c` | sorted sets above `zset-max-listpack-entries` (W2c) |
//! | [`rax`] | `rax.c` | stream ID -> entry and consumer groups (W2d) |
//!
//! [`listpack`] and [`intset`] own a serialized `Vec<u8>` in exactly Redis's
//! on-disk layout, so W3a writes `as_bytes()` straight into an RDB file and
//! rebuilds with the validating `from_bytes`. [`quicklist`], [`skiplist`] and
//! [`rax`] are in-memory only; RDB stores their contents element by element
//! (or, for streams, as listpacks).
//!
//! # Allocation discipline
//!
//! Every read accessor returns borrowed data: [`listpack::ListpackEntry`] is
//! `Int(i64) | Str(&[u8])`, the skiplist yields `(&Bytes, f64)`, and
//! [`rax::RaxIter`] lends `(&[u8], &V)`. Nothing on a read path allocates
//! (§5.1). Where a caller genuinely needs an owned value -- `LPOP`, say --
//! the return type is a `SmallVec` that stays inline for short values.

pub mod intset;
pub mod listpack;
pub mod quicklist;
pub mod rax;
pub mod skiplist;

pub use intset::Intset;
pub use listpack::{Listpack, ListpackEntry};
pub use quicklist::Quicklist;
pub use rax::{Rax, Seek};
pub use skiplist::{LexBound, LexRange, ScoreBound, ScoreRange, Skiplist};
