//! Quicklist -- a doubly linked list of (optionally LZF-compressed) listpacks.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! Backs lists above `list-max-listpack-size`. `list-compress-depth` controls
//! how many nodes at each end stay uncompressed; the rest go through
//! `crate::util::lzf`.
