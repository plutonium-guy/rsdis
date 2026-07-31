//! Rax -- the compressed radix tree behind streams and cluster metadata.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! Keys are big-endian 128-bit stream IDs, so lexicographic order is numeric
//! order and range scans are prefix walks. This module is expected to contain
//! `unsafe`; §6 requires a `// SAFETY:` comment on every block.
