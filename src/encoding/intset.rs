//! Intset -- a sorted array of same-width integers.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! Layout from `intset.c`: 4-byte encoding LE (2, 4 or 8), 4-byte length LE,
//! then `length` little-endian integers of that width, sorted ascending.
//! Upgrades in place when a wider value is inserted. RDB (W3a) writes it
//! verbatim.
