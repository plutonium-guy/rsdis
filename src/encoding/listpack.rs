//! Listpack -- Redis's compact, allocation-free-ish sequence encoding.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! The on-disk layout must match `listpack.c` byte for byte, because RDB
//! (W3a) writes listpacks verbatim into the file and real Redis reads them
//! back. Header: 4-byte total-bytes LE, 2-byte num-elements LE, then entries,
//! then the 0xFF terminator.
