//! RDB -- real, file-compatible snapshots.
//!
//! Owned by W3a; do not edit if you are not that agent.
//!
//! Both directions are in scope: rsdis must load a file written by Redis 7.4
//! and Redis 7.4 must load a file written by rsdis. That means the version
//! header (`REDIS0011`), the opcode set, the length-prefix encoding, the
//! `RDB_ENC_INT8/16/32` and `RDB_ENC_LZF` string encodings, the listpack /
//! intset / quicklist / stream-listpacks object encodings, the aux fields, and
//! the trailing CRC64.
//!
//! Everything the byte-level work needs is already in `crate::util`:
//! `crc64::crc64` for the trailer, `lzf::compress`/`lzf::decompress` for
//! compressed strings.

/// The magic + version rsdis writes. Redis 7.4 writes `REDIS0011`.
pub const RDB_MAGIC_VERSION: &[u8; 9] = b"REDIS0011";
