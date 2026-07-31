//! RESP wire protocol.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! F0 wrote a working request parser (multibulk + inline) so the foundation
//! could serve real clients. It is deliberately small but not dishonest: the
//! limits, the error strings and the zero-copy discipline are all real.
//! **Extend it; do not delete and restart.**
//!
//! Still to do (W1b): a proper reply *parser* (needed only if we ever act as a
//! client, e.g. for replication), `MONITOR` formatting, and the incremental
//! parser state machine that avoids re-scanning a partial multibulk on every
//! `read()`. Today a partial frame is re-scanned from the top each time, which
//! is O(n^2) for a very large pipelined burst arriving in small chunks -- the
//! first thing to fix when W1b takes ownership.

pub mod parser;

pub use parser::{ProtoError, RequestParser};
