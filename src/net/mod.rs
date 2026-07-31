//! Networking.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! F0 wrote the minimum needed to serve real clients: `SO_REUSEPORT`
//! listeners, `TCP_NODELAY`, a read/execute/flush loop with full pipelining,
//! and out-of-band delivery plumbing. **Extend it; do not delete and
//! restart.** The shape is what W1b needs anyway.
//!
//! Still to do (W1b):
//!
//! * unix socket listener (`unixsocket`);
//! * `timeout` / `tcp-keepalive` enforcement;
//! * `maxclients` admission control;
//! * real blocking: today `Outcome::Blocked` replies with the timeout value
//!   immediately, because nothing can wake a client yet (see `conn.rs`);
//! * output buffer limits and the `client-output-buffer-limit` config;
//! * vectored writes, so `ReplyWriter::bulk_from` can be genuinely zero-copy.

pub mod conn;
pub mod listener;

pub use listener::{ServerHandle, serve};
