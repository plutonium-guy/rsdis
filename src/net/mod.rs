//! Networking.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! ```text
//! listener.rs   SO_REUSEPORT accept loops, TCP_NODELAY, tcp-keepalive,
//!               the unix socket, and shutdown.
//! conn.rs       the per-connection read/execute/flush loop: pipelining,
//!               maxclients, idle timeout, buffer reclaim, CLIENT REPLY.
//! output.rs     the two-tier output buffer: staging BytesMut + a vectored
//!               queue of Bytes, plus client-output-buffer-limit.
//! registry.rs   what CLIENT LIST / INFO / KILL report about every peer.
//! ```
//!
//! # Delivered by W1b
//!
//! * unix socket listener (`unixsocket`), sharing the whole protocol path
//!   with TCP;
//! * `timeout` and `tcp-keepalive` enforcement;
//! * `maxclients` admission control, with Redis's refusal string;
//! * output buffer limits (see the caveat below);
//! * a vectored write queue (see §9.10 below);
//! * `CLIENT LIST/INFO/KILL/NO-EVICT/NO-TOUCH/UNPAUSE/SETINFO/REPLY` and the
//!   registry that makes them report real values for *other* connections.
//!
//! # Still open, and why
//!
//! * **Real blocking.** `Outcome::Blocked` still replies with the timeout
//!   value immediately, because nothing can wake a client yet. That is W3b's
//!   `blocking` registry; the out-of-band channel it needs is already wired
//!   (`OutOfBand::Unblock`).
//! * **§9.10 zero-copy `bulk_from`.** The vectored queue exists and is used
//!   ([`output::OutputBuffer::push_bytes`]), and the *decision* §9.10 asks for
//!   has been made and measured -- queue above ~2 KiB, memcpy below, exposed
//!   as [`output::OutputBuffer::should_queue`]. What is missing is the wiring:
//!   `ReplyWriter` is `{ buf: &mut BytesMut }` in the frozen `src/reply.rs`
//!   and holds no handle to the queue, so `bulk_from` cannot reach it from
//!   inside `src/net`. **§5.1 is therefore not yet satisfied for large
//!   values**; nobody may claim otherwise. The numbers are in
//!   [`output`]'s docs and `benches/protocol_bench.rs`.
//! * **`client-output-buffer-limit` is not configurable.** `Config` has no
//!   field for it, so [`output::OutputLimits`] hard-codes Redis's defaults.
//!   The enforcement is real; only the tuning knob is missing.

pub mod conn;
pub mod listener;
pub mod output;
pub mod registry;

pub use conn::{serve_connection, serve_stream};
pub use listener::{ServerHandle, serve};
pub use output::{ClassLimit, ClientClass, OutputBuffer, OutputLimits};
pub use registry::{ConnEntry, ConnSnapshot};
