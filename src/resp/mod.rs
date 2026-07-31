//! RESP wire protocol.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! [`parser`] holds the request side: a resumable, zero-copy state machine
//! that handles RESP2 and RESP3 requests (they share one request grammar --
//! RESP3 changed replies, not requests), inline commands, and unlimited
//! pipelining. Its module docs carry the incrementality argument and the
//! limit table.
//!
//! Deliberately **not** built, stated so nobody has to guess:
//!
//! * a *reply* parser. Nothing in v1 acts as a client; replication (§1, out of
//!   scope for v1) is the first consumer that would need one.
//! * `MONITOR` line formatting. `MONITOR` is W3c's command; the connection
//!   layer already carries the `CLIENT_MONITOR` flag and the out-of-band
//!   channel it will be delivered over.

pub mod parser;

pub use parser::{
    INLINE_MAX_SIZE, MULTIBULK_MAX, PROTO_MAX_BULK_LEN, Parsed, ProtoError, RequestParser,
};
