//! rsdis -- an optimized Redis-compatible server.
//!
//! Wire-compatible with real Redis clients (RESP2 + RESP3), file-compatible
//! with real RDB, command-compatible for the core command set.
//!
//! # Reading order
//!
//! * [`shard`] -- the sharded keyspace and the locking primitives (§2).
//! * [`engine`] -- key resolution, lock ordering (with the deadlock-freedom
//!   proof) and dispatch (§2.1).
//! * [`ctx`] -- what a command handler is allowed to touch (§4.5).
//! * [`reply`] -- the one place RESP2/RESP3 divergence is handled (§4.3).
//! * [`command`] -- the command table and the single registration list (§4.4).
//!
//! # Ownership
//!
//! `docs/ARCHITECTURE.md` §3 assigns every path to exactly one agent. Each
//! module's header names its owner. If you need something from another
//! agent's module and it is not already in the frozen foundation, that is a
//! contract gap: report it, do not reach across.

pub mod aof;
pub mod blocking;
pub mod command;
pub mod config;
pub mod ctx;
pub mod encoding;
pub mod engine;
pub mod error;
pub mod info;
pub mod net;
pub mod notify;
pub mod object;
pub mod pubsub;
pub mod rdb;
pub mod reply;
pub mod resp;
pub mod shard;
pub mod slowlog;
pub mod types;
pub mod util;

pub use config::{Cli, Config};
pub use ctx::{ClientState, Ctx, ServerShared};
pub use error::{CmdError, CmdResult};
pub use object::{Entry, Key, Robj};
pub use reply::ReplyWriter;

/// The version reported by `--version`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
