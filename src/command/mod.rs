//! Command specifications, the command table, and the single registration
//! list for the whole server.
//!
//! Owner: F0. **FROZEN.**
//!
//! ## Why this file is written once and never edited again
//!
//! §3: every `mod` declaration and every `register()` call below already
//! exists, pointing at modules that start as stubs. That is what keeps six
//! parallel agents out of each other's merge path -- **no agent ever edits a
//! shared registration list.** If you are a wave agent, implement your own
//! `src/command/<yours>.rs` and add your `t.add(...)` calls inside your own
//! `register()`. Do not touch this file.

// §6: handlers must never panic on client input.
#![deny(clippy::unwrap_used, clippy::indexing_slicing)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::indexing_slicing))]

pub mod bit;
pub mod connection;
pub mod generic;
pub mod geo;
pub mod hash;
pub mod hll;
pub mod list;
pub mod pubsub;
pub mod server;
pub mod set;
pub mod stream;
pub mod string;
pub mod transaction;
pub mod zset;

use bitflags::bitflags;
use bytes::Bytes;
use smallvec::SmallVec;

use crate::ctx::Ctx;
use crate::error::CmdError;
use crate::util::{FoldMap, fold_map_with_capacity};

/// A command's arguments, including `argv[0]` (the command name as the client
/// spelled it).
///
/// This is an unsized slice type on purpose: the connection stores arguments
/// in an [`ArgVec`], which derefs to it, so passing them to a handler costs a
/// fat pointer and never a copy (§5.2, §5.4).
pub type Args = [Bytes];

/// Owned storage for [`Args`]. Inline capacity 8 covers the overwhelming
/// majority of commands without touching the allocator (§5.4).
pub type ArgVec = SmallVec<[Bytes; 8]>;

/// Positions of a command's keys within its [`Args`].
pub type KeyPositions = SmallVec<[usize; 8]>;

/// Every handler has this shape.
pub type Handler = fn(&mut Ctx<'_>, &Args) -> Result<(), CmdError>;

bitflags! {
    /// Command attributes. Mirrors Redis's `CMD_*` flags where they matter to
    /// us; `COMMAND INFO` renders them back into Redis's spelling.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CmdFlags: u32 {
        const WRITE      = 1 << 0;
        const READONLY   = 1 << 1;
        const DENYOOM    = 1 << 2;
        const ADMIN      = 1 << 3;
        const PUBSUB     = 1 << 4;
        const NOSCRIPT   = 1 << 5;
        const BLOCKING   = 1 << 6;
        const FAST       = 1 << 7;
        const LOADING    = 1 << 8;
        const STALE      = 1 << 9;
        const NO_MULTI   = 1 << 10;
        /// Key positions cannot be derived from first/last/step; use
        /// [`CommandSpec::get_keys`].
        const MOVABLE_KEYS = 1 << 11;

        // ------------------------------------------------------------------
        // Additions by F0 beyond the §4.4 list. These are new bits, not
        // changes to existing ones, and each is reported in the W0 handover.
        // ------------------------------------------------------------------

        /// The command operates on the whole keyspace and therefore needs
        /// every shard locked (`FLUSHALL`, `DBSIZE`, `KEYS`, `SWAPDB`).
        ///
        /// Needed because §4.4 gives no way to say "no declared keys, but I
        /// still touch the keyspace": `first_key == 0` otherwise means "locks
        /// nothing", which is right for `PING` and wrong for `FLUSHDB`.
        const ALL_SHARDS = 1 << 12;
        /// The command is handled entirely by the connection layer and must
        /// never reach `engine::dispatch`'s locking path (`SUBSCRIBE`,
        /// `MULTI`, `RESET`). Purely informational today.
        const NO_SHARDS  = 1 << 13;
    }
}

/// Everything the engine, `COMMAND INFO` and (later) cluster mode need to know
/// about a command.
///
/// `arity`, `first_key`, `last_key` and `key_step` are not decoration: the
/// engine derives the shard set from them, so getting them wrong is a
/// correctness bug, not a cosmetic one. Compare against real
/// `COMMAND INFO <name>` output.
pub struct CommandSpec {
    /// Lowercase, as `COMMAND INFO` reports it.
    pub name: &'static str,
    /// Redis semantics: a negative value means "at least `|arity|`".
    /// Counts `argv[0]`.
    pub arity: i32,
    pub flags: CmdFlags,
    /// Index of the first key in `argv`, or `0` for "no keys".
    pub first_key: i32,
    /// Index of the last key; negative counts back from the end
    /// (`-1` = last argument).
    pub last_key: i32,
    /// Stride between keys.
    pub key_step: i32,
    pub handler: Handler,
    /// Required when [`CmdFlags::MOVABLE_KEYS`] is set.
    pub get_keys: Option<fn(&Args) -> KeyPositions>,
    pub tips: &'static [&'static str],
    pub since: &'static str,
    pub summary: &'static str,
}

impl std::fmt::Debug for CommandSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandSpec")
            .field("name", &self.name)
            .field("arity", &self.arity)
            .field("flags", &self.flags)
            .field("first_key", &self.first_key)
            .field("last_key", &self.last_key)
            .field("key_step", &self.key_step)
            .finish()
    }
}

impl CommandSpec {
    /// Redis's arity rule: `arity >= 0` means exactly, `< 0` means at least.
    #[inline]
    pub fn arity_ok(&self, argc: usize) -> bool {
        let argc = argc as i32;
        if self.arity >= 0 {
            argc == self.arity
        } else {
            argc >= -self.arity
        }
    }

    #[inline]
    pub fn is_write(&self) -> bool {
        self.flags.contains(CmdFlags::WRITE)
    }

    #[inline]
    pub fn is_readonly(&self) -> bool {
        self.flags.contains(CmdFlags::READONLY)
    }

    /// Resolve this command's key positions within `args`.
    ///
    /// Mirrors `server.c:getKeysUsingKeySpec()` for the common case:
    ///
    /// ```text
    /// last = last_key < 0 ? argc + last_key : last_key
    /// for i in (first_key ..= last).step_by(key_step)
    /// ```
    ///
    /// Returns an empty list for keyless commands. Out-of-range positions are
    /// dropped rather than trusted, because `argc` is client-controlled.
    pub fn key_positions(&self, args: &Args) -> KeyPositions {
        let mut out = KeyPositions::new();
        if self.flags.contains(CmdFlags::MOVABLE_KEYS) {
            if let Some(f) = self.get_keys {
                for pos in f(args) {
                    if pos < args.len() {
                        out.push(pos);
                    }
                }
            }
            return out;
        }
        if self.first_key <= 0 || self.key_step <= 0 {
            return out;
        }
        let argc = args.len() as i32;
        let last = if self.last_key < 0 {
            argc + self.last_key
        } else {
            self.last_key
        };
        let mut i = self.first_key;
        while i <= last && i < argc {
            if i > 0 {
                out.push(i as usize);
            }
            i += self.key_step;
        }
        out
    }
}

/// The registry. Built once at startup; read-only thereafter.
pub struct CommandTable {
    specs: Vec<CommandSpec>,
    /// Lowercased name -> index into `specs`.
    index: FoldMap<Box<[u8]>, usize>,
}

/// Longest command name we will even try to look up. The longest real Redis
/// command is `georadiusbymember_ro` (20).
const MAX_CMD_NAME: usize = 32;

impl CommandTable {
    pub fn new() -> Self {
        CommandTable {
            specs: Vec::with_capacity(256),
            index: fold_map_with_capacity(256),
        }
    }

    /// Register a command. Panics on a duplicate or a malformed spec -- this
    /// runs once at startup, long before any client input, and a duplicate
    /// name is a programming error that must not be silently tolerated.
    pub fn add(&mut self, spec: CommandSpec) {
        assert!(
            spec.name.len() <= MAX_CMD_NAME,
            "command name '{}' exceeds {MAX_CMD_NAME} bytes",
            spec.name
        );
        assert!(
            spec.name.bytes().all(|b| !b.is_ascii_uppercase()),
            "command name '{}' must be registered lowercase",
            spec.name
        );
        assert!(
            !spec.flags.contains(CmdFlags::MOVABLE_KEYS) || spec.get_keys.is_some(),
            "command '{}' is MOVABLE_KEYS but has no get_keys",
            spec.name
        );
        assert!(
            spec.first_key <= 0 || spec.key_step > 0,
            "command '{}' declares keys but key_step is {}",
            spec.name,
            spec.key_step
        );
        let key: Box<[u8]> = spec.name.as_bytes().into();
        let idx = self.specs.len();
        if self.index.insert(key, idx).is_some() {
            panic!("duplicate command registration: '{}'", spec.name);
        }
        self.specs.push(spec);
    }

    /// Case-insensitive lookup with no allocation on the hot path.
    #[inline]
    pub fn lookup(&self, name: &[u8]) -> Option<&CommandSpec> {
        let mut buf = [0u8; MAX_CMD_NAME];
        let lowered = lowercase_into(name, &mut buf)?;
        let idx = *self.index.get(lowered)?;
        self.specs.get(idx)
    }

    /// `COMMAND COUNT`.
    #[inline]
    pub fn len(&self) -> usize {
        self.specs.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    /// Iterate every registered command. `COMMAND` / `COMMAND DOCS`.
    pub fn iter(&self) -> impl Iterator<Item = &CommandSpec> {
        self.specs.iter()
    }
}

impl Default for CommandTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CommandTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandTable")
            .field("commands", &self.specs.len())
            .finish()
    }
}

/// Lowercase `name` into `buf` and return the slice, or `None` if it is too
/// long to be a command name.
#[inline]
fn lowercase_into<'b>(name: &[u8], buf: &'b mut [u8; MAX_CMD_NAME]) -> Option<&'b [u8]> {
    if name.is_empty() || name.len() > MAX_CMD_NAME {
        return None;
    }
    for (dst, src) in buf.iter_mut().zip(name.iter()) {
        *dst = src.to_ascii_lowercase();
    }
    buf.get(..name.len())
}

/// Build the complete command table.
///
/// **This is the single registration list for the entire server.** It is
/// already complete: every module named in §3 is called here, whether or not
/// it currently registers anything.
pub fn build_table() -> CommandTable {
    let mut t = CommandTable::new();

    // W1b
    connection::register(&mut t);
    // W1c
    generic::register(&mut t);
    // W2a
    string::register(&mut t);
    bit::register(&mut t);
    hll::register(&mut t);
    // W2b
    list::register(&mut t);
    hash::register(&mut t);
    set::register(&mut t);
    // W2c
    zset::register(&mut t);
    geo::register(&mut t);
    // W2d
    stream::register(&mut t);
    // W3b
    pubsub::register(&mut t);
    transaction::register(&mut t);
    // W3c
    server::register(&mut t);

    t
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

/// Ergonomics for [`Args`] that keep handlers free of `unwrap` and raw
/// indexing (§6). Implemented for `[Bytes]`, so it applies to `ArgVec` too.
pub trait ArgsExt {
    /// The command name as the client spelled it.
    fn name(&self) -> &[u8];
    /// Argument `i`, or a `Syntax` error. Arity checking runs before the
    /// handler, so a miss here means the handler's own indexing is wrong.
    fn at(&self, i: usize) -> Result<&Bytes, CmdError>;
    /// Argument `i` parsed with Redis's `string2ll` rules.
    fn i64_at(&self, i: usize) -> Result<i64, CmdError>;
    /// Argument `i` parsed with Redis's `getDoubleFromObject` rules.
    fn f64_at(&self, i: usize) -> Result<f64, CmdError>;
    /// True when argument `i` equals `kw`, ignoring ASCII case.
    fn kw_at(&self, i: usize, kw: &str) -> bool;
}

impl ArgsExt for Args {
    #[inline]
    fn name(&self) -> &[u8] {
        self.first().map_or(&[][..], |b| &b[..])
    }

    #[inline]
    fn at(&self, i: usize) -> Result<&Bytes, CmdError> {
        self.get(i).ok_or(CmdError::Syntax)
    }

    #[inline]
    fn i64_at(&self, i: usize) -> Result<i64, CmdError> {
        crate::util::strnum::string2ll(self.at(i)?).ok_or(CmdError::NotAnInteger)
    }

    #[inline]
    fn f64_at(&self, i: usize) -> Result<f64, CmdError> {
        crate::util::strnum::string2d(self.at(i)?).ok_or(CmdError::NotAFloat)
    }

    #[inline]
    fn kw_at(&self, i: usize, kw: &str) -> bool {
        self.get(i)
            .is_some_and(|a| crate::util::eq_ignore_ascii_case(a, kw.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noop(_c: &mut Ctx<'_>, _a: &Args) -> Result<(), CmdError> {
        Ok(())
    }

    fn spec(name: &'static str, arity: i32, first: i32, last: i32, step: i32) -> CommandSpec {
        CommandSpec {
            name,
            arity,
            flags: CmdFlags::empty(),
            first_key: first,
            last_key: last,
            key_step: step,
            handler: noop,
            get_keys: None,
            tips: &[],
            since: "1.0.0",
            summary: "",
        }
    }

    fn args(items: &[&str]) -> ArgVec {
        items
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }

    #[test]
    fn arity_semantics_match_redis() {
        // GET: arity 2, exact.
        let get = spec("get", 2, 1, 1, 1);
        assert!(!get.arity_ok(1));
        assert!(get.arity_ok(2));
        assert!(!get.arity_ok(3));
        // SET: arity -3, at least.
        let set = spec("set", -3, 1, 1, 1);
        assert!(!set.arity_ok(2));
        assert!(set.arity_ok(3));
        assert!(set.arity_ok(9));
    }

    #[test]
    fn key_positions_single_key() {
        let get = spec("get", 2, 1, 1, 1);
        assert_eq!(&get.key_positions(&args(&["get", "k"]))[..], &[1]);
    }

    #[test]
    fn key_positions_variadic() {
        // DEL: first 1, last -1, step 1.
        let del = spec("del", -2, 1, -1, 1);
        assert_eq!(
            &del.key_positions(&args(&["del", "a", "b", "c"]))[..],
            &[1, 2, 3]
        );
        assert_eq!(&del.key_positions(&args(&["del"]))[..], &[] as &[usize]);
    }

    #[test]
    fn key_positions_stepped() {
        // MSET: first 1, last -1, step 2.
        let mset = spec("mset", -3, 1, -1, 2);
        assert_eq!(
            &mset.key_positions(&args(&["mset", "a", "1", "b", "2"]))[..],
            &[1, 3]
        );
    }

    #[test]
    fn key_positions_bounded_last() {
        // SMOVE: first 1, last 2, step 1.
        let smove = spec("smove", 4, 1, 2, 1);
        assert_eq!(
            &smove.key_positions(&args(&["smove", "src", "dst", "m"]))[..],
            &[1, 2]
        );
    }

    #[test]
    fn key_positions_keyless() {
        let ping = spec("ping", -1, 0, 0, 0);
        assert!(ping.key_positions(&args(&["ping"])).is_empty());
    }

    #[test]
    fn key_positions_never_run_past_argc() {
        // A malformed spec (or a short argv) must not produce a bad index.
        let smove = spec("smove", 4, 1, 5, 1);
        let positions = smove.key_positions(&args(&["smove", "a"]));
        assert_eq!(&positions[..], &[1]);
    }

    #[test]
    fn movable_keys() {
        fn georadius_keys(a: &Args) -> KeyPositions {
            let mut v = KeyPositions::new();
            v.push(1);
            if a.len() > 6 {
                v.push(6);
            }
            v
        }
        let mut s = spec("georadius", -6, 1, 1, 1);
        s.flags = CmdFlags::MOVABLE_KEYS;
        s.get_keys = Some(georadius_keys);
        assert_eq!(
            &s.key_positions(&args(&["georadius", "k", "a", "b", "c", "d", "store"]))[..],
            &[1, 6]
        );
        assert_eq!(&s.key_positions(&args(&["georadius", "k"]))[..], &[1]);
    }

    #[test]
    fn table_lookup_is_case_insensitive() {
        let mut t = CommandTable::new();
        t.add(spec("get", 2, 1, 1, 1));
        assert!(t.lookup(b"get").is_some());
        assert!(t.lookup(b"GET").is_some());
        assert!(t.lookup(b"GeT").is_some());
        assert!(t.lookup(b"gets").is_none());
        assert!(t.lookup(b"").is_none());
        assert!(t.lookup(&[b'x'; 64]).is_none());
        assert_eq!(t.len(), 1);
    }

    #[test]
    #[should_panic(expected = "duplicate command registration")]
    fn duplicate_registration_panics() {
        let mut t = CommandTable::new();
        t.add(spec("get", 2, 1, 1, 1));
        t.add(spec("get", 2, 1, 1, 1));
    }

    #[test]
    #[should_panic(expected = "must be registered lowercase")]
    fn uppercase_registration_panics() {
        let mut t = CommandTable::new();
        t.add(spec("GET", 2, 1, 1, 1));
    }

    #[test]
    fn the_real_table_builds_and_is_consistent() {
        let t = build_table();
        assert!(t.len() >= 10, "foundation must register its smoke commands");
        for s in t.iter() {
            assert!(s.arity != 0, "{}: arity 0 is never valid", s.name);
            assert!(
                !s.flags.contains(CmdFlags::MOVABLE_KEYS) || s.get_keys.is_some(),
                "{}: MOVABLE_KEYS without get_keys",
                s.name
            );
            assert!(
                s.first_key <= 0 || s.key_step > 0,
                "{}: keys declared without a step",
                s.name
            );
            assert!(
                !(s.flags.contains(CmdFlags::WRITE) && s.flags.contains(CmdFlags::READONLY)),
                "{}: cannot be both WRITE and READONLY",
                s.name
            );
            assert!(t.lookup(s.name.as_bytes()).is_some());
        }
    }

    #[test]
    fn args_ext() {
        let a = args(&["set", "k", "42", "1.5"]);
        assert_eq!(a.name(), b"set");
        assert_eq!(&a.at(1).unwrap()[..], b"k");
        assert_eq!(a.i64_at(2).unwrap(), 42);
        assert!((a.f64_at(3).unwrap() - 1.5).abs() < f64::EPSILON);
        assert!(a.kw_at(0, "SET"));
        assert!(!a.kw_at(0, "get"));
        assert!(matches!(a.at(99), Err(CmdError::Syntax)));
        assert!(matches!(a.i64_at(1), Err(CmdError::NotAnInteger)));
        assert!(matches!(a.f64_at(1), Err(CmdError::NotAFloat)));
    }
}
