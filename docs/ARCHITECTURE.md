# rsdis — architecture & build contract

An optimized Redis-compatible server in Rust. Wire-compatible with real Redis clients
(RESP2 + RESP3), file-compatible with real RDB, command-compatible for the core
command set.

**This document is the contract between parallel implementation agents. Types and
signatures marked FROZEN must not be changed by an implementation agent — if one
looks wrong, stop and report it instead of editing it.**

---

## 1. Scope

In scope (v1):

- All core data types: string, list, hash, set, sorted set, bitmap, HyperLogLog,
  stream, geo.
- Key lifecycle: expiry (lazy + active), eviction (`maxmemory` + all 8 policies),
  `OBJECT ENCODING`-visible encodings.
- Protocol: RESP2 and RESP3, inline commands, pipelining, `HELLO`, push messages.
- Pub/Sub (channels, patterns, RESP3 shard channels), keyspace notifications.
- Transactions: `MULTI`/`EXEC`/`DISCARD`/`WATCH` with real optimistic locking.
- Blocking commands: `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `BZPOPMIN/MAX`, `BZMPOP`,
  `XREAD BLOCK`, `WAIT`.
- Persistence: RDB (read + write, real format) and AOF (append + rewrite + all
  three fsync policies).
- Admin: `INFO`, `CONFIG GET/SET`, `COMMAND`/`COMMAND DOCS`/`COMMAND COUNT`,
  `CLIENT *`, `DBSIZE`, `FLUSHDB`/`FLUSHALL`, `DEBUG JMAP`-style subset, `SLOWLOG`,
  `MEMORY USAGE`, `LATENCY`.

Out of scope for v1, but the design must not foreclose it:

- Replication (`REPLICAOF`, `PSYNC`) — the propagation path (§7) is built now so
  replication is a consumer of it later.
- Lua scripting / `FUNCTION`.
- Cluster mode — but key→shard mapping already goes through the 16384-slot CRC16
  function, so slots are already there.
- ACL beyond `AUTH` with `requirepass`.

---

## 2. Concurrency model

The keyspace is **sharded**. `N` shards, `N` = next power of two ≥ core count
(configurable via `shard-count`). Every shard owns an independent keyspace, its own
expiry index, its own dirty counter, and its own propagation buffer. Nothing on the
hot path is shared between shards.

Key → shard:

```
slot  = crc16(key_or_hashtag) & 16383      // identical to Redis Cluster
shard = slot & (N - 1)
```

Hash tags (`{...}`) are honoured now, so multi-key commands on a common tag land on
one shard and cluster mode later is a config change, not a rewrite.

### 2.1 Shard access discipline — read this twice

A worker thread executes a command by locking exactly the shards that command's keys
resolve to, **in ascending shard index order**, and releasing them when the handler
returns. Ascending order is the only thing preventing deadlock between two
multi-key commands; there are no exceptions to it, including in `EXEC`.

Locking is `parking_lot::Mutex` over a `CachePadded` shard. With 16–64 shards and
uniform keys, contention is negligible; skew is the failure mode to watch and the
benchmark suite must cover it.

> Deviation, stated deliberately: the fully thread-per-core actor design (shard
> pinned to a core, commands delivered by channel) removes the lock entirely but
> costs a channel hop per command and makes blocking ops and `EXEC` substantially
> harder. Every command in this design declares its keys through the same
> `KeySpec` → `ShardSet` path an actor model needs, so switching the transport from
> "lock the shard" to "post to the shard's queue" is a localized change in
> `engine.rs`, not a rewrite of 200 command handlers. It is a planned Phase 5
> optimization, measured against the lock version rather than assumed better.

### 2.2 I/O

`tokio` multi-threaded runtime, worker count = core count. Listener uses `socket2`
with `SO_REUSEPORT` and one accept loop per worker. Per connection: `TCP_NODELAY`,
a read buffer and a write buffer, and full pipelining — parse as many complete
commands as the read buffer holds, execute them, flush the replies once.

### 2.3 Databases

`SELECT 0..15`. Each shard holds `Vec<Db>`; a `Db` is a dict plus an expiry index.
Database index is part of the shard lookup, never part of the shard hash.

---

## 3. Repository layout & ownership

Exactly one agent owns each path. **Do not create, edit, or delete a file outside
your own paths.** If you need something from another agent's module, it is either
already in the frozen foundation or it is a contract gap — report it, do not
reach across.

```
Cargo.toml                    F0 only (full dependency set is declared up front)
src/main.rs                   F0
src/lib.rs                    F0
src/object.rs                 F0   FROZEN  Robj enum, encodings
src/error.rs                  F0   FROZEN  CmdError
src/reply.rs                  F0   FROZEN  ReplyWriter (RESP2/RESP3)
src/ctx.rs                    F0   FROZEN  Ctx, handler-facing API
src/command/mod.rs            F0   FROZEN  CommandSpec, CommandTable, registration
src/shard/mod.rs              F0   FROZEN  Shard, Db, Entry, ShardSet, slot fn
src/engine.rs                 F0   FROZEN  key resolution, lock ordering, dispatch
src/config.rs                 F0
src/util/{crc16,crc64,lzf,strnum,rand}.rs   F0

src/encoding/listpack.rs      W1a
src/encoding/intset.rs        W1a
src/encoding/quicklist.rs     W1a
src/encoding/skiplist.rs      W1a
src/encoding/rax.rs           W1a
src/encoding/mod.rs           W1a

src/net/**                    W1b
src/resp/**                   W1b
src/command/connection.rs     W1b

src/shard/expire.rs           W1c
src/shard/evict.rs            W1c
src/notify.rs                 W1c
src/command/generic.rs        W1c

src/types/string.rs           W2a
src/types/hll.rs              W2a
src/command/{string,bit,hll}.rs   W2a

src/types/{list,hash,set}.rs  W2b
src/command/{list,hash,set}.rs    W2b

src/types/zset.rs             W2c
src/command/{zset,geo}.rs     W2c
src/util/geohash.rs           W2c

src/types/stream.rs           W2d
src/command/stream.rs         W2d

src/rdb/**                    W3a
src/aof/**                    W3a

src/pubsub.rs                 W3b
src/blocking.rs               W3b
src/command/{pubsub,transaction}.rs   W3b

src/command/server.rs         W3c
src/info.rs                   W3c
src/slowlog.rs                W3c

tests/**                      W3d (each other agent may add tests/<own_area>_test.rs)
benches/**                    W3d
```

`src/command/mod.rs` is written once by F0 with every `mod` declaration and every
`register(&mut table)` call already present, pointing at modules that start as
empty stubs. That is what keeps parallel agents out of each other's merge path:
**no agent ever edits a shared registration list.**

---

## 4. Frozen core types

### 4.1 Object

```rust
pub type Key = bytes::Bytes;

pub struct Entry {
    pub obj: Robj,
    pub expire_at_ms: Option<u64>,   // None = no TTL
    pub lru: AtomicU32,              // clock or LFU counter, per maxmemory-policy
}

pub enum Robj {
    Str(StrObj),        // also backs bitmaps and HyperLogLog
    List(ListObj),
    Hash(HashObj),
    Set(SetObj),
    ZSet(ZSetObj),      // also backs geo
    Stream(StreamObj),
}

impl Robj {
    pub fn type_name(&self) -> &'static str;   // "string" | "list" | ...
    pub fn encoding(&self) -> &'static str;    // OBJECT ENCODING string
    pub fn mem_usage(&self) -> usize;          // MEMORY USAGE / maxmemory accounting
}
```

Each `*Obj` type is declared by F0 as an opaque stub and implemented by its owning
agent. **The `Robj` enum itself never changes.** `StrObj` must support the int
encoding (`OBJECT ENCODING` → `int`), `embstr` (≤44 bytes), and `raw`, because
real clients and the test suite check for them.

### 4.2 Errors

```rust
pub enum CmdError {
    WrongType,                    // WRONGTYPE Operation against a key ...
    WrongArity(&'static str),
    NotAnInteger,                 // value is not an integer or out of range
    NotAFloat,
    Syntax,
    OutOfRange,
    IndexOutOfRange,
    NoSuchKey,
    ValueOverflow,
    Unauthenticated,
    NoProto,
    Oom,
    Custom(&'static str, String), // (code, message) e.g. ("BUSYGROUP", "...")
    Io(std::io::Error),
}
```

Error strings must match real Redis byte for byte. Client libraries match on them.

### 4.3 Reply writer

A concrete struct, not a trait — this is on the hot path and must inline.

```rust
pub struct ReplyWriter<'a> { buf: &'a mut BytesMut, pub proto: u8 }

impl<'a> ReplyWriter<'a> {
    pub fn simple(&mut self, s: &str);
    pub fn error(&mut self, e: &CmdError);
    pub fn int(&mut self, v: i64);
    pub fn bulk(&mut self, b: &[u8]);
    pub fn bulk_from(&mut self, b: &Bytes);      // zero-copy path where possible
    pub fn null(&mut self);                       // $-1 on RESP2, _ on RESP3
    pub fn null_array(&mut self);                 // *-1 on RESP2, _ on RESP3
    pub fn array(&mut self, len: usize);          // header only; caller writes items
    pub fn map(&mut self, len: usize);            // %n on RESP3, *2n on RESP2
    pub fn set_header(&mut self, len: usize);     // ~n on RESP3, *n on RESP2
    pub fn double(&mut self, v: f64);             // ,x on RESP3, bulk on RESP2
    pub fn boolean(&mut self, v: bool);           // #t/#f on RESP3, :1/:0 on RESP2
    pub fn verbatim(&mut self, fmt: &str, b: &[u8]);
    pub fn big_number(&mut self, s: &str);
    pub fn push(&mut self, len: usize);           // >n on RESP3, *n on RESP2
    pub fn ok(&mut self);
}
```

Every handler writes exactly one top-level reply. RESP2/RESP3 divergence is handled
here and **nowhere else** — no handler may branch on `proto` except where the reply
*shape* genuinely differs per the Redis docs (e.g. `XPENDING`, `CONFIG GET`).

### 4.4 Command spec

```rust
bitflags! {
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
        const MOVABLE_KEYS = 1 << 11;   // key positions need a helper fn
        // Added during W0 — the original list could not express these.
        const ALL_SHARDS   = 1 << 12;   // touches the whole keyspace: FLUSHALL,
                                        // DBSIZE, KEYS, SWAPDB. Distinct from
                                        // first_key == 0, which means "locks
                                        // nothing" (PING) — not the same thing.
        const NO_SHARDS    = 1 << 13;   // handled entirely in the connection
                                        // layer, never reaches the lock path:
                                        // SUBSCRIBE, MULTI, RESET.
    }
}

pub struct CommandSpec {
    pub name: &'static str,
    pub arity: i32,            // Redis semantics: negative means "at least |arity|"
    pub flags: CmdFlags,
    pub first_key: i32,
    pub last_key: i32,         // negative counts from the end
    pub key_step: i32,
    pub handler: Handler,
    pub get_keys: Option<fn(&Args) -> SmallVec<[usize; 8]>>,   // for MOVABLE_KEYS
    pub tips: &'static [&'static str],
    pub since: &'static str,
    pub summary: &'static str,
}

pub type Handler = fn(&mut Ctx<'_>, &Args) -> Result<(), CmdError>;
```

`arity`, `first_key`, `last_key`, `key_step` are not decoration — `COMMAND INFO`
returns them, the engine uses them to resolve the shard set, and cluster mode will
use them for slot checks. Get them right; compare against real
`COMMAND INFO <name>` output.

Registration, from each command module:

```rust
pub fn register(t: &mut CommandTable) {
    t.add(CommandSpec { name: "get", arity: 2, flags: READONLY | FAST,
                        first_key: 1, last_key: 1, key_step: 1,
                        handler: cmd_get, .. });
}
```

Lookup is a perfect-hash / case-insensitive static map built once at startup, never
a linear scan.

### 4.5 Handler context

```rust
pub struct Ctx<'a> {
    pub out: ReplyWriter<'a>,
    pub client: &'a mut ClientState,   // id, name, db index, proto, flags, ...
    pub server: &'a ServerShared,      // config snapshot, clock, stats (atomics)
    shards: ShardGuards<'a>,           // the locked shards, already ordered
    pub now_ms: u64,                   // one clock read per command, cached
}

impl<'a> Ctx<'a> {
    /// Read access, expiry-aware: returns None if missing or logically expired.
    pub fn lookup_read(&mut self, key: &Key) -> Option<&Robj>;
    /// Write access; bumps dirty and touches LRU/LFU.
    pub fn lookup_write(&mut self, key: &Key) -> Option<&mut Robj>;
    pub fn insert(&mut self, key: Key, obj: Robj, expire_at_ms: Option<u64>);
    pub fn remove(&mut self, key: &Key) -> bool;

    /// Type-checked helpers. These are the ONLY sanctioned way to get a typed
    /// object; they return CmdError::WrongType, so no handler hand-rolls it.
    pub fn get_list(&mut self, key: &Key) -> Result<Option<&mut ListObj>, CmdError>;
    // ... one per type

    pub fn signal_modified(&mut self, key: &Key);        // dirty++, WATCH invalidation
    pub fn notify(&mut self, class: NotifyClass, event: &str, key: &Key);
    pub fn propagate(&mut self, args: &[&[u8]]);         // override what hits AOF
    pub fn propagate_none(&mut self);                    // suppress default
    pub fn block_on(&mut self, keys: &[Key], timeout_ms: u64, on: BlockKind);
}
```

Default propagation: a command with `WRITE` that ended with `dirty` increased is
propagated verbatim. Non-deterministic commands (`SPOP`, `EXPIRE`, `SETEX`,
`INCRBYFLOAT`, `GETEX`, anything using randomness or the clock) **must** call
`propagate` with the deterministic equivalent — `SPOP` → `SREM`, `EXPIRE` →
`PEXPIREAT`, `INCRBYFLOAT` → `SET`. This is the single most common correctness bug
in Redis clones; every write handler must be reviewed for it.

---

## 5. Performance rules

These are requirements, not aspirations. Every implementation agent is held to them
in review.

1. **No allocation on a read hit.** `GET`, `HGET`, `LRANGE`, `ZSCORE` on an existing
   key must not allocate. Replies are written into the connection's existing
   `BytesMut`; keys and values are `Bytes` slices of buffers that already exist.
2. **Parse without copying.** The RESP parser yields `Bytes` slices into the read
   buffer. A command argument is never a fresh `Vec<u8>` or `String`.
3. **`foldhash` (or `ahash`) everywhere.** The default SipHash is banned for
   internal maps. Never `std::collections::HashMap` on a hot path — `hashbrown`.
4. **`SmallVec` for argument lists and small collections.** Typical commands have
   ≤ 8 arguments; that must not touch the allocator.
5. **Small-object encodings are mandatory, not optional.** listpack for small
   hash/list/zset, intset for small all-integer sets, int/embstr for strings. These
   are 5–10× memory wins on realistic workloads and `OBJECT ENCODING` must report
   them correctly. Conversion thresholds come from config
   (`hash-max-listpack-entries` etc.) and match Redis defaults.
6. **One clock read per command.** `Ctx::now_ms` is cached; a coarse clock is
   updated by a background ticker at 1 ms. No `SystemTime::now()` in a handler.
7. **No `format!` in a reply path.** Integers via `itoa`, floats via `ryu`, both
   written straight into the output buffer.
8. **Bounded work per command.** `KEYS` and `SCAN` never hold a shard lock across
   the whole keyspace; `SCAN` uses the reverse-binary cursor so it is correct
   across rehashes, and returns per shard.
9. **`#[inline]` deliberately, not everywhere.** Mark small hot accessors; leave
   everything else to the optimizer.
10. **Benchmark before claiming.** Any "this is faster" statement in a PR body needs
    a `redis-benchmark` or criterion number next to it.
11. **`Robj` has a size budget: 32 bytes, enforced by a `const` assertion.**
    An enum is as wide as its widest variant, so a fat `StreamObj` or `ZSetObj`
    inflates *every* dict entry in the server — including the millions of plain
    strings that never touch a stream — and costs cache misses keyspace-wide.
    Any variant whose natural representation exceeds the budget must box its
    payload. Redis keeps `robj` at 16 bytes for exactly this reason. If you are
    a W2 agent and your type does not fit, box it; do not raise the assertion.
12. **No allocation in a reply path.** Related to §5.7 and violated once already:
    formatting a non-integral double must use a stack buffer, never a `Vec`.
    `ZSCORE`, `INCRBYFLOAT`, `GEODIST` and `ZINCRBY` all land here.

Release profile (F0 sets this): `lto = "fat"`, `codegen-units = 1`,
`panic = "abort"`, `opt-level = 3`. A `bench` profile inherits release with
`debug = true` for profiling symbols.

---

## 6. Correctness bar

- **A handler never panics on client input.** No `unwrap`, `expect`, or slice
  indexing on anything derived from the network. Malformed input is a `CmdError`.
  Clippy is configured to deny `unwrap_used` and `indexing_slicing` in
  `src/command/**`.
- **`unsafe` requires a `// SAFETY:` comment** stating the invariant and why it
  holds. In the encoding modules (listpack, rax) some `unsafe` is expected and
  fine; in command handlers it should be zero.
- **Every command gets a test that runs against real Redis semantics.** Where
  behaviour is ambiguous, the reference is Redis 7.4 — check its behaviour, don't
  guess.
- Edge cases that must be covered per type: empty key auto-create, key auto-delete
  when the collection empties, `0`/negative/oversized ranges, `+inf`/`-inf`/`nan`
  scores, 512 MB value limit, integer overflow on `INCR`, expiry mid-command.

---

## 7. Propagation & persistence path

One path, built now, shared by AOF and later by replication:

```
handler → Ctx::propagate → shard.repl_buf (per shard, ordered by shard seq)
                              ↓
                    aggregator (background) → AOF buffer → fsync per policy
                                            → (later) replication backlog
```

Per-shard buffers are drained by a background task that assigns a global sequence
number. AOF ordering within a shard is total; across shards it is causally
consistent, which is all AOF needs since cross-shard commands hold both locks.

---

## 8. Build waves

Each wave runs as parallel agents in isolated git worktrees. I review and optimize
the merged result before the next wave starts.

- **W0 — foundation** (solo, blocking): everything marked F0 above. Compiles, runs,
  serves `PING`/`ECHO`/`SET`/`GET` end to end against `redis-cli`, with every other
  module present as a registered stub. Commits to `master`.
- **W1** — `a` encodings · `b` net + RESP · `c` keyspace/expiry/evict/generic.
- **W2** — `a` string/bitops/HLL · `b` list/hash/set · `c` zset/geo · `d` stream.
- **W3** — `a` RDB + AOF · `b` pubsub/transactions/blocking · `c` admin/INFO ·
  `d` conformance + benchmark suite.
- **W4** — my optimization pass: profile, fix the allocation and contention hot
  spots, then measure against real Redis with `redis-benchmark` and `memtier`.

Definition of done for any wave agent: `cargo build --release` clean,
`cargo clippy --all-targets -- -D warnings` clean, `cargo test` green, `cargo fmt`
applied, and a summary of what was implemented, what was skipped, and the numbers
for anything claimed to be fast.

---

## 9. Binding amendments from W0

W0 found eleven places where §§1–8 were wrong or underspecified. These are now
part of the contract and override anything above that contradicts them.

**9.1 `Args` is a slice, not a struct.** §4.4 used `&Args` without defining it.

```rust
pub type Args    = [Bytes];                  // unsized; handlers take &Args
pub type ArgVec  = SmallVec<[Bytes; 8]>;     // owned storage, derefs to Args
pub type KeyPositions = SmallVec<[usize; 8]>;
```

`ArgsExt` provides `at` / `i64_at` / `f64_at` / `kw_at`. Use it; do not index.

**9.2 Wave state hangs off `ClientState` / `ServerShared` through your own
type.** Both live in the frozen `ctx.rs`, so you cannot add fields to them. Each
wave gets exactly one slot, already declared, whose type your module owns:

| slot | owner | type |
|---|---|---|
| `ClientState::subs` | W3b | `pubsub::ClientSubs` |
| `ClientState::block` | W3b | `blocking::ClientBlockState` |
| `ClientState::multi` | W3b | `transaction::MultiState` |
| `ServerShared::{pubsub, blocking, slowlog, stats}` | W3b / W3c | their own types |

Put everything your wave needs inside that type. This is the rule that keeps
`ctx.rs` frozen.

**9.3 Read commands must use the `*_read` accessors.** `lookup_write` and
`get_<type>` bump the dirty counter, which invalidates `WATCH` and triggers
verbatim propagation. A read-only command that reaches for `&mut` will abort
concurrent transactions and write junk to the AOF. `LRANGE`, `ZSCORE`, `HGET`,
`SMEMBERS`, `GETRANGE` and friends use `get_<type>_read` / `lookup_read`. This is
not a style preference; it is a correctness requirement, and it is the easiest
mistake in the entire project to make silently.

Note also that the typed write accessors type-check *before* signalling dirty, so
a `WRONGTYPE` does not dirty the key. Preserve that ordering.

**9.4 Float formatting is `strnum::d2string`, and it is not negotiable.** `ryu`
produces the right *digits* and the wrong *presentation*. Redis routes integral
doubles through `double2ll`, whose window is `LLONG_MAX/2` (~4.6e18) and **not**
2^52 — which is why `1e18` prints `1000000000000000000` but `1e19` prints
`1e+19`. Everything else goes through `fpconv.c:emit_digits`, whose plain-vs-
scientific thresholds no stock formatter reproduces. `-0.0` prints `-0`, not `0`.
This is validated against a live Redis over 1033 doubles. **Do not "simplify" it,
and do not call `format!("{}", x)` on a score.**

The formatters take `&mut impl ByteSink`, so write into the reply buffer directly
or into a stack `strnum::NumBuf`. Never into a fresh `Vec` (§5.12).

**9.5 Propagation anchors on the lowest-indexed locked shard.** A cross-shard
command must appear exactly once in the AOF stream, so it goes into one shard's
`repl_buf` — the lowest-indexed one it holds. Propagation is gated on
`ServerShared::propagation_enabled` and costs nothing until W3a enables AOF.

**9.6 `EXEC` locks the union of its queued commands' keys, once, ascending.**
`transaction::intercept` runs *before* any lock is taken, so queuing never
touches a shard. Locking per queued command inside `EXEC` breaks the
deadlock-freedom proof in §2.1. There is no escape hatch; do not add one.

**9.7 `Entry` lives in `object.rs`** and is re-exported from `crate::shard`.
§3 and §4.1 both claimed it.

**9.8 `Robj` is capped at 40 bytes and `Entry` at 64,** enforced by a `const`
assertion in `object.rs` plus a test that reports actual sizes on failure. 40 is
the natural floor: `StrObj` is a `Bytes` (32) plus a discriminant (8). `ZSetObj`
and `StreamObj` will not fit and must box their payloads. See §5.11.

**9.9 `CommandSpec` has no subcommand support.** `CLIENT`, `CONFIG`, `XGROUP`,
`OBJECT` etc. dispatch subcommands inside their own handler. Consequences to
accept for v1: `COMMAND DOCS` is incomplete for containers, and an arity error
says `'client'` rather than `'client|setname'`.

**9.10 `ReplyWriter::bulk_from` is not yet zero-copy.** It memcpys today. Real
zero-copy needs a vectored write queue of `Bytes` in `src/net` — **W1b owns this
decision.** Until W1b does it, nobody may claim §5.1 is satisfied for large
values.

**9.12 Benchmarks are `#[ignore]`d self-timing tests, not criterion.** All three
W1 agents independently hit the same wall: `Cargo.toml` is F0's, agents cannot
add a `[[bench]] harness = false` target, so a criterion `main` never runs and
trips `clippy --all-targets -D warnings` as dead code. Rather than fight it,
this is now the project convention. Write benchmarks as `#[ignore]`d tests that
time themselves and print, and run them with:

```
cargo test --release --bench <name> -- --ignored --nocapture
```

`benches/encoding_bench.rs`, `benches/protocol_bench.rs` and
`benches/keyspace_bench.rs` are the reference style. The `criterion`
dev-dependency stays for W4, which will register proper targets when it does
the comparative benchmarking against real Redis. **Name your bench file in your
handover.**

**9.11 Config quirks.** `port 0` is legal (means ephemeral) and `Config::validate`
runs on every `CONFIG SET`, not just at startup. `Cli` is not a plain clap derive:
`rsdis --port 6399` has no positional before the first flag, which clap rejects,
so `Cli::from_env` splits argv the way `redis-server` does and defers to clap only
for `--help` / `--version`.

---

## 10. Known gaps, deferred to W4

Measured, owned, and deliberately not fixed yet. Listed here so nobody claims
they are done and nobody re-discovers them.

**10.1 `bulk_from` does not use the vectored write queue — §5.1 is NOT met for
large values.** W1b built the queue and measured the wiring it cannot do:

| value | memcpy + write | queue + writev | delta |
|---|---|---|---|
| 8 B | 3.10 µs | 3.43 µs | −10% |
| 512 B | 3.43 µs | 3.62 µs | −3% |
| 2 KiB | 5.95 µs | 5.42 µs | +9% |
| 4 KiB | 10.4 µs | 7.7 µs | +26% |
| 64 KiB | 120 µs | 97 µs | +20% |

Corroborated end to end: 64 KB `GET` is 53.5k/s against real Redis 8.6's 61.7k/s
on the same host, while 8 B and 512 B are at parity. That gap is exactly this
memcpy.

The blocker is structural, not a matter of effort: `ReplyWriter` is
`{ buf: &'a mut BytesMut }` and holds no handle to `net`, and it cannot be made
generic over a sink because `Handler` is a plain `fn` pointer — generics cannot
cross a function-pointer command table. The fix is to move the staging/queue
split *into* `reply.rs` (a `FrameQueue` that `net::OutputBuffer` consumes rather
than defines) so the dispatch stays static. That restructures the two hottest
types in the server at once, which is a W4 job with every agent finished — not
something to attempt while waves are in flight.

The threshold is already chosen and shipped as `OutputBuffer::should_queue`
(~2 KiB); only the `bulk_from` call site is missing.

**10.2 `ClientHandle` has no wave-owned slot,** so `CLIENT LIST` cannot report
`db`/`resp`/`flags`/`qbuf`/`obl` for *other* connections. W1b worked around it
with a process-wide table in `src/net/registry.rs`. The right fix is the §9.2
treatment: one slot on `ClientHandle` whose type `net` owns. Retire the registry
when that lands.

**10.3 `client-output-buffer-limit` is enforced with Redis's defaults but has no
config knob.** Enforcement is real; only tuning is missing. **W3c owns
`CONFIG GET`/`SET` and should wire this** rather than F0 adding a field nobody
reads.

**10.4 Container commands re-invent subcommand reporting.** Per §9.9 there is no
subcommand support in `CommandSpec`, so W1b set `client.last_command` by hand to
get `cmd=client|list` and `'client|setname'` arity errors. Every container
command (`CONFIG`, `XGROUP`, `OBJECT`, `MEMORY`) will repeat this. Worth a shared
helper in W4, or real subcommand specs if `COMMAND DOCS` fidelity matters.

**9.13 `list-compress-depth` is out of scope for v1.** W1a left the quicklist
node-compression seam open and unimplemented. `src/util/lzf.rs` exists (RDB
needs it) but quicklist does not call it. Revisit only if memory benchmarks
justify it.
