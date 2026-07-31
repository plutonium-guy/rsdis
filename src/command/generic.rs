//! Key-lifecycle commands that are not specific to one value type.
//!
//! Owned by W1c; do not edit if you are not that agent.
//!
//! # The three things worth reading before changing anything here
//!
//! **1. Read commands use the `*_read` accessors (§9.3).** `EXISTS`, `TYPE`,
//! `TTL`, `KEYS`, `SCAN`, `RANDOMKEY`, `OBJECT`, `DUMP` and `SORT_RO` never
//! reach for `lookup_write` or a `get_<type>` accessor: those bump the dirty
//! counter, which aborts concurrent `WATCH`ers and makes the engine propagate
//! the read command verbatim into the AOF. `OBJECT IDLETIME` additionally has
//! to avoid `lookup_read`, because that *touches* the LRU clock and would make
//! every key report an idle time of zero; it goes through
//! [`with_entry`], which does lazy expiry and nothing else.
//!
//! **2. Non-deterministic writes propagate their deterministic equivalent
//! (§4.5).** `EXPIRE`/`PEXPIRE`/`EXPIREAT` all resolve to an absolute
//! millisecond deadline and propagate `PEXPIREAT`; an `EXPIRE` with a deadline
//! in the past propagates the `DEL`/`UNLINK` it performed instead; `RESTORE`
//! with a relative TTL propagates the absolute one plus `ABSTTL`, exactly as
//! `cluster.c:restoreCommand` rewrites its own argv.
//!
//! **3. `SCAN` never holds more than one shard lock.** See [`cmd_scan`] for
//! the cursor scheme and the locking argument.
//!
//! # Seams into waves that do not exist yet
//!
//! * [`ValueCodec`] is the RDB value serializer, owned by W3a. `DUMP` and
//!   `RESTORE` implement the framing (2-byte RDB version + CRC64 footer) and
//!   the command semantics; the payload itself is the codec's. `COPY` uses the
//!   same seam to deep-copy an aggregate.
//! * [`sort_elements`] is where `SORT` reads a list/set/zset. The payload
//!   types are W2b's and W2c's and are still empty placeholders, so it
//!   currently yields nothing -- which is the correct answer for an empty
//!   collection, and the only reachable one today.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;
use smallvec::SmallVec;

use crate::command::{ArgVec, Args, ArgsExt, CmdFlags, CommandSpec, CommandTable, KeyPositions};
use crate::ctx::Ctx;
use crate::error::{CmdError, CmdResult};
use crate::notify::NotifyClass;
use crate::object::{Entry, Key, Robj, lru};
use crate::shard::{Db, evict};
use crate::util::{crc64, eq_ignore_ascii_case};

// ---------------------------------------------------------------------------
// Small shared helpers
// ---------------------------------------------------------------------------

#[inline]
fn key_at(args: &Args, i: usize) -> Result<Key, CmdError> {
    Ok(args.at(i)?.clone())
}

/// `ERR invalid expire time in '<cmd>' command`.
fn invalid_expire(name: &str) -> CmdError {
    CmdError::err(format!("invalid expire time in '{name}' command"))
}

/// Read an entry without touching its LRU clock, dirty counter or hit/miss
/// counters -- but *with* lazy expiry, so an expired key reads as missing.
///
/// This is what `OBJECT` needs: `lookup_read` would refresh the very clock
/// `OBJECT IDLETIME` is asking about.
fn with_entry<R>(ctx: &mut Ctx<'_>, key: &Key, f: impl FnOnce(&Entry) -> R) -> Option<R> {
    if !ctx.exists(key) {
        return None;
    }
    let db_idx = ctx.db_index();
    ctx.shards()
        .for_key(key)
        .and_then(|s| s.db(db_idx))
        .and_then(|db| db.dict.get(key))
        .map(f)
}

/// Take a key's whole `Entry` out of `db_idx` on the shard that owns it.
///
/// Used by `RENAME`/`MOVE`, which must carry the TTL *and* the LRU/LFU state
/// across rather than re-creating the value.
fn take_entry(ctx: &mut Ctx<'_>, key: &Key, db_idx: usize) -> Option<Entry> {
    let entry = ctx
        .shards()
        .for_key(key)
        .and_then(|s| s.db(db_idx))
        .and_then(|db| db.remove_entry(key));
    if entry.is_some() {
        signal_db_watch(ctx, key, db_idx);
    }
    entry
}

/// Put an `Entry` into `db_idx` on the shard that owns `key`, replacing
/// whatever was there.
fn put_entry(ctx: &mut Ctx<'_>, key: Key, entry: Entry, db_idx: usize) {
    if let Some(db) = ctx.shards().for_key(&key).and_then(|s| s.db(db_idx)) {
        db.signal_watch(&key);
        db.set_entry(key, entry);
    }
}

/// Invalidate `WATCH` on `key` in a database that is not the client's current
/// one. `Ctx::signal_modified` always signals `client.db`; `MOVE` and
/// `COPY ... DB n` also touch another slice.
fn signal_db_watch(ctx: &mut Ctx<'_>, key: &Key, db_idx: usize) {
    if let Some(db) = ctx.shards().for_key(key).and_then(|s| s.db(db_idx)) {
        db.signal_watch(key);
    }
}

/// True when `key` is present and *live* in `db_idx`, which is not
/// necessarily the client's current database.
///
/// A logically-expired entry reads as absent but is deliberately left in
/// place: every caller either overwrites it ([`put_entry`]) or does nothing,
/// and reaping it here would delete a key without the matching `DEL` in the
/// propagation stream. The active cycle or the next lookup takes care of it.
fn exists_in_db(ctx: &mut Ctx<'_>, key: &Key, db_idx: usize) -> bool {
    let now = ctx.now_ms;
    ctx.shards()
        .for_key(key)
        .and_then(|s| s.db(db_idx))
        .and_then(|db| db.dict.get(key))
        .is_some_and(|e| !e.is_expired(now))
}

// ---------------------------------------------------------------------------
// Glob matching (`stringmatchlen`)
// ---------------------------------------------------------------------------

/// Redis's `util.c:stringmatchlen()`, iteratively.
///
/// The recursion in the C original is replaced by star backtracking so that a
/// pathological pattern (`"*a*a*a*..."` against a long key) costs time rather
/// than stack. `KEYS`, `SCAN MATCH` and `SORT`'s patterns all come from the
/// network, so this must not be able to blow up (§6).
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let (mut p, mut s) = (0usize, 0usize);
    // Position to resume from after a `*` when a later literal fails.
    let mut star_p: Option<usize> = None;
    let mut star_s = 0usize;

    while s < string.len() {
        let advanced = match pattern.get(p) {
            Some(b'*') => {
                // Collapse runs of stars; remember where to backtrack to.
                star_p = Some(p);
                star_s = s;
                p += 1;
                continue;
            }
            Some(b'?') => {
                p += 1;
                s += 1;
                true
            }
            Some(b'[') => match match_class(pattern, p, string.get(s).copied().unwrap_or(0)) {
                (true, next) => {
                    p = next;
                    s += 1;
                    true
                }
                (false, _) => false,
            },
            Some(b'\\') => match (pattern.get(p + 1), string.get(s)) {
                (Some(lit), Some(c)) if lit == c => {
                    p += 2;
                    s += 1;
                    true
                }
                // A trailing backslash matches itself, as in Redis.
                (None, Some(c)) if *c == b'\\' => {
                    p += 1;
                    s += 1;
                    true
                }
                _ => false,
            },
            Some(lit) => match string.get(s) {
                Some(c) if lit == c => {
                    p += 1;
                    s += 1;
                    true
                }
                _ => false,
            },
            None => false,
        };
        if advanced {
            continue;
        }
        match star_p {
            Some(sp) => {
                // The last `*` swallows one more character and we retry.
                star_s += 1;
                s = star_s;
                p = sp + 1;
                if star_s > string.len() {
                    return false;
                }
            }
            None => return false,
        }
    }

    while pattern.get(p) == Some(&b'*') {
        p += 1;
    }
    p >= pattern.len()
}

/// Match one `[...]` class against `c`, starting at the `[`.
/// Returns `(matched, position just past the class)`.
fn match_class(pattern: &[u8], open: usize, c: u8) -> (bool, usize) {
    let mut p = open + 1;
    let negate = pattern.get(p) == Some(&b'^');
    if negate {
        p += 1;
    }
    let mut matched = false;
    loop {
        match pattern.get(p).copied() {
            // Unterminated class: Redis stops at the end of the pattern.
            None => break,
            Some(b']') => {
                p += 1;
                break;
            }
            Some(b'\\') => {
                if let Some(lit) = pattern.get(p + 1).copied() {
                    if lit == c {
                        matched = true;
                    }
                    p += 2;
                } else {
                    p += 1;
                }
            }
            Some(start) => {
                let is_range = pattern.get(p + 1) == Some(&b'-')
                    && matches!(pattern.get(p + 2), Some(&e) if e != b']');
                if is_range {
                    let end = pattern.get(p + 2).copied().unwrap_or(start);
                    let (lo, hi) = if start <= end {
                        (start, end)
                    } else {
                        (end, start)
                    };
                    if c >= lo && c <= hi {
                        matched = true;
                    }
                    p += 3;
                } else {
                    if start == c {
                        matched = true;
                    }
                    p += 1;
                }
            }
        }
    }
    (matched != negate, p)
}

// ---------------------------------------------------------------------------
// The RDB value codec seam (owner: W3a)
// ---------------------------------------------------------------------------

/// The RDB version rsdis stamps into a `DUMP` footer, matching `REDIS0011`
/// (Redis 7.x). `RESTORE` accepts anything up to this.
pub const DUMP_RDB_VERSION: u16 = 11;

/// `DUMP` footer: 2 bytes of RDB version, 8 bytes of CRC64.
pub const DUMP_FOOTER_LEN: usize = 10;

/// Serialization of a value in RDB's object encoding.
///
/// **Owner: W3a** (`src/rdb/**`). This module deliberately does not implement
/// it: a second, parallel serializer would produce payloads that real Redis
/// cannot `RESTORE`, which is worse than not shipping `DUMP` at all. What
/// lives here instead is the part of the format that is not RDB's -- the
/// version + CRC64 footer -- plus the command semantics around it.
///
/// W3a installs an implementation once at startup:
///
/// ```ignore
/// generic::install_value_codec(Arc::new(rdb::ObjectCodec));
/// ```
pub trait ValueCodec: Send + Sync {
    /// Append the RDB object-type byte and body for `obj` to `out`.
    /// Returns false for a value the codec cannot represent.
    fn dump(&self, obj: &Robj, out: &mut Vec<u8>) -> bool;

    /// Rebuild a value from a payload previously produced by [`Self::dump`]
    /// (or by a real Redis). The footer has already been verified and
    /// stripped.
    fn restore(&self, payload: &[u8]) -> Option<Robj>;

    /// Deep-copy a value, for `COPY`. The default round-trips through the
    /// serializer, which is what Redis's `COPY` falls back to for types
    /// without a bespoke duplicator.
    fn duplicate(&self, obj: &Robj) -> Option<Robj> {
        let mut buf = Vec::new();
        if !self.dump(obj, &mut buf) {
            return None;
        }
        self.restore(&buf)
    }
}

static CODEC: RwLock<Option<Arc<dyn ValueCodec>>> = RwLock::new(None);

/// Install the RDB value codec, returning whatever was installed before.
pub fn install_value_codec(codec: Arc<dyn ValueCodec>) -> Option<Arc<dyn ValueCodec>> {
    CODEC.write().replace(codec)
}

/// Remove the RDB value codec.
pub fn clear_value_codec() -> Option<Arc<dyn ValueCodec>> {
    CODEC.write().take()
}

/// The installed codec, if any.
pub fn value_codec() -> Option<Arc<dyn ValueCodec>> {
    CODEC.read().clone()
}

fn no_codec() -> CmdError {
    CmdError::err("DUMP/RESTORE requires the RDB serializer, which is not loaded")
}

/// Append the `DUMP` footer: RDB version (little endian u16) then CRC64 of
/// everything before it (little endian u64).
fn seal_dump(mut body: Vec<u8>) -> Vec<u8> {
    body.extend_from_slice(&DUMP_RDB_VERSION.to_le_bytes());
    let crc = crc64::digest(&body);
    body.extend_from_slice(&crc.to_le_bytes());
    body
}

/// `cluster.c:verifyDumpPayload()`. Returns the value payload with the footer
/// stripped, or `None` when the version is too new or the CRC does not match.
fn verify_dump(buf: &[u8]) -> Option<&[u8]> {
    if buf.len() < DUMP_FOOTER_LEN {
        return None;
    }
    let split = buf.len() - DUMP_FOOTER_LEN;
    let body = buf.get(..split)?;
    let footer = buf.get(split..)?;
    let version = u16::from_le_bytes([*footer.first()?, *footer.get(1)?]);
    if version > DUMP_RDB_VERSION {
        return None;
    }
    let stored = u64::from_le_bytes(footer.get(2..10)?.try_into().ok()?);
    let computed = crc64::digest(buf.get(..buf.len() - 8)?);
    if stored != computed {
        return None;
    }
    Some(body)
}

/// Deep-copy a value for `COPY`.
///
/// Strings copy natively; every aggregate needs the codec, because `Robj` is
/// not `Clone` and the payload types expose no duplicator (see the handover
/// note).
fn duplicate_value(obj: &Robj) -> Option<Robj> {
    match obj {
        Robj::Str(s) => Some(Robj::Str(s.clone())),
        other => value_codec().and_then(|c| c.duplicate(other)),
    }
}

// ---------------------------------------------------------------------------
// DEL / UNLINK / EXISTS / TYPE / TOUCH
// ---------------------------------------------------------------------------

fn cmd_del(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let mut removed = 0i64;
    for i in 1..args.len() {
        let key = key_at(args, i)?;
        if ctx.remove(&key) {
            removed += 1;
            ctx.notify(NotifyClass::GENERIC, "del", &key);
        }
    }
    ctx.out.int(removed);
    Ok(())
}

fn cmd_exists(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    // EXISTS counts repeats: `EXISTS k k` on a live key replies 2.
    let mut found = 0i64;
    for i in 1..args.len() {
        let key = key_at(args, i)?;
        if ctx.exists(&key) {
            found += 1;
        }
    }
    ctx.out.int(found);
    Ok(())
}

fn cmd_type(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let key = key_at(args, 1)?;
    match ctx.type_name(&key) {
        Some(t) => ctx.out.simple(t),
        // TYPE on a missing key is `+none`, not a null.
        None => ctx.out.simple("none"),
    }
    Ok(())
}

/// `TOUCH` is the one read command that is *supposed* to have a side effect:
/// it refreshes the LRU clock, which is exactly what `lookup_read` does.
fn cmd_touch(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let mut touched = 0i64;
    for i in 1..args.len() {
        let key = key_at(args, i)?;
        if ctx.lookup_read(&key).is_some() {
            touched += 1;
        }
    }
    ctx.out.int(touched);
    Ok(())
}

// ---------------------------------------------------------------------------
// TTL / PTTL / EXPIRETIME / PEXPIRETIME / PERSIST
// ---------------------------------------------------------------------------

/// Shared by `TTL` (seconds) and `PTTL` (milliseconds).
///
/// Redis replies `-2` when the key does not exist and `-1` when it exists but
/// has no TTL.
fn ttl_generic(ctx: &mut Ctx<'_>, args: &Args, millis: bool) -> CmdResult {
    let key = key_at(args, 1)?;
    if !ctx.exists(&key) {
        ctx.out.int(-2);
        return Ok(());
    }
    match ctx.expire_at(&key) {
        None => ctx.out.int(-1),
        Some(at) => {
            let remaining = at.saturating_sub(ctx.now_ms);
            if millis {
                ctx.out.int(remaining as i64);
            } else {
                // Redis rounds to the nearest second: `(ttl+500)/1000`.
                ctx.out.int(((remaining + 500) / 1000) as i64);
            }
        }
    }
    Ok(())
}

fn cmd_ttl(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    ttl_generic(ctx, args, false)
}

fn cmd_pttl(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    ttl_generic(ctx, args, true)
}

/// `EXPIRETIME` / `PEXPIRETIME`: the absolute deadline rather than what is
/// left of it.
fn expiretime_generic(ctx: &mut Ctx<'_>, args: &Args, millis: bool) -> CmdResult {
    let key = key_at(args, 1)?;
    if !ctx.exists(&key) {
        ctx.out.int(-2);
        return Ok(());
    }
    match ctx.expire_at(&key) {
        None => ctx.out.int(-1),
        Some(at) => {
            if millis {
                ctx.out.int(at as i64);
            } else {
                ctx.out.int(((at + 500) / 1000) as i64);
            }
        }
    }
    Ok(())
}

fn cmd_expiretime(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    expiretime_generic(ctx, args, false)
}

fn cmd_pexpiretime(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    expiretime_generic(ctx, args, true)
}

fn cmd_persist(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let key = key_at(args, 1)?;
    if !ctx.exists(&key) || ctx.expire_at(&key).is_none() {
        ctx.propagate_none();
        ctx.out.int(0);
        return Ok(());
    }
    ctx.set_expire(&key, None);
    ctx.notify(NotifyClass::GENERIC, "persist", &key);
    ctx.out.int(1);
    Ok(())
}

// ---------------------------------------------------------------------------
// EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpireCond {
    Always,
    /// Only when the key has no TTL.
    Nx,
    /// Only when the key already has a TTL.
    Xx,
    /// Only when the new deadline is later than the current one.
    Gt,
    /// Only when the new deadline is earlier than the current one.
    Lt,
}

fn parse_expire_cond(args: &Args, from: usize) -> Result<ExpireCond, CmdError> {
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    for i in from..args.len() {
        let tok = args.at(i)?;
        if eq_ignore_ascii_case(tok, b"NX") {
            nx = true;
        } else if eq_ignore_ascii_case(tok, b"XX") {
            xx = true;
        } else if eq_ignore_ascii_case(tok, b"GT") {
            gt = true;
        } else if eq_ignore_ascii_case(tok, b"LT") {
            lt = true;
        } else {
            return Err(CmdError::err(format!(
                "Unsupported option {}",
                String::from_utf8_lossy(tok)
            )));
        }
    }
    if gt && lt {
        return Err(CmdError::err(
            "GT and LT options at the same time are not compatible",
        ));
    }
    if nx && (xx || gt || lt) {
        return Err(CmdError::err(
            "NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    Ok(if nx {
        ExpireCond::Nx
    } else if xx {
        ExpireCond::Xx
    } else if gt {
        ExpireCond::Gt
    } else if lt {
        ExpireCond::Lt
    } else {
        ExpireCond::Always
    })
}

/// Redis's condition table. A key with no TTL counts as "expires at
/// infinity", which is why `GT` can never fire on it and `LT` always can.
fn cond_allows(cond: ExpireCond, current: Option<u64>, when_ms: i64) -> bool {
    match (cond, current) {
        (ExpireCond::Always, _) => true,
        (ExpireCond::Nx, Some(_)) => false,
        (ExpireCond::Nx, None) => true,
        (ExpireCond::Xx, None) => false,
        (ExpireCond::Xx, Some(_)) => true,
        (ExpireCond::Gt, None) => false,
        (ExpireCond::Gt, Some(cur)) => when_ms > cur as i64,
        (ExpireCond::Lt, None) => true,
        (ExpireCond::Lt, Some(cur)) => when_ms < cur as i64,
    }
}

/// `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`.
///
/// `unit_ms` selects the argument's unit, `absolute` whether it is a deadline
/// or an offset. Whatever the spelling, the command propagates as
/// `PEXPIREAT <key> <absolute ms>` -- an offset replayed from the AOF hours
/// later would resurrect a key that should be long gone (§4.5).
fn expire_generic(
    ctx: &mut Ctx<'_>,
    args: &Args,
    unit_ms: bool,
    absolute: bool,
    name: &'static str,
) -> CmdResult {
    let key = key_at(args, 1)?;
    let raw = args.i64_at(2)?;
    let cond = parse_expire_cond(args, 3)?;

    // Overflow checks, in Redis's order: scale to ms first, then rebase.
    let mut when = raw;
    if !unit_ms {
        if !(i64::MIN / 1000..=i64::MAX / 1000).contains(&when) {
            return Err(invalid_expire(name));
        }
        when *= 1000;
    }
    let basetime = if absolute { 0 } else { ctx.now_ms as i64 };
    if when > i64::MAX - basetime {
        return Err(invalid_expire(name));
    }
    when += basetime;

    if !ctx.exists(&key) {
        ctx.propagate_none();
        ctx.out.int(0);
        return Ok(());
    }
    let current = ctx.expire_at(&key);
    if !cond_allows(cond, current, when) {
        ctx.propagate_none();
        ctx.out.int(0);
        return Ok(());
    }

    if when <= ctx.now_ms as i64 {
        // A deadline in the past deletes the key outright, and propagates the
        // deletion rather than the expiry.
        let lazy = ctx.config().lazyfree_lazy_expire;
        ctx.remove(&key);
        ctx.notify(NotifyClass::GENERIC, "del", &key);
        let verb: &[u8] = if lazy { b"UNLINK" } else { b"DEL" };
        ctx.propagate(&[verb, &key]);
        ctx.out.int(1);
        return Ok(());
    }

    ctx.set_expire(&key, Some(when as u64));
    ctx.notify(NotifyClass::GENERIC, "expire", &key);
    let mut fmt = itoa::Buffer::new();
    ctx.propagate(&[b"PEXPIREAT", &key, fmt.format(when).as_bytes()]);
    ctx.out.int(1);
    Ok(())
}

fn cmd_expire(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    expire_generic(ctx, args, false, false, "expire")
}

fn cmd_pexpire(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    expire_generic(ctx, args, true, false, "pexpire")
}

fn cmd_expireat(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    expire_generic(ctx, args, false, true, "expireat")
}

fn cmd_pexpireat(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    expire_generic(ctx, args, true, true, "pexpireat")
}

// ---------------------------------------------------------------------------
// RENAME / RENAMENX
// ---------------------------------------------------------------------------

fn rename_generic(ctx: &mut Ctx<'_>, args: &Args, nx: bool) -> CmdResult {
    let src = key_at(args, 1)?;
    let dst = key_at(args, 2)?;
    let db_idx = ctx.db_index();

    if !ctx.exists(&src) {
        return Err(CmdError::NoSuchKey);
    }
    if src == dst {
        // Renaming a key to itself is a no-op that still succeeds -- and must
        // not clear the TTL or dirty the key.
        ctx.propagate_none();
        if nx {
            ctx.out.int(0);
        } else {
            ctx.out.ok();
        }
        return Ok(());
    }
    if exists_in_db(ctx, &dst, db_idx) && nx {
        ctx.propagate_none();
        ctx.out.int(0);
        return Ok(());
    }

    // Moving the whole `Entry` is what carries the TTL and the LRU/LFU state
    // across, which re-inserting the value would not.
    let Some(entry) = take_entry(ctx, &src, db_idx) else {
        return Err(CmdError::NoSuchKey);
    };
    put_entry(ctx, dst.clone(), entry, db_idx);
    ctx.signal_modified(&src);
    ctx.signal_modified(&dst);
    ctx.notify(NotifyClass::GENERIC, "rename_from", &src);
    ctx.notify(NotifyClass::GENERIC, "rename_to", &dst);

    if nx {
        ctx.out.int(1);
    } else {
        ctx.out.ok();
    }
    Ok(())
}

fn cmd_rename(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    rename_generic(ctx, args, false)
}

fn cmd_renamenx(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    rename_generic(ctx, args, true)
}

// ---------------------------------------------------------------------------
// COPY / MOVE
// ---------------------------------------------------------------------------

fn cmd_copy(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let src = key_at(args, 1)?;
    let dst = key_at(args, 2)?;
    let src_db = ctx.db_index();
    let mut dst_db = src_db;
    let mut replace = false;

    let mut i = 3usize;
    while i < args.len() {
        let tok = args.at(i)?;
        if eq_ignore_ascii_case(tok, b"REPLACE") {
            replace = true;
            i += 1;
        } else if eq_ignore_ascii_case(tok, b"DB") {
            let n = args.i64_at(i + 1)?;
            let databases = ctx.server.shards.databases() as i64;
            if n < 0 || n >= databases {
                return Err(CmdError::err("DB index is out of range"));
            }
            dst_db = n as usize;
            i += 2;
        } else {
            return Err(CmdError::Syntax);
        }
    }

    if src == dst && src_db == dst_db {
        return Err(CmdError::err("source and destination objects are the same"));
    }

    evict::check_oom(ctx.server, ctx.config())?;

    if !ctx.exists(&src) {
        ctx.propagate_none();
        ctx.out.int(0);
        return Ok(());
    }
    if exists_in_db(ctx, &dst, dst_db) && !replace {
        ctx.propagate_none();
        ctx.out.int(0);
        return Ok(());
    }

    // The value has to be duplicated, not moved: `Robj` is not `Clone`, so
    // anything but a string needs W3a's codec (see `duplicate_value`).
    let expire_at = ctx.expire_at(&src);
    let copied = {
        let Some(obj) = ctx.lookup_read(&src) else {
            ctx.propagate_none();
            ctx.out.int(0);
            return Ok(());
        };
        match duplicate_value(obj) {
            Some(o) => o,
            None => {
                return Err(CmdError::err(
                    "COPY of this type requires the RDB serializer, which is not loaded",
                ));
            }
        }
    };

    let now = ctx.now_ms;
    let lfu = ctx.config().maxmemory_policy.is_lfu();
    put_entry(
        ctx,
        dst.clone(),
        Entry::new(copied, expire_at, now, lfu),
        dst_db,
    );
    ctx.signal_modified(&dst);
    if dst_db != src_db {
        signal_db_watch(ctx, &dst, dst_db);
    }
    ctx.notify(NotifyClass::GENERIC, "copy_to", &dst);
    ctx.out.int(1);
    Ok(())
}

fn cmd_move(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let key = key_at(args, 1)?;
    let target = args.i64_at(2)?;
    let src_db = ctx.db_index();
    let databases = ctx.server.shards.databases() as i64;
    if target < 0 || target >= databases {
        return Err(CmdError::err("DB index is out of range"));
    }
    let dst_db = target as usize;
    if dst_db == src_db {
        return Err(CmdError::err("source and destination objects are the same"));
    }

    if !ctx.exists(&key) || exists_in_db(ctx, &key, dst_db) {
        ctx.propagate_none();
        ctx.out.int(0);
        return Ok(());
    }
    let Some(entry) = take_entry(ctx, &key, src_db) else {
        ctx.propagate_none();
        ctx.out.int(0);
        return Ok(());
    };
    put_entry(ctx, key.clone(), entry, dst_db);
    ctx.signal_modified(&key);
    signal_db_watch(ctx, &key, dst_db);
    ctx.notify(NotifyClass::GENERIC, "move_from", &key);
    // `move_to` belongs to the destination database; `Ctx::notify` publishes
    // against the client's current one, so it is emitted directly.
    let configured = ctx.config().notify_keyspace_events;
    crate::notify::dispatch(
        ctx.server,
        configured,
        NotifyClass::GENERIC,
        "move_to",
        dst_db,
        &key,
    );
    ctx.out.int(1);
    Ok(())
}

// ---------------------------------------------------------------------------
// KEYS / RANDOMKEY
// ---------------------------------------------------------------------------

fn cmd_keys(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let pattern = key_at(args, 1)?;
    let now = ctx.now_ms;
    let all = pattern.as_ref() == b"*";
    let mut found: Vec<Key> = Vec::new();
    ctx.for_each_db(|db| {
        for (key, entry) in db.dict.iter() {
            if entry.is_expired(now) {
                continue;
            }
            if all || glob_match(&pattern, key) {
                found.push(key.clone());
            }
        }
    });
    ctx.out.array(found.len());
    for key in &found {
        ctx.out.bulk_from(key);
    }
    Ok(())
}

/// How many times `RANDOMKEY` re-rolls when it lands on a logically expired
/// key before giving up and reporting nil.
///
/// Each roll costs an `Iterator::nth` into a database slice, which is `O(n)`
/// rather than Redis's `O(1)` `dictGetRandomKey`, because `Dict` exposes no
/// bucket index (see [`cmd_scan`] and the handover note). At 1M keys over 16
/// shards that is ~30k cheap steps per roll; the re-roll only happens when the
/// pick lands on a key that is logically expired.
const RANDOMKEY_TRIES: usize = 16;

fn cmd_randomkey(ctx: &mut Ctx<'_>, _args: &Args) -> CmdResult {
    let now = ctx.now_ms;
    // One pass to size the slices, so the pick is uniform over the whole
    // database rather than uniform over shards.
    let mut lens: SmallVec<[usize; 32]> = SmallVec::new();
    ctx.for_each_db(|db| lens.push(db.len()));
    let total: usize = lens.iter().sum();
    if total == 0 {
        ctx.out.null();
        return Ok(());
    }

    for _ in 0..RANDOMKEY_TRIES {
        let mut target = crate::util::rand::below(total);
        let mut picked: Option<Key> = None;
        let mut seen = 0usize;
        ctx.for_each_db(|db| {
            if picked.is_some() {
                return;
            }
            let len = db.len();
            if target >= len {
                target -= len;
                seen += len;
                return;
            }
            if let Some((key, entry)) = db.dict.iter().nth(target)
                && !entry.is_expired(now)
            {
                picked = Some(key.clone());
            }
            // Consumed the pick either way.
            target = usize::MAX;
        });
        if let Some(key) = picked {
            ctx.out.bulk_from(&key);
            return Ok(());
        }
    }
    ctx.out.null();
    Ok(())
}

// ---------------------------------------------------------------------------
// SCAN
// ---------------------------------------------------------------------------

/// Virtual bucket bits, i.e. `log2` of the largest number of virtual buckets a
/// shard's keyspace is split into.
///
/// Every extra bit halves the size of a `SCAN` reply and doubles the number of
/// round trips needed for a full iteration, and each round trip costs one pass
/// over the shard's dict (see the cursor discussion on [`cmd_scan`]). 4 bits =
/// at most 16 passes per shard, so a full iteration is `O(16 N)` -- linear,
/// with a reply of about `keys / 16` per shard visit.
const MAX_SCAN_BITS: u32 = 4;

/// Default `COUNT`, as in Redis.
const SCAN_DEFAULT_COUNT: usize = 10;

/// A key's virtual bucket is derived from a hash that is **stable for the
/// life of the process and independent of the dict's internal state**, which
/// is what lets the cursor survive a rehash.
static SCAN_HASHER: foldhash::fast::FixedState =
    foldhash::fast::FixedState::with_seed(0x7264_6973_5343_414e);

#[inline]
fn scan_hash(key: &[u8]) -> u64 {
    use std::hash::BuildHasher;
    SCAN_HASHER.hash_one(key)
}

/// Number of virtual bucket bits for a slice of `len` keys and a `count` hint.
fn scan_bits(len: usize, count: usize) -> u32 {
    let count = count.max(1);
    let mut bits = 0u32;
    while bits < MAX_SCAN_BITS && (len >> bits) > count {
        bits += 1;
    }
    bits
}

/// Redis's reverse-binary cursor increment (`dict.c:dictScan`).
///
/// Setting the bits above the mask, reversing, incrementing and reversing back
/// makes the cursor count in reverse bit order, so that when the mask grows
/// or shrinks between calls the already-visited buckets stay visited.
#[inline]
fn scan_advance(cursor: u64, mask: u64) -> u64 {
    let mut v = cursor | !mask;
    v = v.reverse_bits();
    v = v.wrapping_add(1);
    v.reverse_bits()
}

struct ScanOptions {
    count: usize,
    pattern: Option<Bytes>,
    type_filter: Option<Bytes>,
}

fn parse_scan_options(args: &Args, from: usize) -> Result<ScanOptions, CmdError> {
    let mut o = ScanOptions {
        count: SCAN_DEFAULT_COUNT,
        pattern: None,
        type_filter: None,
    };
    let mut i = from;
    while i < args.len() {
        let tok = args.at(i)?;
        if eq_ignore_ascii_case(tok, b"COUNT") {
            let n = args.i64_at(i + 1)?;
            if n < 1 {
                return Err(CmdError::Syntax);
            }
            o.count = n as usize;
            i += 2;
        } else if eq_ignore_ascii_case(tok, b"MATCH") {
            o.pattern = Some(args.at(i + 1)?.clone());
            i += 2;
        } else if eq_ignore_ascii_case(tok, b"TYPE") {
            o.type_filter = Some(args.at(i + 1)?.clone());
            i += 2;
        } else {
            return Err(CmdError::Syntax);
        }
    }
    Ok(o)
}

/// Visit one virtual bucket of one database slice.
///
/// Returns `(entries examined, next intra-shard cursor)` and appends the
/// matching keys to `out`.
fn scan_db(
    db: &Db,
    cursor: u64,
    now_ms: u64,
    opts: &ScanOptions,
    out: &mut Vec<Key>,
) -> (usize, u64) {
    let bits = scan_bits(db.dict.len(), opts.count);
    let mask = (1u64 << bits) - 1;
    let bucket = cursor & mask;

    let mut examined = 0usize;
    for (key, entry) in db.dict.iter() {
        examined += 1;
        if (scan_hash(key) & mask) != bucket {
            continue;
        }
        if entry.is_expired(now_ms) {
            continue;
        }
        if let Some(t) = &opts.type_filter
            && !eq_ignore_ascii_case(t, entry.obj.type_name().as_bytes())
        {
            continue;
        }
        if let Some(p) = &opts.pattern
            && p.as_ref() != b"*"
            && !glob_match(p, key)
        {
            continue;
        }
        out.push(key.clone());
    }
    (examined, scan_advance(cursor, mask))
}

/// `SCAN cursor [MATCH pattern] [COUNT n] [TYPE t]`.
///
/// # Cursor scheme
///
/// ```text
/// cursor = (intra << shard_bits) | shard_index
/// shard_bits = log2(shard_count)          // shard_count is a power of two
/// ```
///
/// The low bits name the shard being iterated; the high bits are a
/// reverse-binary cursor over that shard's current database slice. Cursor `0`
/// means both "start at shard 0" and "finished", exactly as in Redis.
///
/// The intra-shard cursor is Redis's `dictScan` cursor over a *virtual* table:
/// a key's bucket is `stable_hash(key) & mask` with
/// `mask = 2^scan_bits(len, count) - 1`. It is virtual because `Dict` is a
/// `hashbrown::HashMap`, which exposes no bucket index (contract gap -- see
/// the handover note); the guarantee does not depend on the buckets being the
/// dict's own, only on:
///
/// * the bucket of a key never changing while the key exists -- true, because
///   `stable_hash` is a pure function of the key bytes and is independent of
///   the dict's capacity, its seed and its rehash state;
/// * masks being nested (`2^k - 1`), so growing or shrinking the virtual table
///   splits or merges buckets rather than reshuffling them -- true by
///   construction;
/// * the reverse-binary increment, which is what makes a split or merge
///   between two calls leave the already-visited prefix visited.
///
/// Together those give the Redis guarantee: **a key present from the start of
/// a full iteration to its end is returned at least once.** The shard
/// dimension does not weaken it, because a key's shard is a pure function of
/// its name (§2) and therefore never changes either; a full iteration visits
/// every shard exactly once, in order.
///
/// # Locking
///
/// `SCAN` declares **no keys and not `ALL_SHARDS`**, so the engine locks
/// nothing, and this handler locks one shard at a time through
/// `ServerShared::shards`, releasing each before touching the next. A thread
/// that holds no lock while acquiring one cannot be part of a wait-for cycle,
/// so the deadlock argument in `engine.rs` is preserved -- and, unlike
/// `ALL_SHARDS`, a scan of a million keys never stalls a client whose key
/// lives on a different shard.
fn cmd_scan(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let cursor_arg = args.at(1)?;
    let Some(mut cursor) = crate::util::strnum::string2ull(cursor_arg) else {
        return Err(CmdError::err("invalid cursor"));
    };
    let opts = parse_scan_options(args, 2)?;

    // Locking one shard at a time is only safe from a thread that holds none.
    // `SCAN` declares no keys, so the engine hands the handler an empty guard
    // set and this is always true today. It would stop being true if a future
    // `EXEC` ran a queued `SCAN` inside a lock set it took for the other
    // queued commands (§9.6) -- so refuse rather than self-deadlock on a
    // non-reentrant mutex. `CmdFlags::NO_MULTI` marks the command for W3b.
    if !ctx.shards().is_empty() {
        return Err(CmdError::err(
            "SCAN is not allowed while other keys are locked",
        ));
    }

    let server = ctx.server;
    let db_idx = ctx.db_index();
    let now = ctx.now_ms;
    let shard_count = server.shards.len();
    let shard_bits = shard_count.trailing_zeros();
    let shard_mask = (shard_count - 1) as u64;

    let mut keys: Vec<Key> = Vec::new();
    let mut examined = 0usize;

    loop {
        let shard_index = (cursor & shard_mask) as usize;
        let intra = cursor >> shard_bits;
        let Some(handle) = server.shards.get(shard_index) else {
            cursor = 0;
            break;
        };

        // ---- exactly one shard locked, and only for this bucket -----------
        let next_intra = {
            let shard = handle.lock();
            match shard.db_ref(db_idx) {
                Some(db) => {
                    let (n, next) = scan_db(db, intra, now, &opts, &mut keys);
                    examined += n;
                    next
                }
                None => 0,
            }
        };
        // ---- lock released ------------------------------------------------

        if next_intra == 0 {
            // This shard is done; move to the next one.
            let next_shard = shard_index + 1;
            if next_shard >= shard_count {
                cursor = 0;
                break;
            }
            cursor = next_shard as u64;
        } else {
            cursor = (next_intra << shard_bits) | shard_index as u64;
        }

        // `COUNT` is a budget on work, not on results: a `MATCH` that filters
        // everything out must still terminate the call.
        if examined >= opts.count || keys.len() >= opts.count {
            break;
        }
    }

    ctx.out.array(2);
    let mut fmt = itoa::Buffer::new();
    ctx.out.bulk(fmt.format(cursor).as_bytes());
    ctx.out.array(keys.len());
    for key in &keys {
        ctx.out.bulk_from(key);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// OBJECT
// ---------------------------------------------------------------------------

/// Redis reports `INT_MAX` for values that would be shared integers, because
/// `OBJECT REFCOUNT` on a shared object is meaningless. The shared range is
/// `0..10000`.
const SHARED_INTEGER_REFCOUNT: i64 = 2_147_483_647;

fn cmd_object(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let sub = args.at(1)?.clone();

    if eq_ignore_ascii_case(&sub, b"HELP") {
        let lines = [
            "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "ENCODING <key>",
            "    Return the kind of internal representation used in order to store the value associated with <key>.",
            "FREQ <key>",
            "    Return the access frequency index of the <key>. The returned integer is proportional to the logarithm of the real access frequency.",
            "IDLETIME <key>",
            "    Return the idle time of the <key>, that is the approximated number of seconds elapsed since the last access to the value.",
            "REFCOUNT <key>",
            "    Return the number of references of the value associated with <key>.",
            "HELP",
            "    Print this help.",
        ];
        ctx.out.array(lines.len());
        for l in lines {
            ctx.out.simple(l);
        }
        return Ok(());
    }

    if args.len() != 3 {
        return Err(CmdError::WrongArity("object"));
    }
    let key = key_at(args, 2)?;
    let now = ctx.now_ms;
    let policy = ctx.config().maxmemory_policy;

    if eq_ignore_ascii_case(&sub, b"ENCODING") {
        let enc = with_entry(ctx, &key, |e| e.obj.encoding());
        match enc {
            Some(e) => ctx.out.bulk(e.as_bytes()),
            None => return Err(CmdError::NoSuchKey),
        }
        return Ok(());
    }
    if eq_ignore_ascii_case(&sub, b"REFCOUNT") {
        let rc = with_entry(ctx, &key, |e| match &e.obj {
            Robj::Str(s) => match s.as_i64() {
                Some(v) if (0..10_000).contains(&v) && s.as_slice().is_none() => {
                    SHARED_INTEGER_REFCOUNT
                }
                _ => 1,
            },
            _ => 1,
        });
        match rc {
            Some(v) => ctx.out.int(v),
            None => return Err(CmdError::NoSuchKey),
        }
        return Ok(());
    }
    if eq_ignore_ascii_case(&sub, b"IDLETIME") {
        if policy.is_lfu() {
            return Err(CmdError::err(
                "An LFU maxmemory policy is selected, access time not tracked. Please note that when switching between maxmemory policies at runtime LFU and LRU data will take some time to adjust.",
            ));
        }
        let idle = with_entry(ctx, &key, |e| {
            lru::idle_ms(e.lru.load(std::sync::atomic::Ordering::Relaxed), now) / 1000
        });
        match idle {
            Some(v) => ctx.out.int(v as i64),
            None => return Err(CmdError::NoSuchKey),
        }
        return Ok(());
    }
    if eq_ignore_ascii_case(&sub, b"FREQ") {
        if !policy.is_lfu() {
            return Err(CmdError::err(
                "An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between maxmemory policies at runtime LFU and LRU data will take some time to adjust.",
            ));
        }
        let freq = with_entry(ctx, &key, |e| {
            lru::lfu_counter(e.lru.load(std::sync::atomic::Ordering::Relaxed))
        });
        match freq {
            Some(v) => ctx.out.int(i64::from(v)),
            None => return Err(CmdError::NoSuchKey),
        }
        return Ok(());
    }

    Err(CmdError::err(format!(
        "Unknown subcommand or wrong number of arguments for '{}'. Try OBJECT HELP.",
        String::from_utf8_lossy(&sub)
    )))
}

// ---------------------------------------------------------------------------
// DUMP / RESTORE
// ---------------------------------------------------------------------------

fn cmd_dump(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let key = key_at(args, 1)?;
    // §9.3: DUMP is read-only, so no `lookup_write` and no typed accessor.
    let payload = {
        let Some(obj) = ctx.lookup_read(&key) else {
            ctx.out.null();
            return Ok(());
        };
        let Some(codec) = value_codec() else {
            return Err(no_codec());
        };
        let mut body = Vec::new();
        if !codec.dump(obj, &mut body) {
            return Err(no_codec());
        }
        seal_dump(body)
    };
    ctx.out.bulk(&payload);
    Ok(())
}

fn cmd_restore(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let key = key_at(args, 1)?;
    let ttl = args.i64_at(2)?;
    let payload = args.at(3)?.clone();

    let mut replace = false;
    let mut absttl = false;
    let mut idletime: Option<i64> = None;
    let mut freq: Option<i64> = None;
    let mut i = 4usize;
    while i < args.len() {
        let tok = args.at(i)?;
        if eq_ignore_ascii_case(tok, b"REPLACE") {
            replace = true;
            i += 1;
        } else if eq_ignore_ascii_case(tok, b"ABSTTL") {
            absttl = true;
            i += 1;
        } else if eq_ignore_ascii_case(tok, b"IDLETIME") {
            let v = args.i64_at(i + 1)?;
            if v < 0 {
                return Err(CmdError::err("Invalid IDLETIME value, must be >= 0"));
            }
            idletime = Some(v);
            i += 2;
        } else if eq_ignore_ascii_case(tok, b"FREQ") {
            let v = args.i64_at(i + 1)?;
            if !(0..=255).contains(&v) {
                return Err(CmdError::err("Invalid FREQ value, must be >= 0 and <= 255"));
            }
            freq = Some(v);
            i += 2;
        } else {
            return Err(CmdError::Syntax);
        }
    }
    if ttl < 0 {
        return Err(CmdError::err("Invalid TTL value, must be >= 0"));
    }

    evict::check_oom(ctx.server, ctx.config())?;

    if ctx.exists(&key) && !replace {
        return Err(CmdError::custom(
            "BUSYKEY",
            "Target key name already exists.",
        ));
    }

    let Some(body) = verify_dump(&payload) else {
        return Err(CmdError::err("DUMP payload version or checksum are wrong"));
    };
    let Some(codec) = value_codec() else {
        return Err(no_codec());
    };
    let Some(obj) = codec.restore(body) else {
        return Err(CmdError::err("Bad data format"));
    };

    let expire_at = if ttl == 0 {
        None
    } else if absttl {
        Some(ttl as u64)
    } else {
        Some((ctx.now_ms as i64).saturating_add(ttl) as u64)
    };

    ctx.insert(key.clone(), obj, expire_at);

    // IDLETIME / FREQ seed the eviction metadata directly.
    if let Some(v) = idletime {
        let clock = lru::clock(ctx.now_ms.saturating_sub((v as u64).saturating_mul(1000)));
        set_lru_raw(ctx, &key, clock);
    } else if let Some(v) = freq {
        let packed = (lru::lfu_clock(ctx.now_ms) << 8) | (v as u32 & 0xff);
        set_lru_raw(ctx, &key, packed);
    }

    ctx.notify(NotifyClass::GENERIC, "restore", &key);

    // §4.5: a relative TTL replayed later would be wrong, so propagate the
    // resolved deadline with ABSTTL -- the same rewrite `restoreCommand` does.
    if !absttl && let Some(at) = expire_at {
        let mut argv = ArgVec::new();
        argv.push(Bytes::from_static(b"RESTORE"));
        argv.push(key.clone());
        let mut fmt = itoa::Buffer::new();
        argv.push(Bytes::copy_from_slice(fmt.format(at).as_bytes()));
        argv.push(payload.clone());
        if replace {
            argv.push(Bytes::from_static(b"REPLACE"));
        }
        argv.push(Bytes::from_static(b"ABSTTL"));
        ctx.propagate_argv(argv);
    }

    ctx.out.ok();
    Ok(())
}

fn set_lru_raw(ctx: &mut Ctx<'_>, key: &Key, value: u32) {
    let db_idx = ctx.db_index();
    if let Some(entry) = ctx
        .shards()
        .for_key(key)
        .and_then(|s| s.db(db_idx))
        .and_then(|db| db.dict.get_mut(key))
    {
        entry.lru.store(value, std::sync::atomic::Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// SORT / SORT_RO
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SortOptions {
    desc: bool,
    alpha: bool,
    limit: Option<(i64, i64)>,
    by: Option<Bytes>,
    get: SmallVec<[Bytes; 4]>,
    store: Option<Key>,
}

fn parse_sort_options(args: &Args, allow_store: bool) -> Result<SortOptions, CmdError> {
    let mut o = SortOptions::default();
    let mut i = 2usize;
    while i < args.len() {
        let tok = args.at(i)?;
        if eq_ignore_ascii_case(tok, b"ASC") {
            o.desc = false;
            i += 1;
        } else if eq_ignore_ascii_case(tok, b"DESC") {
            o.desc = true;
            i += 1;
        } else if eq_ignore_ascii_case(tok, b"ALPHA") {
            o.alpha = true;
            i += 1;
        } else if eq_ignore_ascii_case(tok, b"LIMIT") {
            let offset = args.i64_at(i + 1)?;
            let count = args.i64_at(i + 2)?;
            if offset < 0 {
                return Err(CmdError::err("value is out of range, must be positive"));
            }
            o.limit = Some((offset, count));
            i += 3;
        } else if eq_ignore_ascii_case(tok, b"BY") {
            o.by = Some(args.at(i + 1)?.clone());
            i += 2;
        } else if eq_ignore_ascii_case(tok, b"GET") {
            o.get.push(args.at(i + 1)?.clone());
            i += 2;
        } else if allow_store && eq_ignore_ascii_case(tok, b"STORE") {
            o.store = Some(args.at(i + 1)?.clone());
            i += 2;
        } else {
            return Err(CmdError::Syntax);
        }
    }
    Ok(o)
}

/// `SORT`'s element source.
///
/// **Seam into W2b/W2c.** `ListObj`, `SetObj` and `ZSetObj` are still F0's
/// placeholders and expose no iterator, so a list/set/zset yields nothing --
/// which is the right answer for an empty collection, and they are always
/// empty today. When the real types land, this function is the only thing that
/// changes.
fn sort_elements(ctx: &mut Ctx<'_>, key: &Key) -> Result<Option<Vec<Bytes>>, CmdError> {
    match ctx.lookup_read(key) {
        None => Ok(None),
        Some(Robj::List(_)) | Some(Robj::Set(_)) | Some(Robj::ZSet(_)) => Ok(Some(Vec::new())),
        Some(_) => Err(CmdError::WrongType),
    }
}

/// `lookupKeyByPattern`: substitute `*` in `pattern` with `subst` and read
/// the resulting key (or hash field, for `key*->field`).
fn lookup_by_pattern(ctx: &mut Ctx<'_>, pattern: &[u8], subst: &[u8]) -> Option<Bytes> {
    let star = pattern.iter().position(|&c| c == b'*')?;
    // `key*->field` splits into a key pattern and a hash field.
    let arrow = pattern
        .windows(2)
        .position(|w| w == b"->")
        .filter(|&p| p > star && p + 2 < pattern.len());

    let key_pat = match arrow {
        Some(p) => pattern.get(..p)?,
        None => pattern,
    };
    let mut key = BytesMut::with_capacity(key_pat.len() + subst.len());
    key.extend_from_slice(key_pat.get(..star)?);
    key.extend_from_slice(subst);
    key.extend_from_slice(key_pat.get(star + 1..)?);
    let key = key.freeze();

    if arrow.is_some() {
        // Hash-field lookups need W2b's `HashObj`; until then a `BY hash->f`
        // pattern behaves like a missing field, which Redis treats as nil.
        return None;
    }
    let obj = ctx.get_str_read(&key).ok().flatten()?;
    Some(obj.to_bytes())
}

fn sort_generic(ctx: &mut Ctx<'_>, args: &Args, allow_store: bool) -> CmdResult {
    let key = key_at(args, 1)?;
    let mut opts = parse_sort_options(args, allow_store)?;

    let Some(mut elements) = sort_elements(ctx, &key)? else {
        // A missing key sorts to nothing. With STORE that means deleting the
        // destination and reporting a length of zero.
        if let Some(dst) = opts.store.clone() {
            let removed = ctx.remove(&dst);
            if removed {
                ctx.notify(NotifyClass::GENERIC, "del", &dst);
                ctx.propagate(&[b"DEL", &dst]);
            } else {
                ctx.propagate_none();
            }
            ctx.out.int(0);
        } else {
            ctx.out.empty_array();
        }
        return Ok(());
    };

    // `BY` with a pattern that has no `*` means "do not sort at all".
    let dontsort = opts.by.as_ref().is_some_and(|p| !p.contains(&b'*'));

    // Redis forces an ALPHA sort when the input is an unordered set and the
    // output has to be deterministic, because otherwise the AOF and the
    // replica would see a different order (§4.5).
    let is_set = matches!(ctx.lookup_read(&key), Some(Robj::Set(_)));
    let dontsort = if dontsort && is_set && opts.store.is_some() {
        opts.alpha = true;
        opts.by = None;
        false
    } else {
        dontsort
    };

    if !dontsort {
        if opts.alpha {
            match &opts.by {
                None => elements.sort_unstable(),
                Some(pattern) => {
                    let pattern = pattern.clone();
                    let mut keyed: Vec<(Option<Bytes>, Bytes)> = elements
                        .into_iter()
                        .map(|e| (lookup_by_pattern(ctx, &pattern, &e), e))
                        .collect();
                    keyed.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
                    elements = keyed.into_iter().map(|(_, e)| e).collect();
                }
            }
        } else {
            let by = opts.by.clone();
            let mut keyed: Vec<(f64, Bytes)> = Vec::with_capacity(elements.len());
            for e in elements {
                let weight_src = match &by {
                    None => Some(e.clone()),
                    Some(p) => lookup_by_pattern(ctx, p, &e),
                };
                let w = match weight_src {
                    None => 0.0,
                    Some(v) => crate::util::strnum::string2d(&v).ok_or_else(|| {
                        CmdError::err("One or more scores can't be converted into double")
                    })?,
                };
                keyed.push((w, e));
            }
            keyed.sort_unstable_by(|a, b| {
                a.0.partial_cmp(&b.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.1.cmp(&b.1))
            });
            elements = keyed.into_iter().map(|(_, e)| e).collect();
        }
        if opts.desc {
            elements.reverse();
        }
    }

    if let Some((offset, count)) = opts.limit {
        let offset = (offset.max(0) as usize).min(elements.len());
        let mut rest = elements.split_off(offset);
        if count >= 0 {
            rest.truncate(count as usize);
        }
        elements = rest;
    }

    // GET patterns expand each element into one or more outputs.
    let projected: Vec<Option<Bytes>> = if opts.get.is_empty() {
        elements.iter().cloned().map(Some).collect()
    } else {
        let patterns = opts.get.clone();
        let mut out = Vec::with_capacity(elements.len() * patterns.len());
        for e in &elements {
            for p in &patterns {
                if p.as_ref() == b"#" {
                    out.push(Some(e.clone()));
                } else {
                    out.push(lookup_by_pattern(ctx, p, e));
                }
            }
        }
        out
    };

    match opts.store.clone() {
        None => {
            ctx.out.array(projected.len());
            for v in &projected {
                match v {
                    Some(b) => ctx.out.bulk_from(b),
                    None => ctx.out.null(),
                }
            }
        }
        Some(dst) => {
            if projected.is_empty() {
                let removed = ctx.remove(&dst);
                if removed {
                    ctx.notify(NotifyClass::GENERIC, "del", &dst);
                    ctx.propagate(&[b"DEL", &dst]);
                } else {
                    ctx.propagate_none();
                }
                ctx.out.int(0);
            } else {
                // Building the destination list needs `ListObj`, which is
                // W2b's and still a placeholder. Unreachable today, because
                // `sort_elements` cannot produce a non-empty result yet.
                return Err(CmdError::err(
                    "SORT ... STORE needs the list type, which is not available yet",
                ));
            }
        }
    }
    Ok(())
}

fn cmd_sort(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    sort_generic(ctx, args, true)
}

fn cmd_sort_ro(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    sort_generic(ctx, args, false)
}

/// `SORT`'s keys: the input, plus `STORE`'s destination when present.
///
/// `BY`/`GET` patterns also name keys, and in cluster mode Redis refuses them
/// for that reason. Here `SORT` declares `ALL_SHARDS`, so every key is
/// reachable and the pattern lookups just work.
fn sort_keys(args: &Args) -> KeyPositions {
    let mut v = KeyPositions::new();
    v.push(1);
    let mut i = 2usize;
    while i < args.len() {
        if args.kw_at(i, "STORE") && i + 1 < args.len() {
            v.push(i + 1);
            i += 2;
        } else if args.kw_at(i, "LIMIT") {
            i += 3;
        } else if args.kw_at(i, "BY") || args.kw_at(i, "GET") {
            i += 2;
        } else {
            i += 1;
        }
    }
    v
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Owner: W1c.
pub fn register(t: &mut CommandTable) {
    t.add(CommandSpec {
        name: "del",
        arity: -2,
        flags: CmdFlags::WRITE,
        first_key: 1,
        last_key: -1,
        key_step: 1,
        handler: cmd_del,
        get_keys: None,
        tips: &["request_policy:multi_shard", "response_policy:agg_sum"],
        since: "1.0.0",
        summary: "Deletes one or more keys.",
    });
    t.add(CommandSpec {
        name: "unlink",
        arity: -2,
        flags: CmdFlags::WRITE | CmdFlags::FAST,
        first_key: 1,
        last_key: -1,
        key_step: 1,
        handler: cmd_del,
        get_keys: None,
        tips: &["request_policy:multi_shard", "response_policy:agg_sum"],
        since: "4.0.0",
        summary: "Asynchronously deletes one or more keys.",
    });
    t.add(CommandSpec {
        name: "exists",
        arity: -2,
        flags: CmdFlags::READONLY | CmdFlags::FAST,
        first_key: 1,
        last_key: -1,
        key_step: 1,
        handler: cmd_exists,
        get_keys: None,
        tips: &["request_policy:multi_shard", "response_policy:agg_sum"],
        since: "1.0.0",
        summary: "Determines whether one or more keys exist.",
    });
    t.add(CommandSpec {
        name: "type",
        arity: 2,
        flags: CmdFlags::READONLY | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_type,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Determines the type of value stored at a key.",
    });
    t.add(CommandSpec {
        name: "touch",
        arity: -2,
        flags: CmdFlags::READONLY | CmdFlags::FAST,
        first_key: 1,
        last_key: -1,
        key_step: 1,
        handler: cmd_touch,
        get_keys: None,
        tips: &["request_policy:multi_shard", "response_policy:agg_sum"],
        since: "3.2.1",
        summary: "Returns the number of existing keys out of those specified after updating the time they were last accessed.",
    });
    t.add(CommandSpec {
        name: "ttl",
        arity: 2,
        flags: CmdFlags::READONLY | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_ttl,
        get_keys: None,
        tips: &["request_policy:all_shards", "response_policy:agg_min"],
        since: "1.0.0",
        summary: "Returns the expiration time in seconds of a key.",
    });
    t.add(CommandSpec {
        name: "pttl",
        arity: 2,
        flags: CmdFlags::READONLY | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_pttl,
        get_keys: None,
        tips: &[],
        since: "2.6.0",
        summary: "Returns the expiration time in milliseconds of a key.",
    });
    t.add(CommandSpec {
        name: "expiretime",
        arity: 2,
        flags: CmdFlags::READONLY | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_expiretime,
        get_keys: None,
        tips: &[],
        since: "7.0.0",
        summary: "Returns the expiration time of a key as a Unix timestamp.",
    });
    t.add(CommandSpec {
        name: "pexpiretime",
        arity: 2,
        flags: CmdFlags::READONLY | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_pexpiretime,
        get_keys: None,
        tips: &[],
        since: "7.0.0",
        summary: "Returns the expiration time of a key as a Unix milliseconds timestamp.",
    });
    t.add(CommandSpec {
        name: "persist",
        arity: 2,
        flags: CmdFlags::WRITE | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_persist,
        get_keys: None,
        tips: &[],
        since: "2.2.0",
        summary: "Removes the expiration time of a key.",
    });
    t.add(CommandSpec {
        name: "expire",
        arity: -3,
        flags: CmdFlags::WRITE | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_expire,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Sets the expiration time of a key in seconds.",
    });
    t.add(CommandSpec {
        name: "pexpire",
        arity: -3,
        flags: CmdFlags::WRITE | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_pexpire,
        get_keys: None,
        tips: &[],
        since: "2.6.0",
        summary: "Sets the expiration time of a key in milliseconds.",
    });
    t.add(CommandSpec {
        name: "expireat",
        arity: -3,
        flags: CmdFlags::WRITE | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_expireat,
        get_keys: None,
        tips: &[],
        since: "1.2.0",
        summary: "Sets the expiration time of a key to a Unix timestamp.",
    });
    t.add(CommandSpec {
        name: "pexpireat",
        arity: -3,
        flags: CmdFlags::WRITE | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_pexpireat,
        get_keys: None,
        tips: &[],
        since: "2.6.0",
        summary: "Sets the expiration time of a key to a Unix milliseconds timestamp.",
    });
    t.add(CommandSpec {
        name: "rename",
        arity: 3,
        flags: CmdFlags::WRITE,
        first_key: 1,
        last_key: 2,
        key_step: 1,
        handler: cmd_rename,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Renames a key and overwrites the destination.",
    });
    t.add(CommandSpec {
        name: "renamenx",
        arity: 3,
        flags: CmdFlags::WRITE | CmdFlags::FAST,
        first_key: 1,
        last_key: 2,
        key_step: 1,
        handler: cmd_renamenx,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Renames a key only when the target key name doesn't exist.",
    });
    t.add(CommandSpec {
        name: "copy",
        arity: -3,
        flags: CmdFlags::WRITE | CmdFlags::DENYOOM,
        first_key: 1,
        last_key: 2,
        key_step: 1,
        handler: cmd_copy,
        get_keys: None,
        tips: &[],
        since: "6.2.0",
        summary: "Copies the value of a key to a new key.",
    });
    t.add(CommandSpec {
        name: "move",
        arity: 3,
        flags: CmdFlags::WRITE | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_move,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Moves a key to another database.",
    });
    t.add(CommandSpec {
        name: "keys",
        arity: 2,
        flags: CmdFlags::READONLY | CmdFlags::ALL_SHARDS,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_keys,
        get_keys: None,
        tips: &["request_policy:all_shards", "response_policy:special"],
        since: "1.0.0",
        summary: "Returns all key names that match a pattern.",
    });
    t.add(CommandSpec {
        name: "randomkey",
        arity: 1,
        flags: CmdFlags::READONLY | CmdFlags::ALL_SHARDS,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_randomkey,
        get_keys: None,
        tips: &["request_policy:all_shards", "response_policy:special"],
        since: "1.0.0",
        summary: "Returns a random key name from the database.",
    });
    t.add(CommandSpec {
        // Deliberately *not* ALL_SHARDS: the handler locks one shard at a
        // time so a scan never stalls the whole keyspace. See `cmd_scan`.
        name: "scan",
        arity: -2,
        flags: CmdFlags::READONLY | CmdFlags::NO_MULTI,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_scan,
        get_keys: None,
        tips: &["request_policy:special", "response_policy:special"],
        since: "2.8.0",
        summary: "Iterates over the key names in the database.",
    });
    t.add(CommandSpec {
        // §9.9: no subcommand support in `CommandSpec`, so the key position is
        // declared on the container. `OBJECT HELP` has no key at position 2,
        // and `key_positions` drops out-of-range positions.
        name: "object",
        arity: -2,
        flags: CmdFlags::READONLY,
        first_key: 2,
        last_key: 2,
        key_step: 1,
        handler: cmd_object,
        get_keys: None,
        tips: &[],
        since: "2.2.3",
        summary: "A container for object introspection commands.",
    });
    t.add(CommandSpec {
        name: "dump",
        arity: 2,
        flags: CmdFlags::READONLY,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_dump,
        get_keys: None,
        tips: &[],
        since: "2.6.0",
        summary: "Returns a serialized representation of the value stored at a key.",
    });
    t.add(CommandSpec {
        name: "restore",
        arity: -4,
        flags: CmdFlags::WRITE | CmdFlags::DENYOOM,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_restore,
        get_keys: None,
        tips: &[],
        since: "2.6.0",
        summary: "Creates a key from the serialized representation of a value.",
    });
    t.add(CommandSpec {
        name: "sort",
        arity: -2,
        flags: CmdFlags::WRITE
            | CmdFlags::DENYOOM
            | CmdFlags::MOVABLE_KEYS
            | CmdFlags::ALL_SHARDS,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_sort,
        get_keys: Some(sort_keys),
        tips: &[],
        since: "1.0.0",
        summary: "Sorts the elements in a list, a set, or a sorted set, optionally storing the result.",
    });
    t.add(CommandSpec {
        name: "sort_ro",
        arity: -2,
        flags: CmdFlags::READONLY | CmdFlags::MOVABLE_KEYS | CmdFlags::ALL_SHARDS,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_sort_ro,
        get_keys: Some(sort_keys),
        tips: &[],
        since: "7.0.0",
        summary: "Returns the sorted elements of a list, a set, or a sorted set.",
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_literals_and_wildcards() {
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"a"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"a*", b"a"));
        assert!(glob_match(b"a*", b"abc"));
        assert!(!glob_match(b"a*", b"bac"));
        assert!(glob_match(b"*c", b"abc"));
        assert!(glob_match(b"a*c", b"abbbc"));
        assert!(!glob_match(b"a*c", b"abbb"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(!glob_match(b"h?llo", b"heello"));
        assert!(glob_match(b"h*l?o", b"heeeello"));
    }

    #[test]
    fn glob_classes() {
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
        assert!(glob_match(b"h[^e]llo", b"hallo"));
        assert!(!glob_match(b"h[^e]llo", b"hello"));
        assert!(glob_match(b"h[a-c]llo", b"hbllo"));
        assert!(!glob_match(b"h[a-c]llo", b"hdllo"));
        // An empty class matches nothing, as in `stringmatchlen`.
        assert!(!glob_match(b"[]", b"[]"));
        // An unterminated class runs to the end of the pattern.
        assert!(glob_match(b"[abc", b"a"));
        assert!(!glob_match(b"[abc", b"d"));
    }

    #[test]
    fn glob_escapes() {
        assert!(glob_match(br"h\*llo", b"h*llo"));
        assert!(!glob_match(br"h\*llo", b"hello"));
        assert!(glob_match(br"\[abc\]", b"[abc]"));
    }

    #[test]
    fn glob_does_not_blow_up_on_adversarial_patterns() {
        let pattern: Vec<u8> = std::iter::repeat_n(b"*a".as_slice(), 40)
            .flatten()
            .copied()
            .collect();
        let subject = vec![b'a'; 200];
        // The point is that it returns rather than overflowing the stack.
        assert!(glob_match(&pattern, &subject));
        let subject = vec![b'b'; 200];
        assert!(!glob_match(&pattern, &subject));
    }

    #[test]
    fn reverse_binary_cursor_covers_every_bucket_exactly_once() {
        for bits in 0..=4u32 {
            let mask = (1u64 << bits) - 1;
            let mut seen = Vec::new();
            let mut v = 0u64;
            loop {
                seen.push(v & mask);
                v = scan_advance(v, mask);
                if v == 0 {
                    break;
                }
                assert!(
                    seen.len() <= (mask as usize) + 1,
                    "cursor did not terminate"
                );
            }
            assert_eq!(seen.len(), (mask as usize) + 1);
            let mut sorted = seen.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), seen.len(), "a bucket was visited twice");
        }
    }

    #[test]
    fn growing_the_table_mid_iteration_keeps_the_prefix_visited() {
        // The property the reverse-binary cursor exists for: after visiting
        // bucket b under a small mask, every bucket that b splits into under a
        // larger mask must already count as visited.
        let small = 0b1u64;
        let large = 0b11u64;
        let mut visited_small = Vec::new();
        let mut v = 0u64;
        // Visit exactly one bucket under the small mask, then grow.
        visited_small.push(v & small);
        v = scan_advance(v, small);
        assert_ne!(v, 0);

        // Under the larger mask, continue to completion.
        let mut visited_large = Vec::new();
        loop {
            visited_large.push(v & large);
            v = scan_advance(v, large);
            if v == 0 {
                break;
            }
        }
        // Every large bucket is either visited now, or its low bit matches the
        // small bucket we already covered.
        for b in 0..=large {
            let covered = visited_large.contains(&b) || visited_small.contains(&(b & small));
            assert!(covered, "bucket {b} was skipped across the resize");
        }
    }

    #[test]
    fn scan_bits_scale_with_the_slice() {
        assert_eq!(scan_bits(0, 10), 0);
        assert_eq!(scan_bits(10, 10), 0);
        assert_eq!(scan_bits(21, 10), 1);
        assert_eq!(scan_bits(41, 10), 2);
        assert_eq!(scan_bits(1_000_000, 10), MAX_SCAN_BITS);
        assert_eq!(scan_bits(1_000_000, 10_000_000), 0);
    }

    #[test]
    fn dump_framing_round_trips_and_detects_corruption() {
        let sealed = seal_dump(b"payload".to_vec());
        assert_eq!(sealed.len(), 7 + DUMP_FOOTER_LEN);
        assert_eq!(verify_dump(&sealed), Some(&b"payload"[..]));

        // Flip a payload byte.
        let mut bad = sealed.clone();
        if let Some(b) = bad.first_mut() {
            *b ^= 0xff;
        }
        assert_eq!(verify_dump(&bad), None);

        // Flip a CRC byte.
        let mut bad = sealed.clone();
        if let Some(b) = bad.last_mut() {
            *b ^= 0xff;
        }
        assert_eq!(verify_dump(&bad), None);

        // A version from the future is refused.
        let mut future = sealed.clone();
        let split = future.len() - DUMP_FOOTER_LEN;
        if let Some(b) = future.get_mut(split) {
            *b = 99;
        }
        assert_eq!(verify_dump(&future), None);

        // Too short to hold a footer.
        assert_eq!(verify_dump(b"short"), None);
        assert_eq!(verify_dump(b""), None);
    }

    #[test]
    fn expire_conditions_match_redis() {
        use ExpireCond::*;
        assert!(cond_allows(Always, None, 100));
        assert!(cond_allows(Always, Some(50), 100));

        assert!(cond_allows(Nx, None, 100));
        assert!(!cond_allows(Nx, Some(50), 100));

        assert!(!cond_allows(Xx, None, 100));
        assert!(cond_allows(Xx, Some(50), 100));

        // No TTL is "expires at infinity": GT can never beat it, LT always can.
        assert!(!cond_allows(Gt, None, 100));
        assert!(cond_allows(Gt, Some(50), 100));
        assert!(!cond_allows(Gt, Some(100), 100));
        assert!(cond_allows(Lt, None, 100));
        assert!(!cond_allows(Lt, Some(50), 100));
        assert!(cond_allows(Lt, Some(200), 100));
    }
}
