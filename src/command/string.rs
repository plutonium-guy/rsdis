//! String commands.
//!
//! Owned by W2a; do not edit if you are not that agent.
//!
//! F0 seeded `SET` (with the full option set), `GET`, `MSET`, `MGET` and
//! `STRLEN`. `MSET`/`MGET` are here on purpose: they are the foundation's only
//! genuinely multi-key commands, so they are what exercises the engine's
//! cross-shard lock ordering under real traffic.
//!
//! Still to do (W2a): `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`,
//! `APPEND`, `SETRANGE`, `GETRANGE`, `INCR`/`DECR`/`INCRBY`/`DECRBY`,
//! `INCRBYFLOAT`, `MSETNX`, `SUBSTR`, `LCS`.
//!
//! Propagation traps (§4.5): `SETEX`/`SET ... EX` must propagate the resolved
//! absolute time (`SET ... PXAT`), `GETEX` must propagate `PEXPIREAT` or
//! `PERSIST`, and `INCRBYFLOAT` must propagate a plain `SET` with the result.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::command::{ArgVec, Args, ArgsExt, CmdFlags, CommandSpec, CommandTable, KeyPositions};
use crate::ctx::Ctx;
use crate::error::{CmdError, CmdResult};
use crate::notify::NotifyClass;
use crate::object::Robj;
use crate::types::string::StrObj;
use crate::util::eq_ignore_ascii_case;
use crate::util::strnum::string2ll;

/// A string value detached from the keyspace borrow, so the reply can be
/// written while `ctx.out` is borrowed mutably.
///
/// `Raw` still costs a copy today. Removing it needs `StrObj` to hand back a
/// `Bytes` that shares the stored buffer (W2a's call when it owns the type);
/// the int case is already allocation-free via `ReplyWriter::bulk_i64`.
enum StrValue {
    Raw(Bytes),
    Int(i64),
}

fn detach(o: &StrObj) -> StrValue {
    match o.as_slice() {
        Some(b) => StrValue::Raw(Bytes::copy_from_slice(b)),
        None => StrValue::Int(o.as_i64().unwrap_or(0)),
    }
}

#[inline]
fn write_opt(ctx: &mut Ctx<'_>, v: Option<StrValue>) {
    match v {
        None => ctx.out.null(),
        Some(StrValue::Raw(b)) => ctx.out.bulk_from(&b),
        Some(StrValue::Int(i)) => ctx.out.bulk_i64(i),
    }
}

fn cmd_get(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let key = args.at(1)?.clone();
    let value = ctx.get_str_read(&key)?.map(detach);
    write_opt(ctx, value);
    Ok(())
}

fn cmd_strlen(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let key = args.at(1)?.clone();
    let len = ctx.get_str_read(&key)?.map_or(0, StrObj::len);
    ctx.out.int(len as i64);
    Ok(())
}

#[derive(Default)]
struct SetOptions {
    nx: bool,
    xx: bool,
    get: bool,
    keepttl: bool,
    /// Absolute expiry in ms, already resolved against `now_ms`.
    expire_at_ms: Option<u64>,
    /// True when an expiry option was given at all.
    has_expire: bool,
}

fn parse_set_options(args: &Args, now_ms: u64) -> Result<SetOptions, CmdError> {
    let mut o = SetOptions::default();
    let mut i = 3usize;
    while i < args.len() {
        let tok = args.at(i)?;
        if eq_ignore_ascii_case(tok, b"NX") {
            if o.xx {
                return Err(CmdError::Syntax);
            }
            o.nx = true;
            i += 1;
        } else if eq_ignore_ascii_case(tok, b"XX") {
            if o.nx {
                return Err(CmdError::Syntax);
            }
            o.xx = true;
            i += 1;
        } else if eq_ignore_ascii_case(tok, b"GET") {
            o.get = true;
            i += 1;
        } else if eq_ignore_ascii_case(tok, b"KEEPTTL") {
            if o.has_expire {
                return Err(CmdError::Syntax);
            }
            o.keepttl = true;
            i += 1;
        } else if eq_ignore_ascii_case(tok, b"EX")
            || eq_ignore_ascii_case(tok, b"PX")
            || eq_ignore_ascii_case(tok, b"EXAT")
            || eq_ignore_ascii_case(tok, b"PXAT")
        {
            if o.has_expire || o.keepttl {
                return Err(CmdError::Syntax);
            }
            let Some(raw) = args.get(i + 1) else {
                return Err(CmdError::Syntax);
            };
            let Some(v) = string2ll(raw) else {
                return Err(CmdError::NotAnInteger);
            };
            let is_relative = eq_ignore_ascii_case(tok, b"EX") || eq_ignore_ascii_case(tok, b"PX");
            let is_seconds = eq_ignore_ascii_case(tok, b"EX") || eq_ignore_ascii_case(tok, b"EXAT");
            if is_relative && v <= 0 {
                return Err(CmdError::err("invalid expire time in 'set' command"));
            }
            let ms = if is_seconds {
                v.checked_mul(1000)
                    .ok_or_else(|| CmdError::err("invalid expire time in 'set' command"))?
            } else {
                v
            };
            let at = if is_relative {
                (now_ms as i64).saturating_add(ms)
            } else {
                ms
            };
            o.expire_at_ms = Some(at.max(0) as u64);
            o.has_expire = true;
            i += 2;
        } else {
            return Err(CmdError::Syntax);
        }
    }
    Ok(o)
}

fn cmd_set(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let key = args.at(1)?.clone();
    let value = args.at(2)?.clone();
    let opts = parse_set_options(args, ctx.now_ms)?;

    // The GET option reports the *previous* value, and reports WRONGTYPE if
    // the old value is not a string -- even when the SET itself would have
    // replaced it.
    let old: Option<Option<StrValue>> = if opts.get {
        Some(ctx.get_str_read(&key)?.map(detach))
    } else {
        None
    };

    let exists = ctx.exists(&key);
    if (opts.nx && exists) || (opts.xx && !exists) {
        // Aborted: no write, no propagation.
        ctx.propagate_none();
        match old {
            Some(v) => write_opt(ctx, v),
            None => ctx.out.null(),
        }
        return Ok(());
    }

    let expire_at = if opts.keepttl {
        ctx.expire_at(&key)
    } else {
        opts.expire_at_ms
    };

    ctx.insert(
        key.clone(),
        Robj::Str(StrObj::from_bytes(value.clone())),
        expire_at,
    );
    ctx.notify(NotifyClass::STRING, "set", &key);

    // §4.5: a relative expiry is non-deterministic, so propagate the resolved
    // absolute deadline instead of the verbatim command.
    if let Some(at) = expire_at {
        let mut argv = ArgVec::new();
        argv.push(Bytes::from_static(b"SET"));
        argv.push(key.clone());
        argv.push(value);
        argv.push(Bytes::from_static(b"PXAT"));
        let mut fmt = itoa::Buffer::new();
        argv.push(Bytes::copy_from_slice(fmt.format(at).as_bytes()));
        ctx.propagate_argv(argv);
    }

    match old {
        Some(v) => write_opt(ctx, v),
        None => ctx.out.ok(),
    }
    Ok(())
}

fn cmd_mget(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    ctx.out.array(args.len().saturating_sub(1));
    for i in 1..args.len() {
        let key = args.at(i)?.clone();
        // MGET never errors on a wrong type: it replies nil for that element.
        let value = ctx.get_str_read(&key).ok().flatten().map(detach);
        write_opt(ctx, value);
    }
    Ok(())
}

fn cmd_mset(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(CmdError::WrongArity("mset"));
    }
    let mut i = 1usize;
    while i + 1 < args.len() {
        let key = args.at(i)?.clone();
        let value = args.at(i + 1)?.clone();
        ctx.insert(key.clone(), Robj::Str(StrObj::from_bytes(value)), None);
        ctx.notify(NotifyClass::STRING, "set", &key);
        i += 2;
    }
    ctx.out.ok();
    Ok(())
}

/// `MSET`'s keys are at 1, 3, 5, ... which `first_key`/`key_step` already
/// expresses; this exists as the worked example of a `get_keys` function for
/// the wave agents that will need one.
#[allow(dead_code)]
fn mset_keys(args: &Args) -> KeyPositions {
    let mut v: SmallVec<[usize; 8]> = SmallVec::new();
    let mut i = 1;
    while i < args.len() {
        v.push(i);
        i += 2;
    }
    v
}

/// Owner: W2a.
pub fn register(t: &mut CommandTable) {
    t.add(CommandSpec {
        name: "set",
        arity: -3,
        flags: CmdFlags::WRITE | CmdFlags::DENYOOM,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_set,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Sets the string value of a key, ignoring its type.",
    });
    t.add(CommandSpec {
        name: "get",
        arity: 2,
        flags: CmdFlags::READONLY | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_get,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Returns the string value of a key.",
    });
    t.add(CommandSpec {
        name: "strlen",
        arity: 2,
        flags: CmdFlags::READONLY | CmdFlags::FAST,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        handler: cmd_strlen,
        get_keys: None,
        tips: &[],
        since: "2.2.0",
        summary: "Returns the length of a string value.",
    });
    t.add(CommandSpec {
        name: "mget",
        arity: -2,
        flags: CmdFlags::READONLY | CmdFlags::FAST,
        first_key: 1,
        last_key: -1,
        key_step: 1,
        handler: cmd_mget,
        get_keys: None,
        tips: &[
            "request_policy:multi_shard",
            "response_policy:agg_logical_and",
        ],
        since: "1.0.0",
        summary: "Returns the string values of one or more keys.",
    });
    t.add(CommandSpec {
        name: "mset",
        arity: -3,
        flags: CmdFlags::WRITE | CmdFlags::DENYOOM,
        first_key: 1,
        last_key: -1,
        key_step: 2,
        handler: cmd_mset,
        get_keys: None,
        tips: &[
            "request_policy:multi_shard",
            "response_policy:all_succeeded",
        ],
        since: "1.0.1",
        summary: "Atomically creates or modifies the string values of one or more keys.",
    });
}
