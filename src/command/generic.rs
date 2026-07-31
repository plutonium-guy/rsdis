//! Key-lifecycle commands that are not specific to one value type.
//!
//! Owned by W1c; do not edit if you are not that agent.
//!
//! F0 seeded `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `TTL` and `PTTL` so the
//! foundation can be driven end to end. Extend, do not restart.
//!
//! Still to do (W1c): `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` (with the
//! `NX|XX|GT|LT` modifiers), `PERSIST`, `EXPIRETIME`/`PEXPIRETIME`, `RENAME`,
//! `RENAMENX`, `COPY`, `MOVE`, `KEYS`, `SCAN`, `RANDOMKEY`, `TOUCH`, `DUMP`,
//! `RESTORE`, `OBJECT`, `SORT`, `MIGRATE`, `WAIT`.
//!
//! Two propagation traps to remember (§4.5): `EXPIRE` must propagate as
//! `PEXPIREAT` with the resolved absolute time, and a `SCAN`-driven or
//! random-key deletion must propagate the concrete `DEL`.

use crate::command::{ArgsExt, CmdFlags, CommandSpec, CommandTable};
use crate::ctx::Ctx;
use crate::error::CmdResult;

fn cmd_del(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    let mut removed = 0i64;
    for i in 1..args.len() {
        let key = args.at(i)?.clone();
        if ctx.remove(&key) {
            removed += 1;
            ctx.notify(crate::notify::NotifyClass::GENERIC, "del", &key);
        }
    }
    ctx.out.int(removed);
    Ok(())
}

fn cmd_exists(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    // EXISTS counts repeats: `EXISTS k k` on a live key replies 2.
    let mut found = 0i64;
    for i in 1..args.len() {
        let key = args.at(i)?.clone();
        if ctx.exists(&key) {
            found += 1;
        }
    }
    ctx.out.int(found);
    Ok(())
}

fn cmd_type(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    let key = args.at(1)?.clone();
    match ctx.type_name(&key) {
        Some(t) => ctx.out.simple(t),
        // TYPE on a missing key is `+none`, not a null.
        None => ctx.out.simple("none"),
    }
    Ok(())
}

/// Shared by `TTL` (seconds) and `PTTL` (milliseconds).
///
/// Redis replies `-2` when the key does not exist and `-1` when it exists but
/// has no TTL.
fn ttl_generic(ctx: &mut Ctx<'_>, args: &crate::command::Args, millis: bool) -> CmdResult {
    let key = args.at(1)?.clone();
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
                // Redis rounds to the nearest second.
                ctx.out.int(((remaining + 500) / 1000) as i64);
            }
        }
    }
    Ok(())
}

fn cmd_ttl(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    ttl_generic(ctx, args, false)
}

fn cmd_pttl(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    ttl_generic(ctx, args, true)
}

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
}
