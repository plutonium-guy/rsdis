//! Connection-level commands.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! F0 seeded working implementations of `PING`, `ECHO`, `SELECT`, `HELLO`,
//! `AUTH`, `RESET`, `QUIT` and a minimal `CLIENT` so that the foundation is
//! provably alive against `redis-cli`. **Extend these; do not delete and
//! restart.** In particular the RESP2/RESP3 handling in `HELLO` and the
//! subscribe-mode shape of `PING` are already correct and are what real
//! clients probe on connect.
//!
//! Still to do (W1b): `CLIENT LIST/KILL/UNPAUSE/NO-EVICT/NO-TOUCH/UNBLOCK`,
//! `CLIENT TRACKING`, subscribe-mode command filtering, and `CLIENT REPLY`.

use crate::command::{ArgsExt, CmdFlags, CommandSpec, CommandTable};
use crate::ctx::{ClientFlags, Ctx};
use crate::error::{CmdError, CmdResult};
use crate::reply::{RESP2, RESP3};
use crate::util::eq_ignore_ascii_case;
use crate::util::strnum::string2ll;
use bytes::Bytes;

/// What `HELLO` and `INFO server` report as the emulated Redis version.
///
/// Clients gate features on this. 7.4.0 is the reference release for this
/// project (§6), so claiming it is honest only for the commands we actually
/// implement -- revisit if a wave falls short of 7.4 semantics.
pub const REDIS_VERSION: &str = "7.4.0";

fn cmd_ping(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    match args.len() {
        1 => {
            if ctx.client.subs.in_subscribe_mode() && ctx.out.proto == RESP2 {
                // In RESP2 subscribe mode PING replies with a two-element
                // array, not +PONG.
                ctx.out.array(2);
                ctx.out.bulk(b"pong");
                ctx.out.bulk(b"");
            } else {
                ctx.out.simple("PONG");
            }
            Ok(())
        }
        2 => {
            let msg = args.at(1)?;
            if ctx.client.subs.in_subscribe_mode() && ctx.out.proto == RESP2 {
                ctx.out.array(2);
                ctx.out.bulk(b"pong");
                ctx.out.bulk_from(msg);
            } else {
                ctx.out.bulk_from(msg);
            }
            Ok(())
        }
        _ => Err(CmdError::WrongArity("ping")),
    }
}

fn cmd_echo(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    let msg = args.at(1)?;
    ctx.out.bulk_from(msg);
    Ok(())
}

fn cmd_select(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    let raw = args.at(1)?;
    let Some(n) = string2ll(raw) else {
        return Err(CmdError::NotAnInteger);
    };
    let databases = ctx.server.shards.databases() as i64;
    if n < 0 || n >= databases {
        return Err(CmdError::err("DB index is out of range"));
    }
    ctx.client.db = n as usize;
    ctx.out.ok();
    Ok(())
}

fn cmd_quit(ctx: &mut Ctx<'_>, _args: &crate::command::Args) -> CmdResult {
    ctx.client.close_after_reply();
    ctx.out.ok();
    Ok(())
}

fn cmd_auth(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    // AUTH <password> | AUTH <username> <password>
    let (user, pass) = match args.len() {
        2 => (None, args.at(1)?),
        3 => (Some(args.at(1)?), args.at(2)?),
        _ => return Err(CmdError::WrongArity("auth")),
    };
    let configured = ctx.config().requirepass.clone();
    let Some(expected) = configured else {
        return Err(CmdError::err(
            "Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
        ));
    };
    if let Some(u) = user
        && !eq_ignore_ascii_case(u, b"default")
    {
        return Err(CmdError::custom(
            "WRONGPASS",
            "invalid username-password pair or user is disabled.",
        ));
    }
    if pass.as_ref() == expected.as_bytes() {
        ctx.client.authenticated = true;
        ctx.out.ok();
        Ok(())
    } else {
        Err(CmdError::custom(
            "WRONGPASS",
            "invalid username-password pair or user is disabled.",
        ))
    }
}

fn cmd_hello(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    let mut proto = ctx.client.proto;

    if args.len() > 1 {
        let raw = args.at(1)?;
        let Some(v) = string2ll(raw) else {
            return Err(CmdError::err(
                "Protocol version is not an integer or out of range",
            ));
        };
        if !(2..=3).contains(&v) {
            return Err(CmdError::NoProto);
        }
        proto = v as u8;

        // Optional AUTH / SETNAME suffix.
        let mut i = 2usize;
        while i < args.len() {
            if args.kw_at(i, "AUTH") && i + 2 < args.len() {
                let user = args.at(i + 1)?.clone();
                let pass = args.at(i + 2)?.clone();
                let expected = ctx.config().requirepass.clone();
                match expected {
                    Some(p)
                        if eq_ignore_ascii_case(&user, b"default")
                            && pass.as_ref() == p.as_bytes() =>
                    {
                        ctx.client.authenticated = true;
                    }
                    None => {
                        // No password configured: AUTH inside HELLO is a no-op
                        // error in Redis too.
                        return Err(CmdError::err(
                            "Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
                        ));
                    }
                    Some(_) => {
                        return Err(CmdError::custom(
                            "WRONGPASS",
                            "invalid username-password pair or user is disabled.",
                        ));
                    }
                }
                i += 3;
            } else if args.kw_at(i, "SETNAME") && i + 1 < args.len() {
                ctx.client.name = args.at(i + 1)?.clone();
                i += 2;
            } else {
                return Err(CmdError::err(format!(
                    "unknown argument '{}' to HELLO",
                    String::from_utf8_lossy(args.at(i)?)
                )));
            }
        }
    }

    if !ctx.client.authenticated {
        return Err(CmdError::custom(
            "NOAUTH",
            "HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time",
        ));
    }

    // The protocol switch takes effect for this very reply.
    ctx.client.proto = proto;
    ctx.out.proto = proto;

    ctx.out.map(7);
    ctx.out.bulk(b"server");
    ctx.out.bulk(b"redis");
    ctx.out.bulk(b"version");
    ctx.out.bulk(REDIS_VERSION.as_bytes());
    ctx.out.bulk(b"proto");
    ctx.out.int(i64::from(proto));
    ctx.out.bulk(b"id");
    ctx.out.int(ctx.client.id as i64);
    ctx.out.bulk(b"mode");
    ctx.out.bulk(b"standalone");
    ctx.out.bulk(b"role");
    ctx.out.bulk(b"master");
    ctx.out.bulk(b"modules");
    ctx.out.array(0);
    Ok(())
}

fn cmd_reset(ctx: &mut Ctx<'_>, _args: &crate::command::Args) -> CmdResult {
    ctx.client.db = 0;
    ctx.client.proto = RESP2;
    ctx.out.proto = RESP2;
    ctx.client.name = Bytes::new();
    ctx.client.multi.reset();
    ctx.client
        .flags
        .remove(ClientFlags::MULTI | ClientFlags::DIRTY_CAS | ClientFlags::DIRTY_EXEC);
    if ctx.config().requirepass.is_some() {
        ctx.client.authenticated = false;
    }
    ctx.out.simple("RESET");
    Ok(())
}

fn cmd_client(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    let sub = args.at(1)?;
    if eq_ignore_ascii_case(sub, b"id") {
        ctx.out.int(ctx.client.id as i64);
        return Ok(());
    }
    if eq_ignore_ascii_case(sub, b"getname") {
        if ctx.client.name.is_empty() {
            ctx.out.null();
        } else {
            let n = ctx.client.name.clone();
            ctx.out.bulk_from(&n);
        }
        return Ok(());
    }
    if eq_ignore_ascii_case(sub, b"setname") {
        let name = args.at(2)?;
        if name.iter().any(|&c| !(b'!'..=b'~').contains(&c)) {
            return Err(CmdError::err(
                "Client names cannot contain spaces, newlines or special characters.",
            ));
        }
        ctx.client.name = name.clone();
        ctx.out.ok();
        return Ok(());
    }
    if eq_ignore_ascii_case(sub, b"setinfo") {
        let attr = args.at(2)?;
        let value = args.at(3)?.clone();
        if eq_ignore_ascii_case(attr, b"lib-name") {
            ctx.client.lib_name = value;
        } else if eq_ignore_ascii_case(attr, b"lib-ver") {
            ctx.client.lib_ver = value;
        } else {
            return Err(CmdError::err(format!(
                "Unrecognized option '{}'",
                String::from_utf8_lossy(attr)
            )));
        }
        ctx.out.ok();
        return Ok(());
    }
    if eq_ignore_ascii_case(sub, b"info") {
        let line = client_info_line(ctx);
        ctx.out.bulk(line.as_bytes());
        return Ok(());
    }
    if eq_ignore_ascii_case(sub, b"list") {
        // Owner: W3c/W1b -- this is the honest minimum: only self.
        let line = client_info_line(ctx);
        let mut s = line;
        s.push('\n');
        ctx.out.bulk(s.as_bytes());
        return Ok(());
    }
    Err(CmdError::err(format!(
        "Unknown subcommand or wrong number of arguments for '{}'. Try CLIENT HELP.",
        String::from_utf8_lossy(sub)
    )))
}

fn client_info_line(ctx: &Ctx<'_>) -> String {
    let age = ctx.now_ms.saturating_sub(ctx.client.created_ms) / 1000;
    let idle = ctx.now_ms.saturating_sub(ctx.client.last_interaction_ms) / 1000;
    format!(
        "id={} addr={} laddr={} fd={} name={} age={} idle={} flags=N db={} sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 tot-net-in=0 tot-net-out=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user=default redir=-1 resp={} lib-name={} lib-ver={}",
        ctx.client.id,
        ctx.client.addr,
        ctx.client.laddr,
        ctx.client.fd,
        String::from_utf8_lossy(&ctx.client.name),
        age,
        idle,
        ctx.client.db,
        ctx.client.last_command,
        ctx.client.proto,
        String::from_utf8_lossy(&ctx.client.lib_name),
        String::from_utf8_lossy(&ctx.client.lib_ver),
    )
}

/// Owner: W1b.
pub fn register(t: &mut CommandTable) {
    t.add(CommandSpec {
        name: "ping",
        arity: -1,
        flags: CmdFlags::FAST | CmdFlags::LOADING | CmdFlags::STALE,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_ping,
        get_keys: None,
        tips: &["request_policy:all_shards", "response_policy:all_succeeded"],
        since: "1.0.0",
        summary: "Returns the server's liveliness response.",
    });
    t.add(CommandSpec {
        name: "echo",
        arity: 2,
        flags: CmdFlags::FAST | CmdFlags::LOADING | CmdFlags::STALE,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_echo,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Returns the given string.",
    });
    t.add(CommandSpec {
        name: "select",
        arity: 2,
        flags: CmdFlags::FAST | CmdFlags::LOADING | CmdFlags::STALE,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_select,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Changes the selected database.",
    });
    t.add(CommandSpec {
        name: "quit",
        arity: -1,
        flags: CmdFlags::FAST
            | CmdFlags::LOADING
            | CmdFlags::STALE
            | CmdFlags::NOSCRIPT
            | CmdFlags::NO_SHARDS,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_quit,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Closes the connection.",
    });
    t.add(CommandSpec {
        name: "auth",
        arity: -2,
        flags: CmdFlags::FAST
            | CmdFlags::LOADING
            | CmdFlags::STALE
            | CmdFlags::NOSCRIPT
            | CmdFlags::NO_SHARDS,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_auth,
        get_keys: None,
        tips: &[],
        since: "1.0.0",
        summary: "Authenticates the connection.",
    });
    t.add(CommandSpec {
        name: "hello",
        arity: -1,
        flags: CmdFlags::FAST
            | CmdFlags::LOADING
            | CmdFlags::STALE
            | CmdFlags::NOSCRIPT
            | CmdFlags::NO_SHARDS,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_hello,
        get_keys: None,
        tips: &[],
        since: "6.0.0",
        summary: "Handshakes with the server.",
    });
    t.add(CommandSpec {
        name: "reset",
        arity: 1,
        flags: CmdFlags::FAST
            | CmdFlags::LOADING
            | CmdFlags::STALE
            | CmdFlags::NOSCRIPT
            | CmdFlags::NO_SHARDS,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_reset,
        get_keys: None,
        tips: &[],
        since: "6.2.0",
        summary: "Resets the connection.",
    });
    t.add(CommandSpec {
        name: "client",
        arity: -2,
        flags: CmdFlags::ADMIN | CmdFlags::NOSCRIPT | CmdFlags::LOADING | CmdFlags::STALE,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_client,
        get_keys: None,
        tips: &[],
        since: "2.4.0",
        summary: "A container for client connection commands.",
    });

    // W1b: `subscribe`, `unsubscribe`, `psubscribe`, `punsubscribe` and the
    // shard variants are registered by `command::pubsub` (W3b), not here.
    let _ = RESP3;
}
