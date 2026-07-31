//! Connection-level commands: `PING`, `ECHO`, `SELECT`, `HELLO`, `AUTH`,
//! `QUIT`, `RESET` and the `CLIENT` container.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! # `CLIENT LIST` / `CLIENT INFO` field format
//!
//! Real tooling parses these lines -- `redis-cli --stat`, RedisInsight and
//! `redis_exporter` all split on spaces and then on the first `=`. The field
//! set and its **order** are Redis 7.4's `catClientInfoString()`:
//!
//! ```text
//! id addr laddr fd name age idle flags db sub psub ssub multi watch
//! qbuf qbuf-free argv-mem multi-mem tot-net-in tot-net-out rbs rbp
//! obl oll omem tot-mem events cmd user redir resp lib-name lib-ver
//! ```
//!
//! `CLIENT INFO` is that line with no trailing newline; `CLIENT LIST` is one
//! such line per connection, each terminated by `\n`. Both are bulk strings.
//! `cmd=` uses Redis's `container|subcommand` spelling for container commands
//! (`cmd=client|list`), which §9.9 otherwise leaves unavailable -- the handler
//! sets it directly rather than relying on the dispatcher.
//!
//! Values for connections other than the caller come from
//! [`crate::net::registry`]; see its module docs for why `ClientHandle` alone
//! is not enough.

use crate::command::{ArgsExt, CmdFlags, CommandSpec, CommandTable};
use crate::ctx::{ClientFlags, Ctx};
use crate::error::{CmdError, CmdResult};
use crate::net::registry::{self, ConnSnapshot};
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

/// Fields in the `HELLO` reply map.
const HELLO_FIELDS: usize = 7;

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

/// The `WRONGPASS` error, which must be byte-identical to Redis's: client
/// libraries match on it to distinguish a bad password from a missing one.
fn wrongpass() -> CmdError {
    CmdError::custom(
        "WRONGPASS",
        "invalid username-password pair or user is disabled.",
    )
}

fn no_password_set() -> CmdError {
    CmdError::err(
        "Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
    )
}

/// Check a `(username, password)` pair against `requirepass`.
///
/// §1 puts ACLs out of scope, so the only user is `default`.
fn check_auth(ctx: &Ctx<'_>, user: Option<&Bytes>, pass: &Bytes) -> Result<(), CmdError> {
    let Some(expected) = ctx.config().requirepass.as_deref() else {
        return Err(no_password_set());
    };
    if let Some(u) = user
        && !eq_ignore_ascii_case(u, b"default")
    {
        return Err(wrongpass());
    }
    // Length-then-content: `requirepass` is not secret material an attacker
    // can time out of us in a single round trip, and Redis compares plainly
    // too, but there is no reason to short-circuit on the first byte.
    if constant_time_eq(pass, expected.as_bytes()) {
        Ok(())
    } else {
        Err(wrongpass())
    }
}

/// Compare without an early exit on the first differing byte.
#[inline]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

fn cmd_auth(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    // AUTH <password> | AUTH <username> <password>
    let (user, pass) = match args.len() {
        2 => (None, args.at(1)?.clone()),
        3 => (Some(args.at(1)?.clone()), args.at(2)?.clone()),
        _ => return Err(CmdError::WrongArity("auth")),
    };
    check_auth(ctx, user.as_ref(), &pass)?;
    ctx.client.authenticated = true;
    ctx.out.ok();
    Ok(())
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

        // Optional AUTH / SETNAME suffix, in any order and any number of
        // times, matching `helloCommand()`.
        let mut i = 2usize;
        while i < args.len() {
            if args.kw_at(i, "AUTH") && i + 2 < args.len() {
                let user = args.at(i + 1)?.clone();
                let pass = args.at(i + 2)?.clone();
                check_auth(ctx, Some(&user), &pass)?;
                ctx.client.authenticated = true;
                i += 3;
            } else if args.kw_at(i, "SETNAME") && i + 1 < args.len() {
                let name = args.at(i + 1)?.clone();
                validate_client_name(&name)?;
                ctx.client.name = name;
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

    ctx.out.map(HELLO_FIELDS);
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
    ctx.client.flags.remove(
        ClientFlags::MULTI
            | ClientFlags::DIRTY_CAS
            | ClientFlags::DIRTY_EXEC
            | ClientFlags::MONITOR
            | ClientFlags::REPLY_OFF
            | ClientFlags::REPLY_SKIP
            | ClientFlags::REPLY_SKIP_NEXT,
    );
    if ctx.config().requirepass.is_some() {
        ctx.client.authenticated = false;
    }
    ctx.out.simple("RESET");
    Ok(())
}

// ---------------------------------------------------------------------------
// CLIENT
// ---------------------------------------------------------------------------

/// Redis rejects any client name containing a character outside `!`..`~`,
/// because the name is embedded unescaped in the `CLIENT LIST` line.
fn validate_client_name(name: &[u8]) -> Result<(), CmdError> {
    if name.iter().any(|&c| !(b'!'..=b'~').contains(&c)) {
        return Err(CmdError::err(
            "Client names cannot contain spaces, newlines or special characters.",
        ));
    }
    Ok(())
}

fn unknown_subcommand(container: &str, sub: &[u8]) -> CmdError {
    CmdError::err(format!(
        "Unknown subcommand or wrong number of arguments for '{}'. Try {} HELP.",
        String::from_utf8_lossy(sub),
        container.to_ascii_uppercase()
    ))
}

/// §9.9: `CommandSpec` has no subcommand support, so the arity of a `CLIENT`
/// subcommand is checked here. Redis reports these as `'client|setname'`.
fn sub_arity(args: &crate::command::Args, exact: usize, name: &'static str) -> CmdResult {
    if args.len() == exact {
        Ok(())
    } else {
        Err(CmdError::WrongArity(name))
    }
}

fn cmd_client(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    let sub = args.at(1)?.clone();

    if eq_ignore_ascii_case(&sub, b"id") {
        ctx.client.last_command = "client|id";
        sub_arity(args, 2, "client|id")?;
        ctx.out.int(ctx.client.id as i64);
        return Ok(());
    }

    if eq_ignore_ascii_case(&sub, b"getname") {
        ctx.client.last_command = "client|getname";
        sub_arity(args, 2, "client|getname")?;
        if ctx.client.name.is_empty() {
            ctx.out.null();
        } else {
            let n = ctx.client.name.clone();
            ctx.out.bulk_from(&n);
        }
        return Ok(());
    }

    if eq_ignore_ascii_case(&sub, b"setname") {
        ctx.client.last_command = "client|setname";
        sub_arity(args, 3, "client|setname")?;
        let name = args.at(2)?.clone();
        validate_client_name(&name)?;
        ctx.client.name = name;
        ctx.out.ok();
        return Ok(());
    }

    if eq_ignore_ascii_case(&sub, b"setinfo") {
        ctx.client.last_command = "client|setinfo";
        sub_arity(args, 4, "client|setinfo")?;
        let attr = args.at(2)?.clone();
        let value = args.at(3)?.clone();
        validate_client_name(&value)?;
        if eq_ignore_ascii_case(&attr, b"lib-name") {
            ctx.client.lib_name = value;
        } else if eq_ignore_ascii_case(&attr, b"lib-ver") {
            ctx.client.lib_ver = value;
        } else {
            return Err(CmdError::err(format!(
                "Unrecognized option '{}'",
                String::from_utf8_lossy(&attr)
            )));
        }
        ctx.out.ok();
        return Ok(());
    }

    if eq_ignore_ascii_case(&sub, b"info") {
        ctx.client.last_command = "client|info";
        sub_arity(args, 2, "client|info")?;
        let mut line = String::with_capacity(320);
        write_info_line(&mut line, &self_snapshot(ctx), ctx.now_ms);
        ctx.out.bulk(line.as_bytes());
        return Ok(());
    }

    if eq_ignore_ascii_case(&sub, b"list") {
        ctx.client.last_command = "client|list";
        return client_list(ctx, args);
    }

    if eq_ignore_ascii_case(&sub, b"kill") {
        ctx.client.last_command = "client|kill";
        return client_kill(ctx, args);
    }

    if eq_ignore_ascii_case(&sub, b"no-evict") {
        ctx.client.last_command = "client|no-evict";
        sub_arity(args, 3, "client|no-evict")?;
        set_flag(ctx, args, ClientFlags::NO_EVICT)?;
        ctx.out.ok();
        return Ok(());
    }

    if eq_ignore_ascii_case(&sub, b"no-touch") {
        ctx.client.last_command = "client|no-touch";
        sub_arity(args, 3, "client|no-touch")?;
        set_flag(ctx, args, ClientFlags::NO_TOUCH)?;
        ctx.out.ok();
        return Ok(());
    }

    if eq_ignore_ascii_case(&sub, b"unpause") {
        ctx.client.last_command = "client|unpause";
        sub_arity(args, 2, "client|unpause")?;
        // `CLIENT PAUSE` is not implemented (it needs the global command
        // gate W3c owns), so there is never anything to un-pause. Redis
        // replies `+OK` whether or not a pause was in effect, so this is the
        // correct observable behaviour rather than a stub.
        ctx.out.ok();
        return Ok(());
    }

    if eq_ignore_ascii_case(&sub, b"reply") {
        ctx.client.last_command = "client|reply";
        sub_arity(args, 3, "client|reply")?;
        return client_reply(ctx, args);
    }

    if eq_ignore_ascii_case(&sub, b"help") {
        ctx.client.last_command = "client|help";
        return client_help(ctx);
    }

    Err(unknown_subcommand("client", &sub))
}

/// `CLIENT NO-EVICT ON|OFF`, `CLIENT NO-TOUCH ON|OFF`.
fn set_flag(ctx: &mut Ctx<'_>, args: &crate::command::Args, flag: ClientFlags) -> CmdResult {
    if args.kw_at(2, "on") {
        ctx.client.flags |= flag;
        Ok(())
    } else if args.kw_at(2, "off") {
        ctx.client.flags.remove(flag);
        Ok(())
    } else {
        Err(CmdError::Syntax)
    }
}

/// `CLIENT REPLY ON|OFF|SKIP`.
///
/// Only `ON` produces a reply. The connection layer enforces the suppression
/// (see `net::conn::execute_batch`), because the writer cannot un-write a
/// reply that has already left the handler.
fn client_reply(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    if args.kw_at(2, "on") {
        ctx.client.flags.remove(
            ClientFlags::REPLY_OFF | ClientFlags::REPLY_SKIP | ClientFlags::REPLY_SKIP_NEXT,
        );
        ctx.out.ok();
        Ok(())
    } else if args.kw_at(2, "off") {
        ctx.client.flags |= ClientFlags::REPLY_OFF;
        Ok(())
    } else if args.kw_at(2, "skip") {
        if !ctx.client.flags.contains(ClientFlags::REPLY_OFF) {
            ctx.client.flags |= ClientFlags::REPLY_SKIP_NEXT;
        }
        Ok(())
    } else {
        Err(CmdError::Syntax)
    }
}

fn client_help(ctx: &mut Ctx<'_>) -> CmdResult {
    const LINES: &[&str] = &[
        "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GETNAME",
        "    Return the name of the current connection.",
        "ID",
        "    Return the ID of the current connection.",
        "INFO",
        "    Return information about the current client connection.",
        "KILL <ip:port>",
        "    Kill connection made from <ip:port>.",
        "KILL <option> <value> [<option> <value> [...]]",
        "    Kill connections. Options are:",
        "    * ADDR (<ip:port>|<unixsocket>:0)",
        "      Kill connections made from the specified address",
        "    * LADDR (<ip:port>|<unixsocket>:0)",
        "      Kill connections made to specified local address",
        "    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
        "      Kill connections by type.",
        "    * ID <client-id>",
        "      Kill connections by client id.",
        "    * MAXAGE <maxage>",
        "      Kill connections older than the specified age.",
        "    * SKIPME (YES|NO)",
        "      Skip killing current client (default: yes).",
        "LIST [options ...]",
        "    Return information about client connections. Options:",
        "    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
        "      Return clients of specified type.",
        "    * ID <id> [<id>...]",
        "      Return clients with the specified IDs.",
        "NO-EVICT (ON|OFF)",
        "    Protect current client connection from eviction.",
        "NO-TOUCH (ON|OFF)",
        "    Will not touch LRU/LFU stats when this mode is on.",
        "REPLY (ON|OFF|SKIP)",
        "    Control the replies sent to the current connection.",
        "SETINFO <option> <value>",
        "    Set client meta attr. Options are:",
        "    * LIB-NAME: the client lib name.",
        "    * LIB-VER: the client lib version.",
        "SETNAME <name>",
        "    Assign the name <name> to the current connection.",
        "UNPAUSE",
        "    Stop the current client pause, resuming traffic.",
        "HELP",
        "    Print this help.",
    ];
    ctx.out.array(LINES.len());
    for l in LINES {
        ctx.out.simple(l);
    }
    Ok(())
}

// ------------------------------------------------------------------ LIST

/// `CLIENT LIST [TYPE <type>] [ID <id> ...]`
fn client_list(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    let mut want_type: Option<ClientType> = None;
    let mut want_ids: Vec<u64> = Vec::new();

    let mut i = 2usize;
    while i < args.len() {
        if args.kw_at(i, "type") {
            let raw = args.at(i + 1).map_err(|_| CmdError::Syntax)?;
            let Some(t) = ClientType::parse(raw) else {
                return Err(CmdError::err(format!(
                    "Unknown client type '{}'",
                    String::from_utf8_lossy(raw)
                )));
            };
            want_type = Some(t);
            i += 2;
        } else if args.kw_at(i, "id") {
            if i + 1 >= args.len() {
                return Err(CmdError::Syntax);
            }
            i += 1;
            while i < args.len() {
                let Some(v) = string2ll(args.at(i)?) else {
                    return Err(CmdError::err("Invalid client ID"));
                };
                if v < 0 {
                    return Err(CmdError::err("Invalid client ID"));
                }
                want_ids.push(v as u64);
                i += 1;
            }
        } else {
            return Err(CmdError::Syntax);
        }
    }

    let now = ctx.now_ms;
    let me = self_snapshot(ctx);
    let mut body = String::with_capacity(512);
    for entry in registry::snapshot(ctx.server) {
        // The caller's own row comes from live `ClientState`, not from the
        // registry: the registry is refreshed once per batch, so its copy of
        // `db`/`resp`/`cmd` is one command stale for the client asking.
        let snap = if entry.id == me.id {
            me.clone()
        } else {
            entry.snapshot()
        };
        if let Some(t) = want_type
            && t != ClientType::of(&snap)
        {
            continue;
        }
        if !want_ids.is_empty() && !want_ids.contains(&snap.id) {
            continue;
        }
        write_info_line(&mut body, &snap, now);
        body.push('\n');
    }
    ctx.out.bulk(body.as_bytes());
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientType {
    Normal,
    Master,
    Replica,
    PubSub,
}

impl ClientType {
    fn parse(raw: &[u8]) -> Option<Self> {
        if eq_ignore_ascii_case(raw, b"normal") {
            Some(ClientType::Normal)
        } else if eq_ignore_ascii_case(raw, b"master") {
            Some(ClientType::Master)
        } else if eq_ignore_ascii_case(raw, b"replica") || eq_ignore_ascii_case(raw, b"slave") {
            Some(ClientType::Replica)
        } else if eq_ignore_ascii_case(raw, b"pubsub") {
            Some(ClientType::PubSub)
        } else {
            None
        }
    }

    /// §1 puts replication out of scope, so every connection is either a
    /// normal client or a subscriber.
    fn of(snap: &ConnSnapshot) -> Self {
        if snap.sub + snap.psub > 0 {
            ClientType::PubSub
        } else {
            ClientType::Normal
        }
    }
}

// ------------------------------------------------------------------ KILL

/// `CLIENT KILL <addr>` (old form, replies `+OK`) or
/// `CLIENT KILL <filter> <value> ...` (new form, replies with a count).
fn client_kill(ctx: &mut Ctx<'_>, args: &crate::command::Args) -> CmdResult {
    if args.len() < 3 {
        return Err(CmdError::WrongArity("client|kill"));
    }

    let mut filter = KillFilter::default();
    let old_form = args.len() == 3;
    if old_form {
        // `CLIENT KILL addr:port`
        filter.addr = Some(args.at(2)?.clone());
        filter.skipme = false;
    } else {
        let mut i = 2usize;
        while i < args.len() {
            let Ok(value) = args.at(i + 1) else {
                return Err(CmdError::Syntax);
            };
            if args.kw_at(i, "id") {
                let Some(v) = string2ll(value) else {
                    return Err(CmdError::err("client-id should be greater than 0"));
                };
                if v < 1 {
                    return Err(CmdError::err("client-id should be greater than 0"));
                }
                filter.id = Some(v as u64);
            } else if args.kw_at(i, "addr") {
                filter.addr = Some(value.clone());
            } else if args.kw_at(i, "laddr") {
                filter.laddr = Some(value.clone());
            } else if args.kw_at(i, "type") {
                let Some(t) = ClientType::parse(value) else {
                    return Err(CmdError::err(format!(
                        "Unknown client type '{}'",
                        String::from_utf8_lossy(value)
                    )));
                };
                filter.class = Some(t);
            } else if args.kw_at(i, "user") {
                // §1: ACLs are out of scope, so `default` is the only user.
                if !eq_ignore_ascii_case(value, b"default") {
                    return Err(CmdError::err(format!(
                        "No such user '{}'",
                        String::from_utf8_lossy(value)
                    )));
                }
            } else if args.kw_at(i, "skipme") {
                if eq_ignore_ascii_case(value, b"yes") {
                    filter.skipme = true;
                } else if eq_ignore_ascii_case(value, b"no") {
                    filter.skipme = false;
                } else {
                    return Err(CmdError::Syntax);
                }
            } else if args.kw_at(i, "maxage") {
                let Some(v) = string2ll(value) else {
                    return Err(CmdError::NotAnInteger);
                };
                if v < 0 {
                    return Err(CmdError::NotAnInteger);
                }
                filter.maxage_secs = Some(v as u64);
            } else {
                return Err(CmdError::Syntax);
            }
            i += 2;
        }
    }

    let me = ctx.client.id;
    let now = ctx.now_ms;
    let mut killed = 0usize;
    let mut kill_self = false;

    for entry in registry::snapshot(ctx.server) {
        let snap = entry.snapshot();
        if !filter.matches(&snap, now) {
            continue;
        }
        if filter.skipme && snap.id == me {
            continue;
        }
        if snap.id == me {
            // Redis closes the caller *after* the reply, so the count it just
            // sent actually reaches the client.
            kill_self = true;
            killed += 1;
            continue;
        }
        if entry.kill() {
            killed += 1;
        }
    }

    if old_form {
        if killed == 0 {
            return Err(CmdError::err("No such client address in client list"));
        }
        ctx.out.ok();
    } else {
        ctx.out.int(killed as i64);
    }
    if kill_self {
        ctx.client.close_after_reply();
    }
    Ok(())
}

#[derive(Debug)]
struct KillFilter {
    id: Option<u64>,
    addr: Option<Bytes>,
    laddr: Option<Bytes>,
    class: Option<ClientType>,
    maxage_secs: Option<u64>,
    /// Redis defaults `SKIPME` to yes for the filter form, and to *no* for the
    /// old positional form (`CLIENT KILL addr:port` may kill the caller).
    skipme: bool,
}

impl Default for KillFilter {
    fn default() -> Self {
        KillFilter {
            id: None,
            addr: None,
            laddr: None,
            class: None,
            maxage_secs: None,
            skipme: true,
        }
    }
}

impl KillFilter {
    fn matches(&self, snap: &ConnSnapshot, now_ms: u64) -> bool {
        if let Some(id) = self.id
            && snap.id != id
        {
            return false;
        }
        if let Some(a) = &self.addr
            && a.as_ref() != snap.addr.as_bytes()
        {
            return false;
        }
        if let Some(a) = &self.laddr
            && a.as_ref() != snap.laddr.as_bytes()
        {
            return false;
        }
        if let Some(c) = self.class
            && c != ClientType::of(snap)
        {
            return false;
        }
        if let Some(max) = self.maxage_secs {
            let age = now_ms.saturating_sub(snap.created_ms) / 1000;
            if age < max {
                return false;
            }
        }
        true
    }
}

// ------------------------------------------------------------- rendering

/// The caller's own row, straight from live state.
fn self_snapshot(ctx: &Ctx<'_>) -> ConnSnapshot {
    let mut snap = registry::get(ctx.server, ctx.client.id)
        .map(|e| e.snapshot())
        .unwrap_or_else(|| ConnSnapshot::new(ctx.client, ctx.client.fd));
    snap.refresh_from(ctx.client);
    snap.db = ctx.client.db;
    snap.multi = if ctx.client.flags.contains(ClientFlags::MULTI) {
        ctx.client.multi.queue.len() as i64
    } else {
        -1
    };
    snap.watch = ctx.client.multi.watched.len();
    snap
}

/// One `CLIENT LIST` line. See the module docs for the field order.
fn write_info_line(out: &mut String, snap: &ConnSnapshot, now_ms: u64) {
    use std::fmt::Write as _;

    let age = now_ms.saturating_sub(snap.created_ms) / 1000;
    let idle = now_ms.saturating_sub(snap.last_interaction_ms) / 1000;
    let _ = write!(
        out,
        "id={id} addr={addr} laddr={laddr} fd={fd} name={name} age={age} idle={idle} \
         flags={flags} db={db} sub={sub} psub={psub} ssub={ssub} multi={multi} watch={watch} \
         qbuf={qbuf} qbuf-free={qbuf_free} argv-mem={argv_mem} multi-mem={multi_mem} \
         tot-net-in={tot_in} tot-net-out={tot_out} rbs={rbs} rbp={rbp} obl={obl} oll={oll} \
         omem={omem} tot-mem={tot_mem} events={events} cmd={cmd} user=default redir=-1 \
         resp={resp} lib-name={lib_name} lib-ver={lib_ver}",
        id = snap.id,
        addr = snap.addr,
        laddr = snap.laddr,
        fd = snap.fd,
        name = String::from_utf8_lossy(&snap.name),
        age = age,
        idle = idle,
        flags = snap.flag_string(),
        db = snap.db,
        sub = snap.sub,
        psub = snap.psub,
        ssub = snap.ssub,
        multi = snap.multi,
        watch = snap.watch,
        qbuf = snap.qbuf,
        qbuf_free = snap.qbuf_free,
        argv_mem = snap.argv_mem,
        multi_mem = snap.multi_mem,
        tot_in = snap.tot_net_in,
        tot_out = snap.tot_net_out,
        rbs = snap.rbs,
        rbp = snap.rbp,
        obl = snap.obl,
        oll = snap.oll,
        omem = snap.omem,
        tot_mem = snap.tot_mem,
        events = snap.events(),
        cmd = if snap.last_command.is_empty() {
            "NULL"
        } else {
            snap.last_command
        },
        resp = snap.resp,
        lib_name = String::from_utf8_lossy(&snap.lib_name),
        lib_ver = String::from_utf8_lossy(&snap.lib_ver),
    );
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Split a `CLIENT LIST`/`CLIENT INFO` line into its `key=value` pairs the
    /// way real tooling does.
    fn fields(line: &str) -> std::collections::HashMap<&str, &str> {
        line.split(' ')
            .filter_map(|kv| kv.split_once('='))
            .collect()
    }

    #[test]
    fn info_line_has_every_field_redis_7_4_emits_in_order() {
        let client = crate::ctx::ClientState::new(
            42,
            "127.0.0.1:5000".into(),
            "127.0.0.1:6379".into(),
            1_000,
            false,
        );
        let snap = ConnSnapshot::new(&client, 9);
        let mut line = String::new();
        write_info_line(&mut line, &snap, 61_000);

        let keys: Vec<&str> = line
            .split(' ')
            .filter_map(|kv| kv.split_once('=').map(|(k, _)| k))
            .collect();
        assert_eq!(
            keys,
            vec![
                "id",
                "addr",
                "laddr",
                "fd",
                "name",
                "age",
                "idle",
                "flags",
                "db",
                "sub",
                "psub",
                "ssub",
                "multi",
                "watch",
                "qbuf",
                "qbuf-free",
                "argv-mem",
                "multi-mem",
                "tot-net-in",
                "tot-net-out",
                "rbs",
                "rbp",
                "obl",
                "oll",
                "omem",
                "tot-mem",
                "events",
                "cmd",
                "user",
                "redir",
                "resp",
                "lib-name",
                "lib-ver",
            ]
        );

        let f = fields(&line);
        assert_eq!(f.get("id"), Some(&"42"));
        assert_eq!(f.get("addr"), Some(&"127.0.0.1:5000"));
        assert_eq!(f.get("laddr"), Some(&"127.0.0.1:6379"));
        assert_eq!(f.get("fd"), Some(&"9"));
        assert_eq!(f.get("age"), Some(&"60"));
        assert_eq!(f.get("idle"), Some(&"60"));
        assert_eq!(f.get("flags"), Some(&"N"));
        assert_eq!(f.get("db"), Some(&"0"));
        assert_eq!(f.get("multi"), Some(&"-1"));
        assert_eq!(f.get("resp"), Some(&"2"));
        assert_eq!(f.get("user"), Some(&"default"));
        assert_eq!(f.get("redir"), Some(&"-1"));
        assert_eq!(f.get("events"), Some(&"r"));
        assert!(!line.contains('\n'), "a line must never contain a newline");
    }

    #[test]
    fn client_name_validation_matches_redis() {
        assert!(validate_client_name(b"app-1").is_ok());
        assert!(validate_client_name(b"").is_ok());
        assert!(validate_client_name(b"has space").is_err());
        assert!(validate_client_name(b"has\nnewline").is_err());
    }

    #[test]
    fn client_type_parsing() {
        assert_eq!(ClientType::parse(b"NORMAL"), Some(ClientType::Normal));
        assert_eq!(ClientType::parse(b"pubsub"), Some(ClientType::PubSub));
        assert_eq!(ClientType::parse(b"slave"), Some(ClientType::Replica));
        assert_eq!(ClientType::parse(b"replica"), Some(ClientType::Replica));
        assert_eq!(ClientType::parse(b"nope"), None);
    }

    #[test]
    fn kill_filters() {
        let client = crate::ctx::ClientState::new(
            7,
            "10.0.0.1:1234".into(),
            "10.0.0.2:6379".into(),
            1_000,
            false,
        );
        let snap = ConnSnapshot::new(&client, 3);

        let by_id = KillFilter {
            id: Some(7),
            ..Default::default()
        };
        assert!(by_id.matches(&snap, 2_000));
        let wrong_id = KillFilter {
            id: Some(8),
            ..Default::default()
        };
        assert!(!wrong_id.matches(&snap, 2_000));

        let by_addr = KillFilter {
            addr: Some(Bytes::from_static(b"10.0.0.1:1234")),
            ..Default::default()
        };
        assert!(by_addr.matches(&snap, 2_000));
        let by_laddr = KillFilter {
            laddr: Some(Bytes::from_static(b"10.0.0.2:6379")),
            ..Default::default()
        };
        assert!(by_laddr.matches(&snap, 2_000));

        // MAXAGE selects clients at least that old.
        let old = KillFilter {
            maxage_secs: Some(10),
            ..Default::default()
        };
        assert!(
            !old.matches(&snap, 5_000),
            "5s old, MAXAGE 10 must not match"
        );
        assert!(old.matches(&snap, 20_000));

        let pubsub_only = KillFilter {
            class: Some(ClientType::PubSub),
            ..Default::default()
        };
        assert!(!pubsub_only.matches(&snap, 2_000));
    }

    #[test]
    fn constant_time_eq_is_still_correct() {
        assert!(constant_time_eq(b"hunter2", b"hunter2"));
        assert!(!constant_time_eq(b"hunter2", b"hunter3"));
        assert!(!constant_time_eq(b"hunter2", b"hunter22"));
        assert!(constant_time_eq(b"", b""));
    }
}
