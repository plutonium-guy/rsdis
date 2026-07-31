//! Administrative and introspection commands.
//!
//! Owned by W3c; do not edit if you are not that agent.
//!
//! F0 seeded `COMMAND` (+ `COUNT`/`INFO`/`DOCS`), `DBSIZE`, `FLUSHDB`,
//! `FLUSHALL`, a skeletal `INFO` and a skeletal `CONFIG GET`/`SET`. Two of
//! those exist for reasons beyond the smoke test:
//!
//! * `redis-cli` issues `COMMAND DOCS` on connect, so it has to answer;
//! * `DBSIZE`/`FLUSHDB`/`FLUSHALL` are the foundation's only users of
//!   [`CmdFlags::ALL_SHARDS`], which is what proves the keyspace-wide locking
//!   path works.
//!
//! Still to do (W3c): the real `INFO` sections, `CONFIG GET` globbing,
//! `CONFIG RESETSTAT`/`REWRITE`, `COMMAND GETKEYS`/`LIST`, `DEBUG` subset,
//! `MEMORY USAGE`, `SLOWLOG`, `LATENCY`, `LASTSAVE`, `SHUTDOWN`, `SWAPDB`,
//! `TIME`, `LOLWUT`.

use std::sync::atomic::Ordering;

use crate::command::{Args, ArgsExt, CmdFlags, CommandSpec, CommandTable};
use crate::ctx::Ctx;
use crate::error::{CmdError, CmdResult};
use crate::util::eq_ignore_ascii_case;

fn cmd_dbsize(ctx: &mut Ctx<'_>, _args: &Args) -> CmdResult {
    let mut n = 0usize;
    ctx.for_each_db(|db| n += db.len());
    ctx.out.int(n as i64);
    Ok(())
}

fn cmd_flushdb(ctx: &mut Ctx<'_>, _args: &Args) -> CmdResult {
    // ASYNC/SYNC are accepted and ignored: we have no lazy-free thread yet.
    let mut removed = 0u64;
    ctx.for_each_db(|db| {
        removed += db.len() as u64;
        db.clear();
    });
    ctx.server.dirty.fetch_add(removed, Ordering::Relaxed);
    ctx.out.ok();
    Ok(())
}

fn cmd_flushall(ctx: &mut Ctx<'_>, _args: &Args) -> CmdResult {
    let mut removed = 0u64;
    for (_, shard) in ctx.shards().iter_mut() {
        for db in shard.dbs.iter_mut() {
            removed += db.len() as u64;
            db.clear();
        }
        shard.dirty += removed;
    }
    ctx.server.dirty.fetch_add(removed, Ordering::Relaxed);
    ctx.out.ok();
    Ok(())
}

/// Render one command's `COMMAND INFO` entry.
///
/// Redis's shape is a 10-element array: name, arity, flags, first_key,
/// last_key, step, acl-categories, tips, key-specs, subcommands.
fn write_command_info(ctx: &mut Ctx<'_>, name: &str) {
    let Some(spec) = ctx.server.commands.lookup(name.as_bytes()) else {
        ctx.out.null_array();
        return;
    };
    let (arity, first, last, step) = (spec.arity, spec.first_key, spec.last_key, spec.key_step);
    let flags = redis_flag_names(spec.flags);
    let tips: Vec<&'static str> = spec.tips.to_vec();
    let name = spec.name;

    ctx.out.array(10);
    ctx.out.bulk(name.as_bytes());
    ctx.out.int(i64::from(arity));
    ctx.out.set_header(flags.len());
    for f in &flags {
        ctx.out.simple(f);
    }
    ctx.out.int(i64::from(first));
    ctx.out.int(i64::from(last));
    ctx.out.int(i64::from(step));
    ctx.out.set_header(0); // acl categories
    ctx.out.set_header(tips.len());
    for t in &tips {
        ctx.out.simple(t);
    }
    ctx.out.array(0); // key specs
    ctx.out.array(0); // subcommands
}

/// Map our flags onto the names `COMMAND INFO` uses.
fn redis_flag_names(flags: CmdFlags) -> Vec<&'static str> {
    let mut v = Vec::new();
    for (flag, name) in [
        (CmdFlags::WRITE, "write"),
        (CmdFlags::READONLY, "readonly"),
        (CmdFlags::DENYOOM, "denyoom"),
        (CmdFlags::ADMIN, "admin"),
        (CmdFlags::PUBSUB, "pubsub"),
        (CmdFlags::NOSCRIPT, "noscript"),
        (CmdFlags::BLOCKING, "blocking"),
        (CmdFlags::FAST, "fast"),
        (CmdFlags::LOADING, "loading"),
        (CmdFlags::STALE, "stale"),
        (CmdFlags::MOVABLE_KEYS, "movablekeys"),
    ] {
        if flags.contains(flag) {
            v.push(name);
        }
    }
    v
}

fn cmd_command(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    if args.len() == 1 {
        // Full table.
        let names: Vec<&'static str> = ctx.server.commands.iter().map(|s| s.name).collect();
        ctx.out.array(names.len());
        for n in names {
            write_command_info(ctx, n);
        }
        return Ok(());
    }
    let sub = args.at(1)?;
    if eq_ignore_ascii_case(sub, b"count") {
        ctx.out.int(ctx.server.commands.len() as i64);
        return Ok(());
    }
    if eq_ignore_ascii_case(sub, b"info") {
        if args.len() == 2 {
            let names: Vec<&'static str> = ctx.server.commands.iter().map(|s| s.name).collect();
            ctx.out.array(names.len());
            for n in names {
                write_command_info(ctx, n);
            }
            return Ok(());
        }
        ctx.out.array(args.len() - 2);
        for i in 2..args.len() {
            let raw = args.at(i)?.clone();
            let name = ctx.server.commands.lookup(&raw).map(|s| s.name);
            match name {
                Some(n) => write_command_info(ctx, n),
                None => ctx.out.null_array(),
            }
        }
        return Ok(());
    }
    if eq_ignore_ascii_case(sub, b"docs") {
        // redis-cli calls this on connect. A map of name -> summary/since is
        // enough for it to proceed; W3c fills in arguments and reply schemas.
        let entries: Vec<(&'static str, &'static str, &'static str)> = ctx
            .server
            .commands
            .iter()
            .map(|s| (s.name, s.summary, s.since))
            .collect();
        ctx.out.map(entries.len());
        for (name, summary, since) in entries {
            ctx.out.bulk(name.as_bytes());
            ctx.out.map(2);
            ctx.out.bulk(b"summary");
            ctx.out.bulk(summary.as_bytes());
            ctx.out.bulk(b"since");
            ctx.out.bulk(since.as_bytes());
        }
        return Ok(());
    }
    Err(CmdError::err(format!(
        "Unknown subcommand or wrong number of arguments for '{}'. Try COMMAND HELP.",
        String::from_utf8_lossy(sub)
    )))
}

fn cmd_info(ctx: &mut Ctx<'_>, _args: &Args) -> CmdResult {
    // Owner: W3c. This is the honest minimum: the fields clients actually
    // parse to decide what the server supports.
    let cfg = ctx.config();
    let shards = ctx.server.shards.len();
    let databases = ctx.server.shards.databases();
    let mut keyspace = String::new();
    for db in 0..databases {
        let n = ctx.server.shards.db_size(db);
        if n > 0 {
            keyspace.push_str(&format!("db{db}:keys={n},expires=0,avg_ttl=0\r\n"));
        }
    }
    let text = format!(
        "# Server\r\n\
         redis_version:{version}\r\n\
         rsdis_version:{crate_version}\r\n\
         redis_mode:standalone\r\n\
         os:{os}\r\n\
         arch_bits:64\r\n\
         process_id:{pid}\r\n\
         run_id:{run_id}\r\n\
         tcp_port:{port}\r\n\
         uptime_in_seconds:{uptime}\r\n\
         uptime_in_days:{uptime_days}\r\n\
         shard_count:{shards}\r\n\
         \r\n# Clients\r\n\
         connected_clients:{clients}\r\n\
         blocked_clients:0\r\n\
         \r\n# Stats\r\n\
         total_connections_received:{conns}\r\n\
         total_commands_processed:{cmds}\r\n\
         keyspace_hits:{hits}\r\n\
         keyspace_misses:{misses}\r\n\
         expired_keys:{expired}\r\n\
         evicted_keys:{evicted}\r\n\
         \r\n# Persistence\r\n\
         loading:0\r\n\
         rdb_changes_since_last_save:{dirty}\r\n\
         aof_enabled:{aof}\r\n\
         \r\n# Replication\r\n\
         role:master\r\n\
         connected_slaves:0\r\n\
         \r\n# Keyspace\r\n{keyspace}",
        version = super::connection::REDIS_VERSION,
        crate_version = env!("CARGO_PKG_VERSION"),
        os = std::env::consts::OS,
        pid = ctx.server.pid,
        run_id = ctx.server.run_id,
        port = cfg.port,
        uptime = ctx.server.uptime_secs(),
        uptime_days = ctx.server.uptime_secs() / 86_400,
        shards = shards,
        clients = ctx.server.clients.len(),
        conns = crate::info::Stats::get(&ctx.server.stats.connections_received),
        cmds = crate::info::Stats::get(&ctx.server.stats.commands_processed),
        hits = crate::info::Stats::get(&ctx.server.stats.keyspace_hits),
        misses = crate::info::Stats::get(&ctx.server.stats.keyspace_misses),
        expired = crate::info::Stats::get(&ctx.server.stats.expired_keys),
        evicted = crate::info::Stats::get(&ctx.server.stats.evicted_keys),
        dirty = ctx.server.dirty.load(Ordering::Relaxed),
        aof = i32::from(cfg.appendonly),
        keyspace = keyspace,
    );
    ctx.out.verbatim("txt", text.as_bytes());
    Ok(())
}

fn cmd_config(ctx: &mut Ctx<'_>, args: &Args) -> CmdResult {
    let sub = args.at(1)?;
    if eq_ignore_ascii_case(sub, b"get") {
        // Owner: W3c -- exact-name lookup only, no globbing yet.
        let mut pairs: Vec<(String, String)> = Vec::new();
        let cfg = ctx.config().clone();
        for i in 2..args.len() {
            let name = String::from_utf8_lossy(args.at(i)?).to_ascii_lowercase();
            if let Some(v) = config_get_one(&cfg, &name) {
                pairs.push((name, v));
            }
        }
        ctx.out.map(pairs.len());
        for (k, v) in pairs {
            ctx.out.bulk(k.as_bytes());
            ctx.out.bulk(v.as_bytes());
        }
        return Ok(());
    }
    if eq_ignore_ascii_case(sub, b"set") {
        if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
            return Err(CmdError::WrongArity("config|set"));
        }
        let mut directives: Vec<(String, String)> = Vec::new();
        let mut i = 2usize;
        while i + 1 < args.len() {
            directives.push((
                String::from_utf8_lossy(args.at(i)?).to_string(),
                String::from_utf8_lossy(args.at(i + 1)?).to_string(),
            ));
            i += 2;
        }
        ctx.server
            .config
            .update(|c| {
                for (k, v) in &directives {
                    c.apply(k, std::slice::from_ref(v))?;
                }
                Ok(())
            })
            .map_err(|e| CmdError::err(format!("CONFIG SET failed - {e}")))?;
        ctx.out.ok();
        return Ok(());
    }
    if eq_ignore_ascii_case(sub, b"resetstat") {
        ctx.out.ok();
        return Ok(());
    }
    Err(CmdError::err(format!(
        "Unknown subcommand or wrong number of arguments for '{}'. Try CONFIG HELP.",
        String::from_utf8_lossy(sub)
    )))
}

fn config_get_one(cfg: &crate::config::Config, name: &str) -> Option<String> {
    Some(match name {
        "port" => cfg.port.to_string(),
        "databases" => cfg.databases.to_string(),
        "maxmemory" => cfg.maxmemory.to_string(),
        "maxmemory-policy" => cfg.maxmemory_policy.as_str().to_string(),
        "appendonly" => if cfg.appendonly { "yes" } else { "no" }.to_string(),
        "appendfsync" => cfg.appendfsync.as_str().to_string(),
        "requirepass" => cfg.requirepass.clone().unwrap_or_default(),
        "timeout" => cfg.timeout.to_string(),
        "tcp-keepalive" => cfg.tcp_keepalive.to_string(),
        "hash-max-listpack-entries" => cfg.hash_max_listpack_entries.to_string(),
        "hash-max-listpack-value" => cfg.hash_max_listpack_value.to_string(),
        "list-max-listpack-size" => cfg.list_max_listpack_size.to_string(),
        "set-max-intset-entries" => cfg.set_max_intset_entries.to_string(),
        "set-max-listpack-entries" => cfg.set_max_listpack_entries.to_string(),
        "zset-max-listpack-entries" => cfg.zset_max_listpack_entries.to_string(),
        "zset-max-listpack-value" => cfg.zset_max_listpack_value.to_string(),
        "proto-max-bulk-len" => cfg.proto_max_bulk_len.to_string(),
        "notify-keyspace-events" => cfg.notify_keyspace_events.to_config_string(),
        "shard-count" => cfg.effective_shard_count().to_string(),
        "save" => cfg
            .save
            .iter()
            .map(|p| format!("{} {}", p.seconds, p.changes))
            .collect::<Vec<_>>()
            .join(" "),
        _ => return None,
    })
}

/// Owner: W3c.
pub fn register(t: &mut CommandTable) {
    t.add(CommandSpec {
        name: "command",
        arity: -1,
        flags: CmdFlags::LOADING | CmdFlags::STALE,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_command,
        get_keys: None,
        tips: &["request_policy:all_shards", "nondeterministic_output_order"],
        since: "2.8.13",
        summary: "Returns detailed information about all commands.",
    });
    t.add(CommandSpec {
        name: "dbsize",
        arity: 1,
        flags: CmdFlags::READONLY | CmdFlags::FAST | CmdFlags::ALL_SHARDS,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_dbsize,
        get_keys: None,
        tips: &["request_policy:all_shards", "response_policy:agg_sum"],
        since: "1.0.0",
        summary: "Returns the number of keys in the database.",
    });
    t.add(CommandSpec {
        name: "flushdb",
        arity: -1,
        flags: CmdFlags::WRITE | CmdFlags::ALL_SHARDS,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_flushdb,
        get_keys: None,
        tips: &["request_policy:all_shards", "response_policy:all_succeeded"],
        since: "1.0.0",
        summary: "Removes all keys from the current database.",
    });
    t.add(CommandSpec {
        name: "flushall",
        arity: -1,
        flags: CmdFlags::WRITE | CmdFlags::ALL_SHARDS,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_flushall,
        get_keys: None,
        tips: &["request_policy:all_shards", "response_policy:all_succeeded"],
        since: "1.0.0",
        summary: "Removes all keys from all databases.",
    });
    t.add(CommandSpec {
        name: "info",
        arity: -1,
        flags: CmdFlags::LOADING | CmdFlags::STALE,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_info,
        get_keys: None,
        tips: &["request_policy:all_shards", "nondeterministic_output"],
        since: "1.0.0",
        summary: "Returns information and statistics about the server.",
    });
    t.add(CommandSpec {
        name: "config",
        arity: -2,
        flags: CmdFlags::ADMIN | CmdFlags::NOSCRIPT | CmdFlags::LOADING | CmdFlags::STALE,
        first_key: 0,
        last_key: 0,
        key_step: 0,
        handler: cmd_config,
        get_keys: None,
        tips: &[],
        since: "2.0.0",
        summary: "A container for server configuration commands.",
    });
}
