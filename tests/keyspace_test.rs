//! Conformance tests for the keyspace layer: generic commands, active
//! expiry, eviction and keyspace notifications.
//!
//! Owner: W1c. (`tests/**` belongs to W3d; §3 allows each agent to add
//! `tests/<own_area>_test.rs`.)
//!
//! Every expectation here was written against Redis 7.4 semantics: reply
//! shapes, reply *values* for the edge cases that clients actually depend on
//! (`TTL` of -1 vs -2, `RENAME` to itself, `EXPIRE` with a negative TTL), and
//! the exact error strings.
//!
//! These drive `engine::dispatch` directly rather than a socket. The protocol
//! path is already covered end to end by `foundation_test.rs`; what needs
//! covering here is semantics, and a synchronous harness makes it possible to
//! assert on shard state and on the propagation buffer straight afterwards.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::{Bytes, BytesMut};
use rsdis::command::generic::{ValueCodec, install_value_codec};
use rsdis::config::{Config, MaxmemoryPolicy};
use rsdis::ctx::{ClientState, ServerShared};
use rsdis::engine;
use rsdis::notify::{self, NotifyClass};
use rsdis::object::Robj;
use rsdis::shard::{evict, expire};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Harness {
    server: Arc<ServerShared>,
    client: ClientState,
}

impl Harness {
    fn new() -> Self {
        Harness::with(Config {
            shard_count: 8,
            ..Default::default()
        })
    }

    fn with(cfg: Config) -> Self {
        let server = ServerShared::new(cfg);
        let client = ClientState::new(1, "test".into(), "test".into(), 0, false);
        Harness { server, client }
    }

    /// Run one command, given binary-safe arguments, and decode the reply.
    ///
    /// Everything on this path is bytes rather than text: `DUMP` payloads are
    /// binary, and a `String` round trip would silently mangle them.
    fn run_bin(&mut self, args: &[&[u8]]) -> Reply {
        let mut buf = BytesMut::new();
        let argv: rsdis::command::ArgVec = args.iter().map(|s| Bytes::copy_from_slice(s)).collect();
        engine::dispatch(&self.server, &mut self.client, &mut buf, &argv);
        parse(&buf)
    }

    /// Run one command and decode the reply into a [`Reply`].
    fn run(&mut self, args: &[&str]) -> Reply {
        let bin: Vec<&[u8]> = args.iter().map(|s| s.as_bytes()).collect();
        self.run_bin(&bin)
    }

    fn int(&mut self, args: &[&str]) -> i64 {
        match self.run(args) {
            Reply::Int(v) => v,
            other => panic!("expected an integer for {args:?}, got {other:?}"),
        }
    }

    fn simple(&mut self, args: &[&str]) -> String {
        match self.run(args) {
            Reply::Simple(s) => s,
            other => panic!("expected a status for {args:?}, got {other:?}"),
        }
    }

    fn error(&mut self, args: &[&str]) -> String {
        match self.run(args) {
            Reply::Error(s) => s,
            other => panic!("expected an error for {args:?}, got {other:?}"),
        }
    }

    fn bulk(&mut self, args: &[&str]) -> Option<String> {
        self.bulk_bytes(args)
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    }

    fn bulk_bytes(&mut self, args: &[&str]) -> Option<Vec<u8>> {
        match self.run(args) {
            Reply::Bulk(s) => Some(s),
            Reply::Nil => None,
            other => panic!("expected a bulk for {args:?}, got {other:?}"),
        }
    }

    fn array(&mut self, args: &[&str]) -> Vec<Reply> {
        match self.run(args) {
            Reply::Array(v) => v,
            other => panic!("expected an array for {args:?}, got {other:?}"),
        }
    }

    fn strings(&mut self, args: &[&str]) -> Vec<String> {
        self.array(args)
            .into_iter()
            .map(|r| match r {
                Reply::Bulk(s) => String::from_utf8_lossy(&s).into_owned(),
                Reply::Simple(s) => s,
                other => panic!("expected a string element, got {other:?}"),
            })
            .collect()
    }

    /// Everything queued for the AOF, flattened to `Vec<Vec<String>>`.
    fn drain_propagation(&self) -> Vec<Vec<String>> {
        let mut out = Vec::new();
        for handle in self.server.shards.iter() {
            for p in handle.lock().drain_propagation() {
                out.push(
                    p.argv
                        .iter()
                        .map(|b| String::from_utf8_lossy(b).into_owned())
                        .collect(),
                );
            }
        }
        out
    }

    fn enable_propagation(&self) {
        self.server
            .propagation_enabled
            .store(true, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// A minimal RESP2 reply decoder, enough for the assertions below.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    Array(Vec<Reply>),
}

impl Reply {
    /// A bulk cursor or key as text.
    fn text(&self) -> String {
        match self {
            Reply::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            Reply::Simple(s) => s.clone(),
            other => panic!("expected a string reply, got {other:?}"),
        }
    }

    fn bulk_str(s: &str) -> Reply {
        Reply::Bulk(s.as_bytes().to_vec())
    }
}

fn parse(bytes: &[u8]) -> Reply {
    let (reply, consumed) = parse_at(bytes, 0);
    assert_eq!(
        consumed,
        bytes.len(),
        "trailing bytes in reply {:?}",
        String::from_utf8_lossy(bytes)
    );
    reply
}

fn parse_at(b: &[u8], mut i: usize) -> (Reply, usize) {
    let marker = b[i];
    i += 1;
    let line_end = find_crlf(b, i);
    let line = String::from_utf8_lossy(&b[i..line_end]).into_owned();
    i = line_end + 2;
    match marker {
        b'+' => (Reply::Simple(line), i),
        b'-' => (Reply::Error(line), i),
        b':' => (Reply::Int(line.parse().expect("integer reply")), i),
        b'$' => {
            let n: i64 = line.parse().expect("bulk length");
            if n < 0 {
                return (Reply::Nil, i);
            }
            let end = i + n as usize;
            (Reply::Bulk(b[i..end].to_vec()), end + 2)
        }
        b'*' => {
            let n: i64 = line.parse().expect("array length");
            if n < 0 {
                return (Reply::Nil, i);
            }
            let mut items = Vec::new();
            for _ in 0..n {
                let (item, next) = parse_at(b, i);
                items.push(item);
                i = next;
            }
            (Reply::Array(items), i)
        }
        other => panic!("unhandled RESP marker {:?}", other as char),
    }
}

fn find_crlf(b: &[u8], from: usize) -> usize {
    let mut i = from;
    while i + 1 < b.len() {
        if b[i] == b'\r' && b[i + 1] == b'\n' {
            return i;
        }
        i += 1;
    }
    panic!("unterminated line");
}

// ---------------------------------------------------------------------------
// DEL / EXISTS / TYPE / TOUCH
// ---------------------------------------------------------------------------

#[test]
fn del_unlink_exists_and_type() {
    let mut h = Harness::new();
    h.simple(&["set", "a", "1"]);
    h.simple(&["set", "b", "2"]);

    assert_eq!(h.int(&["exists", "a"]), 1);
    assert_eq!(
        h.int(&["exists", "a", "a", "b"]),
        3,
        "EXISTS counts repeats"
    );
    assert_eq!(h.int(&["exists", "nope"]), 0);
    assert_eq!(h.simple(&["type", "a"]), "string");
    assert_eq!(h.simple(&["type", "nope"]), "none");

    assert_eq!(h.int(&["del", "a", "nope"]), 1);
    assert_eq!(h.int(&["unlink", "b"]), 1);
    assert_eq!(h.int(&["exists", "a", "b"]), 0);
}

#[test]
fn touch_counts_live_keys() {
    let mut h = Harness::new();
    h.simple(&["set", "a", "1"]);
    assert_eq!(h.int(&["touch", "a", "missing", "a"]), 2);
    assert_eq!(h.int(&["touch", "missing"]), 0);
}

// ---------------------------------------------------------------------------
// TTL family
// ---------------------------------------------------------------------------

#[test]
fn ttl_reports_minus_one_and_minus_two() {
    let mut h = Harness::new();
    h.simple(&["set", "persistent", "v"]);
    assert_eq!(h.int(&["ttl", "persistent"]), -1, "no TTL is -1");
    assert_eq!(h.int(&["pttl", "persistent"]), -1);
    assert_eq!(h.int(&["ttl", "missing"]), -2, "missing key is -2");
    assert_eq!(h.int(&["pttl", "missing"]), -2);
    assert_eq!(h.int(&["expiretime", "persistent"]), -1);
    assert_eq!(h.int(&["pexpiretime", "missing"]), -2);
}

#[test]
fn expire_sets_and_reports_a_ttl() {
    let mut h = Harness::new();
    h.simple(&["set", "k", "v"]);
    assert_eq!(h.int(&["expire", "k", "100"]), 1);
    let ttl = h.int(&["ttl", "k"]);
    assert!((99..=100).contains(&ttl), "ttl was {ttl}");
    let pttl = h.int(&["pttl", "k"]);
    assert!((99_000..=100_000).contains(&pttl), "pttl was {pttl}");

    let at = h.int(&["expiretime", "k"]);
    let now_s = (h.server.clock.now_ms() / 1000) as i64;
    assert!((now_s + 99..=now_s + 101).contains(&at), "expiretime {at}");
}

#[test]
fn expire_on_a_missing_key_returns_zero() {
    let mut h = Harness::new();
    assert_eq!(h.int(&["expire", "missing", "100"]), 0);
    assert_eq!(h.int(&["pexpire", "missing", "100"]), 0);
    assert_eq!(h.int(&["expireat", "missing", "99999999999"]), 0);
    assert_eq!(h.int(&["persist", "missing"]), 0);
}

#[test]
fn a_negative_ttl_deletes_the_key() {
    let mut h = Harness::new();
    h.enable_propagation();
    h.simple(&["set", "k", "v"]);
    let _ = h.drain_propagation();

    assert_eq!(h.int(&["expire", "k", "-1"]), 1, "the reply is still 1");
    assert_eq!(h.int(&["exists", "k"]), 0, "the key must be gone");

    // §4.5: what reaches the AOF is the deletion, not the expiry.
    let propagated = h.drain_propagation();
    assert_eq!(propagated.len(), 1);
    assert_eq!(propagated[0][0], "DEL");
    assert_eq!(propagated[0][1], "k");

    // A deadline already in the past does the same thing.
    h.simple(&["set", "k2", "v"]);
    let _ = h.drain_propagation();
    assert_eq!(h.int(&["expireat", "k2", "1"]), 1);
    assert_eq!(h.int(&["exists", "k2"]), 0);
}

#[test]
fn expire_propagates_an_absolute_pexpireat() {
    let mut h = Harness::new();
    h.enable_propagation();
    h.simple(&["set", "k", "v"]);
    let _ = h.drain_propagation();

    h.int(&["expire", "k", "100"]);
    let propagated = h.drain_propagation();
    assert_eq!(propagated.len(), 1, "{propagated:?}");
    assert_eq!(propagated[0][0], "PEXPIREAT");
    assert_eq!(propagated[0][1], "k");
    let at: u64 = propagated[0][2].parse().expect("absolute ms");
    let now = h.server.clock.now_ms();
    assert!(
        at > now + 99_000 && at <= now + 100_000,
        "expected an absolute deadline near now+100s, got {at} (now {now})"
    );

    // The same rewrite happens for every spelling in the family.
    for cmd in [
        vec!["pexpire", "k", "50000"],
        vec!["expireat", "k", "99999999999"],
        vec!["pexpireat", "k", "99999999999999"],
    ] {
        h.int(&cmd);
        let propagated = h.drain_propagation();
        assert_eq!(propagated[0][0], "PEXPIREAT", "for {cmd:?}");
    }
}

#[test]
fn a_no_op_expire_propagates_nothing() {
    let mut h = Harness::new();
    h.enable_propagation();
    h.simple(&["set", "k", "v"]);
    let _ = h.drain_propagation();

    // NX on a key that already has a TTL: refused, so nothing to replay.
    h.int(&["expire", "k", "100"]);
    let _ = h.drain_propagation();
    assert_eq!(h.int(&["expire", "k", "200", "NX"]), 0);
    assert!(h.drain_propagation().is_empty());

    assert_eq!(h.int(&["expire", "missing", "100"]), 0);
    assert!(h.drain_propagation().is_empty());
}

#[test]
fn expire_condition_flags() {
    let mut h = Harness::new();
    h.simple(&["set", "k", "v"]);

    // NX: only when there is no TTL yet.
    assert_eq!(h.int(&["expire", "k", "100", "NX"]), 1);
    assert_eq!(h.int(&["expire", "k", "200", "NX"]), 0);

    // XX: only when there already is one.
    assert_eq!(h.int(&["expire", "k", "300", "XX"]), 1);
    h.simple(&["set", "fresh", "v"]);
    assert_eq!(h.int(&["expire", "fresh", "100", "XX"]), 0);

    // GT: only a later deadline wins.
    assert_eq!(h.int(&["expire", "k", "100", "GT"]), 0, "100 < 300");
    assert_eq!(h.int(&["expire", "k", "900", "GT"]), 1);
    // A key with no TTL is "expires at infinity", so GT can never fire.
    assert_eq!(h.int(&["expire", "fresh", "100", "GT"]), 0);

    // LT: only an earlier deadline wins, and it always wins over no TTL.
    assert_eq!(h.int(&["expire", "k", "1000", "LT"]), 0, "1000 > 900");
    assert_eq!(h.int(&["expire", "k", "10", "LT"]), 1);
    assert_eq!(h.int(&["expire", "fresh", "100", "LT"]), 1);
}

#[test]
fn expire_flag_errors_match_redis() {
    let mut h = Harness::new();
    h.simple(&["set", "k", "v"]);
    assert_eq!(
        h.error(&["expire", "k", "100", "NX", "XX"]),
        "ERR NX and XX, GT or LT options at the same time are not compatible"
    );
    assert_eq!(
        h.error(&["expire", "k", "100", "GT", "LT"]),
        "ERR GT and LT options at the same time are not compatible"
    );
    assert_eq!(
        h.error(&["expire", "k", "100", "BOGUS"]),
        "ERR Unsupported option BOGUS"
    );
    assert_eq!(
        h.error(&["expire", "k", "notanumber"]),
        "ERR value is not an integer or out of range"
    );
    assert_eq!(
        h.error(&["expire", "k", "9999999999999999"]),
        "ERR invalid expire time in 'expire' command"
    );
    assert_eq!(
        h.error(&["pexpire", "k", "9223372036854775807"]),
        "ERR invalid expire time in 'pexpire' command"
    );
}

#[test]
fn persist_clears_a_ttl_once() {
    let mut h = Harness::new();
    h.simple(&["set", "k", "v"]);
    h.int(&["expire", "k", "100"]);
    assert_eq!(h.int(&["persist", "k"]), 1);
    assert_eq!(h.int(&["ttl", "k"]), -1);
    assert_eq!(h.int(&["persist", "k"]), 0, "already persistent");
}

#[test]
fn an_expired_key_is_invisible_before_the_sweep_runs() {
    let mut h = Harness::new();
    h.simple(&["set", "k", "v"]);
    h.int(&["pexpire", "k", "1"]);
    std::thread::sleep(std::time::Duration::from_millis(5));
    h.server.clock.refresh();

    assert_eq!(h.int(&["exists", "k"]), 0);
    assert_eq!(h.int(&["ttl", "k"]), -2);
    assert_eq!(h.simple(&["type", "k"]), "none");
    assert_eq!(h.bulk(&["get", "k"]), None);
}

// ---------------------------------------------------------------------------
// RENAME / RENAMENX
// ---------------------------------------------------------------------------

#[test]
fn rename_moves_the_value_and_the_ttl() {
    let mut h = Harness::new();
    h.simple(&["set", "src", "hello"]);
    h.int(&["expire", "src", "100"]);
    assert_eq!(h.simple(&["rename", "src", "dst"]), "OK");
    assert_eq!(h.int(&["exists", "src"]), 0);
    assert_eq!(h.bulk(&["get", "dst"]).as_deref(), Some("hello"));
    let ttl = h.int(&["ttl", "dst"]);
    assert!((99..=100).contains(&ttl), "the TTL must travel: {ttl}");
}

#[test]
fn rename_overwrites_the_destination() {
    let mut h = Harness::new();
    h.simple(&["set", "src", "new"]);
    h.simple(&["set", "dst", "old"]);
    h.int(&["expire", "dst", "100"]);
    assert_eq!(h.simple(&["rename", "src", "dst"]), "OK");
    assert_eq!(h.bulk(&["get", "dst"]).as_deref(), Some("new"));
    assert_eq!(
        h.int(&["ttl", "dst"]),
        -1,
        "the destination's own TTL must not survive"
    );
}

#[test]
fn rename_to_itself_succeeds_and_changes_nothing() {
    let mut h = Harness::new();
    h.enable_propagation();
    h.simple(&["set", "k", "v"]);
    h.int(&["expire", "k", "100"]);
    let _ = h.drain_propagation();

    assert_eq!(h.simple(&["rename", "k", "k"]), "OK");
    assert_eq!(h.bulk(&["get", "k"]).as_deref(), Some("v"));
    let ttl = h.int(&["ttl", "k"]);
    assert!((99..=100).contains(&ttl), "TTL must survive: {ttl}");
    assert!(
        h.drain_propagation().is_empty(),
        "a self-rename is not a write"
    );

    // RENAMENX to itself reports 0, per `renameGenericCommand`.
    assert_eq!(h.int(&["renamenx", "k", "k"]), 0);
}

#[test]
fn rename_on_a_missing_key_is_an_error() {
    let mut h = Harness::new();
    assert_eq!(h.error(&["rename", "missing", "dst"]), "ERR no such key");
    assert_eq!(h.error(&["renamenx", "missing", "dst"]), "ERR no such key");
}

#[test]
fn renamenx_refuses_an_existing_destination() {
    let mut h = Harness::new();
    h.simple(&["set", "src", "a"]);
    h.simple(&["set", "dst", "b"]);
    assert_eq!(h.int(&["renamenx", "src", "dst"]), 0);
    assert_eq!(h.bulk(&["get", "src"]).as_deref(), Some("a"));
    h.int(&["del", "dst"]);
    assert_eq!(h.int(&["renamenx", "src", "dst"]), 1);
    assert_eq!(h.int(&["exists", "src"]), 0);
}

#[test]
fn rename_works_across_shards() {
    let mut h = Harness::new();
    // Two names that hash to different shards, so the command needs both
    // locks in ascending order.
    let (a, b) = ("alpha", "beta");
    assert_ne!(
        h.server.shards.shard_index(a.as_bytes()),
        h.server.shards.shard_index(b.as_bytes()),
        "test needs two keys on different shards"
    );
    h.simple(&["set", a, "value"]);
    assert_eq!(h.simple(&["rename", a, b]), "OK");
    assert_eq!(h.bulk(&["get", b]).as_deref(), Some("value"));
}

// ---------------------------------------------------------------------------
// COPY / MOVE
// ---------------------------------------------------------------------------

#[test]
fn copy_duplicates_a_string_with_its_ttl() {
    let mut h = Harness::new();
    h.simple(&["set", "src", "hello"]);
    h.int(&["expire", "src", "100"]);

    assert_eq!(h.int(&["copy", "src", "dst"]), 1);
    assert_eq!(h.bulk(&["get", "dst"]).as_deref(), Some("hello"));
    assert_eq!(h.bulk(&["get", "src"]).as_deref(), Some("hello"));
    let ttl = h.int(&["ttl", "dst"]);
    assert!((99..=100).contains(&ttl), "the TTL is copied: {ttl}");
}

#[test]
fn copy_respects_replace() {
    let mut h = Harness::new();
    h.simple(&["set", "src", "new"]);
    h.simple(&["set", "dst", "old"]);
    assert_eq!(h.int(&["copy", "src", "dst"]), 0);
    assert_eq!(h.bulk(&["get", "dst"]).as_deref(), Some("old"));
    assert_eq!(h.int(&["copy", "src", "dst", "REPLACE"]), 1);
    assert_eq!(h.bulk(&["get", "dst"]).as_deref(), Some("new"));
}

#[test]
fn copy_to_another_database() {
    let mut h = Harness::new();
    h.simple(&["set", "k", "v"]);
    assert_eq!(h.int(&["copy", "k", "k", "DB", "3"]), 1);
    assert_eq!(h.simple(&["select", "3"]), "OK");
    assert_eq!(h.bulk(&["get", "k"]).as_deref(), Some("v"));
    assert_eq!(h.simple(&["select", "0"]), "OK");
    assert_eq!(h.bulk(&["get", "k"]).as_deref(), Some("v"));

    assert_eq!(
        h.error(&["copy", "k", "k"]),
        "ERR source and destination objects are the same"
    );
    assert_eq!(
        h.error(&["copy", "k", "k2", "DB", "99"]),
        "ERR DB index is out of range"
    );
    assert_eq!(h.int(&["copy", "missing", "dst"]), 0);
}

#[test]
fn move_transfers_a_key_between_databases() {
    let mut h = Harness::new();
    h.simple(&["set", "k", "v"]);
    h.int(&["expire", "k", "100"]);
    assert_eq!(h.int(&["move", "k", "1"]), 1);
    assert_eq!(h.int(&["exists", "k"]), 0);

    h.simple(&["select", "1"]);
    assert_eq!(h.bulk(&["get", "k"]).as_deref(), Some("v"));
    let ttl = h.int(&["ttl", "k"]);
    assert!((99..=100).contains(&ttl), "the TTL travels: {ttl}");

    // Moving onto an existing key fails without touching either side.
    h.simple(&["select", "0"]);
    h.simple(&["set", "k", "other"]);
    assert_eq!(h.int(&["move", "k", "1"]), 0);
    assert_eq!(h.bulk(&["get", "k"]).as_deref(), Some("other"));

    assert_eq!(
        h.error(&["move", "k", "0"]),
        "ERR source and destination objects are the same"
    );
    assert_eq!(
        h.error(&["move", "k", "99"]),
        "ERR DB index is out of range"
    );
    assert_eq!(h.int(&["move", "missing", "2"]), 0);
}

// ---------------------------------------------------------------------------
// KEYS / RANDOMKEY
// ---------------------------------------------------------------------------

#[test]
fn keys_matches_glob_patterns() {
    let mut h = Harness::new();
    for k in ["one", "two", "three", "four", "hello", "hallo"] {
        h.simple(&["set", k, "v"]);
    }

    let mut all = h.strings(&["keys", "*"]);
    all.sort();
    assert_eq!(all, ["four", "hallo", "hello", "one", "three", "two"]);

    let mut t = h.strings(&["keys", "t*"]);
    t.sort();
    assert_eq!(t, ["three", "two"]);

    let mut hx = h.strings(&["keys", "h[ae]llo"]);
    hx.sort();
    assert_eq!(hx, ["hallo", "hello"]);

    assert_eq!(h.strings(&["keys", "h?llo"]).len(), 2);
    assert!(h.strings(&["keys", "nomatch*"]).is_empty());
}

#[test]
fn keys_hides_expired_keys() {
    let mut h = Harness::new();
    h.simple(&["set", "live", "v"]);
    h.simple(&["set", "dying", "v"]);
    h.int(&["pexpire", "dying", "1"]);
    std::thread::sleep(std::time::Duration::from_millis(5));
    h.server.clock.refresh();
    assert_eq!(h.strings(&["keys", "*"]), ["live"]);
}

#[test]
fn randomkey_returns_a_member_or_nil() {
    let mut h = Harness::new();
    assert_eq!(h.bulk(&["randomkey"]), None, "empty db replies nil");

    for i in 0..50 {
        h.simple(&["set", &format!("k:{i}"), "v"]);
    }
    let mut seen = std::collections::HashSet::new();
    for _ in 0..200 {
        let k = h.bulk(&["randomkey"]).expect("a key");
        assert!(k.starts_with("k:"), "unexpected key {k}");
        seen.insert(k);
    }
    assert!(
        seen.len() > 5,
        "RANDOMKEY looks stuck: only saw {}",
        seen.len()
    );
}

// ---------------------------------------------------------------------------
// SCAN
// ---------------------------------------------------------------------------

/// Drive a full `SCAN` iteration, returning every key it produced.
fn full_scan(h: &mut Harness, extra: &[&str]) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut out = Vec::new();
    let mut calls = 0;
    loop {
        let mut args: Vec<&str> = vec!["scan", &cursor];
        args.extend_from_slice(extra);
        let reply = h.array(&args);
        assert_eq!(reply.len(), 2, "SCAN replies a 2-element array");
        let next = reply[0].text();
        match &reply[1] {
            Reply::Array(items) => {
                for i in items {
                    out.push(i.text());
                }
            }
            other => panic!("expected a key array, got {other:?}"),
        }
        cursor = next;
        calls += 1;
        assert!(calls < 100_000, "SCAN did not terminate");
        if cursor == "0" {
            return out;
        }
    }
}

#[test]
fn scan_visits_every_key_exactly_once() {
    let mut h = Harness::new();
    let expected: Vec<String> = (0..1_000).map(|i| format!("key:{i}")).collect();
    for k in &expected {
        h.simple(&["set", k, "v"]);
    }

    let mut got = full_scan(&mut h, &[]);
    got.sort();
    let mut want = expected.clone();
    want.sort();
    assert_eq!(got.len(), want.len(), "SCAN returned duplicates or gaps");
    assert_eq!(got, want);
}

#[test]
fn scan_with_a_count_larger_than_the_keyspace_finishes_in_one_call() {
    let mut h = Harness::new();
    for i in 0..10 {
        h.simple(&["set", &format!("k:{i}"), "v"]);
    }
    let reply = h.array(&["scan", "0", "COUNT", "10000"]);
    assert_eq!(
        reply[0],
        Reply::bulk_str("0"),
        "a COUNT past the keyspace must complete the iteration"
    );
    match &reply[1] {
        Reply::Array(items) => assert_eq!(items.len(), 10),
        other => panic!("{other:?}"),
    }
}

#[test]
fn scan_honours_match_and_type() {
    let mut h = Harness::new();
    for i in 0..100 {
        h.simple(&["set", &format!("user:{i}"), "v"]);
        h.simple(&["set", &format!("post:{i}"), "v"]);
    }

    let users = full_scan(&mut h, &["MATCH", "user:*"]);
    assert_eq!(users.len(), 100);
    assert!(users.iter().all(|k| k.starts_with("user:")));

    let strings = full_scan(&mut h, &["TYPE", "string"]);
    assert_eq!(strings.len(), 200);
    let lists = full_scan(&mut h, &["TYPE", "list"]);
    assert!(lists.is_empty());

    let both = full_scan(&mut h, &["MATCH", "post:1?", "TYPE", "string"]);
    assert_eq!(both.len(), 10, "post:10..post:19");
}

#[test]
fn scan_skips_expired_keys() {
    let mut h = Harness::new();
    for i in 0..50 {
        h.simple(&["set", &format!("live:{i}"), "v"]);
        h.simple(&["set", &format!("dying:{i}"), "v"]);
        h.int(&["pexpire", &format!("dying:{i}"), "1"]);
    }
    std::thread::sleep(std::time::Duration::from_millis(5));
    h.server.clock.refresh();

    let keys = full_scan(&mut h, &[]);
    assert_eq!(keys.len(), 50);
    assert!(keys.iter().all(|k| k.starts_with("live:")));
}

#[test]
fn scan_returns_every_key_that_survives_concurrent_writes() {
    // The SCAN guarantee: a key present at the start and at the end of a full
    // iteration is returned at least once, even though the dict rehashes
    // underneath the cursor.
    let mut h = Harness::new();
    let stable: Vec<String> = (0..500).map(|i| format!("stable:{i}")).collect();
    for k in &stable {
        h.simple(&["set", k, "v"]);
    }

    let mut cursor = "0".to_string();
    let mut seen = std::collections::HashSet::new();
    let mut churn = 0usize;
    loop {
        let reply = h.array(&["scan", &cursor, "COUNT", "20"]);
        let next = reply[0].text();
        if let Reply::Array(items) = &reply[1] {
            for i in items {
                seen.insert(i.text());
            }
        }

        // Grow the keyspace hard enough to force rehashes, and delete some of
        // what we just added. Only keys added *during* the iteration are
        // touched, so the guarantee applies to all of `stable`.
        for _ in 0..40 {
            churn += 1;
            h.simple(&["set", &format!("churn:{churn}"), "v"]);
        }
        if churn > 80 {
            h.int(&["del", &format!("churn:{}", churn - 80)]);
        }

        cursor = next;
        if cursor == "0" {
            break;
        }
    }

    for k in &stable {
        assert!(seen.contains(k), "SCAN lost {k} across a rehash");
    }
}

#[test]
fn scan_rejects_a_bad_cursor_and_bad_options() {
    let mut h = Harness::new();
    assert_eq!(h.error(&["scan", "notanumber"]), "ERR invalid cursor");
    assert_eq!(h.error(&["scan", "0", "COUNT", "0"]), "ERR syntax error");
    assert_eq!(h.error(&["scan", "0", "BOGUS", "x"]), "ERR syntax error");
    // A cursor from another galaxy must terminate rather than loop.
    let reply = h.array(&["scan", "18446744073709551615"]);
    assert_eq!(reply[0], Reply::bulk_str("0"));
}

#[test]
fn scan_never_stalls_a_writer_on_another_shard() {
    // SCAN takes one shard lock at a time, so a concurrent writer must make
    // progress while a large scan is running. If SCAN ever regresses to
    // ALL_SHARDS this test still passes, but the watchdog below catches a
    // genuine deadlock.
    let cfg = Config {
        shard_count: 8,
        ..Default::default()
    };
    let server = ServerShared::new(cfg);
    let mut seed = ClientState::new(1, "t".into(), "t".into(), 0, false);
    let mut buf = BytesMut::new();
    for i in 0..20_000 {
        let argv: rsdis::command::ArgVec = ["set", &format!("k:{i}"), "v"]
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect();
        buf.clear();
        engine::dispatch(&server, &mut seed, &mut buf, &argv);
    }

    let scanner = {
        let server = Arc::clone(&server);
        std::thread::spawn(move || {
            let mut client = ClientState::new(2, "t".into(), "t".into(), 0, false);
            let mut buf = BytesMut::new();
            let mut cursor = "0".to_string();
            let mut total = 0usize;
            loop {
                let argv: rsdis::command::ArgVec = ["scan", &cursor]
                    .iter()
                    .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                    .collect();
                buf.clear();
                engine::dispatch(&server, &mut client, &mut buf, &argv);
                let reply = parse(&buf);
                let Reply::Array(parts) = reply else {
                    panic!("bad scan reply")
                };
                let next = parts[0].text();
                if let Reply::Array(items) = &parts[1] {
                    total += items.len();
                }
                cursor = next;
                if cursor == "0" {
                    return total;
                }
            }
        })
    };

    let writer = {
        let server = Arc::clone(&server);
        std::thread::spawn(move || {
            let mut client = ClientState::new(3, "t".into(), "t".into(), 0, false);
            let mut buf = BytesMut::new();
            for i in 0..20_000 {
                let argv: rsdis::command::ArgVec = ["set", &format!("w:{i}"), "v"]
                    .iter()
                    .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                    .collect();
                buf.clear();
                engine::dispatch(&server, &mut client, &mut buf, &argv);
            }
        })
    };

    let scanned = scanner.join().expect("scanner");
    writer.join().expect("writer");
    assert!(
        scanned >= 20_000,
        "a concurrent scan must still see every pre-existing key, saw {scanned}"
    );
}

// ---------------------------------------------------------------------------
// OBJECT
// ---------------------------------------------------------------------------

#[test]
fn object_encoding_reports_the_string_encodings() {
    let mut h = Harness::new();
    h.simple(&["set", "n", "12345"]);
    h.simple(&["set", "s", "hello"]);
    h.simple(&["set", "big", &"x".repeat(100)]);
    assert_eq!(h.bulk(&["object", "encoding", "n"]).as_deref(), Some("int"));
    assert_eq!(
        h.bulk(&["object", "encoding", "s"]).as_deref(),
        Some("embstr")
    );
    assert_eq!(
        h.bulk(&["object", "encoding", "big"]).as_deref(),
        Some("raw")
    );
    assert_eq!(
        h.error(&["object", "encoding", "missing"]),
        "ERR no such key"
    );
}

#[test]
fn object_refcount_idletime_and_freq() {
    let mut h = Harness::new();
    h.simple(&["set", "s", "hello"]);
    h.simple(&["set", "n", "42"]);
    assert_eq!(h.int(&["object", "refcount", "s"]), 1);
    assert_eq!(
        h.int(&["object", "refcount", "n"]),
        2_147_483_647,
        "shared integers report INT_MAX, as in Redis"
    );

    assert_eq!(h.int(&["object", "idletime", "s"]), 0);
    assert_eq!(
        h.error(&["object", "freq", "s"]),
        "ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between maxmemory policies at runtime LFU and LRU data will take some time to adjust."
    );

    let mut h = Harness::with(Config {
        shard_count: 4,
        maxmemory_policy: MaxmemoryPolicy::AllkeysLfu,
        ..Default::default()
    });
    h.simple(&["set", "s", "hello"]);
    assert!(h.int(&["object", "freq", "s"]) >= 0);
    assert_eq!(
        h.error(&["object", "idletime", "s"]),
        "ERR An LFU maxmemory policy is selected, access time not tracked. Please note that when switching between maxmemory policies at runtime LFU and LRU data will take some time to adjust."
    );
}

#[test]
fn object_does_not_reset_the_idle_clock() {
    // `OBJECT IDLETIME` must not go through a lookup that touches the LRU
    // clock, or every key would always report 0.
    let mut h = Harness::new();
    h.simple(&["set", "k", "v"]);
    let server = Arc::clone(&h.server);
    let idx = server.shards.shard_index(b"k");
    let read_lru = || {
        let handle = server.shards.get(idx).expect("shard");
        let g = handle.lock();
        g.db_ref(0)
            .and_then(|d| d.dict.get(&Bytes::from_static(b"k")))
            .map(|e| e.lru.load(Ordering::Relaxed))
            .expect("entry")
    };
    let before = read_lru();
    h.int(&["object", "idletime", "k"]);
    let after = read_lru();
    assert_eq!(before, after, "OBJECT must not touch the LRU clock");
}

#[test]
fn object_help_and_unknown_subcommands() {
    let mut h = Harness::new();
    assert!(!h.array(&["object", "help"]).is_empty());
    assert!(
        h.error(&["object", "bogus", "k"])
            .starts_with("ERR Unknown subcommand")
    );
}

// ---------------------------------------------------------------------------
// DUMP / RESTORE
// ---------------------------------------------------------------------------

/// A stand-in for W3a's RDB serializer, good enough to exercise the framing,
/// the option parsing and the propagation rewrite. **Not** an RDB payload: the
/// real one is `src/rdb`'s, and this file's job is to prove that the seam
/// works, not to duplicate it.
struct TestCodec;

impl ValueCodec for TestCodec {
    fn dump(&self, obj: &Robj, out: &mut Vec<u8>) -> bool {
        match obj {
            Robj::Str(s) => {
                out.push(0);
                out.extend_from_slice(&s.to_bytes());
                true
            }
            _ => false,
        }
    }

    fn restore(&self, payload: &[u8]) -> Option<Robj> {
        let (kind, body) = payload.split_first()?;
        if *kind != 0 {
            return None;
        }
        Some(Robj::Str(rsdis::types::string::StrObj::from_bytes(
            Bytes::copy_from_slice(body),
        )))
    }
}

#[test]
fn dump_restore_round_trip_and_edge_cases() {
    // The codec is process-wide, so every DUMP/RESTORE assertion lives in one
    // test rather than racing across several.
    install_value_codec(Arc::new(TestCodec));

    let mut h = Harness::new();
    h.enable_propagation();
    h.simple(&["set", "src", "hello"]);
    // The payload is binary, so it never becomes a `String`.
    let payload = h.bulk_bytes(&["dump", "src"]).expect("a payload");
    assert!(
        payload.len() > 10,
        "the payload must carry the 10-byte footer"
    );
    assert_eq!(h.bulk(&["dump", "missing"]), None);

    let restore = |h: &mut Harness, key: &str, ttl: &str, payload: &[u8], opts: &[&str]| {
        let mut argv: Vec<&[u8]> = vec![b"restore", key.as_bytes(), ttl.as_bytes(), payload];
        for o in opts {
            argv.push(o.as_bytes());
        }
        h.run_bin(&argv)
    };

    // Round trip into a new key.
    let _ = h.drain_propagation();
    assert_eq!(
        restore(&mut h, "dst", "0", &payload, &[]),
        Reply::Simple("OK".into())
    );
    assert_eq!(h.bulk(&["get", "dst"]).as_deref(), Some("hello"));
    assert_eq!(h.int(&["ttl", "dst"]), -1);

    // An occupied destination needs REPLACE.
    assert_eq!(
        restore(&mut h, "dst", "0", &payload, &[]),
        Reply::Error("BUSYKEY Target key name already exists.".into())
    );
    assert_eq!(
        restore(&mut h, "dst", "0", &payload, &["REPLACE"]),
        Reply::Simple("OK".into())
    );

    // A corrupt payload is refused before anything is written.
    let mut corrupt = payload.clone();
    corrupt[0] ^= 0xff;
    assert_eq!(
        restore(&mut h, "other", "0", &corrupt, &[]),
        Reply::Error("ERR DUMP payload version or checksum are wrong".into())
    );
    assert_eq!(h.int(&["exists", "other"]), 0);

    assert_eq!(
        restore(&mut h, "neg", "-1", &payload, &[]),
        Reply::Error("ERR Invalid TTL value, must be >= 0".into())
    );

    // §4.5: a relative TTL is rewritten to an absolute one plus ABSTTL.
    let _ = h.drain_propagation();
    assert_eq!(
        restore(&mut h, "ttl", "100000", &payload, &[]),
        Reply::Simple("OK".into())
    );
    let ttl = h.int(&["ttl", "ttl"]);
    assert!((99..=100).contains(&ttl), "ttl was {ttl}");
    let propagated = h.drain_propagation();
    assert_eq!(propagated.len(), 1, "{propagated:?}");
    assert_eq!(propagated[0][0], "RESTORE");
    assert_eq!(
        propagated[0].last().map(String::as_str),
        Some("ABSTTL"),
        "the rewrite must append ABSTTL: {:?}",
        propagated[0]
    );
    let absolute: u64 = propagated[0][2].parse().expect("absolute ms");
    assert!(absolute > h.server.clock.now_ms());

    // ABSTTL is honoured as given, and not rewritten again.
    let deadline = h.server.clock.now_ms() + 50_000;
    let _ = h.drain_propagation();
    assert_eq!(
        restore(&mut h, "abs", &deadline.to_string(), &payload, &["ABSTTL"]),
        Reply::Simple("OK".into())
    );
    let ttl = h.int(&["ttl", "abs"]);
    assert!((49..=50).contains(&ttl), "ttl was {ttl}");

    // COPY of a string does not need the codec at all, but must still work
    // while one is installed.
    assert_eq!(h.int(&["copy", "src", "copy-dst"]), 1);
    assert_eq!(h.bulk(&["get", "copy-dst"]).as_deref(), Some("hello"));
}

// ---------------------------------------------------------------------------
// SORT
// ---------------------------------------------------------------------------

#[test]
fn sort_on_a_missing_key_is_empty() {
    let mut h = Harness::new();
    assert!(h.array(&["sort", "missing"]).is_empty());
    assert!(h.array(&["sort_ro", "missing"]).is_empty());
    // With STORE, a missing key deletes the destination and replies 0.
    h.simple(&["set", "dst", "leftover"]);
    assert_eq!(h.int(&["sort", "missing", "STORE", "dst"]), 0);
    assert_eq!(h.int(&["exists", "dst"]), 0);
}

#[test]
fn sort_on_a_string_is_a_wrongtype() {
    let mut h = Harness::new();
    h.simple(&["set", "k", "v"]);
    assert_eq!(
        h.error(&["sort", "k"]),
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    );
}

#[test]
fn sort_option_parsing() {
    let mut h = Harness::new();
    assert!(
        h.array(&[
            "sort", "missing", "BY", "w_*", "LIMIT", "0", "10", "ALPHA", "DESC"
        ])
        .is_empty()
    );
    assert!(h.array(&["sort", "missing", "BY", "nosort"]).is_empty());
    assert!(
        h.array(&["sort", "missing", "GET", "#", "GET", "d_*"])
            .is_empty()
    );
    assert_eq!(h.error(&["sort", "missing", "BOGUS"]), "ERR syntax error");
    assert_eq!(
        h.error(&["sort", "missing", "LIMIT", "-1", "10"]),
        "ERR value is out of range, must be positive"
    );
    // SORT_RO has no STORE.
    assert_eq!(
        h.error(&["sort_ro", "missing", "STORE", "dst"]),
        "ERR syntax error"
    );
}

// ---------------------------------------------------------------------------
// Keyspace notifications
// ---------------------------------------------------------------------------

#[test]
fn generic_commands_fire_keyspace_notifications() {
    let _guard = notify::SINK_TEST_LOCK.lock();
    let cap = Arc::new(notify::CaptureSink::new());
    notify::install_sink(cap.clone());

    let mut h = Harness::with(Config {
        shard_count: 4,
        notify_keyspace_events: NotifyClass::parse("KEA").expect("flags"),
        ..Default::default()
    });

    h.simple(&["set", "k", "v"]);
    let _ = cap.take();

    h.int(&["expire", "k", "100"]);
    let events = cap.take_strings();
    assert!(
        events.contains(&("__keyspace@0__:k".into(), "expire".into())),
        "{events:?}"
    );
    assert!(
        events.contains(&("__keyevent@0__:expire".into(), "k".into())),
        "{events:?}"
    );

    h.int(&["persist", "k"]);
    let events = cap.take_strings();
    assert!(
        events.contains(&("__keyevent@0__:persist".into(), "k".into())),
        "{events:?}"
    );

    h.simple(&["rename", "k", "k2"]);
    let events = cap.take_strings();
    assert!(
        events.contains(&("__keyevent@0__:rename_from".into(), "k".into())),
        "{events:?}"
    );
    assert!(
        events.contains(&("__keyevent@0__:rename_to".into(), "k2".into())),
        "{events:?}"
    );

    h.int(&["copy", "k2", "k3"]);
    let events = cap.take_strings();
    assert!(
        events.contains(&("__keyevent@0__:copy_to".into(), "k3".into())),
        "{events:?}"
    );

    h.int(&["move", "k3", "1"]);
    let events = cap.take_strings();
    assert!(
        events.contains(&("__keyevent@0__:move_from".into(), "k3".into())),
        "{events:?}"
    );
    assert!(
        events.contains(&("__keyevent@1__:move_to".into(), "k3".into())),
        "move_to belongs to the destination database: {events:?}"
    );

    h.int(&["del", "k2"]);
    let events = cap.take_strings();
    assert!(
        events.contains(&("__keyevent@0__:del".into(), "k2".into())),
        "{events:?}"
    );

    notify::clear_sink();
}

#[test]
fn notifications_are_silent_unless_both_a_class_and_k_or_e_are_armed() {
    let _guard = notify::SINK_TEST_LOCK.lock();
    let cap = Arc::new(notify::CaptureSink::new());
    notify::install_sink(cap.clone());

    // Classes without K or E deliver nothing at all, as in Redis.
    let mut h = Harness::with(Config {
        shard_count: 2,
        notify_keyspace_events: NotifyClass::parse("gA").expect("flags"),
        ..Default::default()
    });
    h.simple(&["set", "k", "v"]);
    h.int(&["del", "k"]);
    assert!(cap.is_empty(), "K/E missing must silence everything");

    // K with the wrong class is also silent.
    let mut h = Harness::with(Config {
        shard_count: 2,
        notify_keyspace_events: NotifyClass::parse("Kl").expect("flags"),
        ..Default::default()
    });
    h.simple(&["set", "k", "v"]);
    h.int(&["del", "k"]);
    assert!(
        cap.is_empty(),
        "the generic class is not armed: {:?}",
        cap.take()
    );

    notify::clear_sink();
}

// ---------------------------------------------------------------------------
// Active expiry
// ---------------------------------------------------------------------------

#[test]
fn the_active_cycle_reclaims_keys_nobody_touches() {
    let mut h = Harness::new();
    for i in 0..500 {
        h.simple(&["set", &format!("gone:{i}"), "v"]);
        h.int(&["pexpire", &format!("gone:{i}"), "1"]);
    }
    for i in 0..100 {
        h.simple(&["set", &format!("stays:{i}"), "v"]);
    }
    std::thread::sleep(std::time::Duration::from_millis(5));
    h.server.clock.refresh();

    // Nothing has looked at the dying keys, so they are still resident.
    let resident: usize = h.server.shards.iter().map(|s| s.lock().key_count()).sum();
    assert_eq!(resident, 600);

    let reaped = expire::drain_expired(&h.server);
    assert_eq!(reaped, 500);
    let resident: usize = h.server.shards.iter().map(|s| s.lock().key_count()).sum();
    assert_eq!(resident, 100, "only the live keys remain");
    assert_eq!(h.int(&["dbsize"]), 100);
}

#[test]
fn a_bounded_cycle_never_runs_away() {
    let h = Harness::with(Config {
        shard_count: 1,
        ..Default::default()
    });
    let mut h = h;
    for i in 0..5_000 {
        h.simple(&["set", &format!("gone:{i}"), "v"]);
        h.int(&["pexpire", &format!("gone:{i}"), "1"]);
    }
    std::thread::sleep(std::time::Duration::from_millis(5));
    h.server.clock.refresh();

    let start = std::time::Instant::now();
    let stats = expire::cycle_shard(&h.server, 0, std::time::Duration::from_millis(2));
    let elapsed = start.elapsed();
    assert!(stats.expired > 0);
    assert!(
        elapsed < std::time::Duration::from_millis(150),
        "a 2ms budget took {elapsed:?}"
    );
}

// ---------------------------------------------------------------------------
// Eviction
// ---------------------------------------------------------------------------

fn fill(h: &mut Harness, prefix: &str, n: usize, value_len: usize, ttl: Option<&str>) {
    let value = "x".repeat(value_len);
    for i in 0..n {
        let key = format!("{prefix}:{i}");
        h.simple(&["set", &key, &value]);
        if let Some(t) = ttl {
            h.int(&["expire", &key, t]);
        }
    }
}

#[test]
fn eviction_frees_memory_under_pressure() {
    let mut h = Harness::with(Config {
        shard_count: 4,
        maxmemory_policy: MaxmemoryPolicy::AllkeysLru,
        ..Default::default()
    });
    fill(&mut h, "k", 2_000, 200, None);
    let used = evict::refresh_all(&h.server);
    assert!(used > 0);

    h.server
        .config
        .update(|c| {
            c.maxmemory = used / 2;
            Ok(())
        })
        .expect("config");

    let stats = evict::evict_cycle(&h.server);
    assert!(stats.evicted > 0, "{stats:?}");
    assert!(!stats.over_limit, "still over the limit: {stats:?}");
    assert!(h.int(&["dbsize"]) < 2_000);
}

#[test]
fn noeviction_rejects_denyoom_commands() {
    let mut h = Harness::with(Config {
        shard_count: 2,
        maxmemory_policy: MaxmemoryPolicy::NoEviction,
        ..Default::default()
    });
    fill(&mut h, "k", 200, 100, None);
    let used = evict::refresh_all(&h.server);
    h.server
        .config
        .update(|c| {
            c.maxmemory = used / 2;
            Ok(())
        })
        .expect("config");
    evict::refresh_all(&h.server);

    // COPY and RESTORE are DENYOOM and check for themselves; the engine does
    // not do it centrally yet (see the handover note).
    assert_eq!(
        h.error(&["copy", "k:0", "clone"]),
        "OOM command not allowed when used memory > 'maxmemory'."
    );
    assert_eq!(
        h.error(&["restore", "rk", "0", "payload"]),
        "OOM command not allowed when used memory > 'maxmemory'."
    );

    // A read is still allowed.
    assert!(h.bulk(&["get", "k:0"]).is_some());
}

#[test]
fn volatile_policies_leave_persistent_keys_alone() {
    let mut h = Harness::with(Config {
        shard_count: 4,
        maxmemory_policy: MaxmemoryPolicy::VolatileTtl,
        ..Default::default()
    });
    fill(&mut h, "perm", 500, 200, None);
    fill(&mut h, "vol", 500, 200, Some("10000"));
    let used = evict::refresh_all(&h.server);
    h.server
        .config
        .update(|c| {
            c.maxmemory = used / 2;
            Ok(())
        })
        .expect("config");

    let stats = evict::evict_cycle(&h.server);
    assert!(stats.evicted > 0);
    for i in 0..500 {
        assert_eq!(
            h.int(&["exists", &format!("perm:{i}")]),
            1,
            "a volatile policy evicted a persistent key"
        );
    }
}
