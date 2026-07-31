//! Keyspace benchmarks: active expiry, eviction under memory pressure, and
//! `SCAN` over a million keys.
//!
//! Owner: W1c.
//!
//! # Why these are not criterion benches
//!
//! `Cargo.toml` is F0's and declares no `[[bench]]` target, so cargo
//! auto-discovers this file with the **default libtest harness**
//! (`harness = true`). Under that harness a `criterion_main!`-generated `main`
//! is never called and is reported as dead code, which fails
//! `cargo clippy --all-targets -- -D warnings`. Rather than edit a file I do
//! not own, the measurements are plain `#[ignore]`d tests with their own
//! timing. Adding
//!
//! ```toml
//! [[bench]]
//! name = "keyspace_bench"
//! harness = false
//! ```
//!
//! is the one-line fix; see the W1c handover note.
//!
//! # Running
//!
//! ```text
//! cargo test --release --bench keyspace_bench -- --ignored --nocapture
//! ```
//!
//! `RSDIS_BENCH_KEYS` overrides the keyspace size (default 1_000_000) and
//! `RSDIS_BENCH_SHARDS` the shard count (default 16).

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rsdis::command::ArgVec;
use rsdis::config::{Config, MaxmemoryPolicy};
use rsdis::ctx::{ClientState, ServerShared};
use rsdis::engine;
use rsdis::object::{Entry, Robj};
use rsdis::shard::{evict, expire};
use rsdis::types::string::StrObj;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn keys() -> usize {
    env_usize("RSDIS_BENCH_KEYS", 1_000_000)
}

fn shards() -> usize {
    env_usize("RSDIS_BENCH_SHARDS", 16)
}

fn new_server(policy: MaxmemoryPolicy) -> Arc<ServerShared> {
    ServerShared::new(Config {
        shard_count: shards(),
        maxmemory_policy: policy,
        ..Default::default()
    })
}

/// Populate the keyspace directly rather than through `SET`, so the number
/// being reported is the cost of the thing under test and not of dispatch.
fn populate(server: &ServerShared, n: usize, value_len: usize, expire_at_ms: Option<u64>) {
    let now = server.clock.now_ms();
    let lfu = server.config().maxmemory_policy.is_lfu();
    let value = Bytes::from(vec![b'x'; value_len]);
    for i in 0..n {
        let key = Bytes::from(format!("key:{i}"));
        let idx = server.shards.shard_index(&key);
        let Some(handle) = server.shards.get(idx) else {
            continue;
        };
        let mut shard = handle.lock();
        if let Some(db) = shard.db(0) {
            db.set_entry(
                key,
                Entry::new(
                    Robj::Str(StrObj::raw(value.clone())),
                    expire_at_ms,
                    now,
                    lfu,
                ),
            );
        }
    }
    for handle in server.shards.iter() {
        let shard = handle.lock();
        handle.sync_stats(&shard);
    }
}

fn live_keys(server: &ServerShared) -> usize {
    server.shards.iter().map(|h| h.lock().key_count()).sum()
}

fn rate(n: usize, elapsed: Duration) -> f64 {
    n as f64 / elapsed.as_secs_f64()
}

fn report(name: &str, lines: &[String]) {
    println!("\n=== {name} ===");
    for l in lines {
        println!("  {l}");
    }
}

// ---------------------------------------------------------------------------
// Active expiry
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark"]
fn bench_active_expire_throughput() {
    let n = keys();
    let server = new_server(MaxmemoryPolicy::NoEviction);
    // Everything already past its deadline: the sweep's best case, which is
    // what "throughput" means for a reaper.
    populate(&server, n, 32, Some(1));
    assert_eq!(live_keys(&server), n);

    let start = Instant::now();
    let reaped = expire::drain_expired(&server);
    let elapsed = start.elapsed();
    assert_eq!(reaped as usize, n);

    // The realistic case: a mostly-live keyspace where the sweep must find
    // the few stale keys without disturbing anybody.
    let server2 = new_server(MaxmemoryPolicy::NoEviction);
    let future = server2.clock.now_ms() + 3_600_000;
    populate(&server2, n, 32, Some(future));
    let budget = expire::shard_budget(server2.shards.len());
    let start = Instant::now();
    let stats = expire::cycle_all(&server2, budget);
    let idle_cycle = start.elapsed();

    report(
        "active expire",
        &[
            format!(
                "reap {n} expired keys: {elapsed:?} ({:.0} keys/s)",
                rate(n, elapsed)
            ),
            format!(
                "one full cycle over {n} live TTL'd keys: {idle_cycle:?} \
                 (sampled {}, expired {})",
                stats.sampled, stats.expired
            ),
            format!("per-shard budget: {budget:?}"),
        ],
    );
}

#[test]
#[ignore = "benchmark"]
fn bench_active_expire_lock_hold_time() {
    // The property that matters more than throughput: a sweep must never hold
    // a shard lock long enough for a client to notice.
    let n = keys() / 4;
    let server = new_server(MaxmemoryPolicy::NoEviction);
    populate(&server, n, 32, Some(1));

    let mut worst = Duration::ZERO;
    let mut client = ClientState::new(1, "b".into(), "b".into(), 0, false);
    let mut buf = BytesMut::new();
    let probe: ArgVec = ["get", "key:0"]
        .iter()
        .map(|s| Bytes::from_static(s.as_bytes()))
        .collect();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sweeper = {
        let server = Arc::clone(&server);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let budget = expire::shard_budget(server.shards.len());
            let mut total = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let s = expire::cycle_all(&server, budget);
                total += s.expired;
                if s.expired == 0 {
                    break;
                }
            }
            total
        })
    };

    let start = Instant::now();
    let mut probes = 0usize;
    while start.elapsed() < Duration::from_millis(500) {
        let t = Instant::now();
        buf.clear();
        engine::dispatch(&server, &mut client, &mut buf, &probe);
        worst = worst.max(t.elapsed());
        probes += 1;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let reaped = sweeper.join().expect("sweeper");

    report(
        "active expire, client latency during a sweep",
        &[
            format!("reaped {reaped} keys while probing"),
            format!("{probes} probes, worst GET latency {worst:?}"),
        ],
    );
    assert!(
        worst < Duration::from_millis(50),
        "a sweep stalled a client for {worst:?}"
    );
}

// ---------------------------------------------------------------------------
// Eviction
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark"]
fn bench_eviction_under_memory_pressure() {
    let n = keys() / 4;
    for policy in [
        MaxmemoryPolicy::AllkeysLru,
        MaxmemoryPolicy::AllkeysLfu,
        MaxmemoryPolicy::AllkeysRandom,
        MaxmemoryPolicy::VolatileTtl,
    ] {
        let server = new_server(policy);
        let volatile = matches!(policy, MaxmemoryPolicy::VolatileTtl);
        let deadline = server.clock.now_ms() + 3_600_000;
        populate(&server, n, 100, volatile.then_some(deadline));

        let start = Instant::now();
        let used = evict::refresh_all(&server);
        let estimate_time = start.elapsed();

        // Ask for half the keyspace back.
        server
            .config
            .update(|c| {
                c.maxmemory = used / 2;
                Ok(())
            })
            .expect("config");

        let start = Instant::now();
        let mut total = evict::EvictStats::default();
        // One cycle is time-bounded on purpose; loop until it converges.
        for _ in 0..1_000 {
            let s = evict::evict_cycle(&server);
            total.evicted += s.evicted;
            total.freed_bytes += s.freed_bytes;
            total.rounds += s.rounds;
            if !s.over_limit || s.exhausted {
                break;
            }
        }
        let elapsed = start.elapsed();

        report(
            &format!("eviction, {}", policy.as_str()),
            &[
                format!(
                    "estimate {n} keys ({} MiB): {estimate_time:?}",
                    used / (1024 * 1024)
                ),
                format!(
                    "evicted {} keys / {} MiB in {elapsed:?} ({:.0} keys/s, {} rounds)",
                    total.evicted,
                    total.freed_bytes / (1024 * 1024),
                    rate(total.evicted as usize, elapsed),
                    total.rounds
                ),
                format!("keys left: {}", live_keys(&server)),
            ],
        );
        assert!(total.evicted > 0);
    }
}

// ---------------------------------------------------------------------------
// SCAN
// ---------------------------------------------------------------------------

fn drive_full_scan(server: &ServerShared, count: &str) -> (usize, usize, Duration) {
    let mut client = ClientState::new(1, "b".into(), "b".into(), 0, false);
    let mut buf = BytesMut::new();
    let mut cursor = "0".to_string();
    let mut returned = 0usize;
    let mut calls = 0usize;

    let start = Instant::now();
    loop {
        let argv: ArgVec = ["scan", &cursor, "COUNT", count]
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect();
        buf.clear();
        engine::dispatch(server, &mut client, &mut buf, &argv);
        let (next, n) = parse_scan(&buf);
        returned += n;
        calls += 1;
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    (returned, calls, start.elapsed())
}

/// Pull the cursor and the element count out of a `*2` SCAN reply without
/// materialising the keys.
fn parse_scan(buf: &[u8]) -> (String, usize) {
    let text = String::from_utf8_lossy(buf);
    let mut lines = text.split("\r\n");
    assert_eq!(lines.next(), Some("*2"), "unexpected SCAN reply");
    let _cursor_len = lines.next();
    let cursor = lines.next().unwrap_or("0").to_string();
    let header = lines.next().unwrap_or("*0");
    let n: usize = header.trim_start_matches('*').parse().unwrap_or(0);
    (cursor, n)
}

#[test]
#[ignore = "benchmark"]
fn bench_scan_over_a_million_keys() {
    let n = keys();
    let server = new_server(MaxmemoryPolicy::NoEviction);
    populate(&server, n, 32, None);
    assert_eq!(live_keys(&server), n);

    let mut lines = Vec::new();
    for count in ["10", "100", "1000"] {
        let (returned, calls, elapsed) = drive_full_scan(&server, count);
        assert_eq!(
            returned, n,
            "a full SCAN must return every key exactly once"
        );
        lines.push(format!(
            "COUNT {count:>4}: {returned} keys in {calls} calls, {elapsed:?} \
             ({:.0} keys/s, {:.1} us/call, {} keys/call)",
            rate(returned, elapsed),
            elapsed.as_secs_f64() * 1e6 / calls as f64,
            returned / calls.max(1),
        ));
    }

    // KEYS for comparison: one pass, every shard locked.
    let mut client = ClientState::new(1, "b".into(), "b".into(), 0, false);
    let mut buf = BytesMut::new();
    let argv: ArgVec = ["keys", "*"]
        .iter()
        .map(|s| Bytes::from_static(s.as_bytes()))
        .collect();
    let start = Instant::now();
    engine::dispatch(&server, &mut client, &mut buf, &argv);
    lines.push(format!(
        "KEYS *: {:?} for {n} keys ({} MiB of reply)",
        start.elapsed(),
        buf.len() / (1024 * 1024)
    ));

    let argv: ArgVec = ["keys", "key:1*"]
        .iter()
        .map(|s| Bytes::from_static(s.as_bytes()))
        .collect();
    buf.clear();
    let start = Instant::now();
    engine::dispatch(&server, &mut client, &mut buf, &argv);
    lines.push(format!("KEYS key:1*: {:?}", start.elapsed()));

    report(&format!("SCAN over {n} keys"), &lines);
}

#[test]
#[ignore = "benchmark"]
fn bench_scan_with_match_and_type() {
    let n = keys() / 4;
    let server = new_server(MaxmemoryPolicy::NoEviction);
    populate(&server, n, 32, None);

    let mut client = ClientState::new(1, "b".into(), "b".into(), 0, false);
    let mut buf = BytesMut::new();
    let mut cursor = "0".to_string();
    let mut returned = 0usize;
    let mut calls = 0usize;
    let start = Instant::now();
    loop {
        let argv: ArgVec = ["scan", &cursor, "COUNT", "100", "MATCH", "key:1*"]
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect();
        buf.clear();
        engine::dispatch(&server, &mut client, &mut buf, &argv);
        let (next, got) = parse_scan(&buf);
        returned += got;
        calls += 1;
        cursor = next;
        if cursor == "0" {
            break;
        }
    }
    let elapsed = start.elapsed();
    report(
        "SCAN MATCH",
        &[format!(
            "{returned} matches out of {n} keys in {calls} calls, {elapsed:?}"
        )],
    );
}
