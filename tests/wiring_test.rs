//! Tests that the background cycles are actually *wired up*, not merely
//! implemented.
//!
//! Owner: F0 (architecture/integration).
//!
//! W1c built active expiry and eviction correctly and both were dead code:
//! nothing in `main.rs` ever spawned them, and the existing test harnesses
//! only started the clock ticker, so the whole feature could be absent
//! without a single test going red. Likewise `CmdError::Oom` was unreachable
//! because the frozen dispatch path never consulted `DENYOOM`.
//!
//! These tests exercise the seams where "implemented" and "reachable" come
//! apart. They are deliberately black-box, over a real socket.

use std::sync::Arc;
use std::time::Duration;

use rsdis::config::Config;
use rsdis::ctx::{CronTasks, ServerShared};
use rsdis::net::ServerHandle;

/// Start a server with the full background cron running.
async fn start_with_cron(cfg: Config) -> (ServerHandle, CronTasks, redis::Client) {
    let server = ServerShared::new(Config {
        port: 0,
        bind: vec!["127.0.0.1".to_string()],
        ..cfg
    });
    let cron = server.spawn_cron();
    let handle = rsdis::net::serve(Arc::clone(&server))
        .await
        .expect("server must bind");
    let addr = handle.local_addr().expect("a bound address");
    let client = redis::Client::open(format!("redis://{addr}/")).expect("client must open");
    (handle, cron, client)
}

/// One clock ticker + one expire task per shard + one eviction task.
#[tokio::test]
async fn cron_spawns_a_task_per_shard_plus_clock_and_evict() {
    let server = ServerShared::new(Config {
        port: 0,
        shard_count: 8,
        ..Default::default()
    });
    let cron = server.spawn_cron();
    assert_eq!(cron.len(), 8 + 2, "clock + 8 expire shards + evict");
}

/// The regression this file exists for: a key that expires and is **never
/// touched again** must still be reclaimed. Lazy expiry cannot do this --
/// only the active cycle can -- so this fails if the expire tasks are not
/// spawned.
#[tokio::test]
async fn active_expiry_reclaims_untouched_keys() {
    let (handle, _cron, client) = start_with_cron(Config {
        shard_count: 4,
        ..Default::default()
    })
    .await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    // 200 keys, all expiring almost immediately.
    for i in 0..200 {
        let _: String = redis::cmd("SET")
            .arg(format!("victim:{i}"))
            .arg("x")
            .arg("PX")
            .arg(50)
            .query_async(&mut con)
            .await
            .expect("set");
    }
    let before: i64 = redis::cmd("DBSIZE")
        .query_async(&mut con)
        .await
        .expect("dbsize");
    assert_eq!(before, 200);

    // Wait for the active cycle. Crucially we never read the victim keys, so
    // lazy expiry has no opportunity to fire.
    let mut remaining = before;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        remaining = redis::cmd("DBSIZE")
            .query_async(&mut con)
            .await
            .expect("dbsize");
        if remaining == 0 {
            break;
        }
    }

    assert_eq!(
        remaining, 0,
        "active expire cycle did not reclaim untouched expired keys \
         (still {remaining} of {before}) -- are the expire tasks spawned?"
    );
    handle.shutdown();
}

/// `DENYOOM` must be enforced by the dispatch path. With `noeviction` and a
/// maxmemory below current usage, a write is rejected with `OOM` while reads
/// and `DEL` keep working -- an over-limit server must stay recoverable.
#[tokio::test]
async fn denyoom_rejects_writes_but_not_reads_or_deletes() {
    let (handle, _cron, client) = start_with_cron(Config {
        shard_count: 4,
        maxmemory_policy: rsdis::config::MaxmemoryPolicy::NoEviction,
        ..Default::default()
    })
    .await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    // Populate, then pull the limit down under what is already resident.
    for i in 0..500 {
        let _: String = redis::cmd("SET")
            .arg(format!("k:{i}"))
            .arg("v".repeat(256))
            .query_async(&mut con)
            .await
            .expect("set");
    }
    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("1")
        .query_async(&mut con)
        .await
        .expect("config set maxmemory");

    // Give the eviction cycle a moment; under `noeviction` it must not help.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let write: Result<String, _> = redis::cmd("SET")
        .arg("newkey")
        .arg("v")
        .query_async(&mut con)
        .await;
    let err = write.expect_err("SET must be refused when over maxmemory with noeviction");
    assert!(
        err.to_string().contains("OOM"),
        "expected an OOM error, got: {err}"
    );

    // A read is not DENYOOM and must still work.
    let got: Option<String> = redis::cmd("GET")
        .arg("k:0")
        .query_async(&mut con)
        .await
        .expect("get");
    assert!(got.is_some(), "reads must keep working when over maxmemory");

    // DEL is the recovery path and must not be gated.
    let deleted: i64 = redis::cmd("DEL")
        .arg("k:0")
        .query_async(&mut con)
        .await
        .expect("DEL must work when over maxmemory, or the server is unrecoverable");
    assert_eq!(deleted, 1);

    handle.shutdown();
}
